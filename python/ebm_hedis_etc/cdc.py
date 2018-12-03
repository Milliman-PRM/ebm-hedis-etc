"""
### CODE OWNERS: Alexander Olivero

### OBJECTIVE:
    Calculate the Comprehensive Diabetes Care HEDIS measures.

### DEVELOPER NOTES:
  <none>
"""
import datetime

import pyspark.sql.functions as spark_funcs
from pyspark.sql import DataFrame
from prm.dates.windows import decouple_common_windows
from ebm_hedis_etc.base_classes import QualityMeasure

# pylint does not recognize many of the spark functions
# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _exclude_elig_gaps(
        eligible_member_time: DataFrame,
        allowable_gaps: int=0,
        allowable_gap_length: int=0
) -> DataFrame:
    """Find eligibility gaps and exclude members """
    decoupled_windows = decouple_common_windows(
        eligible_member_time,
        'member_id',
        'date_start',
        'date_end',
        create_windows_for_gaps=True
    )

    gaps_df = decoupled_windows.join(
        eligible_member_time,
        ['member_id', 'date_start', 'date_end'],
        how='left_outer'
    ).where(
        spark_funcs.col('cover_medical').isNull()
    ).select(
        'member_id',
        'date_start',
        'date_end',
        spark_funcs.datediff(
            spark_funcs.col('date_end'),
            spark_funcs.col('date_start')
        ).alias('date_diff')
    )

    long_gaps_df = gaps_df.where(
        spark_funcs.col('date_diff') > allowable_gap_length
    ).select(
        'member_id'
    )

    gap_count_df = gaps_df.groupBy(
        'member_id'
    ).agg(
        spark_funcs.count('*').alias('num_of_gaps')
    ).where(
        spark_funcs.col('num_of_gaps') > allowable_gaps
    ).select(
        'member_id'
    )

    return long_gaps_df.union(
        gap_count_df
    ).select(
        spark_funcs.col('member_id').alias('exclude_member_id')
    ).distinct()


def _rx_dispensed_event(
        rx_claims: DataFrame,
        rx_reference: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Identify members who were dispensed insulin or hypoglycemics/antihyperglycemics during the
    measurement year or the year prior"""
    return rx_claims.where(
        spark_funcs.col('fromdate').between(
            spark_funcs.lit(datetime.date(performance_yearstart.year-1, 1, 1)),
            spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31))
        )
    ).join(
        rx_reference.where(
            spark_funcs.col('medication_list') == 'Diabetes Medications'
        ),
        spark_funcs.col('ndc') == spark_funcs.col('ndc_code'),
        how='inner'
    ).select(
        'member_id'
    ).distinct()


def _identify_med_event(
        med_claims: DataFrame,
        reference_df: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Identify members who had diabetes diagnoses with either two outpatient visits or
    one inpatient encounter"""
    restricted_med_claims = med_claims.where(
        spark_funcs.col('fromdate').between(
            spark_funcs.lit(datetime.date(performance_yearstart.year - 1, 1, 1)),
            spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31))
        )
    )

    acute_inpatient_df = restricted_med_claims.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Acute Inpatient')
                & spark_funcs.col('code_system').isin('UBREV')
            )
        ),
        spark_funcs.col('revcode') == spark_funcs.col('code'),
        how='inner'
    ).union(
        restricted_med_claims.join(
            spark_funcs.broadcast(
                reference_df.where(
                    spark_funcs.col('value_set_name').isin('Acute Inpatient')
                    & spark_funcs.col('code_system').isin('CPT')
                )
            ),
            spark_funcs.col('hcpcs') == spark_funcs.col('code'),
            how='inner'
        )
    )

    acute_diags_explode_df = acute_inpatient_df.select(
        'member_id',
        'fromdate',
        restricted_med_claims.icdversion,
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in acute_inpatient_df.columns if
                 col.find('icddiag') > -1]
            )
        ).alias('diag')
    )

    acute_diabetes_encounter_members = acute_diags_explode_df.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Diabetes')
            )
        ),
        [
            acute_diags_explode_df.icdversion == reference_df.icdversion,
            spark_funcs.col('diag') == spark_funcs.col('code')
        ]
    ).select(
        'member_id'
    ).distinct()

    non_acute_encounters_df = restricted_med_claims.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('code_system').isin('UBREV')
                & spark_funcs.col('value_set_name').isin('Outpatient', 'ED', 'Nonacute Inpatient')
            ),
        ),
        spark_funcs.col('revcode') == spark_funcs.col('code'),
        how='inner'
    ).union(
        restricted_med_claims.join(
            spark_funcs.broadcast(
                reference_df.where(
                    spark_funcs.col('code_system').isin('CPT', 'HCPCS')
                    & spark_funcs.col('value_set_name').isin('Outpatient', 'ED',
                                                             'Nonacute Inpatient')
                )
            ),
            spark_funcs.col('hcpcs') == spark_funcs.col('code'),
            how='inner'
        )
    ).distinct()

    non_acute_diags_explode_df = non_acute_encounters_df.select(
        'member_id',
        'fromdate',
        restricted_med_claims.icdversion,
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in non_acute_encounters_df.columns if
                 col.find('icddiag') > -1]
            )
        ).alias('diag')
    )

    non_acute_diabetes_encounter_members = non_acute_diags_explode_df.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Diabetes')
            )
        ),
        [
            non_acute_diags_explode_df.icdversion == reference_df.icdversion,
            spark_funcs.col('diag') == spark_funcs.col('code')
        ]
    ).groupBy(
        'member_id'
    ).agg(
        spark_funcs.countDistinct('fromdate').alias('distinct_fromdate_count')
    ).where(
        spark_funcs.col('distinct_fromdate_count') >= 2
    ).select(
        'member_id'
    ).distinct()

    return acute_diabetes_encounter_members.union(
        non_acute_diabetes_encounter_members
    ).distinct()


class CDC(QualityMeasure):
    """Object to house logic to calculate comprehensive diabetes care measures"""
    def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart=datetime.date,
    ):
        reference_df = dfs_input['reference'].withColumn(
            'code',
            spark_funcs.regexp_replace(spark_funcs.col('code'), r'\.', '')
        ).withColumn(
            'icdversion',
            spark_funcs.when(
                spark_funcs.col('code_system').contains('ICD'),
                spark_funcs.regexp_extract(
                    spark_funcs.col('code_system'),
                    r'\d+',
                    0
                )
            )
        )

        pharmacy_eligible_members_df = _rx_dispensed_event(
            dfs_input['rx_claims'],
            dfs_input['ndc'],
            performance_yearstart
        )

        medical_eligible_members_df = _identify_med_event(
            dfs_input['claims'],
            reference_df,
            performance_yearstart
        )

        eligible_event_diag_members_df = pharmacy_eligible_members_df.union(
            medical_eligible_members_df
        ).distinct()

        eligible_members_df = dfs_input['member_time'].join(
            eligible_event_diag_members_df,
            'member_id',
            how='inner'
        )

        return results_df
