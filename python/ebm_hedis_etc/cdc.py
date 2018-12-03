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
    acute_inpatient_df = med_claims.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Acute Inpatient')
            ),
            spark_funcs
        )
    )


class CDC(QualityMeasure):
    """Object to house logic to calculate comprehensive diabetes care measures"""
    def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart=datetime.date,
    ):
        pharmacy_eligible_members_df = _rx_dispensed_event(
            dfs_input['rx_claims'],
            dfs_input['ndc'],
            performance_yearstart
        )

        medical_eligible_members_df = _identify_med_event(
            dfs_input['claims'],
            dfs_input['reference'],
            performance_yearstart
        )

        return results_df
