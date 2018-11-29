"""
### CODE OWNERS: Alexander Olivero

### OBJECTIVE:
    Calculate the Annual Monitoring of Diuretics HEDIS measure.

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


class MPM3(QualityMeasure):
    """Object to house logic to calculate persistence of beta-blockers after heart attack measure"""
    def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart=datetime.date,
    ):
        diuretic_claims_df = dfs_input['rx_claims'].where(
            spark_funcs.col('fromdate').between(
                spark_funcs.lit(performance_yearstart),
                spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31))
            )
        ).join(
            dfs_input['ndc'].where(
                spark_funcs.col('medication_list') == 'Diuretic Medications'
            ),
            spark_funcs.col('ndc') == spark_funcs.col('ndc_code'),
            how='inner'
        ).where(
            spark_funcs.col('dayssupply') > 0
        ).withColumn(
            'fromdate_to_dayssupply',
            spark_funcs.least(
                spark_funcs.expr('date_add(fromdate, dayssupply)'),
                spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31))
            )
        )

        covered_treatment_days_df = diuretic_claims_df.groupBy(
            'member_id'
        ).agg(
            spark_funcs.sum(
                spark_funcs.datediff(
                    spark_funcs.col('fromdate_to_dayssupply'),
                    spark_funcs.col('fromdate')
                )
            ).alias('treatment_days')
        ).where(
            spark_funcs.col('treatment_days') >= 180
        ).select(
            'member_id'
        )

        eligible_membership_df = covered_treatment_days_df.join(
            dfs_input['member_time'],
            'member_id',
            how='inner'
        ).where(
            (spark_funcs.col('date_start') >= performance_yearstart)
            & (spark_funcs.col('date_end')
               <= spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31)))
            & (spark_funcs.col('cover_medical') == 'Y')
            & (spark_funcs.col('cover_rx') == 'Y')
        ).join(
            dfs_input['member'],
            'member_id',
            how='left_outer'
        ).where(
            spark_funcs.lit(spark_funcs.datediff(
                spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31)),
                spark_funcs.col('dob')
            ) / 365) >= 18
        )

        eligible_members_no_gaps_df = eligible_membership_df.join(
            _exclude_elig_gaps(
                eligible_membership_df,
                1,
                45
            ),
            spark_funcs.col('member_id') == spark_funcs.col('exclude_member_id'),
            how='left_outer'
        ).where(
            spark_funcs.col('exclude_member_id').isNull()
        ).select(
            'member_id'
        ).distinct()

        eligible_pop_claims_df = dfs_input['claims'].join(
            eligible_members_no_gaps_df,
            'member_id',
            how='inner'
        ).where(
            (spark_funcs.col('fromdate') >= performance_yearstart)
            & (spark_funcs.col('fromdate')
               <= spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31)))
        )

        lab_panel_df = eligible_pop_claims_df.join(
            dfs_input['reference'].where(
                spark_funcs.col('value_set_name').isin('Lab Panel')
                & spark_funcs.col('code_system').isin('CPT')
            ),
            spark_funcs.col('hcpcs') == spark_funcs.col('code'),
            how='inner'
        ).select(
            'member_id'
        ).distinct()

        serum_monitoring_df = eligible_pop_claims_df.join(
            dfs_input['reference'].where(
                spark_funcs.col('value_set_name').isin('Serum Potassium')
                & spark_funcs.col('code_system').isin('CPT')
            ),
            spark_funcs.col('hcpcs') == spark_funcs.col('code'),
            how='inner'
        ).select(
            'member_id'
        ).distinct().intersect(
            eligible_pop_claims_df.join(
                dfs_input['reference'].where(
                    spark_funcs.col('value_set_name').isin('Serum Creatinine')
                    & spark_funcs.col('code_system').isin('CPT')
                ),
                spark_funcs.col('hcpcs') == spark_funcs.col('code'),
                how='inner'
            ).select(
                'member_id'
            ).distinct()
        ).distinct()

        numer_df = lab_panel_df.union(
            serum_monitoring_df
        ).distinct()

        results_df = dfs_input['member'].select(
            'member_id'
        ).distinct().join(
            eligible_members_no_gaps_df,
            dfs_input['member'].member_id == eligible_members_no_gaps_df.member_id,
            how='left_outer'
        ).join(
            numer_df,
            dfs_input['member'].member_id == numer_df.member_id,
            how='left_outer'
        ).select(
            dfs_input['member'].member_id,
            spark_funcs.lit('MPM3').alias('comp_quality_short'),
            spark_funcs.when(
                numer_df.member_id.isNotNull(),
                spark_funcs.lit(1)
            ).otherwise(
                spark_funcs.lit(0)
            ).alias('comp_quality_numerator'),
            spark_funcs.when(
                eligible_members_no_gaps_df.member_id.isNotNull(),
                spark_funcs.lit(1)
            ).otherwise(
                spark_funcs.lit(0)
            ).alias('comp_quality_denominator'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_date_last'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_date_actionable'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_comments')
        )

        return results_df
