"""
### CODE OWNERS: Alexander Olivero, Demerrick Moton

### OBJECTIVE:
    Calculate the Persistence of Beta-Blockers after Heart Attack HEDIS measure.

### DEVELOPER NOTES:
  <none>
"""
import datetime

import pyspark.sql.functions as spark_funcs
from pyspark.sql import Window
from prm.dates.windows import decouple_common_windows
from ebm_hedis_etc.base_classes import QualityMeasure

# pylint does not recognize many of the spark functions
# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class PBH(QualityMeasure):
    """Object to house logic to calculate persistence of beta-blockers after heart attack measure"""
    def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart=datetime.date,
    ):
        measure_start = performance_yearstart
        measure_end = datetime.date(
            measure_start.year,
            12,
            31,
        )

        reference_df = dfs_input['reference']

        inpatient_disch_ref_df = reference_df.where(
            spark_funcs.col('value_set_name').isin('Inpatient Stay', 'Nonacute Inpatient Stay')
        ).groupBy(
            'code'
        ).count().where(
            spark_funcs.col('count') < 2
        ).join(
            reference_df.where(spark_funcs.col('value_set_name') == 'Inpatient Stay'),
            'code',
            how='inner'
        )

        acute_inpat_disch_df = dfs_input['claims'].join(
            inpatient_disch_ref_df,
            spark_funcs.col('revcode') == spark_funcs.col('code'),
            how='inner'
        ).withColumn(
            'transfer_date',
            spark_funcs.date_sub(spark_funcs.col('admitdate'), 1)
        )

        non_acute_inpat_disch_df = dfs_input['claims'].join(
            reference_df.where(
                spark_funcs.col('value_set_name') == 'Nonacute Inpatient Stay'
            ),
            spark_funcs.col('revcode') == spark_funcs.col('code'),
            how='inner'
        ).select(
            'claimid',
            'member_id',
            spark_funcs.date_sub(spark_funcs.col('admitdate'), 1).alias('transfer_date')
        )

        diags_explode_df = acute_inpat_disch_df.select(
            'member_id',
            'claimid',
            'dischdate',
            'icdversion',
            spark_funcs.explode(
                spark_funcs.array(
                    [spark_funcs.col(col)
                     for col in dfs_input['claims'].columns if col.find('icddiag') > -1]
                )
            ).alias('diag')
        ).where(
            spark_funcs.col('dischdate').between(
                datetime.date(measure_start.year-1, 7, 1),
                datetime.date(measure_start.year, 6, 30)
            )
        )

        ami_diag_df = diags_explode_df.join(
            reference_df.where(
                spark_funcs.col('value_set_name') == 'AMI'
            ),
            [
                diags_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                          r'\d+', 0),
                diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, r'\.', '')
            ],
            how='inner'
        ).select(
            'claimid',
            'member_id',
            'dischdate'
        ).distinct()

        direct_transfer_df = ami_diag_df.join(
            non_acute_inpat_disch_df,
            [
                ami_diag_df.member_id == non_acute_inpat_disch_df.member_id,
                ami_diag_df.dischdate == non_acute_inpat_disch_df.transfer_date
            ],
            how='left_outer'
        ).join(
            acute_inpat_disch_df.withColumnRenamed(
                'member_id',
                'join_member_id'
            ).withColumnRenamed(
                'dischdate',
                'transfer_dischdate'
            ),
            [
                ami_diag_df.member_id == spark_funcs.col('join_member_id'),
                ami_diag_df.dischdate == acute_inpat_disch_df.transfer_date
            ],
            how='left_outer'
        ).where(
            non_acute_inpat_disch_df.transfer_date.isNull()
        ).select(
            ami_diag_df.claimid,
            ami_diag_df.member_id,
            spark_funcs.coalesce(
                spark_funcs.col('transfer_dischdate'),
                spark_funcs.col('dischdate')
            ).alias('dischdate')
        )

        multiple_episode_window = Window().partitionBy('member_id').orderBy('dischdate', 'claimid')

        ami_reduce_df = direct_transfer_df.withColumn(
            'row',
            spark_funcs.row_number().over(multiple_episode_window)
        ).where(
            spark_funcs.col('row') == 1
        ).drop('row')

        elig_pop_covered = dfs_input['member_time'].where(
            spark_funcs.col('cover_medical').isin('Y')
            & spark_funcs.col('cover_rx').isin('Y')
        ).join(
            dfs_input['member'].select(
                'member_id',
                'dob'
            ),
            'member_id',
            'left_outer'
        ).where(
            spark_funcs.lit(spark_funcs.datediff(
                spark_funcs.lit(measure_end),
                spark_funcs.col('dob')
            ) / 365) >= 18
        ).join(
            ami_reduce_df,
            'member_id',
            how='inner'
        )

        decoupled_windows = decouple_common_windows(
            elig_pop_covered,
            'member_id',
            'date_start',
            'date_end',
            create_windows_for_gaps=True
        )

        gaps_df = decoupled_windows.join(
            elig_pop_covered,
            ['member_id', 'date_start', 'date_end'],
            'left_outer'
        ).where(
            spark_funcs.col('cover_medical').isNull()
        ).select(
            'member_id',
            'date_start',
            'date_end'
        ).withColumn(
            'date_diff',
            spark_funcs.datediff(
                spark_funcs.col('date_end'),
                spark_funcs.col('date_start')
            )
        ).join(
            ami_reduce_df.select(
                'member_id',
                'dischdate'
            ),
            'member_id',
            how='left_outer'
        )

        long_gap_df = gaps_df.where(
            spark_funcs.col('date_start').between(
                spark_funcs.col('dischdate'),
                spark_funcs.date_add(
                    spark_funcs.col('dischdate'),
                    179
                )
            ) &
            (spark_funcs.col('date_diff') >= 45)
        ).select('member_id')

        gap_count_df = gaps_df.groupBy('member_id').agg(
            spark_funcs.count('*').alias('num_of_gaps')
        ).where(
            spark_funcs.col('num_of_gaps') > 1
        ).select('member_id')

        excluded_members_df = long_gap_df.union(
            gap_count_df
        ).distinct().withColumnRenamed(
            'member_id',
            'exclude_member_id'
        )

        eligible_members_df = elig_pop_covered.select(
            'member_id',
            'dischdate'
        ).distinct().join(
            excluded_members_df,
            spark_funcs.col('member_id') == spark_funcs.col('exclude_member_id'),
            how='left_outer'
        ).where(
            spark_funcs.col('exclude_member_id').isNull()
        )

        ndc_reference_df = dfs_input['ndc']

        beta_blockers_df = ndc_reference_df.where(
            spark_funcs.col('medication_list') == 'Beta-Blocker Medications'
        )

        eligible_rx_claims_df = dfs_input['rx_claims'].join(
            eligible_members_df,
            'member_id',
            how='inner'
        ).join(
            beta_blockers_df,
            spark_funcs.col('ndc') == spark_funcs.col('ndc_code'),
            how='inner'
        ).select(
            'member_id',
            'claimid',
            'fromdate',
            spark_funcs.expr(
                'date_add(fromdate, dayssupply)'
            ).alias('todate_dayssupply'),
            'dayssupply',
            'dischdate'
        ).where(
            (spark_funcs.col('todate_dayssupply') >= spark_funcs.col('dischdate'))
            | (spark_funcs.col('fromdate') >= spark_funcs.col('dischdate'))
        ).withColumn(
            'coverage_start',
            spark_funcs.greatest(
                spark_funcs.col('dischdate'),
                spark_funcs.col('fromdate')
            )
        ).withColumn(
            'coverage_end',
            spark_funcs.least(
                spark_funcs.date_add(
                    spark_funcs.col('dischdate'),
                    179
                ),
                spark_funcs.col('todate_dayssupply')
            )
        )

        decoupled_coverage_df = decouple_common_windows(
            eligible_rx_claims_df.where(
                spark_funcs.col('coverage_start') < spark_funcs.col('coverage_end')
            ),
            'member_id',
            'coverage_start',
            'coverage_end'
        )

        coverage_count_df = decoupled_coverage_df.groupBy(
            'member_id',
        ).agg(
            spark_funcs.sum(
                spark_funcs.datediff(
                    spark_funcs.col('coverage_end'),
                    spark_funcs.col('coverage_start')
                )
            ).alias('beta_covered_days')
        )

        numer_df = coverage_count_df.where(
            spark_funcs.col('beta_covered_days') >= 135
        )

        results_df = dfs_input['member'].select(
            'member_id'
        ).distinct().join(
            eligible_members_df,
            dfs_input['member'].member_id == eligible_members_df.member_id,
            how='left_outer'
        ).join(
            numer_df,
            dfs_input['member'].member_id == numer_df.member_id,
            how='left_outer'
        ).select(
            '*',
            spark_funcs.when(
                numer_df.member_id.isNotNull(),
                spark_funcs.lit(1)
            ).otherwise(
                spark_funcs.lit(0)
            ).alias('comp_quality_numerator'),
            spark_funcs.when(
                eligible_members_df.member_id.isNotNull(),
                spark_funcs.lit(1)
            ).otherwise(
                spark_funcs.lit(0)
            ).alias('comp_quality_denominator'),
        ).select(
            dfs_input['member'].member_id,
            spark_funcs.lit('PBH').alias('comp_quality_short'),
            'comp_quality_numerator',
            'comp_quality_denominator',
            spark_funcs.lit(None).cast('string').alias('comp_quality_date_last'),
            spark_funcs.date_add(
                spark_funcs.col('dischdate'),
                179,
            ).alias('comp_quality_date_actionable'),
            spark_funcs.when(
                (spark_funcs.col('comp_quality_denominator') == 1)
                & (spark_funcs.col('comp_quality_numerator') == 0),
                spark_funcs.lit('Patient received less than 135 days of treatment with beta-blockers'),
            ).otherwise(
                spark_funcs.concat_ws(
                    ' ',
                    spark_funcs.lit('Patient hospitalized with AMI on'),
                    spark_funcs.col('dischdate'),
                )
            ).alias('comp_quality_comments'),
        )

        return results_df
