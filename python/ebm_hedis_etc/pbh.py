"""
### CODE OWNERS:
### OBJECTIVE:

### DEVELOPER NOTES:
  <none>
"""
import logging
import datetime

import pyspark.sql.functions as spark_funcs
from pyspark.sql import DataFrame, Window
from prm.dates.windows import decouple_common_windows

LOGGER = logging.getLogger(__name__)

# pylint does not recognize many of the spark functions
# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _calc_measures(
        dfs_input: "typing.Mapping[str, DataFrame]",
        performance_yearstart=datetime.date,
):
    measure_start = performance_yearstart
    measure_end = datetime.date(performance_yearstart.year, 12, 31)

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
    )

    diags_explode_df = acute_inpat_disch_df.select(
        'member_id',
        'claimid',
        'fromdate',
        'dischdate',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col)
                 for col in dfs_input['claims'].columns if col.find('icddiag') > -1]
            )
        ).alias('diag')
    ).where(
        spark_funcs.col('fromdate').between(
            datetime.date(performance_yearstart.year-1, 7, 1),
            datetime.date(performance_yearstart.year, 6, 30)
        )
    )

    ami_diag_df = diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'AMI'
        ),
        [
            diags_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                      '\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, '\.', '')
        ],
        how='inner'
    ).select(
        'claimid',
        'member_id',
        'dischdate'
    ).distinct()

    multiple_episode_window = Window().partitionBy('member_id').orderBy('dischdate', 'claimid')

    ami_reduce_df = ami_diag_df.withColumn(
        'row',
        spark_funcs.row_number().over(multiple_episode_window)
    ).where(
        spark_funcs.col('row') == 1
    ).drop('row')

    # TODO: Direct Transfer logic

    elig_pop_covered = dfs_input['member_time'].where(
        spark_funcs.col('cover_medical').isin('Y')
        & spark_funcs.col('cover_rx').isin('Y')
    ).join(
        dfs_input['member'].select(
            'member_id',
            'age'
        ),
        'member_id',
        'left_outer'
    ).where(
        spark_funcs.col('age') >= 18
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
        spark_funcs.col('assignment_indicator').isNull()
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
    )

    long_gap_df = gaps_df.where(
        spark_funcs.col('date_start').between(
            spark_funcs.date_add(
                spark_funcs.col('dischdate'),
                180
            ),
            spark_funcs.col('dischdate')
        ) &
        (spark_funcs.col('date_diff') > 45)
    ).select('member_id')

    gap_count_df = gaps_df.groupBy('member_id').agg(
        spark_funcs.count('*').alias('num_of_gaps')
    ).where(
        spark_funcs.col('num_of_gaps') > 1
    ).select('member_id')

    excluded_members_df = long_gap_df.union(
        gap_count_df
    ).distinct()

    eligible_members_df = elig_pop_covered.select(
        'member_id',
        'dischdate'
    ).distinct().join(
        excluded_members_df,
        'member_id',
        how='left_outer'
    ).where(
        excluded_members_df.member_id.isNotNull()
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
        'member_id'
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



