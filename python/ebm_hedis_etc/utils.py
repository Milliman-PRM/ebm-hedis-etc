"""
### CODE OWNERS: Alexander Olivero

### OBJECTIVE:
  House common used functions among the HEDIS quality measures.

### DEVELOPER NOTES:
  <None>
"""
import logging

import pyspark.sql.functions as spark_funcs
from pyspark.sql import Column, DataFrame, Window

from prm.dates.windows import decouple_common_windows

LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def find_elig_gaps(
    member_time: DataFrame,
    ce_start_date: Column,
    ce_end_date: Column
) -> DataFrame:
    """
    Find the number of gaps in enrollment and the longest length gap.  Additionally, check if
    member had eligibility on anchor date.
    Args:

    Returns:

    """
    decoupled_windows = decouple_common_windows(
        member_time.where(
            (spark_funcs.col('date_start') >= ce_start_date)
            & (spark_funcs.col('date_end') <= ce_end_date)
        ),
        'member_id',
        'date_start',
        'date_end',
        create_windows_for_gaps=True
    )

    gaps_df = decoupled_windows.join(
        member_time,
        ['member_id', 'date_start', 'date_end'],
        how='full_outer'
    ).fillna(
        {
            'cover_medical': 'N',
        }
    ).select(
        'member_id',
        'date_start',
        'date_end',
        'cover_medical',
        (spark_funcs.datediff(
            spark_funcs.col('date_end'),
            spark_funcs.col('date_start')
        ) + 1).alias('date_diff')
    )

    gaps_to_end_df = gaps_df.groupBy(
        'member_id',
        'cover_medical'
    ).agg(
        spark_funcs.when(
            spark_funcs.col('cover_medical').isin('N'),
            spark_funcs.max('date_diff')
        ).alias('longest_gap'),
        spark_funcs.when(
            spark_funcs.col('cover_medical').isin('N'),
            spark_funcs.count('*')
        ).alias('gap_count'),
        spark_funcs.when(
            spark_funcs.col('cover_medical').isin('Y'),
            spark_funcs.datediff(
                ce_end_date,
                spark_funcs.max('date_end')
            )
        ).alias('gap_to_end')
    ).withColumn(
        'gap_count',
        spark_funcs.when(
            spark_funcs.col('gap_to_end') > 0,
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.col('gap_count')
        )
    ).select(
        'member_id',
        spark_funcs.greatest(
            spark_funcs.col('longest_gap'),
            spark_funcs.col('gap_to_end'),
            spark_funcs.lit(0)
        ).alias('largest_gap'),
        spark_funcs.coalesce(
            spark_funcs.col('gap_count'),
            spark_funcs.lit(0)
        ).alias('gap_count')
    )

    window = Window().partitionBy(
        'member_id'
    ).orderBy(
        spark_funcs.col('largest_gap').desc(),
        spark_funcs.col('gap_count').desc()
    )

    return gaps_to_end_df.withColumn(
        'row',
        spark_funcs.row_number().over(window)
    ).where(
        spark_funcs.col('row') == 1
    ).drop(
        'row'
    )


def flag_gap_exclusions(
        member_gap_df: DataFrame,
        allowable_gap_length: int = 0,
        allowable_gap_count: int = 0
) -> DataFrame:
    """
    Add flag of member exclusion based on allowable gaps in enrollment
    Args:
        member_gap_df: df with longest gap and number of gaps for each member
        allowable_gap_length: max number of days with no medical coverage allowed
        allowable_gap_count: max number of gaps in in medical coverage allowed

    Returns:
        df with gap_exclusion YN column
    """
    return member_gap_df.select(
        'member_id',
        spark_funcs.when(
            (spark_funcs.col('largest_gap') > allowable_gap_length)
            | (spark_funcs.col('gap_count') > allowable_gap_count),
            spark_funcs.lit('Y')
        ).otherwise(
            spark_funcs.lit('N')
        ).alias('enrollment_gap_excluded')
    )
