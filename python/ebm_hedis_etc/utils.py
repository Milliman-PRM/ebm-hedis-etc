"""
### CODE OWNERS: Alexander Olivero, Matthew Hawthorne

### OBJECTIVE:
  House common used functions among the HEDIS quality measures.

### DEVELOPER NOTES:
  <None>
"""
import logging

import pyspark.sql.functions as spark_funcs
from pyspark.sql import Column, DataFrame

from prm.dates.windows import decouple_common_windows

LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def find_elig_gaps(
        member_time: DataFrame,
        ce_start_date: Column,
        ce_end_date: Column
) -> DataFrame: # pragma: no cover
    """
    Find the number of gaps in enrollment and the longest length gap.  Additionally, check if
    member had eligibility on anchor date.
    Args:

    Returns:

    """
    member_time_to_edges_df = member_time.where(
        (spark_funcs.col('date_start') >= ce_start_date)
        & (spark_funcs.col('date_end') <= ce_end_date)
    ).select(
        'member_id',
        'date_start',
        'date_end'
    ).union(
        member_time.select(
            'member_id',
            spark_funcs.lit(ce_start_date).alias('date_start'),
            spark_funcs.lit(ce_end_date).alias('date_end')
        )
    )

    decoupled_windows_df = decouple_common_windows(
        member_time_to_edges_df,
        'member_id',
        'date_start',
        'date_end',
        create_windows_for_gaps=True
    )

    gaps_df = decoupled_windows_df.join(
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
        spark_funcs.when(
            spark_funcs.col('cover_medical').isin('N'),
            (spark_funcs.datediff(
                spark_funcs.col('date_end'),
                spark_funcs.col('date_start')
            ) + 1)
        ).alias('elig_gap_date_diff'),
        spark_funcs.when(
            spark_funcs.col('cover_medical').isin('N'),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('elig_gap_flag')

    )

    return gaps_df.groupBy(
        'member_id'
    ).agg(
        spark_funcs.max(
            spark_funcs.col('elig_gap_date_diff')
        ).alias('largest_gap'),
        spark_funcs.sum(
            spark_funcs.col('elig_gap_flag')
        ).alias('gap_count')
    )


def flag_gap_exclusions(
        member_gap_df: DataFrame,
        allowable_gap_length: int=0,
        allowable_gap_count: int=0
) -> DataFrame: # pragma: no cover
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
    ).distinct()
