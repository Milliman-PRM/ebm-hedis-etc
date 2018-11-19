"""
### CODE OWNERS: Alexander Olivero

### OBJECTIVE:
    Calculate the Statin Therapy for Patients with Diabetes HEDIS measure.

### DEVELOPER NOTES:
  <none>

"""
import logging
import datetime

import pyspark.sql.functions as spark_funcs
from pyspark.sql import DataFrame, Window
from prm.dates.windows import decouple_common_windows, massage_windows
from ebm_hedis_etc.base_classes import QualityMeasure

LOGGER = logging.getLogger(__name__)

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


class SPD(QualityMeasure):
    """Object to house the logic to calculate statin therapy for patients iwth diabetes"""
    def _calc_measure(
            self,
            dfs_input: 'typing.Mapping[str, DataFrame]',
            performance_yearstart=datetime.date,
            **kwargs
    ):
        eligible_membership_df = dfs_input['member_time'].where(
            (spark_funcs.col('date_start') >= datetime.date(performance_yearstart.year-1, 1, 1))
            & (spark_funcs.col('date_end') <= datetime.date(performance_yearstart.year, 12, 31))
        ).join(
            dfs_input['member'].select(
                'member_id',
                'dob'
            ),
            'member_id',
            how='left_outer'
        ).where(
            spark_funcs.col('cover_medical').isin('Y')
            & spark_funcs.col('cover_rx').isin('Y')
        ).where(
            spark_funcs.lit(spark_funcs.datediff(
                spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31)),
                spark_funcs.col('dob')
            ) / 365).between(
                40,
                75
            )
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
