"""
### CODE OWNERS: Ben Copeland

### OBJECTIVE:
    Calculate the Avoidance of Antibiotic Treatment in Adults With
    Acute Bronchitis (AAB) measure.

### DEVELOPER NOTES:
  <none>

"""
import logging
import datetime

import pyspark.sql.functions as spark_funcs
import pyspark.sql.types as spark_types
from pyspark.sql import DataFrame
from prm.dates.windows import decouple_common_windows
from ebm_hedis_etc.base_classes import QualityMeasure

LOGGER = logging.getLogger(__name__)

AGE_LOWER = 18
AGE_UPPER = 64

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


def _calculate_age_criteria():
    """Determine which members meet the age criteria"""


def _prep_reference_data():
    """Gather all of our reference data into a form that can be used easily"""


def _identify_acute_bronchitis_events():
    """Find all qualifying visits with diagnosis of acute bronchitis"""


def _identify_negative_condition_events():
    """Find dates for all medical events that could disqualify an index episode"""


def _identify_negative_medication_events():
    """Find dates for all prescription events that could disqualify an index episode"""


def _identify_competing_diagnoses():
    """Find dates for all competing diagnosis events that could disqualify an index episode"""


def _exclude_negative_history():
    """Apply exclusions for index episodes from negative history events"""


def _apply_elig_gap_exclusions():
    """Apply exclusions for index episodes that don't meet continuous enrollment criteria"""


def _select_earliest_episode():
    """Limit to the earliest eligible episode per member"""


def _identify_aab_prescriptions():
    """Find prescriptions of interest for the episode numerator calculation"""


def _calculate_numerator():
    """Apply AAB prescriptions to index episodes to determine numerator compliance"""


def _format_measure_results():
    """Get our measure results in the format expected by the pipeline"""


class AAB(QualityMeasure):
    """Object to house the logic to calculate AAB measure"""
    def _calc_measure(
            self,
            dfs_input: 'typing.Mapping[str, DataFrame]',
            performance_yearstart=datetime.date,
            **kwargs
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
                    0)
            )
        )

        return
