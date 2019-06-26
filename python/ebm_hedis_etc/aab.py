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
import typing

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


def _calculate_age_criteria(
        members: DataFrame,
        date_performanceyearstart: datetime.date,
        date_performanceyearend: datetime.date,
    ) -> DataFrame:
    """Determine which members meet the age criteria"""

    return elig_members


def _prep_reference_data(
        reference_df: DataFrame,
    ) -> "typing.Mapping[str, DataFrame]":
    """Gather all of our reference data into a form that can be used easily"""
    reference_munge = reference_df.withColumn(
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

    return map_references


def _identify_acute_bronchitis_events(
        outclaims: DataFrame,
        map_references: "typing.Mapping[str, DataFrame]",
        date_performanceyearstart: datetime.date,
        date_performanceyearend: datetime.date,
    ) -> DataFrame:
    """Find all qualifying visits with diagnosis of acute bronchitis"""

    return acute_bronchitis_events


def _identify_negative_condition_events(
        outclaims: DataFrame,
        map_references: "typing.Mapping[str, DataFrame]",
    ):
    """Find dates for all medical events that could disqualify an index episode"""

    return negative_condition_events


def _identify_negative_medication_events(
        outpharmacy: DataFrame,
        map_references: "typing.Mapping[str, DataFrame]",
    ) -> DataFrame:
    """Find dates for all prescription events that could disqualify an index episode"""

    return negative_rx_events


def _identify_competing_diagnoses(
        outclaims: DataFrame,
        map_references: "typing.Mapping[str, DataFrame]",
    ) -> DataFrame:
    """Find dates for all competing diagnosis events that could disqualify an index episode"""

    return negative_comp_diags


def _exclude_negative_history(
        acute_bronchitis_events: DataFrame,
        negative_condition_events: DataFrame,
        negative_rx_events: DataFrame,
        negative_comp_diags: DataFrame,
    ) -> DataFrame:
    """Apply exclusions for index episodes from negative history events"""

    return excluded_negative_history


def _apply_elig_gap_exclusions(
        excluded_negative_history: DataFrame,
        member_time: DataFrame,
    ) -> DataFrame:
    """Apply exclusions for index episodes that don't meet continuous enrollment criteria"""

    return elig_gap_exclusions


def _apply_age_exclusions(
        elig_gap_exclusions: DataFrame,
        elig_members: DataFrame,
    ) -> DataFrame:
    """Apply exclusions for index episodes that don't meet continuous enrollment criteria"""

    return age_exclusions


def _select_earliest_episode(
        age_exclusions: DataFrame,
    ) -> DataFrame:
    """Limit to the earliest eligible episode per member"""

    return earliest_episode


def _identify_aab_prescriptions(
        outpharmacy: DataFrame,
        map_references: "typing.Mapping[str, DataFrame]",
    ) -> DataFrame:
    """Find prescriptions of interest for the episode numerator calculation"""

    return aab_rx


def _calculate_numerator(
        earliest_episode: DataFrame,
        aab_rx: DataFrame,
    ):
    """Apply AAB prescriptions to index episodes to determine numerator compliance"""

    return numerator_flagged


def _format_measure_results(
        numerator_flagged: DataFrame,
    ) -> DataFrame:
    """Get our measure results in the format expected by the pipeline"""

    return measure_formatted


def _flag_denominator(
        dfs_input: "typing.Mapping[str, DataFrame]",
        map_references: "typing.Mapping[str, DataFrame]",
        date_performanceyearstart: datetime.date,
        date_performanceyearend: datetime.date,
    ) -> DataFrame:
    """Wrap execution of denominator flagging logic"""
    elig_members = _calculate_age_criteria(
        dfs_input['members'],
        date_performanceyearstart,
        date_performanceyearend,
    )
    acute_bronchitis_events = _identify_acute_bronchitis_events(
        dfs_input['outclaims'],
        map_references,
        date_performanceyearstart,
        date_performanceyearend,
    )
    negative_condition_events = _identify_negative_condition_events(
        dfs_input['outclaims'],
        map_references,
    )
    negative_rx_events = _identify_negative_medication_events(
        dfs_input['outpharmacy'],
        map_references,
    )
    negative_comp_diags = _identify_competing_diagnoses(
        dfs_input['outclaims'],
        map_references,
    )
    excluded_negative_history = _exclude_negative_history(
        acute_bronchitis_events,
        negative_condition_events,
        negative_rx_events,
        negative_comp_diags,
    )
    elig_gap_exclusions = _apply_elig_gap_exclusions(
        excluded_negative_history,
        dfs_input['member_time'],
    )
    age_exclusions = _apply_age_exclusions(
        elig_gap_exclusions,
        elig_members,
    )
    earliest_episode = _select_earliest_episode(
        age_exclusions,
    )
    return earliest_episode


class AAB(QualityMeasure):
    """Object to house the logic to calculate AAB measure"""
    def _calc_measure(
            self,
            dfs_input: 'typing.Mapping[str, DataFrame]',
            performance_yearstart=datetime.date,
            **kwargs
    ):
        map_references = _prep_reference_data(
            dfs_input['reference'],
        )
        denominator = _flag_denominator(
            dfs_input,
            map_references,
            date_performanceyearstart,
            date_performanceyearend,
        )
        aab_rx = _identify_aab_prescriptions(
            dfs_input['outpharmacy'],
            map_references,
        )
        numerator_flagged = _calculate_numerator(
            denominator,
            aab_rx,
        )
        measure_results = _format_measure_results(
            numerator_flagged,
        )

        return measure_results
