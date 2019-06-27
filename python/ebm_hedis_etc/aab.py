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

    date_performanceyearstart_prior = datetime.date(
        date_performanceyearstart.year - 1,
        date_performanceyearstart.month,
        date_performanceyearstart.day,
    )

    elig_members = members.select(
        '*',
        (
            spark_funcs.datediff(
                spark_funcs.lit(date_performanceyearstart_prior),
                spark_funcs.col('dob'),
            )
            / 365.25
        ).alias('age_py_start_prior'),
        (
            spark_funcs.datediff(
                spark_funcs.lit(date_performanceyearend),
                spark_funcs.col('dob'),
            )
            / 365.25
        ).alias('age_py_end'),
    ).select(
        'member_id',
        'dob',
        'age_py_start_prior',
        'age_py_end',
        spark_funcs.when(
            (spark_funcs.col('age_py_start_prior') < AGE_LOWER)
            | (spark_funcs.col('age_py_end') > AGE_UPPER),
            spark_funcs.lit(0),
        ).otherwise(
            spark_funcs.lit(1),
        ).alias('meets_age_criteria'),
    )

    return elig_members


def _prep_reference_data(
        value_set_reference_df: DataFrame,
        ndc_reference_df: DataFrame,
    ) -> "typing.Mapping[str, DataFrame]":
    """Gather all of our reference data into a form that can be used easily"""
    map_med_reference_codesets = {
        'index_visit':  [
            'Outpatient',
            'Observation',
            'ED',
        ],
        'index_diag': [
            'Acute Bronchitis',
        ],
        'index_ip_excl': [
            'Inpatient Stay',
        ],
        'negative_conditions': [
            'HIV',
            'HIV Type 2',
            'Malignant Neoplasms',
            'Emphysema',
            'COPD',
            'Cystic Fibrosis',
            'Comorbid Conditions',
            'Disorders of the Immune System',
        ],
        'negative_comp_diag': [
            'Pharyngitis',
            'Competing Diagnosis',
        ]
    }
    map_rx_reference_codesets = {
        'aab_rx': [
            'Aminoglycosides',
            'Aminopenicillins',
            #'Antipseudomonal penicillins', # Listed in specs but not found in reference
            'Beta-lactamase inhibitors',
            'First generation cephalosporins',
            'Fourth generation cephalosporins',
            'Ketolides',
            'Lincomycin derivatives',
            'Macrolides',
            'Miscellaneous antibiotics',
            'Natural penicillins',
            'Penicillinase-resistant penicillins',
            'Quinolones',
            'Rifamycin derivatives',
            'Second generation cephalosporins',
            'Sulfonamides',
            'Tetracyclines',
            'Third generation cephalosporins',
            'Urinary anti-infectives',
        ],
    }
    value_set_munge = value_set_reference_df.withColumn(
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

    map_references = dict()
    for key_reference, iter_code_sets in map_med_reference_codesets.items():
        map_references[key_reference] = value_set_munge.filter(
            spark_funcs.col('value_set_name').isin(iter_code_sets)
        ).cache()

        found_value_sets = {
            row['value_set_name']
            for row in map_references[key_reference].select('value_set_name').distinct().collect()
            }
        missing_value_sets = set(iter_code_sets) - found_value_sets
        assert not(missing_value_sets), 'Did not find {} value sets in {} grouping'.format(
            missing_value_sets,
            key_reference,
        )

    for key_reference, iter_code_sets in map_rx_reference_codesets.items():
        map_references[key_reference] = ndc_reference_df.filter(
            (spark_funcs.col('medication_list') == 'AAB Antibiotic Medications')
            & spark_funcs.col('description').isin(iter_code_sets)
        ).cache()

        found_value_sets = {
            row['description']
            for row in map_references[key_reference].select('description').distinct().collect()
            }
        missing_value_sets = set(iter_code_sets) - found_value_sets
        assert not(missing_value_sets), 'Did not find {} value sets in {} grouping'.format(
            missing_value_sets,
            key_reference,
        )

    return map_references


def _identify_aab_prescriptions(
        outpharmacy: DataFrame,
        map_references: "typing.Mapping[str, DataFrame]",
    ) -> DataFrame:
    """Find prescriptions of interest for the episode numerator calculation"""

    aab_rx = outpharmacy.join(
        spark_funcs.broadcast(
            map_references['aab_rx'].select(spark_funcs.col('ndc_code').alias('ndc'))
        ),
        on='ndc',
        how='inner',
    ).select(
        'member_id',
        'fromdate',
    ).distinct()

    return aab_rx


def _identify_acute_bronchitis_events(
        outclaims: DataFrame,
        map_references: "typing.Mapping[str, DataFrame]",
        date_performanceyearstart: datetime.date,
        date_performanceyearend: datetime.date,
    ) -> DataFrame:
    """Find all qualifying visits with diagnosis of acute bronchitis"""

    index_cpt_hcpcs = map_references['index_visit'].filter(
        spark_funcs.col('code_system').isin('HCPCS', 'CPT')
    ).select(
        spark_funcs.col('code').alias('hcpcs'),
        spark_funcs.lit(1).alias('index_cpt')
    )
    index_rev = map_references['index_visit'].filter(
        spark_funcs.col('code_system') == 'UBREV'
    ).select(
        spark_funcs.col('code').alias('revcode'),
        spark_funcs.lit(1).alias('index_rev')
    )
    assert (
        (index_cpt_hcpcs.count() + index_rev.count())
        == map_references['index_visit'].count()
        ), 'Some "index_visit" code systems have not been accounted for'

    assert map_references['index_diag'].select('code_system').distinct().count() == 1, \
        'Some "index_diag" code systems have not been accounted for'
    assert map_references['index_ip_excl'].select('code_system').distinct().count() == 1, \
        'Some "index_ip_excl" code systems have not been accounted for'

    index_events = outclaims.join(
        spark_funcs.broadcast(index_cpt_hcpcs),
        on='hcpcs',
        how='left',
    ).join(
        spark_funcs.broadcast(index_rev),
        on='revcode',
        how='left',
    ).filter(
        (spark_funcs.col('index_cpt') == 1)
        | (spark_funcs.col('index_rev') == 1)
    ).select(
        '*',
        spark_funcs.explode(
            spark_funcs.array([
                spark_funcs.col(colname)
                for colname in outclaims.columns
                if colname.startswith('icddiag')
            ])
        ).alias('icddiag')
    ).join(
        spark_funcs.broadcast(
            map_references['index_diag'].select(
                spark_funcs.col('code').alias('icddiag'),
                spark_funcs.lit(1).alias('index_diag')
            )
        ),
        on='icddiag',
        how='inner',
    ).select(
        'member_id',
        'fromdate',
    ).distinct().alias('index')

    ip_excl_dates = outclaims.join(
        spark_funcs.broadcast(
            map_references['index_ip_excl'].select(
                spark_funcs.col('code').alias('revcode'),
                spark_funcs.lit(1).alias('ip_excl')
            )
        ),
        on='revcode',
        how='inner',
    ).select(
        'member_id',
        'admitdate',
        'ip_excl',
    ).distinct().alias('ip_excl')

    acute_bronchitis_events = index_events.join(
        ip_excl_dates,
        on=[
            spark_funcs.col('index.member_id') == spark_funcs.col('ip_excl.member_id'),
            spark_funcs.col('ip_excl.admitdate').between(
                spark_funcs.col('index.fromdate'),
                spark_funcs.date_add(spark_funcs.col('index.fromdate'), 1),
            ),
        ],
        how='left',
    ).filter(
        spark_funcs.col('ip_excl').isNull()
    ).select(
        'index.member_id',
        'index.fromdate',
    ).distinct().filter(
        spark_funcs.col('fromdate').between(
            spark_funcs.lit(date_performanceyearstart),
            spark_funcs.lit(date_performanceyearend),
        )
    )

    return acute_bronchitis_events


def _identify_negative_condition_events(
        outclaims: DataFrame,
        map_references: "typing.Mapping[str, DataFrame]",
    ):
    """Find dates for all medical events that could disqualify an index episode"""
    assert map_references['negative_conditions'].filter(
        ~spark_funcs.col('code_system').isin('ICD10CM', 'ICD9CM')
    ).count() == 0, \
        'Some "negative_conditions" code systems have not been accounted for'

    negative_condition_events = outclaims.select(
        '*',
        spark_funcs.explode(
            spark_funcs.array([
                spark_funcs.col(colname)
                for colname in outclaims.columns
                if colname.startswith('icddiag')
            ])
        ).alias('icddiag'),
    ).join(
        spark_funcs.broadcast(
            map_references['negative_conditions'].select(
                spark_funcs.col('code').alias('icddiag'),
                spark_funcs.lit(1).alias('negative_conditions')
            )
        ),
        on='icddiag',
        how='inner',
    ).select(
        'member_id',
        'fromdate',
        'negative_conditions',
    ).distinct()

    return negative_condition_events


def _identify_competing_diagnoses(
        outclaims: DataFrame,
        map_references: "typing.Mapping[str, DataFrame]",
    ) -> DataFrame:
    """Find dates for all competing diagnosis events that could disqualify an index episode"""
    assert map_references['negative_comp_diag'].filter(
        ~spark_funcs.col('code_system').isin('ICD10CM', 'ICD9CM')
    ).count() == 0, \
        'Some "negative_conditions" code systems have not been accounted for'

    negative_comp_diags = outclaims.select(
        '*',
        spark_funcs.explode(
            spark_funcs.array([
                spark_funcs.col(colname)
                for colname in outclaims.columns
                if colname.startswith('icddiag')
            ])
        ).alias('icddiag'),
    ).join(
        spark_funcs.broadcast(
            map_references['negative_comp_diag'].select(
                spark_funcs.col('code').alias('icddiag'),
                spark_funcs.lit(1).alias('negative_comp_diag')
            )
        ),
        on='icddiag',
        how='inner',
    ).select(
        'member_id',
        'fromdate',
        'negative_comp_diag',
    ).distinct()

    return negative_comp_diags


def _exclude_negative_history(
        acute_bronchitis_events: DataFrame,
        negative_condition_events: DataFrame,
        aab_rx: DataFrame,
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
        aab_rx: DataFrame,
        map_references: "typing.Mapping[str, DataFrame]",
        date_performanceyearstart: datetime.date,
        date_performanceyearend: datetime.date,
    ) -> DataFrame:
    """Wrap execution of denominator flagging logic"""
    elig_members = _calculate_age_criteria(
        dfs_input['member'],
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
    negative_comp_diags = _identify_competing_diagnoses(
        dfs_input['outclaims'],
        map_references,
    )
    excluded_negative_history = _exclude_negative_history(
        acute_bronchitis_events,
        negative_condition_events,
        aab_rx,
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
        date_performanceyearstart = performance_yearstart
        date_performanceyearend = datetime.date(
            performance_yearstart.year + 1,
            performance_yearstart.month,
            performance_yearstart.day,
        ) - datetime.timedelta(days=8)

        map_references = _prep_reference_data(
            dfs_input['reference'],
            dfs_input['ndc'],
        )
        aab_rx = _identify_aab_prescriptions(
            dfs_input['outpharmacy'],
            map_references,
        )
        denominator = _flag_denominator(
            dfs_input,
            aab_rx,
            map_references,
            date_performanceyearstart,
            date_performanceyearend,
        )
        numerator_flagged = _calculate_numerator(
            denominator,
            aab_rx,
        )
        measure_results = _format_measure_results(
            numerator_flagged,
        )

        return measure_results
