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


def _identify_med_claims(
        members_no_gaps: DataFrame,
        claims_df: DataFrame,
        reference_df: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Find medical claims that meet criteria for events that qualify members for denominator"""
    restricted_claims_df = claims_df.join(
        members_no_gaps,
        'member_id',
        how='inner'
    ).where(
        (spark_funcs.col('fromdate') >= spark_funcs.lit(
            datetime.date(performance_yearstart.year-1, 1, 1)))
        & (spark_funcs.col('todate') <= spark_funcs.lit(
            datetime.date(performance_yearstart.year, 12, 31)))
    )

    visits_df = restricted_claims_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name').isin('Outpatient', 'Observation', 'ED', 'Nonacute Inpatient')
            & spark_funcs.col('code_system').isin('CPT', 'HCPCS')
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).union(
        restricted_claims_df.join(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Outpatient')
                & spark_funcs.col('code_system').isin('UBREV')
            ),
            spark_funcs.col('revcode') == spark_funcs.col('code'),
            how='inner'
        )
    ).distinct()

    diags_explode_df = visits_df.select(
        'member_id',
        'claimid',
        'fromdate',
        'todate',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in restricted_claims_df.columns if
                 col.find('icddiag') > -1]
            )
        ).alias('diag')
    )

    diabetes_diag_member_df = diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Diabetes'
        ),
        [
            diags_explode_df.icdversion == spark_funcs.regexp_extract(
                reference_df.code_system, '\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, '\.', '')
        ],
        how='inner'
    ).groupBy(
        'member_id'
    ).agg(
        spark_funcs.countDistinct('fromdate', 'todate').alias('visit_count')
    ).where(
        spark_funcs.col('visit_count') >= 2
    ).select(
        'member_id'
    ).distinct()

    acute_inpatient_encounter_df = restricted_claims_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name').isin('Acute Inpatient')
            & spark_funcs.col('code_system').isin('CPT')
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='left_outer'
    ).union(
        restricted_claims_df.join(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Acute Inpatient')
                & spark_funcs.col('code_system').isin('UBREV')
            ),
            spark_funcs.col('revcode') == spark_funcs.col('code'),
            how='left_outer'
        )
    ).distinct()

    acute_inpatient_diags_explode_df = acute_inpatient_encounter_df.select(
        'member_id',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in restricted_claims_df.columns if
                 col.find('icddiag') > -1]
            )
        ).alias('diag')
    )

    acute_inpatient_diabetes_diag_member_df = acute_inpatient_diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Diabetes'
        ),
        [
            acute_inpatient_diags_explode_df.icdversion == spark_funcs.regexp_extract(
                reference_df.code_system, '\d+', 0),
            acute_inpatient_diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code,
                                                                                '\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    return diabetes_diag_member_df.union(
        acute_inpatient_diabetes_diag_member_df
    ).distinct()


def _identify_rx_claims(
        members_no_gaps: DataFrame,
        rx_claims_df: DataFrame,
        rx_reference_df: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Find rx claims that meet criteria for prescriptions that qualify members for denominator"""
    restricted_claims_df = rx_claims_df.join(
        members_no_gaps,
        'member_id',
        how='inner'
    ).where(
        spark_funcs.year('fromdate').isin(performance_yearstart.year-1, performance_yearstart.year)
    )

    return restricted_claims_df.join(
        rx_reference_df.where(
            spark_funcs.col('medication_list').isin('Diabetes Medications')
        ),
        spark_funcs.col('ndc') == spark_funcs.col('ndc_code'),
        how='inner'
    ).select(
        'member_id'
    ).distinct()


def _identify_events(
        claims_df: DataFrame,
        reference_df: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Find claims that meet criteria for events to qualify members for denominator"""
    restricted_claims_df = claims_df.where(
        (spark_funcs.col('fromdate') >= spark_funcs.lit(
            datetime.date(performance_yearstart.year-1, 1, 1)))
        & (spark_funcs.col('todate') <= spark_funcs.lit(
            datetime.date(performance_yearstart.year-1, 12, 31)))
    )

    procs_explode_df = restricted_claims_df.select(
        'member_id',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in restricted_claims_df.columns if
                 col.find('icdproc') > -1]
            )
        ).alias('proc')
    )

    inpatient_stays_diag_explode_df = restricted_claims_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Inpatient Stay'
        ),
        spark_funcs.col('revcode') == spark_funcs.col('code'),
        how='inner'
    ).select(
        'member_id',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in restricted_claims_df.columns if
                 col.find('icddiag') > -1]
            )
        ).alias('diag')
    )

    mi_event_member_df = inpatient_stays_diag_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'MI'
        ),
        [
            inpatient_stays_diag_explode_df.icdversion == spark_funcs.regexp_extract(
                reference_df.code_system, '\d+', 0),
            inpatient_stays_diag_explode_df.diag == spark_funcs.regexp_replace(reference_df.code,
                                                                               '\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    cabg_event_member_df = restricted_claims_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name').isin('CABG')
            & spark_funcs.col('code_system').isin('CPT', 'HCPCS')
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).select(
        'member_id'
    ).union(
        procs_explode_df.join(
            reference_df.where(
                spark_funcs.col('value_set_name') == 'CABG'
            ),
            [
                procs_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system, '\d+', 0),
                procs_explode_df.proc == spark_funcs.regexp_replace(reference_df.code, '\.', '')
            ],
            how='inner'
        ).select(
            'member_id'
        )
    ).distinct()

    pci_event_member_df = restricted_claims_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name').isin('PCI')
            & spark_funcs.col('code_system').isin('CPT', 'HCPCS')
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).select(
        'member_id'
    ).union(
        procs_explode_df.join(
            reference_df.where(
                spark_funcs.col('value_set_name') == 'PCI'
            ),
            [
                procs_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                          '\d+', 0),
                procs_explode_df.proc == spark_funcs.regexp_replace(reference_df.code, '\.', '')
            ],
            how='inner'
        ).select(
            'member_id'
        )
    ).distinct()

    other_revascularization_event_member_df = restricted_claims_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Other Revascularization'
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    return mi_event_member_df.union(
        cabg_event_member_df
    ).union(
        pci_event_member_df
    ).union(
        other_revascularization_event_member_df
    ).select(
        'member_id'
    ).distinct()


def _identify_diagnosis(
        claims_df: DataFrame,
        reference_df: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Find claims that meet criteria for events to qualify members for denominator"""
    restricted_claims_df = claims_df.where(
        (spark_funcs.col('fromdate') >= spark_funcs.lit(
            datetime.date(performance_yearstart.year-1, 1, 1)))
        & (spark_funcs.col('todate') <= spark_funcs.lit(
            datetime.date(performance_yearstart.year-1, 12, 31)))
    )

    outpatient_visits_df = restricted_claims_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name').isin('Outpatient')
            & spark_funcs.col('code_system').isin('CPT', 'HCPCS')
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).union(
        restricted_claims_df.join(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Outpatient')
                & spark_funcs.col('code_system').isin('UBREV')
            ),
            spark_funcs.col('revcode') == spark_funcs.col('code'),
            how='inner'
        )
    ).distinct()

    outpatient_diags_explode_df = outpatient_visits_df.select(
        'member_id',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in restricted_claims_df.columns if
                 col.find('icddiag') > -1]
            )
        ).alias('diag')
    )

    outpatient_ivd_diag_member_df = outpatient_diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'IVD'
        ),
        [
            outpatient_diags_explode_df.icdversion == spark_funcs.regexp_extract(
                reference_df.code_system, r'\d+', 0),
            outpatient_diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code,
                                                                           'r\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    acute_inpatient_encounter_df = restricted_claims_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name').isin('Acute Inpatient')
            & spark_funcs.col('code_system').isin('CPT')
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='left_outer'
    ).union(
        restricted_claims_df.join(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Acute Inpatient')
                & spark_funcs.col('code_system').isin('UBREV')
            ),
            spark_funcs.col('revcode') == spark_funcs.col('code'),
            how='left_outer'
        )
    ).distinct()

    acute_inpatient_diags_explode_df = acute_inpatient_encounter_df.select(
        'member_id',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in restricted_claims_df.columns if
                 col.find('icddiag') > -1]
            )
        ).alias('diag')
    )

    acute_inpatient_ivd_diag_member_df = acute_inpatient_diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'IVD'
        ),
        [
            acute_inpatient_diags_explode_df.icdversion == spark_funcs.regexp_extract(
                reference_df.code_system, r'\d+', 0),
            acute_inpatient_diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code,
                                                                                'r\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    return outpatient_ivd_diag_member_df.union(
        acute_inpatient_ivd_diag_member_df
    ).distinct()


def _measure_exclusion(
        claims_df: DataFrame,
        rx_claims_df: DataFrame,
        reference_df: DataFrame,
        rx_reference_df: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Find members who should be excluded from the measure based on certain diagnoses/procedures"""
    diag_explode_df = claims_df.select(
        'member_id',
        'gender',
        'claimid',
        'fromdate',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in claims_df.columns if
                 col.find('icddiag') > -1]
            )
        ).alias('diag')
    )

    proc_explode_df = claims_df.select(
        'member_id',
        'gender',
        'claimid',
        'fromdate',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in claims_df.columns if
                 col.find('icdproc') > -1]
            )
        ).alias('proc')
    )

    pregnancy_member_excl_df = diag_explode_df.where(
        spark_funcs.col('gender').isin('F')
        & spark_funcs.year(
            'fromdate'
        ).isin(performance_yearstart.year-1, performance_yearstart.year)
    ).join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Pregnancy'
        ),
        [
            diag_explode_df.icdversion == spark_funcs.regexp_extract(
                reference_df.code_system, r'\d+', 0),
            diag_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, 'r\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    ivf_member_excl_df = claims_df.where(
        spark_funcs.year(
            'fromdate'
        ).isin(performance_yearstart.year-1, performance_yearstart.year)
    ).join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'IVF'
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    cirrhosis_member_excl_df = diag_explode_df.where(
        spark_funcs.year(
            'fromdate'
        ).isin(performance_yearstart.year-1, performance_yearstart.year)
    ).join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Cirrhosis'
        ),
        [
            diag_explode_df.icdversion == spark_funcs.regexp_extract(
                reference_df.code_system, r'\d+', 0),
            diag_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, 'r\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    muscular_pain_member_excl_df = diag_explode_df.where(
        spark_funcs.year(
            'fromdate'
        ).isin(performance_yearstart.year-1, performance_yearstart.year)
    ).join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Muscular Pain and Disease'
        ),
        [
            diag_explode_df.icdversion == spark_funcs.regexp_extract(
                reference_df.code_system, r'\d+', 0),
            diag_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, 'r\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    estrogen_agonists_member_excl_df = rx_claims_df.where(
        spark_funcs.year('fromdate').isin(performance_yearstart.year-1, performance_yearstart.year)
    ).join(
        rx_reference_df.where(
            spark_funcs.col('medication_list') == 'Estrogen Agonists Medications'
        ),
        spark_funcs.col('ndc') == spark_funcs.col('ndc_code'),
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    esrd_member_excl_df = claims_df.where(
        spark_funcs.year('fromdate').isin(performance_yearstart.year-1, performance_yearstart.year)
    ).join(
        reference_df.where(
            spark_funcs.col('value_set_name').isin('ESRD')
            & spark_funcs.col('code_system').isin('CPT', 'HCPCS')
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).select(
        'member_id'
    ).union(
        claims_df.where(
            spark_funcs.year('fromdate').isin(performance_yearstart.year - 1,
                                              performance_yearstart.year)
        ).join(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('ESRD')
                & spark_funcs.col('code_system').isin('UBREV')
            ),
            spark_funcs.col('revcode') == spark_funcs.col('code'),
            how='inner'
        ).select(
            'member_id'
        )
    ).union(
        claims_df.where(
            spark_funcs.year('fromdate').isin(performance_yearstart.year - 1,
                                              performance_yearstart.year)
        ).join(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('ESRD')
                & spark_funcs.col('code_system').isin('POS')
            ),
            spark_funcs.col('pos') == spark_funcs.col('code'),
            how='inner'
        ).select(
            'member_id'
        )
    ).union(
        diag_explode_df.where(
            spark_funcs.year('fromdate').isin(performance_yearstart.year - 1,
                                              performance_yearstart.year)
        ).join(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('ESRD')
                & spark_funcs.col('code_system').isin('ICD10CM', 'ICD9CM')
            ),
            [
                diag_explode_df.icdversion == spark_funcs.regexp_extract(
                    reference_df.code_system, r'\d+', 0),
                diag_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, 'r\.', '')
            ],
            how='inner'
        ).select(
            'member_id'
        )
    ).union(
        proc_explode_df.where(
            spark_funcs.year('fromdate').isin(performance_yearstart.year - 1,
                                              performance_yearstart.year)
        ).join(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('ESRD')
                & spark_funcs.col('code_system').isin('ICD10PCS', 'ICD9PCS')
            ),
            [
                proc_explode_df.icdversion == spark_funcs.regexp_extract(
                    reference_df.code_system, r'\d+', 0),
                proc_explode_df.proc == spark_funcs.regexp_replace(reference_df.code, 'r\.', '')
            ],
            how='inner'
        ).select(
            'member_id'
        )
    ).distinct()

    cardio_disease_excl_df = _identify_diagnosis(
        claims_df,
        reference_df,
        performance_yearstart
    ).union(
        _identify_events(
            claims_df,
            reference_df,
            performance_yearstart
        )
    ).distinct()

    return cardio_disease_excl_df.union(
        pregnancy_member_excl_df
    ).union(
        ivf_member_excl_df
    ).union(
        esrd_member_excl_df
    ).union(
        cirrhosis_member_excl_df
    ).union(
        muscular_pain_member_excl_df
    ).union(
        estrogen_agonists_member_excl_df
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

        med_event_df = _identify_med_claims(
            eligible_members_no_gaps_df,
            dfs_input['claims'],
            dfs_input['reference'],
            performance_yearstart
        )

        rx_event_df = _identify_rx_claims(
            eligible_members_no_gaps_df,
            dfs_input['rx_claims'],
            dfs_input['ndc'],
            performance_yearstart
        )

        med_rx_combo_df = med_event_df.union(
            rx_event_df
        ).distinct()

        excluded_members_df = _measure_exclusion(
            dfs_input['claims'],
            dfs_input['rx_claims'],
            dfs_input['reference'],
            dfs_input['ndc'],
            performance_yearstart
        )

        rate_one_denom_df = med_rx_combo_df.join(
            excluded_members_df.withColumnRenamed('member_id', 'exclude_member_id'),
            spark_funcs.col('member_id') == spark_funcs.col('exclude_member_id'),
            how='left_outer'
        ).where(
            spark_funcs.col('exclude_member_id').isNull()
        ).select(
            'member_id'
        ).distinct()
