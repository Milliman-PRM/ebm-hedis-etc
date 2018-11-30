"""
### CODE OWNERS: Alexander Olivero, Ben Copeland

### OBJECTIVE:
    Calculate the Statin Therapy for Patients with Diabetes HEDIS measure.

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
        restricted_claims_df.icdversion,
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
            diags_explode_df.icdversion == reference_df.icdversion,
            diags_explode_df.diag == reference_df.code
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
        restricted_claims_df.icdversion,
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
            acute_inpatient_diags_explode_df.icdversion == reference_df.icdversion,
            acute_inpatient_diags_explode_df.diag == reference_df.code
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


def _identify_events_exclusion(
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
        restricted_claims_df.icdversion,
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
            inpatient_stays_diag_explode_df.icdversion == reference_df.icdversion,
            inpatient_stays_diag_explode_df.diag == reference_df.code
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
                procs_explode_df.icdversion == reference_df.icdversion,
                procs_explode_df.proc == reference_df.code
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
                procs_explode_df.icdversion == reference_df.icdversion,
                procs_explode_df.proc == reference_df.code
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


def _identify_diagnosis_exclusion(
        claims_df: DataFrame,
        reference_df: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Find claims that meet criteria for events to qualify members for denominator"""
    restricted_claims_df = claims_df.where(
        (spark_funcs.col('fromdate') >= spark_funcs.lit(performance_yearstart))
        & (spark_funcs.col('todate') <= spark_funcs.lit(
            datetime.date(performance_yearstart.year, 12, 31)))
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
        restricted_claims_df.icdversion,
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
            outpatient_diags_explode_df.icdversion == reference_df.icdversion,
            outpatient_diags_explode_df.diag == reference_df.code
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
        restricted_claims_df.icdversion,
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
            acute_inpatient_diags_explode_df.icdversion == reference_df.icdversion,
            acute_inpatient_diags_explode_df.diag == reference_df.code
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
            diag_explode_df.icdversion == reference_df.icdversion,
            diag_explode_df.diag == reference_df.code
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
            diag_explode_df.icdversion == reference_df.icdversion,
            diag_explode_df.diag == reference_df.code
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
            diag_explode_df.icdversion == reference_df.icdversion,
            diag_explode_df.diag == reference_df.code
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
                diag_explode_df.icdversion == reference_df.icdversion,
                diag_explode_df.diag == reference_df.code
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
                proc_explode_df.icdversion == reference_df.icdversion,
                proc_explode_df.proc == reference_df.code
            ],
            how='inner'
        ).select(
            'member_id'
        )
    ).distinct()

    cardio_disease_excl_df = _identify_diagnosis_exclusion(
        claims_df,
        reference_df,
        performance_yearstart
    ).intersect(
        _identify_diagnosis_exclusion(
            claims_df,
            reference_df,
            datetime.date(performance_yearstart.year-1, 1, 1)
        )
    ).union(
        _identify_events_exclusion(
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


def _calc_rate_one(
        rx_claims_df: DataFrame,
        eligible_members_df: DataFrame,
        rx_reference_df: DataFrame,
        performance_yearstart: datetime
) -> DataFrame:
    """Find members in rx claims that qualify for rate one of the measure"""
    return rx_claims_df.join(
        eligible_members_df,
        'member_id',
        how='inner'
    ).where(
        spark_funcs.year('fromdate') == performance_yearstart.year
    ).join(
        rx_reference_df.where(
            spark_funcs.col('medication_list').isin(
                'High and Moderate-Intensity Statin Medications',
                'Low-Intensity Statin Medications'
            ),
        ),
        spark_funcs.col('ndc') == spark_funcs.col('ndc_code'),
        how='inner'
    ).select(
        'member_id'
    ).distinct()


def _calc_rate_two(
        rx_claims_df: DataFrame,
        eligible_members_df: DataFrame,
        rx_reference_df: DataFrame,
        performance_yearstart
) -> DataFrame:
    """Find members in the rx claims that qualify for rate two of the measure"""
    statin_claims_df = rx_claims_df.join(
        eligible_members_df,
        'member_id',
        how='inner'
    ).where(
        spark_funcs.year('fromdate').isin(performance_yearstart.year)
    ).join(
        spark_funcs.broadcast(
            rx_reference_df.where(
                spark_funcs.col('medication_list').isin(
                    'High and Moderate-Intensity Statin Medications',
                    'Low-Intensity Statin Medications'
                ),
            )
        ),
        spark_funcs.col('ndc') == spark_funcs.col('ndc_code'),
        how='inner'
    ).select(
        'member_id',
        'fromdate',
        'drug_id',
        'dayssupply',
        spark_funcs.expr('date_add(fromdate, dayssupply - 1)').alias('fromdate_to_dayssupply')
    ).where(
        spark_funcs.col('dayssupply') > 0
    ).select(
        '*',
        spark_funcs.create_map(
            spark_funcs.lit('date_start'),
            spark_funcs.col('fromdate'),
            spark_funcs.lit('date_end'),
            spark_funcs.col('fromdate_to_dayssupply'),
            ).alias('map_coverage_window')
    )

    collect_date_windows = statin_claims_df.groupby(
        'member_id',
        'drug_id',
    ).agg(
        spark_funcs.collect_list(spark_funcs.col('map_coverage_window')).alias('array_coverage_windows'),
    )

    def adjust_date_windows(
            array_coverage_windows: "typing.Iterable[typing.Mapping[str, datetime.date]]",
        ) -> "typing.Iterable[typing.Mapping[str, datetime.date]]":
        """Combine and extend overlapping windows"""

        sorted_array = sorted(
            array_coverage_windows,
            key=lambda x: (x['date_start'], x['date_end']),
            )
        output_array = []
        last_date_end = None
        for window_coverage in sorted_array:
            date_start = window_coverage['date_start']
            date_end = window_coverage['date_end']
            if last_date_end and last_date_end >= date_start:
                bump_factor = (last_date_end - date_start).days + 1
                adj_date_start = date_start + datetime.timedelta(days=bump_factor)
                adj_date_end = date_end + datetime.timedelta(days=bump_factor)
                last_date_end = adj_date_end
            else:
                last_date_end = date_end
                adj_date_start = date_start
                adj_date_end = date_end

            output_array.append({
                'date_start': adj_date_start,
                'date_end': adj_date_end,
                })
        return output_array

    udf_adjust_date_windows = spark_funcs.udf(
        adjust_date_windows,
        spark_types.ArrayType(
            spark_types.MapType(
                spark_types.StringType(),
                spark_types.DateType(),
            )
        )
    )

    adjusted_date_windows = collect_date_windows.select(
        '*',
        spark_funcs.explode(
            udf_adjust_date_windows(
                spark_funcs.col('array_coverage_windows')
            )
        ).alias('map_adj_coverage_windows'),
    ).select(
        '*',
        spark_funcs.col('map_adj_coverage_windows')['date_start'].alias('date_start'),
        spark_funcs.col('map_adj_coverage_windows')['date_end'].alias('date_end'),
    )

    decouple_overlapping_coverage = decouple_common_windows(
        adjusted_date_windows,
        'member_id',
        'date_start',
        'date_end',
    ).select(
        'member_id',
        'date_start',
        spark_funcs.least(
            spark_funcs.col('date_end'),
            spark_funcs.lit(
                datetime.date(performance_yearstart.year, 12, 31)
            )
        ).alias('date_end'),
    ).withColumn(
        'days_covered',
        spark_funcs.datediff(
            spark_funcs.col('date_end'),
            spark_funcs.col('date_start'),
            ) + 1,
    )

    member_coverage_summ = decouple_overlapping_coverage.groupby(
        'member_id',
    ).agg(
        spark_funcs.min('date_start').alias('ipsd'),
        spark_funcs.sum('days_covered').alias('days_covered'),
    ).select(
        '*',
        spark_funcs.datediff(
            spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31)),
            spark_funcs.col('ipsd')
        ).alias('treatment_period'),
    ).withColumn(
        'days_covered',
        spark_funcs.least(
            spark_funcs.col('days_covered'),
            spark_funcs.col('treatment_period')
        )
    ).withColumn(
        'pdc',
        spark_funcs.round(
            (spark_funcs.col('days_covered') / spark_funcs.col('treatment_period')),
            2
        )
    )

    return member_coverage_summ


class SPD(QualityMeasure):
    """Object to house the logic to calculate statin therapy for patients with diabetes"""
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
            reference_df,
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
            reference_df,
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

        rate_one_numer_df = _calc_rate_one(
            dfs_input['rx_claims'],
            rate_one_denom_df,
            dfs_input['ndc'],
            performance_yearstart
        )
        rate_one_numer_df.cache()

        rate_two_numer_df = _calc_rate_two(
            dfs_input['rx_claims'],
            rate_one_numer_df,
            dfs_input['ndc'],
            performance_yearstart
        )

        rate_one_output_df = dfs_input['member'].select(
            'member_id'
        ).join(
            rate_one_denom_df,
            dfs_input['member'].member_id == rate_one_denom_df.member_id,
            how='left_outer'
        ).join(
            rate_one_numer_df,
            dfs_input['member'].member_id == rate_one_numer_df.member_id,
            how='left_outer'
        ).select(
            dfs_input['member'].member_id,
            spark_funcs.lit('SPD: Rate One').alias('comp_quality_short'),
            spark_funcs.when(
                rate_one_numer_df.member_id.isNotNull(),
                spark_funcs.lit(1)
            ).otherwise(
                spark_funcs.lit(0)
            ).alias('comp_quality_numerator'),
            spark_funcs.when(
                rate_one_denom_df.member_id.isNotNull(),
                spark_funcs.lit(1)
            ).otherwise(
                spark_funcs.lit(0)
            ).alias('comp_quality_denominator'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_date_last'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_date_actionable'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_comments')
        )

        rate_two_output_df = dfs_input['member'].select(
            'member_id'
        ).join(
            rate_one_numer_df,
            dfs_input['member'].member_id == rate_one_numer_df.member_id,
            how='left_outer'
        ).join(
            rate_two_numer_df.withColumnRenamed('member_id', 'join_member_id'),
            dfs_input['member'].member_id == spark_funcs.col('join_member_id'),
            how='left_outer'
        ).select(
            dfs_input['member'].member_id,
            spark_funcs.lit('SPD: Rate Two').alias('comp_quality_short'),
            spark_funcs.when(
                spark_funcs.col('pdc') > .8,
                spark_funcs.lit(1)
            ).otherwise(
                spark_funcs.lit(0)
            ).alias('comp_quality_numerator'),
            spark_funcs.when(
                rate_one_numer_df.member_id.isNotNull(),
                spark_funcs.lit(1)
            ).otherwise(
                spark_funcs.lit(0)
            ).alias('comp_quality_denominator'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_date_last'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_date_actionable'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_comments')
        )

        return rate_one_output_df.union(
            rate_two_output_df
        ).orderBy(
            'member_id'
        )
