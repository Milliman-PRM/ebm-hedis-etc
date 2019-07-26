"""
### CODE OWNERS: Alexander Olivero, Matthew Hawthorne

### OBJECTIVE:
    Calculate the Comprehensive Diabetes Care HEDIS measures.

### DEVELOPER NOTES:
  <none>
"""
import datetime

import pyspark.sql.functions as spark_funcs
from pyspark.sql import DataFrame
from prm.dates.windows import decouple_common_windows
from ebm_hedis_etc.base_classes import QualityMeasure

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


def _rx_dispensed_event(
        rx_claims: DataFrame,
        rx_reference: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Identify members who were dispensed insulin or hypoglycemics/antihyperglycemics during the
    measurement year or the year prior"""
    return rx_claims.where(
        spark_funcs.col('fromdate').between(
            spark_funcs.lit(datetime.date(performance_yearstart.year-1, 1, 1)),
            spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31))
        )
    ).join(
        rx_reference.where(
            spark_funcs.col('medication_list') == 'Diabetes Medications'
        ),
        spark_funcs.col('ndc') == spark_funcs.col('ndc_code'),
        how='inner'
    ).select(
        'member_id'
    ).distinct()


def _identify_med_event(
        med_claims: DataFrame,
        reference_df: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Identify members who had diabetes diagnoses with either two outpatient visits or
    one inpatient encounter"""
    restricted_med_claims = med_claims.select(
        'member_id',
        'claimid',
        'fromdate',
        'revcode',
        'hcpcs',
        'icdversion',
        spark_funcs.array(
            [spark_funcs.col(col) for col in med_claims.columns if
             col.find('icddiag') > -1]
        ).alias('diag_explode')
    ).distinct().withColumn(
        'diag',
        spark_funcs.explode(
            spark_funcs.col('diag_explode')
        )
    ).drop(
        'diag_explode'
    ).distinct().where(
        spark_funcs.col('fromdate').between(
            spark_funcs.lit(datetime.date(performance_yearstart.year - 1, 1, 1)),
            spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31))
        )
    )

    acute_inpatient_df = restricted_med_claims.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Acute Inpatient')
                & spark_funcs.col('code_system').isin('UBREV')
            )
        ),
        spark_funcs.col('revcode') == spark_funcs.col('code'),
        how='inner'
    ).union(
        restricted_med_claims.join(
            spark_funcs.broadcast(
                reference_df.where(
                    spark_funcs.col('value_set_name').isin('Acute Inpatient')
                    & spark_funcs.col('code_system').isin('CPT')
                )
            ),
            spark_funcs.col('hcpcs') == spark_funcs.col('code'),
            how='inner'
        )
    )

    acute_diags_explode_df = acute_inpatient_df.select(
        'member_id',
        'fromdate',
        restricted_med_claims.icdversion,
        'diag'
    )

    acute_diabetes_encounter_members = acute_diags_explode_df.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Diabetes')
            )
        ),
        [
            acute_diags_explode_df.icdversion == reference_df.icdversion,
            spark_funcs.col('diag') == spark_funcs.col('code')
        ]
    ).select(
        'member_id'
    ).distinct()

    non_acute_encounters_df = restricted_med_claims.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('code_system').isin('UBREV')
                & spark_funcs.col('value_set_name').isin('Outpatient', 'ED', 'Nonacute Inpatient')
            ),
        ),
        spark_funcs.col('revcode') == spark_funcs.col('code'),
        how='inner'
    ).union(
        restricted_med_claims.join(
            spark_funcs.broadcast(
                reference_df.where(
                    spark_funcs.col('code_system').isin('CPT', 'HCPCS')
                    & spark_funcs.col('value_set_name').isin('Outpatient', 'ED',
                                                             'Nonacute Inpatient')
                )
            ),
            spark_funcs.col('hcpcs') == spark_funcs.col('code'),
            how='inner'
        )
    ).distinct()

    non_acute_diags_explode_df = non_acute_encounters_df.select(
        'member_id',
        'fromdate',
        restricted_med_claims.icdversion,
        'diag'
    )

    non_acute_diabetes_encounter_members = non_acute_diags_explode_df.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Diabetes')
            )
        ),
        [
            non_acute_diags_explode_df.icdversion == reference_df.icdversion,
            spark_funcs.col('diag') == spark_funcs.col('code')
        ]
    ).groupBy(
        'member_id'
    ).agg(
        spark_funcs.countDistinct('fromdate').alias('distinct_fromdate_count')
    ).where(
        spark_funcs.col('distinct_fromdate_count') >= 2
    ).select(
        'member_id'
    ).distinct()

    return acute_diabetes_encounter_members.union(
        non_acute_diabetes_encounter_members
    ).distinct()


def identify_a1c_tests(
        eligible_members: DataFrame,
        claims_df: DataFrame,
        reference_df: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Identify members with HbA1c testing during the performance year"""
    return claims_df.join(
        eligible_members,
        'member_id',
        how='inner'
    ).where(
        spark_funcs.col('fromdate').between(
            spark_funcs.lit(performance_yearstart),
            spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31))
        )
    ).join(
        reference_df.where(
            spark_funcs.col('value_set_name').isin('HbA1c Tests')
            & spark_funcs.col('code_system').contains('CPT')
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).groupby(
        'member_id',
    ).agg(
        spark_funcs.max('fromdate').alias('latest_hba1c_test'),
    ).withColumn(
        'comp_quality_comments',
        spark_funcs.concat(
            spark_funcs.lit('HbA1c test performed on '),
            spark_funcs.col('latest_hba1c_test'),
        )
    )


def identify_nephropathy(
        eligible_members: DataFrame,
        claims_df: DataFrame,
        rx_claims_df: DataFrame,
        reference_df: DataFrame,
        rx_reference_df: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Identify members with screening for or evidence of nephropathy during measurement year"""
    restricted_claims_df = claims_df.join(
        eligible_members,
        'member_id',
        how='inner'
    ).where(
        spark_funcs.col('fromdate').between(
            spark_funcs.lit(performance_yearstart),
            spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31))
        )
    )

    diag_explode_df = restricted_claims_df.select(
        'member_id',
        'fromdate',
        restricted_claims_df.icdversion,
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in restricted_claims_df.columns if
                 col.find('icddiag') > -1]
            )
        ).alias('diag')
    ).distinct()

    proc_explode_df = restricted_claims_df.select(
        'member_id',
        'fromdate',
        restricted_claims_df.icdversion,
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in restricted_claims_df.columns if
                 col.find('icdproc') > -1]
            )
        ).alias('proc')
    ).distinct()

    cpt_df = restricted_claims_df.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Urine Protein Tests',
                                                       'Nephropathy Treatment', 'ESRD',
                                                       'Kidney Transplant')
                & spark_funcs.col('code_system').isin('CPT', 'CPT-CAT-II', 'HCPCS')
            )
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_fromdate')
    )

    rev_df = restricted_claims_df.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('ESRD', 'Kidney Transplant')
                & spark_funcs.col('code_system').isin('UBREV', 'UBTOB')
            )
        ),
        spark_funcs.col('revcode') == spark_funcs.col('code'),
        how='inner'
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_fromdate')
    )

    pos_df = restricted_claims_df.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('ESRD')
                & spark_funcs.col('code_system').isin('POS')
            )
        ),
        spark_funcs.col('pos') == spark_funcs.col('code'),
        how='inner'
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_fromdate')
    )

    diag_df = diag_explode_df.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Nephropathy Treatment', 'CKD Stage 4',
                                                       'ESRD', 'Kidney Transplant')
                & spark_funcs.col('code_system').isin('ICD10CM', 'ICD9CM')
            )
        ),
        [
            restricted_claims_df.icdversion == reference_df.icdversion,
            spark_funcs.col('diag') == spark_funcs.col('code')
        ],
        how='inner'
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_fromdate')
    )

    proc_df = proc_explode_df.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('ESRD', 'Kidney Transplant')
                & spark_funcs.col('code_system').isin('ICD10PCS', 'ICD9PCS')
            )
        ),
        [
            restricted_claims_df.icdversion == reference_df.icdversion,
            spark_funcs.col('proc') == spark_funcs.col('code')
        ],
        how='inner'
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_fromdate')
    )

    ace_inhibitor_df = rx_claims_df.join(
        eligible_members,
        'member_id',
        how='inner'
    ).where(
        spark_funcs.col('fromdate').between(
            spark_funcs.lit(performance_yearstart),
            spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31))
        )
    ).join(
        spark_funcs.broadcast(
            rx_reference_df.where(
                spark_funcs.col('medication_list').isin('ACE Inhibitor/ARB Medications')
            )
        ),
        spark_funcs.col('ndc') == spark_funcs.col('ndc_code'),
        how='inner'
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_fromdate')
    )

    nephrologist_df = restricted_claims_df.where(
        spark_funcs.col('specialty') == '39'
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_fromdate')
    )

    numerator_compliant = cpt_df.union(
        pos_df
    ).union(
        rev_df
    ).union(
        diag_df
    ).union(
        proc_df
    ).union(
        ace_inhibitor_df
    ).union(
        nephrologist_df
    ).groupby(
        'member_id',
    ).agg(
        spark_funcs.max('latest_fromdate').alias('latest_fromdate'),
    )
    numerator_comments = numerator_compliant.withColumn(
        'comp_quality_comments',
        spark_funcs.concat(
            spark_funcs.lit('Nephropathy screening, diagnosis, or treatment on '),
            spark_funcs.col('latest_fromdate'),
        )
    )
    return numerator_comments


def identify_eye_exam(
        eligible_members: DataFrame,
        claims_df: DataFrame,
        reference_df: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Identify screening/monitoring for diabetic retinal disease during measurement year"""
    restricted_claims_df = claims_df.join(
        eligible_members,
        'member_id',
        how='inner'
    ).where(
        spark_funcs.col('fromdate').between(
            spark_funcs.lit(performance_yearstart),
            spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31))
        )
    )

    restricted_claims_prior_df = claims_df.join(
        eligible_members,
        'member_id',
        how='inner'
    ).where(
        spark_funcs.col('fromdate').between(
            spark_funcs.lit(datetime.date(performance_yearstart.year-1, 1, 1)),
            spark_funcs.lit(datetime.date(performance_yearstart.year-1, 12, 31))
        )
    )

    screening_df = restricted_claims_df.join(
        spark_funcs.broadcast(
            reference_df.where(spark_funcs.col('value_set_name').isin('Diabetic Retinal Screening'))
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).where(
        spark_funcs.col('specialty').isin('18', '41')
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_eye_screening')
    )

    screening_diag_prior_df = restricted_claims_prior_df.join(
        spark_funcs.broadcast(
            reference_df.where(spark_funcs.col('value_set_name').isin('Diabetic Retinal Screening'))
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).where(
        spark_funcs.col('specialty').isin('18', '41')
    ).select(
        'member_id',
        'fromdate',
        restricted_claims_prior_df.icdversion,
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in restricted_claims_prior_df.columns if
                 col.find('icddiag') > -1]
            )
        ).alias('diag')
    ).join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Diabetes Mellitus Without Complications')
            )
        ),
        [
            restricted_claims_prior_df.icdversion == reference_df.icdversion,
            spark_funcs.col('diag') == spark_funcs.col('code')
        ],
        how='inner'
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_no_complication_eye_screening')
    )

    screening_prof_df = restricted_claims_df.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin(
                    'Diabetic Retinal Screening With Eye Care Professional'
                )
            )
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_eye_screening_professional')
    )
    negative_result_df = restricted_claims_df.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Diabetic Retinal Screening Negative')
            )
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_negative_eye_screening')
    )

    bilateral_df = restricted_claims_df.join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Unilateral Eye Enucleation')
            )
        ),
        spark_funcs.col('hcpcs') == spark_funcs.col('code'),
        how='inner'
    ).where(
        spark_funcs.col('modifier').isin('50')| spark_funcs.col('modifier2').isin('50')
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_bilateral_date')
    )

    two_unilateral_pre_df = restricted_claims_df.select(
        'member_id',
        'fromdate',
        restricted_claims_df.icdversion,
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in restricted_claims_df.columns if
                 col.find('icdproc') > -1]
            )
        ).alias('proc')
    ).join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Unilateral Eye Enucleation Left')
            )
        ),
        [
            restricted_claims_df.icdversion == reference_df.icdversion,
            spark_funcs.col('proc') == spark_funcs.col('code')
        ],
        how='inner'
    )

    two_unilateral_df = two_unilateral_pre_df.join(
        two_unilateral_pre_df.select(
            spark_funcs.col('member_id').alias('join_member_id'),
            spark_funcs.col('fromdate').alias('join_fromdate'),
            spark_funcs.col('value_set_name').alias('join_value_set_name')
        ),
        [
            spark_funcs.col('member_id') == spark_funcs.col('join_member_id'),
            spark_funcs.col('value_set_name') == spark_funcs.col('join_value_set_name'),
            spark_funcs.datediff(
                spark_funcs.col('join_fromdate'),
                spark_funcs.col('fromdate')
            ) >= 14
        ],
        how='inner'
    ).groupby(
        'member_id'
    ).agg(
        spark_funcs.max('fromdate').alias('latest_two_unilateral_date')
    )

    left_and_right_df = restricted_claims_df.select(
        'member_id',
        'fromdate',
        restricted_claims_df.icdversion,
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col) for col in restricted_claims_df.columns if
                 col.find('icdproc') > -1]
            )
        ).alias('proc')
    ).join(
        spark_funcs.broadcast(
            reference_df.where(
                spark_funcs.col('value_set_name').isin('Unilateral Eye Enucleation Left',
                                                       'Unilateral Eye Enucleation Right')
            )
        ),
        [
            restricted_claims_df.icdversion == reference_df.icdversion,
            spark_funcs.col('proc') == spark_funcs.col('code')
        ],
        how='inner'
    ).groupBy(
        'member_id'
    ).agg(
        spark_funcs.countDistinct('value_set_name').alias('left_or_right_count'),
        spark_funcs.max('fromdate').alias('latest_left_or_right_date')
    ).where(
        spark_funcs.col('left_or_right_count') >= 2
    ).select(
        'member_id',
        'latest_left_or_right_date',
    )

    numerator_compliant = screening_df.join(
        screening_diag_prior_df,
        on='member_id',
        how='full_outer',
    ).join(
        screening_prof_df,
        on='member_id',
        how='full_outer',
    ).join(
        negative_result_df,
        on='member_id',
        how='full_outer',
    ).join(
        bilateral_df,
        on='member_id',
        how='full_outer',
    ).join(
        two_unilateral_df,
        on='member_id',
        how='full_outer',
    ).join(
        left_and_right_df,
        on='member_id',
        how='full_outer',
    )
    numerator_comments = numerator_compliant.withColumn(
        'comp_quality_comments',
        spark_funcs.when(
            (
                spark_funcs.col('latest_eye_screening').isNotNull()
                | spark_funcs.col('latest_eye_screening_professional').isNotNull()
            ),
            spark_funcs.concat(
                spark_funcs.lit('Eye exam given on '),
                spark_funcs.greatest(
                    spark_funcs.col('latest_eye_screening'),
                    spark_funcs.col('latest_eye_screening_professional'),
                ),
            )
        ).when(
            (
                spark_funcs.col('latest_bilateral_date').isNotNull()
                | spark_funcs.col('latest_two_unilateral_date').isNotNull()
                | spark_funcs.col('latest_left_or_right_date').isNotNull()
            ),
            spark_funcs.concat(
                spark_funcs.lit('Eye nucleation performed on '),
                spark_funcs.greatest(
                    spark_funcs.col('latest_bilateral_date'),
                    spark_funcs.col('latest_two_unilateral_date'),
                    spark_funcs.col('latest_left_or_right_date'),
                ),
            )
        ).when(
            spark_funcs.col('latest_no_complication_eye_screening').isNotNull(),
            spark_funcs.concat(
                spark_funcs.lit('Previous eye exam for diabetes without complications given on '),
                spark_funcs.col('latest_no_complication_eye_screening'),
            )
        ).when(
            spark_funcs.col('latest_negative_eye_screening').isNotNull(),
            spark_funcs.concat(
                spark_funcs.lit('Previous eye exam negative for retinopathy given on '),
                spark_funcs.col('latest_negative_eye_screening'),
            )
        )
    )
    return numerator_comments

def create_output_table(
        members: DataFrame,
        denominator: DataFrame,
        numerator: DataFrame,
        measure_name: str,
        performance_yearstart: datetime.date,
) -> DataFrame:
    """Prep numerator and denominator information for appropriate output format"""
    return members.join(
        denominator,
        'member_id',
        how='inner'
    ).join(
        numerator,
        members.member_id == numerator.member_id,
        how='left_outer'
    ).select(
        members.member_id,
        spark_funcs.lit(measure_name).alias('comp_quality_short'),
        spark_funcs.when(
            numerator.member_id.isNotNull(),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_numerator'),
        spark_funcs.lit(1).alias('comp_quality_denominator'),
        spark_funcs.lit(None).cast('date').alias('comp_quality_date_last'),
        'comp_quality_comments',
    ).fillna({
        'comp_quality_comments': 'No relevant claim found during performance period',
    }).select(
        '*',
        spark_funcs.when(
            spark_funcs.col('comp_quality_numerator') == 0,
            spark_funcs.lit(
                datetime.date(performance_yearstart.year, 12, 31)
            ),
        ).alias('comp_quality_date_actionable'),
    )


class CDC(QualityMeasure):
    """Object to house logic to calculate comprehensive diabetes care measures"""
    def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart=datetime.date,
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
                    0
                )
            )
        )

        pharmacy_eligible_members_df = _rx_dispensed_event(
            dfs_input['rx_claims'],
            dfs_input['ndc'],
            performance_yearstart
        )

        medical_eligible_members_df = _identify_med_event(
            dfs_input['claims'],
            reference_df,
            performance_yearstart
        )

        eligible_event_diag_members_df = pharmacy_eligible_members_df.union(
            medical_eligible_members_df
        ).distinct()

        eligible_members_df = dfs_input['member_time'].join(
            eligible_event_diag_members_df,
            'member_id',
            how='inner'
        ).where(
            (spark_funcs.col('date_start') >= performance_yearstart)
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
        ).where(
            spark_funcs.lit(spark_funcs.datediff(
                spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31)),
                spark_funcs.col('dob')
            ) / 365).between(
                18,
                75
            )
        )

        eligible_members_no_gaps_df = eligible_members_df.join(
            _exclude_elig_gaps(
                eligible_members_df,
                1,
                45
            ).withColumnRenamed('member_id', 'exclude_member_id'),
            spark_funcs.col('member_id') == spark_funcs.col('exclude_member_id'),
            how='left_outer'
        ).where(
            spark_funcs.col('exclude_member_id').isNull()
        ).select(
            'member_id'
        ).distinct()

        hba1c_testing_df = identify_a1c_tests(
            eligible_members_no_gaps_df,
            dfs_input['claims'],
            reference_df,
            performance_yearstart
        )

        nephropathy_df = identify_nephropathy(
            eligible_members_no_gaps_df,
            dfs_input['claims'],
            dfs_input['rx_claims'],
            reference_df,
            dfs_input['ndc'],
            performance_yearstart
        )

        eye_exam_df = identify_eye_exam(
            eligible_members_no_gaps_df,
            dfs_input['claims'],
            reference_df,
            performance_yearstart
        )

        hba1c_output_df = create_output_table(
            dfs_input['member'],
            eligible_members_no_gaps_df,
            hba1c_testing_df,
            'CDC: HBA1C',
            performance_yearstart,
        )

        eye_exam_output_df = create_output_table(
            dfs_input['member'],
            eligible_members_no_gaps_df,
            eye_exam_df,
            'CDC: EYE',
            performance_yearstart,
        )

        nephropathy_output_df = create_output_table(
            dfs_input['member'],
            eligible_members_no_gaps_df,
            nephropathy_df,
            'CDC: NEP',
            performance_yearstart,
        )

        return hba1c_output_df.union(
            eye_exam_output_df
        ).union(
            nephropathy_output_df
        ).orderBy(
            'member_id'
        )
