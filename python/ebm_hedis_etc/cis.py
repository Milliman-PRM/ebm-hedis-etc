"""
### CODE OWNERS: Alexander Olivero, Demerrick Moton

### OBJECTIVE:
    Calculate the Childhood Immunization Status HEDIS measure.

### DEVELOPER NOTES:
  <none>
"""
import datetime

import pyspark.sql.functions as spark_funcs
from pyspark.sql.dataframe import DataFrame
from prm.dates.windows import decouple_common_windows
from ebm_hedis_etc.base_classes import QualityMeasure

# pylint does not recognize many of the spark functions
# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _calc_simple_cis_measure(
        member_df: DataFrame,
        reference_df: DataFrame,
        eligible_members_df: DataFrame,
        eligible_claims_df: DataFrame,
        value_set_name: str,
        days_between_dob: int,
        vaccine_count: int
) -> DataFrame:
    reference_values = reference_df.where(
        spark_funcs.col('value_set_name') == value_set_name
    )

    claims_df = eligible_claims_df.join(
        reference_values,
        eligible_claims_df.hcpcs == reference_values.code,
        'inner'
    ).where(
        spark_funcs.datediff(
            spark_funcs.col('fromdate'),
            spark_funcs.col('dob')
        ) >= days_between_dob
    )

    vaccines_df = claims_df.groupBy(
        'member_id'
    ).agg(
        spark_funcs.countDistinct('fromdate').alias('vaccine_count'),
        spark_funcs.collect_set('fromdate').alias('vaccine_dates'),
    )

    output_df = member_df.select(
        spark_funcs.col('member_id').alias('base_member_id')
    ).join(
        eligible_members_df,
        spark_funcs.col('base_member_id') == spark_funcs.col('member_id'),
        how='left_outer'
    ).join(
        vaccines_df,
        'member_id',
        how='left_outer'
    ).select(
        '*',
        spark_funcs.when(
            spark_funcs.col('vaccine_count') >= vaccine_count,
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_numerator'),
        spark_funcs.when(
            eligible_members_df.member_id.isNotNull(),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_denominator'),
    ).select(
        spark_funcs.col('base_member_id').alias('member_id'),
        'comp_quality_numerator',
        'comp_quality_denominator',
        spark_funcs.col('second_birthday').alias('comp_quality_date_actionable'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_date_last'),
        spark_funcs.when(
            (spark_funcs.col('comp_quality_denominator') == 1)
            & spark_funcs.col('vaccine_dates').isNotNull(),
            spark_funcs.concat(
                spark_funcs.lit(value_set_name + ' On: '),
                spark_funcs.concat_ws(
                    ', ',
                    spark_funcs.col('vaccine_dates'),
                )
            )
        ).when(
            spark_funcs.col('comp_quality_denominator') == 1,
            spark_funcs.lit('No ' + value_set_name)
        ).alias('comp_quality_comments'),
        'vaccine_count'
    )

    return output_df


def calc_dtap(
        member_df: DataFrame,
        eligible_members_df: DataFrame,
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate DTaP Vaccine Measure"""
    return _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'DTaP Vaccine Administered',
        42,
        4
    ).drop(
        'vaccine_count'
    ).withColumn(
        'comp_quality_short',
        spark_funcs.lit('CIS - DTaP')
    )


def calc_ipv(
        member_df: DataFrame,
        eligible_members_df: DataFrame,
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate IPV Vaccine Measure"""
    return _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Inactivated Polio Vaccine (IPV) Administered',
        42,
        3
    ).drop(
        'vaccine_count'
    ).withColumn(
        'comp_quality_short',
        spark_funcs.lit('CIS - IPV')
    )


def calc_hib(
        member_df: DataFrame,
        eligible_members_df: DataFrame,
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate HiB Vaccine Measure"""
    return _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Haemophilus Influenzae Type B (HiB) Vaccine Administered',
        42,
        3
    ).drop(
        'vaccine_count'
    ).withColumn(
        'comp_quality_short',
        spark_funcs.lit('CIS - HiB')
    )


def calc_pneumococcal(
        member_df: DataFrame,
        eligible_members_df: DataFrame,
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate Pneumococcal Vaccine Measure"""
    return _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Pneumococcal Conjugate Vaccine Administered',
        42,
        4
    ).drop(
        'vaccine_count'
    ).withColumn(
        'comp_quality_short',
        spark_funcs.lit('CIS - Pneumococcal Conjugate')
    )


def calc_influenza(
        member_df: DataFrame,
        eligible_members_df: DataFrame,
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate Influenza Vaccine Measure"""
    return _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Influenza Vaccine Administered',
        180,
        2
    ).drop(
        'vaccine_count'
    ).withColumn(
        'comp_quality_short',
        spark_funcs.lit('CIS - Influenza')
    )


def calc_hepatitis_b(
        member_df: DataFrame,
        eligible_members_df: DataFrame,
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate Hepatitis B Vaccine Measure"""
    diags_explode_df = eligible_claims_df.select(
        'member_id',
        'fromdate',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col)
                 for col in eligible_claims_df.columns if col.find('icddiag') > -1]
            )
        ).alias('diag')
    )

    history_hepatitis_df = diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Hepatitis B'
        ),
        [
            diags_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                      r'\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, r'\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    hep_b_vaccine_df = _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Hepatitis B Vaccine Administered',
        0,
        3
    ).select(
        'member_id',
        'vaccine_count'
    )

    procs_explode_df = eligible_claims_df.select(
        'member_id',
        'dob',
        'fromdate',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col)
                 for col in eligible_claims_df.columns if col.find('icdproc') > -1]
            )
        ).alias('proc')
    )

    newborn_hepatitis_df = procs_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Newborn Hepatitis B Vaccine Administered'
        ),
        [
            procs_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                      r'\d+', 0),
            procs_explode_df.proc == spark_funcs.regexp_replace(reference_df.code, r'\.', '')
        ],
        how='left_outer'
    ).where(
        spark_funcs.datediff(
            spark_funcs.col('fromdate'),
            spark_funcs.col('dob')
        ) < 8
    ).select(
        'member_id'
    ).distinct()

    combine_df = hep_b_vaccine_df.join(
        newborn_hepatitis_df,
        'member_id',
        how='full_outer'
    ).withColumn(
        'vaccine_count',
        spark_funcs.when(
            spark_funcs.col('vaccine_count').isNotNull(),
            spark_funcs.col('vaccine_count') + 1
        ).otherwise(
            spark_funcs.lit(1)
        )
    ).where(
        spark_funcs.col('vaccine_count') >= 3
    )

    output_df = member_df.select(
        spark_funcs.col('member_id').alias('base_member_id')
    ).join(
        eligible_members_df,
        spark_funcs.col('base_member_id') == spark_funcs.col('member_id'),
        how='left_outer'
    ).join(
        combine_df,
        eligible_members_df.member_id == combine_df.member_id,
        how='left_outer'
    ).join(
        history_hepatitis_df,
        eligible_members_df.member_id == history_hepatitis_df.member_id,
        how='left_outer'
    ).select(
        spark_funcs.col('base_member_id').alias('member_id'),
        spark_funcs.lit('CIS - Hepatitis B').alias('comp_quality_short'),
        spark_funcs.when(
            combine_df.member_id.isNotNull() | history_hepatitis_df.member_id.isNotNull(),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_numerator'),
        spark_funcs.when(
            eligible_members_df.member_id.isNotNull(),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_denominator'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_date_actionable'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_date_last'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_comments')
    )

    return output_df


def calc_hepatitis_a(
        member_df: DataFrame,
        eligible_members_df: DataFrame,
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate Hepatitis A Vaccine Measure"""
    hep_a_vaccine_df = _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Hepatitis A Vaccine Administered',
        0,
        1
    ).where(
        spark_funcs.col('comp_quality_numerator') == 1
    ).select(
        'member_id',
        'vaccine_count'
    )

    diags_explode_df = eligible_claims_df.select(
        'member_id',
        'fromdate',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col)
                 for col in eligible_claims_df.columns if col.find('icddiag') > -1]
            )
        ).alias('diag')
    )

    hep_a_history_df = diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Hepatitis A'
        ),
        [
            diags_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                      r'\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, r'\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    output_df = member_df.select(
        spark_funcs.col('member_id').alias('base_member_id')
    ).join(
        eligible_members_df,
        spark_funcs.col('base_member_id') == spark_funcs.col('member_id'),
        how='left_outer'
    ).join(
        hep_a_vaccine_df,
        eligible_members_df.member_id == hep_a_vaccine_df.member_id,
        how='left_outer'
    ).join(
        hep_a_history_df,
        eligible_members_df.member_id == hep_a_history_df.member_id,
        how='left_outer'
    ).select(
        spark_funcs.col('base_member_id').alias('member_id'),
        spark_funcs.lit('CIS - Hepatitis A').alias('comp_quality_short'),
        spark_funcs.when(
            hep_a_vaccine_df.member_id.isNotNull() | hep_a_history_df.member_id.isNotNull(),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_numerator'),
        spark_funcs.when(
            eligible_members_df.member_id.isNotNull(),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_denominator'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_date_actionable'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_date_last'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_comments')
    )

    return output_df


def calc_vzv(
        member_df: DataFrame,
        eligible_members_df: DataFrame,
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate VZV Vaccine Measure"""
    vzv_vaccine_df = _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Varicella Zoster (VZV) Vaccine Administered',
        0,
        1
    ).where(
        spark_funcs.col('comp_quality_numerator') == 1
    ).select(
        'member_id'
    )

    diags_explode_df = eligible_claims_df.select(
        'member_id',
        'fromdate',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col)
                 for col in eligible_claims_df.columns if col.find('icddiag') > -1]
            )
        ).alias('diag')
    )

    vzv_history_df = diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Varicella Zoster'
        ),
        [
            diags_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                      r'\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, r'\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    output_df = member_df.select(
        spark_funcs.col('member_id').alias('base_member_id')
    ).join(
        eligible_members_df,
        spark_funcs.col('base_member_id') == spark_funcs.col('member_id'),
        how='left_outer'
    ).join(
        vzv_vaccine_df,
        eligible_members_df.member_id == vzv_vaccine_df.member_id,
        how='left_outer'
    ).join(
        vzv_history_df,
        eligible_members_df.member_id == vzv_history_df.member_id,
        how='left_outer'
    ).select(
        spark_funcs.col('base_member_id').alias('member_id'),
        spark_funcs.lit('CIS - VZV').alias('comp_quality_short'),
        spark_funcs.when(
            (vzv_vaccine_df.member_id.isNotNull()) | (vzv_history_df.member_id.isNotNull()),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_numerator'),
        spark_funcs.when(
            eligible_members_df.member_id.isNotNull(),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_denominator'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_date_actionable'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_date_last'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_comments')
    )

    return output_df


def calc_rotavirus(
        member_df: DataFrame,
        eligible_members_df: DataFrame,
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate Rotavirus Vaccine Measure"""
    two_two_dose_df = _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Rotavirus Vaccine (2 Dose Schedule) Administered',
        42,
        2
    ).where(
        spark_funcs.col('comp_quality_numerator') == 1
    ).select(
        'member_id'
    )

    three_three_dose_df = _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Rotavirus Vaccine (3 Dose Schedule) Administered',
        42,
        3
    ).where(
        spark_funcs.col('comp_quality_numerator') == 1
    ).select(
        'member_id'
    )

    two_reference_values = reference_df.where(
        spark_funcs.col('value_set_name') == 'Rotavirus Vaccine (2 Dose Schedule) Administered'
    )

    three_reference_values = reference_df.where(
        spark_funcs.col('value_set_name') == 'Rotavirus Vaccine (3 Dose Schedule) Administered'
    )

    two_claims_df = eligible_claims_df.join(
        two_reference_values,
        eligible_claims_df.hcpcs == two_reference_values.code,
        'inner'
    ).where(
        spark_funcs.datediff(
            spark_funcs.col('fromdate'),
            spark_funcs.col('dob')
        ) >= 42
    )

    three_claims_df = eligible_claims_df.join(
        three_reference_values,
        eligible_claims_df.hcpcs == three_reference_values.code,
        'inner'
    ).where(
        spark_funcs.datediff(
            spark_funcs.col('fromdate'),
            spark_funcs.col('dob')
        ) >= 42
    )

    two_three_combination_df = two_claims_df.union(
        three_claims_df
    ).groupBy(
        'member_id',
        'value_set_name'
    ).agg(
        spark_funcs.countDistinct('fromdate').alias('vaccine_count')
    ).where(
        (spark_funcs.col('value_set_name').isin(
            'Rotavirus Vaccine (3 Dose Schedule) Administered')
         & (spark_funcs.col('vaccine_count') >= 2))
        | (spark_funcs.col('value_set_name').isin(
            'Rotavirus Vaccine (2 Dose Schedule) Administered')
           & (spark_funcs.col('vaccine_count') >= 1))
    ).groupBy(
        'member_id'
    ).agg(
        spark_funcs.countDistinct('value_set_name').alias('vaccine_type_count')
    ).where(
        spark_funcs.col('vaccine_type_count') > 1
    )

    output_df = member_df.select(
        spark_funcs.col('member_id').alias('base_member_id')
    ).join(
        eligible_members_df,
        spark_funcs.col('base_member_id') == spark_funcs.col('member_id'),
        how='left_outer'
    ).join(
        two_two_dose_df.withColumnRenamed(
            'member_id',
            'two_two_member_id'
        ),
        spark_funcs.col('member_id') == spark_funcs.col('two_two_member_id'),
        how='left_outer'
    ).join(
        three_three_dose_df.withColumnRenamed(
            'member_id',
            'three_three_member_id'
        ),
        spark_funcs.col('member_id') == spark_funcs.col('three_three_member_id'),
        how='left_outer'
    ).join(
        two_three_combination_df.withColumnRenamed(
            'member_id',
            'two_three_member_id'
        ),
        spark_funcs.col('member_id') == spark_funcs.col('two_three_member_id'),
        how='left_outer'
    ).select(
        spark_funcs.col('base_member_id').alias('member_id'),
        spark_funcs.lit('CIS - Rotavirus').alias('comp_quality_short'),
        spark_funcs.when(
            spark_funcs.col('two_two_member_id').isNotNull()
            | spark_funcs.col('three_three_member_id').isNotNull()
            | spark_funcs.col('two_three_member_id').isNotNull(),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_numerator'),
        spark_funcs.when(
            eligible_members_df.member_id.isNotNull(),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_denominator'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_date_actionable'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_date_last'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_comments')
    )

    return output_df


def calc_mmr(
        member_df: DataFrame,
        eligible_members_df: DataFrame,
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate MMR Vaccine Measure"""
    mmr_vaccine_df = _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Measles, Mumps and Rubella (MMR) Vaccine Administered',
        0,
        1
    ).where(
        spark_funcs.col('comp_quality_numerator') == 1
    ).select(
        'member_id'
    )

    measles_rubella_vaccine_df = _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Measles/Rubella Vaccine Administered',
        0,
        1
    ).where(
        spark_funcs.col('comp_quality_numerator') == 1
    ).select(
        'member_id'
    )

    diags_explode_df = eligible_claims_df.select(
        'member_id',
        'fromdate',
        'icdversion',
        spark_funcs.explode(
            spark_funcs.array(
                [spark_funcs.col(col)
                 for col in eligible_claims_df.columns if col.find('icddiag') > -1]
            )
        ).alias('diag')
    )

    mumps_history_df = diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Mumps'
        ),
        [
            diags_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                      r'\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, r'\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    mumps_vaccine_df = _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Mumps Vaccine Administered',
        0,
        1
    ).where(
        spark_funcs.col('comp_quality_numerator') == 1
    ).select(
        'member_id'
    )

    mumps_vaccine_or_history_df = mumps_history_df.union(
        mumps_vaccine_df.select(
            'member_id'
        )
    ).distinct()

    measles_rubella_and_mumps_df = measles_rubella_vaccine_df.select(
        'member_id'
    ).intersect(
        mumps_vaccine_or_history_df
    ).distinct()

    measles_vaccine_df = _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Measles Vaccine Administered',
        0,
        1
    ).where(
        spark_funcs.col('comp_quality_numerator') == 1
    ).select(
        'member_id'
    )

    measles_history_df = diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Measles'
        ),
        [
            diags_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                      r'\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, r'\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    measles_vaccine_or_history_df = measles_vaccine_df.select(
        'member_id'
    ).union(
        measles_history_df
    ).distinct()

    rubella_vaccine_df = _calc_simple_cis_measure(
        member_df,
        reference_df,
        eligible_members_df,
        eligible_claims_df,
        'Rubella Vaccine Administered',
        0,
        1
    ).where(
        spark_funcs.col('comp_quality_numerator') == 1
    ).select(
        'member_id'
    )

    rubella_history_df = diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Rubella'
        ),
        [
            diags_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                      r'\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, r'\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    rubella_vaccine_or_history_df = rubella_vaccine_df.select(
        'member_id'
    ).union(
        rubella_history_df
    ).distinct()

    measles_and_rubella_and_mumps_df = measles_vaccine_or_history_df.intersect(
        mumps_vaccine_or_history_df
    ).intersect(
        rubella_vaccine_or_history_df
    )

    output_df = member_df.select(
        spark_funcs.col('member_id').alias('base_member_id')
    ).join(
        eligible_members_df,
        spark_funcs.col('base_member_id') == spark_funcs.col('member_id'),
        how='left_outer'
    ).join(
        mmr_vaccine_df.withColumnRenamed(
            'member_id',
            'mmr_vaccine_member_id'
        ),
        spark_funcs.col('member_id') == spark_funcs.col('mmr_vaccine_member_id'),
        how='left_outer'
    ).join(
        measles_rubella_and_mumps_df.withColumnRenamed(
            'member_id',
            'meas_rub_and_mumps_member_id'
        ),
        spark_funcs.col('member_id') == spark_funcs.col('meas_rub_and_mumps_member_id'),
        how='left_outer'
    ).join(
        measles_and_rubella_and_mumps_df.withColumnRenamed(
            'member_id',
            'meas_and_rub_and_mumps_member_id'
        ),
        spark_funcs.col('member_id') == spark_funcs.col('meas_and_rub_and_mumps_member_id'),
        how='left_outer'
    ).select(
        spark_funcs.col('base_member_id').alias('member_id'),
        spark_funcs.lit('CIS - MMR').alias('comp_quality_short'),
        spark_funcs.when(
            spark_funcs.col('mmr_vaccine_member_id').isNotNull()
            | spark_funcs.col('meas_rub_and_mumps_member_id').isNotNull()
            | spark_funcs.col('meas_and_rub_and_mumps_member_id').isNotNull(),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_numerator'),
        spark_funcs.when(
            eligible_members_df.member_id.isNotNull(),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('comp_quality_denominator'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_date_actionable'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_date_last'),
        spark_funcs.lit(None).cast('string').alias('comp_quality_comments')
    )

    return output_df


class CIS(QualityMeasure):
    """Object to house logic to calculate childhood immunization status quality measure"""
    def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart=datetime.date,
    ):
        measure_start = performance_yearstart
        measure_end = datetime.date(performance_yearstart.year, 12, 31)

        elig_pop_covered = dfs_input['member_time'].where(
            spark_funcs.col('cover_medical').isin('Y')
        ).join(
            dfs_input['member'].select(
                'member_id',
                'dob'
            ),
            'member_id',
            'left_outer'
        ).withColumn(
            'second_birthday',
            spark_funcs.date_add(
                spark_funcs.col('dob'),
                365 * 2
            )
        ).where(
            spark_funcs.col('second_birthday').between(
                spark_funcs.lit(measure_start),
                spark_funcs.lit(measure_end)
            )
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
            spark_funcs.col('cover_medical').isNull()
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
        ).join(
            dfs_input['member'].select(
                'member_id',
                'dob'
            ),
            'member_id',
            'left_outer'
        )

        long_gap_df = gaps_df.where(
            spark_funcs.col('date_start').between(
                spark_funcs.date_sub(
                    spark_funcs.col('dob'),
                    365
                ),
                spark_funcs.col('dob')
            ) &
            (spark_funcs.col('date_diff') >= 45)
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
            'second_birthday'
        ).distinct().join(
            excluded_members_df,
            'member_id',
            how='left_outer'
        ).where(
            excluded_members_df.member_id.isNotNull()
        )

        eligible_claims_df = dfs_input['claims'].join(
            eligible_members_df,
            'member_id',
            how='inner'
        ).where(
            spark_funcs.col('fromdate') <= spark_funcs.col('second_birthday')
        )

        measures_dict = {
            'dtap_df': calc_dtap(
                dfs_input['member'],
                eligible_members_df,
                eligible_claims_df,
                dfs_input['reference']
            ),
            'ipv_df': calc_ipv(
                dfs_input['member'],
                eligible_members_df,
                eligible_claims_df,
                dfs_input['reference']
            ),
            'mmr_df': calc_mmr(
                dfs_input['member'],
                eligible_members_df,
                eligible_claims_df,
                dfs_input['reference']
            ),
            'hib_df': calc_hib(
                dfs_input['member'],
                eligible_members_df,
                eligible_claims_df,
                dfs_input['reference']
            ),
            'hepatitis_b_df': calc_hepatitis_b(
                dfs_input['member'],
                eligible_members_df,
                eligible_claims_df,
                dfs_input['reference']
            ),
            'vzv_df': calc_vzv(
                dfs_input['member'],
                eligible_members_df,
                eligible_claims_df,
                dfs_input['reference']
            ),
            'pneumococcal_df': calc_pneumococcal(
                dfs_input['member'],
                eligible_members_df,
                eligible_claims_df,
                dfs_input['reference']
            ),
            'hepatitis_a_df': calc_hepatitis_a(
                dfs_input['member'],
                eligible_members_df,
                eligible_claims_df,
                dfs_input['reference']
            ),
            'rotavirus_df': calc_rotavirus(
                dfs_input['member'],
                eligible_members_df,
                eligible_claims_df,
                dfs_input['reference']
            ),
            'influenza_df': calc_influenza(
                dfs_input['member'],
                eligible_members_df,
                eligible_claims_df,
                dfs_input['reference']
            ),
        }

        results_df = None
        for key, value in measures_dict.items():
            if not results_df:
                results_df = value
            else:
                results_df = results_df.union(value.select(results_df.columns))

        return results_df
