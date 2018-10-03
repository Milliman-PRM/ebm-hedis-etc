"""
### CODE OWNERS:
### OBJECTIVE:

### DEVELOPER NOTES:
  <none>
"""
import logging
import datetime

import pyspark.sql.functions as spark_funcs
from pyspark.sql.dataframe import DataFrame
from prm.dates.windows import decouple_common_windows

LOGGER = logging.getLogger(__name__)

# pylint does not recognize many of the spark functions
# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def _calc_simple_cis_measure(
        reference_df: DataFrame,
        eligible_claims_df: DataFrame,
        value_set_name: str,
        days_between_dob: int,
        distinct_vaccine_count: int
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
        spark_funcs.countDistinct('fromdate').alias('vaccine_count')
    ).where(
        spark_funcs.col('vaccine_count') >= distinct_vaccine_count
    )

    return vaccines_df


def calc_dtap(
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate DTaP Vaccine Measure"""
    return _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'DTaP Vaccine Administered',
        42,
        4
    )


def calc_ipv(
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate IPV Vaccine Measure"""
    return _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'Inactivated Polio Vaccine (IPV) Administered',
        42,
        3
    )


def calc_hib(
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    """Calculate HiB Vaccine Measure"""
    return _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'Haemophilus Influenzae Type B (HiB) Vaccine Administered',
        42,
        3
    )


def calc_pneumococcal(
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    return _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'Pneumococcal Conjugate Vaccine Administered',
        42,
        4
    )


def calc_influenza(
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    return _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'Influenza Vaccine Administered',
        180,
        2
    )


def calc_hepatitis_b(
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
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
                                                                      '\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, '\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    hep_b_vaccine_df = _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'Hepatitis B Vaccine Administered',
        0,
        3
    )

    procs_expolde_df = eligible_claims_df.select(
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

    newborn_hepatitis_df = procs_expolde_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Newborn Hepatitis B Vaccine Administered'
        ),
        [
            procs_expolde_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                      '\d+', 0),
            procs_expolde_df.proc == spark_funcs.regexp_replace(reference_df.code, '\.', '')
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

    return combine_df.select(
        'member_id'
    ).union(
        history_hepatitis_df
    ).distinct()


def calc_hepatitis_a(
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    hep_a_vaccine_df = _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'Hepatits A Vaccine Administered',
        0,
        1
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
                                                                      '\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, '\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    return hep_a_vaccine_df.select(
        'member_id'
    ).union(
        hep_a_history_df
    )


def calc_vzv(
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    vzv_vaccine_df = _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'Varicella Zoster (VZV) Vaccine Administered',
        0,
        1
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
                                                                      '\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, '\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    return vzv_vaccine_df.select(
        'member_id'
    ).union(
        vzv_history_df
    ).distinct()


def calc_rotavirus(
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    two_two_dose_df = _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'Rotavirus Vaccine (2 Dose Schedule) Administered',
        42,
        2
    )

    three_three_dose_df = _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'Rotavirus Vaccine (3 Dose Schedule) Administered',
        42,
        3
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
            'Rotavirus Vaccine (3 Dose Schedule) Administered') & (
                     spark_funcs.col('vaccine_count') >= 2))
        | (spark_funcs.col('value_set_name').isin(
            'Rotavirus Vaccine (2 Dose Schedule) Administered') & (
                       spark_funcs.col('vaccine_count') >= 1))
    ).groupBy(
        'member_id'
    ).agg(
        spark_funcs.countDistinct('value_set_name').alias('vaccine_type_count')
    ).where(
        spark_funcs.col('vaccine_type_count') > 1
    )

    return two_two_dose_df.select(
        'member_id'
    ).union(
        three_three_dose_df.select(
            'member_id'
        )
    ).union(
        two_three_combination_df.select(
            'member_id'
        )
    ).distinct()


def calc_mmr(
        eligible_claims_df: DataFrame,
        reference_df: DataFrame
) -> DataFrame:
    mmr_vaccine_df = _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'Measles, Mumps and Rubella (MMR) Vaccine Administered',
        0,
        1
    )

    measles_rubella_vaccine_df = _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'Measles/Rubella Vaccine Administered',
        0,
        1
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
                                                                      '\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, '\.', '')
        ],
        how='inner'
    ).select(
        'member_id'
    ).distinct()

    mumps_vaccine_df = _calc_simple_cis_measure(
        reference_df,
        eligible_claims_df,
        'Mumps Vaccine Administered',
        0,
        1
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
        reference_df,
        eligible_claims_df,
        'Measles Vaccine Administered',
        0,
        1
    )

    measles_history_df = diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Measles'
        ),
        [
            diags_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                      '\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, '\.', '')
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
        reference_df,
        eligible_claims_df,
        'Rubella Vaccine Administered',
        0,
        1
    )

    rubella_history_df = diags_explode_df.join(
        reference_df.where(
            spark_funcs.col('value_set_name') == 'Rubella'
        ),
        [
            diags_explode_df.icdversion == spark_funcs.regexp_extract(reference_df.code_system,
                                                                      '\d+', 0),
            diags_explode_df.diag == spark_funcs.regexp_replace(reference_df.code, '\.', '')
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

    return mmr_vaccine_df.select(
        'member_id'
    ).union(
        measles_rubella_and_mumps_df
    ).union(
        measles_and_rubella_and_mumps_df
    ).distinct()

def _calc_measures(
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
    ).where(
        spark_funcs.date_add(
            spark_funcs.col('dob'),
            365*2
        ).between(
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
        spark_funcs.col('assignment_indicator').isNull()
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
        (spark_funcs.col('date_diff') > 45)
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
        'member_id'
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
    )

    reference_df = dfs_input['reference']

    dtap_df = calc_dtap(eligible_claims_df, reference_df)

    ipv_df = calc_ipv(eligible_claims_df, reference_df)

    mmr_df = calc_mmr(eligible_claims_df, reference_df)

    hib_df = calc_hib(eligible_claims_df, reference_df)

    hepatitis_b_df = calc_hepatitis_b(eligible_claims_df, reference_df)

    vzv_df = calc_vzv(eligible_claims_df, reference_df)

    pneumococcal_df = calc_pneumococcal(eligible_claims_df, reference_df)

    hepatitis_a_df = calc_hepatitis_a(eligible_claims_df, reference_df)

    rotavirus_df = calc_rotavirus(eligible_claims_df, reference_df)

    influenza_df = calc_influenza(eligible_claims_df, reference_df)



