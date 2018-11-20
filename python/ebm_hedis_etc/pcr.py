"""
### CODE OWNERS: Ben Copeland

### OBJECTIVE:
    Calculate the Plan All-Cause Readmissions HEDIS measure.

### DEVELOPER NOTES:
  <none>
"""
import logging
import datetime
import itertools

import pyspark.sql.functions as spark_funcs
import pyspark.sql.types as spark_types
from pyspark.sql import DataFrame, Window
from prm.dates.windows import decouple_common_windows
from ebm_hedis_etc.base_classes import QualityMeasure

LOGGER = logging.getLogger(__name__)

# pylint does not recognize many of the spark functions
# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _filter_relevant_value_sets(
        reference_df: DataFrame,
) -> "typing.Mapping[str, DataFrame]":
    """Filter the reference dataset down to relevant dataframes"""

    relevant_value_sets = {
        'Inpatient Stay',
        'Nonacute Inpatient Stay',
        'Pregnancy',
        'Perinatal Conditions',
        'Chemotherapy',
        'Rehabilitation',
        'Kidney Transplant',
        'Bone Marrow Transplant',
        'Organ Transplant Other Than Kidney',
        'Potentially Planned Procedures',
        'Acute Condition',
        }
    df_relevant_value_sets = reference_df.where(
        spark_funcs.col('value_set_name').isin(relevant_value_sets)
    )
    found_value_sets = {
        row['value_set_name']
        for row in df_relevant_value_sets.select('value_set_name').distinct().collect()
        }
    missing_value_sets = relevant_value_sets - found_value_sets
    assert not(missing_value_sets), 'Did not find {} value sets'.format(missing_value_sets)

    dfs_relevant_value_sets = dict()
    dfs_relevant_value_sets['hcpcs'] = df_relevant_value_sets.where(
        spark_funcs.col('code_system').isin('CPT', 'HCPCS')
    ).select(
        spark_funcs.col('value_set_name').alias('value_set_name_hcpcs'),
        spark_funcs.col('code').alias('hcpcs'),
    )
    dfs_relevant_value_sets['icdproc'] = df_relevant_value_sets.where(
        spark_funcs.col('code_system').isin('ICD9PCS', 'ICD10PCS')
    ).select(
        spark_funcs.col('value_set_name').alias('value_set_name_icdproc'),
        spark_funcs.regexp_replace(
            spark_funcs.col('code'),
            r'\.',
            '',
            ).alias('icdproc'),
        spark_funcs.when(
            spark_funcs.col('code_system') == spark_funcs.lit('ICD9PCS'),
            spark_funcs.lit('09'),
            ).otherwise(
                spark_funcs.lit('10')
                ).alias('icdversion'),
    )
    dfs_relevant_value_sets['icddiag'] = df_relevant_value_sets.where(
        spark_funcs.col('code_system').isin('ICD9CM', 'ICD10CM')
    ).select(
        spark_funcs.col('value_set_name').alias('value_set_name_icddiag'),
        spark_funcs.regexp_replace(
            spark_funcs.col('code'),
            r'\.',
            '',
            ).alias('icddiag'),
        spark_funcs.when(
            spark_funcs.col('code_system') == spark_funcs.lit('ICD9CM'),
            spark_funcs.lit('09'),
            ).otherwise(
                spark_funcs.lit('10')
                ).alias('icdversion'),
    )
    dfs_relevant_value_sets['revcode'] = df_relevant_value_sets.where(
        spark_funcs.col('code_system') == 'UBREV'
    ).select(
        spark_funcs.col('value_set_name').alias('value_set_name_revcode'),
        spark_funcs.col('code').alias('revcode'),
    )
    dfs_relevant_value_sets['billtype'] = df_relevant_value_sets.where(
        spark_funcs.col('code_system') == 'UBTOB'
    ).select(
        spark_funcs.col('value_set_name').alias('value_set_name_billtype'),
        spark_funcs.col('code').alias('billtype'),
    )

    return dfs_relevant_value_sets

def _summ_claim_diag_value_sets(
        valid_dates_df: DataFrame,
        dfs_relevant_refs: "typing.Mapping[str, DataFrame]",
) -> DataFrame:
    """Identify relevant diagnosis value sets at the claim level"""
    diag_explode = valid_dates_df.select(
        '*',
        spark_funcs.explode(
            spark_funcs.create_map(
                *itertools.chain.from_iterable([
                    [spark_funcs.lit(col_name), spark_funcs.col(col_name)]
                    for col_name in valid_dates_df.columns
                    if col_name.startswith('icddiag')
                ])
            )
        ).alias('icddiag_number', 'icddiag')
    )

    value_set_diag_level = diag_explode.join(
        spark_funcs.broadcast(dfs_relevant_refs['icddiag']),
        on=['icdversion', 'icddiag'],
        how='inner',
        )
    valid_diags = value_set_diag_level.where(
        spark_funcs.col('value_set_name_icddiag').isin(
            'Kidney Transplant Value Set',
            'Bone Marrow Transplant Value Set',
            'Organ Transplant Other Than Kidney',
            )
        | (spark_funcs.col('icddiag_number') == spark_funcs.lit('icddiag1'))
    )
    claim_diag_value_sets = valid_diags.groupby(
        'member_id',
        'claimid',
    ).agg(
        spark_funcs.collect_set('value_set_name_icddiag').alias('value_set_name_icddiag')
    )
    return claim_diag_value_sets

def _summ_claim_proc_value_sets(
        valid_dates_df: DataFrame,
        dfs_relevant_refs: "typing.Mapping[str, DataFrame]",
) -> DataFrame:
    """Identify relevant diagnosis value sets at the claim level"""
    proc_explode = valid_dates_df.select(
        '*',
        spark_funcs.explode(
            spark_funcs.create_map(
                *itertools.chain.from_iterable([
                    [spark_funcs.lit(col_name), spark_funcs.col(col_name)]
                    for col_name in valid_dates_df.columns
                    if col_name.startswith('icdproc')
                ])
            )
        ).alias('icdproc_number', 'icdproc')
    )
    value_set_proc_level = proc_explode.join(
        spark_funcs.broadcast(dfs_relevant_refs['icdproc']),
        on=['icdversion', 'icdproc'],
        how='inner',
        )
    claim_proc_value_sets = value_set_proc_level.groupby(
        'member_id',
        'claimid',
    ).agg(
        spark_funcs.collect_set('value_set_name_icdproc').alias('value_set_name_icdproc')
    )
    return claim_proc_value_sets

def _identify_claim_value_sets(
        claims_df: DataFrame,
        reference_df: DataFrame,
        performance_yearstart: datetime.date
) -> DataFrame:
    """Find claims that meet criteria for events to qualify members for denominator"""

    dfs_relevant_refs = _filter_relevant_value_sets(reference_df)

    valid_dates_df = claims_df.where(
        spark_funcs.col('dischdate') >= performance_yearstart
    )

    claim_diag_value_sets = _summ_claim_diag_value_sets(
        valid_dates_df,
        dfs_relevant_refs,
        )
    claim_proc_value_sets = _summ_claim_proc_value_sets(
        valid_dates_df,
        dfs_relevant_refs,
        )

    value_set_claim_level = valid_dates_df.join(
        spark_funcs.broadcast(dfs_relevant_refs['revcode']),
        on='revcode',
        how='left',
    ).join(
        spark_funcs.broadcast(dfs_relevant_refs['hcpcs']),
        on='hcpcs',
        how='left',
    ).join(
        spark_funcs.broadcast(dfs_relevant_refs['billtype']),
        on='billtype',
        how='left',
    )
    claim_value_sets = value_set_claim_level.groupby(
        'member_id',
        'claimid',
    ).agg(
        spark_funcs.collect_set('value_set_name_revcode').alias('value_set_name_revcode'),
        spark_funcs.collect_set('value_set_name_hcpcs').alias('value_set_name_hcpcs'),
        spark_funcs.collect_set('value_set_name_billtype').alias('value_set_name_billtype'),
        spark_funcs.max(
            spark_funcs.when(
                spark_funcs.col('dischargestatus') == '20',
                spark_funcs.lit(True),
            ).otherwise(spark_funcs.lit(False))
        ).alias('died_during_stay'),
        spark_funcs.min('admitdate').alias('admitdate'),
        spark_funcs.max('dischdate').alias('dischdate'),
    ).join(
        claim_diag_value_sets,
        on=['member_id', 'claimid'],
        how='left',
    ).join(
        claim_proc_value_sets,
        on=['member_id', 'claimid'],
        how='left',
    )

    def concat_arrays(*cols):
        """UDF for concatenating array columns"""
        output_col = []
        for col in cols:
            if col is not None:
                output_col += col

        return output_col

    udf_concat = spark_funcs.udf(
        concat_arrays,
        spark_types.ArrayType(spark_types.StringType())
        )

    claim_value_sets_collect = claim_value_sets.select(
        '*',
        udf_concat(
            spark_funcs.col('value_set_name_revcode'),
            spark_funcs.col('value_set_name_hcpcs'),
            spark_funcs.col('value_set_name_billtype'),
            spark_funcs.col('value_set_name_icddiag'),
            spark_funcs.col('value_set_name_icdproc'),
        ).alias('value_set_names')
    )

    return claim_value_sets_collect

def _flag_calculation_steps(
        claim_value_sets: DataFrame,
    ) -> DataFrame:
    """Condense value sets into more meaningful categories for measure calculation"""

    claims_calc_flags = claim_value_sets.select(
        '*',
        spark_funcs.when(
            spark_funcs.array_contains(
                spark_funcs.col('value_set_names'),
                'Inpatient Stay',
            )
            & ~spark_funcs.array_contains(
                spark_funcs.col('value_set_names'),
                'Nonacute Inpatient Stay',
            ),
            spark_funcs.lit(True),
        ).otherwise(
            spark_funcs.lit(False)
        ).alias('acute_inpatient'),
        spark_funcs.when(
            spark_funcs.col('died_during_stay')
            | spark_funcs.array_contains(
                spark_funcs.col('value_set_names'),
                'Pregnancy',
            )
            | spark_funcs.array_contains(
                spark_funcs.col('value_set_names'),
                'Perinatal Conditions',
            ),
            spark_funcs.lit(True),
        ).otherwise(
            spark_funcs.lit(False)
        ).alias('exclude_base'),
        spark_funcs.when(
            spark_funcs.array_contains(
                spark_funcs.col('value_set_names'),
                'Chemotherapy',
            )
            | spark_funcs.array_contains(
                spark_funcs.col('value_set_names'),
                'Rehabilitation ',
            )
            | spark_funcs.array_contains(
                spark_funcs.col('value_set_names'),
                'Kidney Transplant',
            )
            | spark_funcs.array_contains(
                spark_funcs.col('value_set_names'),
                'Bone Marrow Transplant',
            )
            | spark_funcs.array_contains(
                spark_funcs.col('value_set_names'),
                'Organ Transplant Other Than Kidney',
            )
            | (
                spark_funcs.array_contains(
                    spark_funcs.col('value_set_names'),
                    'Potentially Planned Procedures',
                )
                & ~spark_funcs.array_contains(
                    spark_funcs.col('value_set_names'),
                    'Acute Condition',
                )

            ),
            spark_funcs.lit(True),
        ).otherwise(
            spark_funcs.lit(False)
        ).alias('planned_flag'),
    )

    date_reagg = claims_calc_flags.where(
        spark_funcs.col('acute_inpatient')
    ).groupby(
        'member_id',
        'admitdate',
        'dischdate',
    ).agg(
        spark_funcs.max(spark_funcs.col('exclude_base')).alias('exclude_base'),
        spark_funcs.max(spark_funcs.col('planned_flag')).alias('planned_flag'),
        spark_funcs.max(spark_funcs.col('claimid')).alias('claimid'),
        spark_funcs.max(spark_funcs.col('died_during_stay')).alias('died_during_stay'),
    )

    sorted_stay_window = Window().partitionBy(
        'member_id',
    ).orderBy(
        'admitdate',
        'dischdate',
    )
    flag_transfers = date_reagg.select(
        '*',
        spark_funcs.lag('dischdate').over(sorted_stay_window).alias('last_dischdate'),
        spark_funcs.lag('claimid').over(sorted_stay_window).alias('last_claimid'),
    ).withColumn(
        'is_transfer',
        spark_funcs.when(
            spark_funcs.datediff(
                spark_funcs.col('admitdate'),
                spark_funcs.col('last_dischdate'),
            ) <= 1,
            spark_funcs.lit(True),
        ).otherwise(
            spark_funcs.lit(False)
        )
    ).withColumn(
        'transfer_claimid',
        spark_funcs.when(
            spark_funcs.col('is_transfer'),
            spark_funcs.col('last_claimid'),
        ).otherwise(
            spark_funcs.col('claimid')
        )
    ).withColumn(
        'planned_flag',
        spark_funcs.when(
            spark_funcs.col('is_transfer'),
            spark_funcs.lit(False),
        ).otherwise(
            spark_funcs.col('planned_flag')
        )
    )
    transfer_reagg = flag_transfers.groupby(
        'member_id',
        'transfer_claimid',
    ).agg(
        spark_funcs.max('exclude_base').alias('exclude_base'),
        spark_funcs.max('planned_flag').alias('planned_flag'),
        spark_funcs.max('is_transfer').alias('is_transfer'),
        spark_funcs.min('admitdate').alias('admitdate'),
        spark_funcs.max('dischdate').alias('dischdate'),
    )
    planned_flag = transfer_reagg.select(
        '*',
        spark_funcs.lead('admitdate').over(sorted_stay_window).alias('next_admitdate'),
        spark_funcs.lead('planned_flag').over(sorted_stay_window).alias('next_planned_flag'),
    ).withColumn(
        'exclude_planned',
        spark_funcs.when(
            (
                spark_funcs.datediff(
                    spark_funcs.col('next_admitdate'),
                    spark_funcs.col('dischdate')
                ) <= 30
            )
            & spark_funcs.col('next_planned_flag'),
            spark_funcs.lit(True),
        ).otherwise(
            spark_funcs.lit(False)
        )
    )
    return planned_flag


def _exclude_elig_gaps(
        denominator_events: DataFrame,
) -> DataFrame:
    """Find eligibility gaps and exclude members """
    decoupled_windows = decouple_common_windows(
        denominator_events,
        ['member_id', 'transfer_claimid', 'elig_period'],
        'date_start',
        'date_end',
        create_windows_for_gaps=True
    )
    cover_medical_windows = denominator_events.select(
        'member_id',
        'transfer_claimid',
        'elig_period',
        'date_start',
        'date_end',
        'cover_medical',
        )

    gaps_df = decoupled_windows.join(
        cover_medical_windows,
        ['member_id', 'transfer_claimid', 'elig_period', 'date_start', 'date_end'],
        how='left_outer'
    ).fillna({
        'cover_medical': 'N',
    }).select(
        'member_id',
        'transfer_claimid',
        'elig_period',
        'date_start',
        'date_end',
        'cover_medical',
        (spark_funcs.datediff(
            spark_funcs.col('date_end'),
            spark_funcs.col('date_start')
        ) + 1).alias('date_diff')
    )

    summ_gaps = gaps_df.groupby(
        'member_id',
        'transfer_claimid',
        'elig_period',
    ).agg(
        spark_funcs.sum(
            spark_funcs.when(
                spark_funcs.col('cover_medical') == 'N',
                spark_funcs.lit(1)
            ).otherwise(spark_funcs.lit(0))
        ).alias('count_gaps'),
        spark_funcs.sum(
            spark_funcs.when(
                spark_funcs.col('cover_medical') == 'N',
                spark_funcs.col('date_diff')
            ).otherwise(spark_funcs.lit(0))
        ).alias('count_gap_days'),
    )

    flag_gaps = summ_gaps.withColumn(
        'exclude_gap',
        spark_funcs.when(
            spark_funcs.col('elig_period') == 'prior',
            spark_funcs.when(
                (spark_funcs.col('count_gaps') > 1)
                | (spark_funcs.col('count_gap_days') > 45),
                spark_funcs.lit(True)
            ).otherwise(spark_funcs.lit(False))
        ).otherwise(
            spark_funcs.when(
                spark_funcs.col('count_gaps') > 0,
                spark_funcs.lit(True)
            ).otherwise(spark_funcs.lit(False))
        )
    )
    exclude_gap_summ = flag_gaps.groupby(
        'member_id',
        'transfer_claimid',
    ).agg(
        spark_funcs.max(spark_funcs.col('exclude_gap')).alias('exclude_gap')
    )

    return exclude_gap_summ

def _calc_elig_gap_exclusions(
        staging_calculation_steps: DataFrame,
        member_time: DataFrame,
    ) -> DataFrame:

    index_with_elig_periods = staging_calculation_steps.select(
        '*',
        spark_funcs.explode(
            spark_funcs.array(
                spark_funcs.struct(
                    spark_funcs.date_sub(
                        spark_funcs.col('dischdate'),
                        365,
                        ).alias('elig_date_start'),
                    spark_funcs.col('dischdate').alias('elig_date_end'),
                    spark_funcs.lit('prior').alias('elig_period'),
                ),
                spark_funcs.struct(
                    spark_funcs.col('dischdate').alias('elig_date_start'),
                    spark_funcs.date_add(
                        spark_funcs.col('dischdate'),
                        30,
                        ).alias('elig_date_end'),
                    spark_funcs.lit('after').alias('elig_period'),
                ),
            )
        ).alias('struct_elig')
    ).select(
        '*',
        spark_funcs.col('struct_elig')['elig_date_start'].alias('elig_date_start'),
        spark_funcs.col('struct_elig')['elig_date_end'].alias('elig_date_end'),
        spark_funcs.col('struct_elig')['elig_period'].alias('elig_period'),
    )

    index_stays_with_elig = index_with_elig_periods.join(
        member_time.select(
            'member_id',
            'cover_medical',
            'date_start',
            'date_end',
            ),
        on='member_id',
        how='left',
    ).where(
        (spark_funcs.col('date_end') >= spark_funcs.col('elig_date_start'))
        & (spark_funcs.col('date_start') <= spark_funcs.col('elig_date_end'))
    ).withColumn(
        'date_start',
        spark_funcs.greatest(
            spark_funcs.col('date_start'),
            spark_funcs.col('elig_date_start'),
            )
    ).withColumn(
        'date_end',
        spark_funcs.least(
            spark_funcs.col('date_end'),
            spark_funcs.col('elig_date_end'),
            )
    )
    index_stay_gap_exclusion = _exclude_elig_gaps(
        index_stays_with_elig,
    )
    return index_stay_gap_exclusion

def _flag_readmissions(
        staging_calculation_steps: DataFrame,
        index_stay_gap_exclusion: DataFrame,
        performance_yearstart: datetime.date,
        last_eligible_dischdate: datetime.date,
    ) -> DataFrame:
    """Calculate final readmissions dataframe"""

    acute_ip_index_stays = staging_calculation_steps.join(
        index_stay_gap_exclusion,
        on=['member_id', 'transfer_claimid'],
        how='left',
    ).where(
        (spark_funcs.col('admitdate') != spark_funcs.col('dischdate'))
        & ~spark_funcs.col('exclude_base')
        & ~spark_funcs.col('exclude_planned')
        & ~spark_funcs.col('exclude_gap')
        & (spark_funcs.col('dischdate') >= performance_yearstart)
        & (spark_funcs.col('dischdate') <= last_eligible_dischdate)
    )

    acute_ip_numerator_stays = staging_calculation_steps.where(
        ~spark_funcs.col('exclude_base')
    ).select(
        spark_funcs.col('member_id').alias('readmit_member_id'),
        spark_funcs.col('transfer_claimid').alias('readmit_claimid'),
        spark_funcs.col('admitdate').alias('readmit_admitdate'),
        spark_funcs.col('dischdate').alias('readmit_dischdate'),
    )

    readmissions_flagged = acute_ip_index_stays.join(
        acute_ip_numerator_stays,
        on=[
            (spark_funcs.datediff(
                acute_ip_numerator_stays.readmit_admitdate,
                acute_ip_index_stays.dischdate,
                ) >= spark_funcs.lit(0))
            & (spark_funcs.datediff(
                acute_ip_numerator_stays.readmit_admitdate,
                acute_ip_index_stays.dischdate,
                ) <= spark_funcs.lit(30)),
            acute_ip_numerator_stays.readmit_member_id == acute_ip_index_stays.member_id,
            acute_ip_numerator_stays.readmit_claimid != acute_ip_index_stays.transfer_claimid,
            ],
        how='left',
    ).withColumn(
        'has_readmit',
        spark_funcs.when(
            spark_funcs.col('readmit_claimid').isNotNull(),
            spark_funcs.lit(True),
            ).otherwise(
                spark_funcs.lit(False)
                ),
    ).drop(
        'readmit_member_id'
    )

    readmissions_dedup = readmissions_flagged.groupby(
        'member_id',
        'transfer_claimid',
        'admitdate',
        'dischdate',
    ).agg(
        spark_funcs.max('has_readmit').alias('has_readmit')
    )
    return readmissions_dedup

class PCR(QualityMeasure):
    """Object to house logic to calculate Plan All-cause Readmissions"""
    def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart: datetime.date,
            **kwargs
    ):
        last_eligible_dischdate = datetime.date(
            performance_yearstart.year,
            12,
            1,
            )
        claim_value_sets = _identify_claim_value_sets(
            dfs_input['claims'],
            dfs_input['reference'],
            performance_yearstart,
            )
        staging_calculation_steps = _flag_calculation_steps(claim_value_sets)
        staging_calculation_steps.cache()

        index_stay_gap_exclusion = _calc_elig_gap_exclusions(
            staging_calculation_steps,
            dfs_input['member_time'],
            )

        readmissions_dedup = _flag_readmissions(
            staging_calculation_steps,
            index_stay_gap_exclusion,
            performance_yearstart,
            last_eligible_dischdate,
            )

        results_df = readmissions_dedup.groupby(
            'member_id',
        ).agg(
            spark_funcs.count('*').alias('comp_quality_denominator'),
            spark_funcs.sum(
                spark_funcs.col('has_readmit').cast('int')
                ).alias('comp_quality_numerator'),
        ).select(
            'member_id',
            'comp_quality_denominator',
            'comp_quality_numerator',
            spark_funcs.lit('PCR').alias('comp_quality_short'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_date_last'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_date_actionable'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_comments')
        )
        results_df.cache()

        readmit_results = results_df.agg(
            spark_funcs.sum(
                spark_funcs.col('comp_quality_denominator')
                ).alias('comp_quality_denominator'),
            spark_funcs.sum(
                spark_funcs.col('comp_quality_numerator')
                ).alias('comp_quality_numerator'),
        ).collect()[0]
        LOGGER.info(
            'Identified %s index stays with %s readmissions with an overall readmission rate of %s',
            readmit_results['comp_quality_denominator'],
            readmit_results['comp_quality_numerator'],
            readmit_results['comp_quality_numerator'] / readmit_results['comp_quality_denominator'],
            )
        staging_calculation_steps.unpersist()
        return results_df
