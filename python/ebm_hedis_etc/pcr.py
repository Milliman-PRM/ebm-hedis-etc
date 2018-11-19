"""
### CODE OWNERS: Ben Copeland

### OBJECTIVE:
    Calculate the Plan All-Cause Readmissions HEDIS measure.

### DEVELOPER NOTES:
  <none>
"""
import logging
import datetime
import typing
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
        spark_funcs.col('prm_todate') >= performance_yearstart
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
        ).alias('exclude_planned'),
    )

    transfer_window = Window().partitionBy(
        'member_id',
    ).orderBy(
        'admitdate',
        'dischdate',
    )
    flag_transfers = claims_calc_flags.where(
        spark_funcs.col('acute_inpatient')
    ).select(
        '*',
        spark_funcs.lag('dischdate').over(transfer_window).alias('last_dischdate'),
        spark_funcs.lag('claimid').over(transfer_window).alias('last_claimid'),
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
        'exclude_planned',
        spark_funcs.when(
            spark_funcs.col('is_transfer'),
            spark_funcs.lit(False),
        ).otherwise(
            spark_funcs.col('exclude_planned')
        )
    )
    transfer_reagg = flag_transfers.groupby(
        'member_id',
        'transfer_claimid',
    ).agg(
        spark_funcs.max('exclude_base').alias('exclude_base'),
        spark_funcs.max('exclude_planned').alias('exclude_planned'),
        spark_funcs.max('is_transfer').alias('is_transfer'),
        spark_funcs.min('admitdate').alias('admitdate'),
        spark_funcs.max('dischdate').alias('dischdate'),
    )
    return transfer_reagg

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


class PCR(QualityMeasure):
    """Object to house logic to calculate Plan All-cause Readmissions"""
    def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart: datetime.date,
            **kwargs
    ):
        performance_yearend = datetime.date(
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
        return
