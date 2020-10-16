"""
### CODE OWNERS: Alexander Olivero, Demerrick Moton

### OBJECTIVE:
  Calculate compliance rates for each person for drugs in 3 major therapeutic classes:
      cardiovascular, oral diabetic drugs and statins.   

### DEVELOPER NOTES:
  Based on code `Prod01_Calculate_Rx_Compliance_Rates.sas` in 140_Compliance_Rates
"""
import datetime

import pyspark.sql.functions as spark_funcs
from ebm_hedis_etc.base_classes import QualityMeasure
from prm.dates.windows import decouple_common_windows
from pyspark.sql import DataFrame
from pyspark.sql import Window
# pylint does not recognize many of the spark functions
# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _identify_elig_scripts(
    rx_claims: DataFrame,
) -> DataFrame:  # pragma: no cover
    """Identify Eligible Scripts, members with at least 2 scripts, non-insulin"""
    members_two_scripts = rx_claims.where(
        spark_funcs.col('prm_quality_category').isin(['Statin', 'Diabetes', 'Cardiovascular'])
    ).groupBy(
        'member_id',
        'prm_quality_category'
    ).agg(
        spark_funcs.count('*').alias('scripts')
    ).where(
        spark_funcs.col('scripts') > 1
    )

    members_insulin = [row.member_id for row in rx_claims.where(
        spark_funcs.col('prm_quality_category') == 'Insulin'
    ).select(
        'member_id',
    ).distinct().collect()]

    elig_scripts = rx_claims.join(
        members_two_scripts,
        on=['member_id', 'prm_quality_category'],
        how='inner'
    ).where(
        (spark_funcs.col('prm_quality_category').isin(['Statin', 'Cardiovascular'])) |
        (~spark_funcs.col('member_id').isin(members_insulin))
    )

    return elig_scripts

def _calc_denominator(
    rx_claims: DataFrame,
    date_latestpaid: datetime.date
) -> DataFrame:  # pragma: no cover
    """Calculate days since first script by member and drug class"""
    
    member_denominator = rx_claims.groupBy(
        'member_id',
        'prm_quality_category'
    ).agg(
        spark_funcs.min('prm_fromdate').alias('first_script')
    ).withColumn(
        'denominator',
        spark_funcs.datediff(
            spark_funcs.lit(date_latestpaid),
            spark_funcs.col('first_script')
        )
    )

    return member_denominator

def _calc_numerator(
    rx_claims: DataFrame,
) -> DataFrame:  # pragma: no cover
    """Calculate days covered by member and drug class"""

    rx_day_transpose = eligible_scripts.withColumn(
        'repeat',
        spark_funcs.expr(
            "split(repeat(',', dayssupply), ',')"
        )
    ).select(
        'member_id',
        'prm_quality_category',
        'prm_generic_drug_name',
        'prm_fromdate',
        spark_funcs.posexplode('repeat').alias('day', 'val')
    ).withColumn(
        'script_day',
        spark_funcs.expr(
            "date_add(prm_fromdate, day)"
        )
    )

    days_supply_by_generic = rx_day_transpose.groupBy(
        'member_id',
        'prm_quality_category',
        'prm_generic_drug_name',
        'script_day',
    ).agg(
        spark_funcs.count('*').alias('scripts_on_day')
    )

    day_window = Window().partitionBy(
        'member_id',
        'prm_quality_category',
        'script_day',
    ).orderBy(
        spark_funcs.col('scripts_on_day').desc()
    )

    remove_dupes = days_supply_by_generic.withColumn(
        'row_number',
        spark_funcs.row_number().over(day_window)
    ).where(
        spark_funcs.col('row_number') == 1
    )

    member_numerator = remove_dupes.groupBy(
        'member_id',
        'prm_quality_category',
    ).agg(
        spark_funcs.count('*').alias('days_covered')
    )

    return member_numerator

class MAD(QualityMeasure):  # pragma: no cover
    """Object to house logic to calculate annual monitoring of diuretics measure"""

    def _calc_measure(
        self,
        dfs_input: "typing.Mapping[str, DataFrame]",
        date_latestpaid=datetime.date,
    ):
        rx_claims_trim = dfs_input['rx_claims'].where(
            (spark_funcs.col('prm_fromdate').between(
                spark_funcs.add_months(
                    spark_funcs.lit(date_latestpaid),
                    -12
                ),
                spark_funcs.lit(date_latestpaid)
            )) &
            ((spark_funcs.col('prm_forced_util') != 0) |
            (spark_funcs.col('prm_util') != 0))
        )

        eligible_scripts = _identify_elig_scripts(
            rx_claims_trim
        )

        member_denominator = _calc_denominator(
            eligible_scripts,
            date_latestpaid
        )

        member_numerator = _calc_numerator(
            eligible_scripts
        )
        diuretic_claims_df = (
            dfs_input["rx_claims"]
            .where(
                spark_funcs.col("fromdate").between(
                    spark_funcs.lit(performance_yearstart),
                    spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31)),
                )
            )
            .join(
                dfs_input["ndc"].where(
                    spark_funcs.col("medication_list") == "Diuretic Medications"
                ),
                spark_funcs.col("ndc") == spark_funcs.col("ndc_code"),
                how="inner",
            )
            .where(spark_funcs.col("dayssupply") > 0)
            .withColumn(
                "fromdate_to_dayssupply",
                spark_funcs.least(
                    spark_funcs.expr("date_add(fromdate, dayssupply)"),
                    spark_funcs.lit(datetime.date(performance_yearstart.year, 12, 31)),
                ),
            )
        )

        covered_treatment_days_df = (
            diuretic_claims_df.groupBy("member_id")
            .agg(
                spark_funcs.sum(
                    spark_funcs.datediff(
                        spark_funcs.col("fromdate_to_dayssupply"),
                        spark_funcs.col("fromdate"),
                    )
                ).alias("treatment_days")
            )
            .where(spark_funcs.col("treatment_days") >= 180)
            .select("member_id")
        )

        eligible_membership_df = (
            covered_treatment_days_df.join(
                dfs_input["member_time"], "member_id", how="inner"
            )
            .where(
                (spark_funcs.col("date_start") >= performance_yearstart)
                & (
                    spark_funcs.col("date_end")
                    <= spark_funcs.lit(
                        datetime.date(performance_yearstart.year, 12, 31)
                    )
                )
                & (spark_funcs.col("cover_medical") == "Y")
                & (spark_funcs.col("cover_rx") == "Y")
            )
            .join(dfs_input["member"], "member_id", how="left_outer")
            .where(
                spark_funcs.lit(
                    spark_funcs.datediff(
                        spark_funcs.lit(
                            datetime.date(performance_yearstart.year, 12, 31)
                        ),
                        spark_funcs.col("dob"),
                    )
                    / 365
                )
                >= 18
            )
        )

        eligible_members_no_gaps_df = (
            eligible_membership_df.join(
                _exclude_elig_gaps(eligible_membership_df, 1, 45),
                spark_funcs.col("member_id") == spark_funcs.col("exclude_member_id"),
                how="left_outer",
            )
            .where(spark_funcs.col("exclude_member_id").isNull())
            .select("member_id")
            .distinct()
        )

        eligible_pop_claims_df = (
            dfs_input["claims"]
            .join(eligible_members_no_gaps_df, "member_id", how="inner")
            .where(
                (spark_funcs.col("fromdate") >= performance_yearstart)
                & (
                    spark_funcs.col("fromdate")
                    <= spark_funcs.lit(
                        datetime.date(performance_yearstart.year, 12, 31)
                    )
                )
            )
        )

        lab_panel_df = (
            eligible_pop_claims_df.join(
                dfs_input["reference"].where(
                    spark_funcs.col("value_set_name").isin("Lab Panel")
                    & spark_funcs.col("code_system").isin("CPT")
                ),
                spark_funcs.col("hcpcs") == spark_funcs.col("code"),
                how="inner",
            )
            .select("member_id")
            .distinct()
        )

        serum_monitoring_df = (
            eligible_pop_claims_df.join(
                dfs_input["reference"].where(
                    spark_funcs.col("value_set_name").isin("Serum Potassium")
                    & spark_funcs.col("code_system").isin("CPT")
                ),
                spark_funcs.col("hcpcs") == spark_funcs.col("code"),
                how="inner",
            )
            .select("member_id")
            .distinct()
            .intersect(
                eligible_pop_claims_df.join(
                    dfs_input["reference"].where(
                        spark_funcs.col("value_set_name").isin("Serum Creatinine")
                        & spark_funcs.col("code_system").isin("CPT")
                    ),
                    spark_funcs.col("hcpcs") == spark_funcs.col("code"),
                    how="inner",
                )
                .select("member_id")
                .distinct()
            )
            .distinct()
        )

        numer_df = lab_panel_df.union(serum_monitoring_df).distinct()

        results_df = (
            dfs_input["member"]
            .select("member_id")
            .distinct()
            .join(
                eligible_members_no_gaps_df,
                dfs_input["member"].member_id == eligible_members_no_gaps_df.member_id,
                how="left_outer",
            )
            .join(
                numer_df,
                dfs_input["member"].member_id == numer_df.member_id,
                how="left_outer",
            )
            .select(
                dfs_input["member"].member_id,
                spark_funcs.lit("MPM3").alias("comp_quality_short"),
                spark_funcs.when(numer_df.member_id.isNotNull(), spark_funcs.lit(1))
                .otherwise(spark_funcs.lit(0))
                .alias("comp_quality_numerator"),
                spark_funcs.when(
                    eligible_members_no_gaps_df.member_id.isNotNull(),
                    spark_funcs.lit(1),
                )
                .otherwise(spark_funcs.lit(0))
                .alias("comp_quality_denominator"),
                spark_funcs.lit(None).cast("string").alias("comp_quality_date_last"),
                spark_funcs.lit(None)
                .cast("string")
                .alias("comp_quality_date_actionable"),
                spark_funcs.lit(None).cast("string").alias("comp_quality_comments"),
            )
        )

        return results_df
