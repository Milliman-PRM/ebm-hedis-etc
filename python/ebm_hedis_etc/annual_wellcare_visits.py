"""
### CODE OWNERS: Chas Busenburg

### OBJECTIVE:
    Calculate the percentage of patients who have
    had their Annual Wellness visit in the current plan year.

### DEVELOPER NOTES:
  <none>
"""
import datetime
import os
import typing
from pathlib import Path

import pyspark.sql.functions as spark_funcs
from ebm_hedis_etc.base_classes import QualityMeasure
from ebm_hedis_etc.reference import import_single_file
from prm.dates.windows import decouple_common_windows
from pyspark.sql import DataFrame

PATH_REF = Path(os.environ["EBM_HEDIS_ETC_PATHREF"])

DICT_REFERENCES = {
    "reference": "hedis_codes",
    "reference_awv": "reference_awv",
    "ndc": "hedis_ndc_codes",
}


class AWV(QualityMeasure):  # pragma: no cover
    """ Object to house the logic to calculate AAB measure """

    def __init__(self, sparkapp):
        super().__init__()
        self.sparkapp = sparkapp

    def _get_core_reference_file(self, file_name: str):
        """Gets the core reference files from a compiled state or from the directory structure"""

        try:
            df_loaded = self.sparkapp.load_df(PATH_REF / "{}.parquet".format(file_name))
        except AssertionError:
            df_loaded = import_single_file(self.sparkapp, file_name)

        return df_loaded

    def _create_extra_reference_files(self, _core_references):
        """Combines core reference files and AWV specific reference files"""
        refs_well_care_core = (
            _core_references["reference"]
            .filter(spark_funcs.col("value_set_name") == "Well-Care")
            .select(
                "value_set_name", "code", "definition", "code_system", "code_system_oid"
            )
        )
        refs_well_care_whole = refs_well_care_core.union(
            _core_references["reference_awv"]
        )
        dict_extra_refs = {
            "refs_well_care_core": refs_well_care_core,
            "refs_well_care_whole": refs_well_care_whole,
        }

        return dict_extra_refs

    def get_reference_files(self):
        """get and cache appropriate reference files for AWV"""
        try:
            return self._references
        except AttributeError:
            _core_references = {
                name: self._get_core_reference_file(file_name)
                for name, file_name in DICT_REFERENCES.items()
            }

            _extra_reference_files = self._create_extra_reference_files(
                _core_references
            )

            self._references = {**_core_references, **_extra_reference_files}

            return self._references

    def _split_refs_wellcare(self, refs_well_care) -> typing.Mapping[str, DataFrame]:
        dict_split_wellcare = dict()
        dict_split_wellcare["hcpcs"] = refs_well_care.where(
            spark_funcs.col("code_system").isin("CPT", "HCPCS")
        ).select(
            spark_funcs.col("value_set_name"), spark_funcs.col("code").alias("hcpcs")
        )

        dict_split_wellcare["icd"] = refs_well_care.where(
            spark_funcs.col("code_system").isin("ICD9CM", "ICD10CM")
        ).select(
            spark_funcs.col("value_set_name"),
            spark_funcs.regexp_replace(spark_funcs.col("code"), r"\.", "").alias(
                "icddiag"
            ),
            spark_funcs.when(
                spark_funcs.col("code_system") == spark_funcs.lit("ICD9CM"),
                spark_funcs.lit("09"),
            )
            .otherwise(spark_funcs.lit("10"))
            .alias("icdversion"),
        )

        return dict_split_wellcare

    def _filter_df_by_date(
        self, df_in: DataFrame, datetime_start: datetime.date, str_date_col="fromdate"
    ) -> DataFrame:
        """ filter claims to only include in elig year"""
        filtered_med_claims = df_in.where(
            spark_funcs.col(str_date_col) >= datetime_start
        )

        return filtered_med_claims

    def _identify_eligible_events(
        self, med_claims: DataFrame, dict_refs_wellcare: typing.Mapping[str, DataFrame]
    ) -> DataFrame:
        """ Should identify all eligible claims that are part of the desired value-set defn"""
        exploded_claims = (
            med_claims.select(
                "member_id",
                "claimid",
                "fromdate",
                "revcode",
                "hcpcs",
                "icdversion",
                spark_funcs.array(
                    [
                        spark_funcs.col(col)
                        for col in med_claims.columns
                        if col.find("icddiag") > -1
                    ]
                ).alias("diag_explode"),
            )
            .distinct()
            .withColumn("icddiag", spark_funcs.explode(spark_funcs.col("diag_explode")))
            .drop("diag_explode")
            .distinct()
        )

        df_eligible_claims_hcpcs = exploded_claims.join(
            spark_funcs.broadcast(dict_refs_wellcare["hcpcs"]), on="hcpcs", how="inner"
        )

        df_eligible_claims_procs = exploded_claims.join(
            spark_funcs.broadcast(dict_refs_wellcare["icd"]),
            on=["icddiag", "icdversion"],
            how="inner",
        )

        eligible_claims = df_eligible_claims_hcpcs.union(df_eligible_claims_procs)

        return eligible_claims

    def _exclude_elig_gaps(
        self,
        eligible_member_time: DataFrame,
        allowable_gaps: int = 0,
        allowable_gap_length: int = 0,
    ) -> DataFrame:  # pragma: no cover
        """Find eligibility gaps and exclude members """
        decoupled_windows = decouple_common_windows(
            eligible_member_time,
            "member_id",
            "date_start",
            "date_end",
            create_windows_for_gaps=True,
        )

        gaps_df = (
            decoupled_windows.join(
                eligible_member_time,
                ["member_id", "date_start", "date_end"],
                how="left_outer",
            )
            .where(spark_funcs.col("cover_medical").isNull())
            .select(
                "member_id",
                "date_start",
                "date_end",
                spark_funcs.datediff(
                    spark_funcs.col("date_end"), spark_funcs.col("date_start")
                ).alias("date_diff"),
            )
        )

        long_gaps_df = gaps_df.where(
            spark_funcs.col("date_diff") > allowable_gap_length
        ).select("member_id")

        gap_count_df = (
            gaps_df.groupBy("member_id")
            .agg(spark_funcs.count("*").alias("num_of_gaps"))
            .where(spark_funcs.col("num_of_gaps") > allowable_gaps)
            .select("member_id")
        )

        return (
            long_gaps_df.union(gap_count_df)
            .select(spark_funcs.col("member_id").alias("exclude_member_id"))
            .distinct()
        )

    def _identify_excluded_members(self, med_claims, df_excluded_members):
        """ Exclude appropriate members"""
        filtered_med_claims = (
            med_claims.join(
                df_excluded_members.withColumn("flag", spark_funcs.lit(1)),
                med_claims.member_id == df_excluded_members.exclude_member_id,
                how="left_outer",
            )
            .where(spark_funcs.col("flag").isNull())
            .select(*med_claims.columns)
        )
        return filtered_med_claims

    def _calc_measure(
        self,
        dfs_input: typing.Mapping[str, DataFrame],
        datetime_start: datetime.date,
        *,
        datetime_end: datetime.date,
        filter_reference: typing.Optional[str] = None,
        allowable_gaps: int = 1,
        allowable_gap_length: int = 45,
    ) -> DataFrame:

        if filter_reference is None:
            filter_reference = "refs_well_care_whole"

        df_member_time_start = self._filter_df_by_date(
            dfs_input["member_time"], datetime_start, "date_start"
        )

        df_member_time_end = self._filter_df_by_date(
            dfs_input["member_time"], datetime_end, "date_end"
        )

        df_member_time_py = df_member_time_start.union(df_member_time_end).distinct()

        df_member_eligible = df_member_time_py.where(
            df_member_time_py.cover_medical == "Y"
        ).distinct()

        df_excluded_members = self._exclude_elig_gaps(
            df_member_eligible,
            allowable_gaps=allowable_gaps,
            allowable_gap_length=allowable_gap_length,
        )
        df_member_months_w_excl = self._identify_excluded_members(
            df_member_eligible, df_excluded_members
        )

        df_member_denom_final = (
            df_member_months_w_excl.select("member_id")
            .distinct()
            .join(dfs_input["member"], on="member_id", how="inner")
        )

        df_med_claims_py = self._filter_df_by_date(
            dfs_input["med_claims"], datetime_start
        )

        df_member_claims_py = df_member_denom_final.join(
            df_med_claims_py, on="member_id", how="inner"
        )
        df_eligible_claims = self._identify_eligible_events(
            df_member_claims_py,
            self._split_refs_wellcare(self.get_reference_files()[filter_reference]),
        )

        df_numerator = (
            df_eligible_claims.groupBy("member_id")
            .agg({"member_id": "count", "fromdate": "max"})
            .withColumn(
                "comp_quality_numerator", spark_funcs.col("count(member_id)") >= 1
            )
            .withColumnRenamed("max(fromdate)", "comp_quality_date_last")
        )

        df_final = (
            df_member_denom_final.join(df_numerator, on="member_id", how="left")
            .withColumn(
                "comp_quality_numerator",
                spark_funcs.col("comp_quality_numerator").cast("integer"),
            )
            .withColumn("comp_quality_denominator", spark_funcs.lit(1))
            .withColumn(
                "comp_quality_date_actionable",
                spark_funcs.when(
                    spark_funcs.col("comp_quality_numerator").isNull(), datetime_end
                ).otherwise(None),
            )
            .withColumn(
                "comp_quality_comments",
                spark_funcs.when(
                    spark_funcs.col("comp_quality_date_last").isNotNull(),
                    spark_funcs.concat(
                        spark_funcs.lit("Most Recent Service: "),
                        spark_funcs.col("comp_quality_date_last").cast("string"),
                    ),
                ).otherwise(
                    spark_funcs.concat(
                        spark_funcs.lit("No related services observed since: "),
                        spark_funcs.lit(datetime_start),
                    )
                ),
            )
            .fillna({"comp_quality_numerator": 0})
            .select(
                "member_id",
                "comp_quality_numerator",
                "comp_quality_denominator",
                "comp_quality_date_last",
                "comp_quality_date_actionable",
                "comp_quality_comments",
            )
        )

        return df_final
