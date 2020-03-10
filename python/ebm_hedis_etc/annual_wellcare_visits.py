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
        self.sparkapp = sparkapp

    def _get_core_reference_file(self, file_name: str):
        """Gets the core reference files from a compiled state or from the directory structure"""

        try:
            df = self.sparkapp.load_df(PATH_REF / "{}.parquet".format(file_name))
        except AssertionError:
            df = import_single_file(self.sparkapp, file_name)

        return df

    def _create_extra_reference_files(self, _core_references):
        """Combines core reference files and AWV specific reference files"""
        refs_well_care = (
            _core_references["reference"]
            .filter(spark_funcs.col("value_set_name") == "Well-Care")
            .select(
                "value_set_name", "code", "definition", "code_system", "code_system_oid"
            )
            .union(_core_references["reference_awv"])
        )

        dict_extra_refs = {"refs_well_care": refs_well_care}

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

    @property
    def refs_well_care(self):
        return self.get_reference_files()["refs_well_care"]

    def _filter_df_by_date(
        self,
        df: DataFrame,
        performance_yearstart: datetime.date,
        str_date_col="fromdate",
    ) -> DataFrame:
        """ filter claims to only include in elig year"""
        filtered_med_claims = df.where(
            spark_funcs.col(str_date_col) >= performance_yearstart
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

    def _calc_numerator_flag(self) -> DataFrame:
        """ Should output a df with member_id and numerator_flag """
        ...

    def _calc_denominator_flag(self):
        """ Should output a df with a member_id and numerator_flag"""
        ...

    def _calc_eligible_membership(self):
        """ Should output eligible membership to filter upon """
        ...

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
        performance_yearstart: datetime.date,
        **kwargs
    ) -> DataFrame:

        df_member_time_start = self._filter_df_by_date(
            dfs_input["member_time"], performance_yearstart, "date_start"
        )

        df_member_time_end = self._filter_df_by_date(
            dfs_input["member_time"], performance_yearstart, "date_end"
        )

        df_member_time_py = df_member_time_start.union(df_member_time_end).distinct()

        df_excluded_members = self._exclude_elig_gaps(
            df_member_time_py, allowable_gaps=1, allowable_gap_length=45
        )

        df_med_claims_py = self._filter_df_by_date(
            dfs_input["med_claims"], performance_yearstart
        )
        df_member_claims_py = self._identify_excluded_members(
            df_med_claims_py, df_excluded_members
        )
        df_eligible_claims = self._identify_eligible_events(
            df_member_claims_py, self._split_refs_wellcare(self.refs_well_care)
        )
