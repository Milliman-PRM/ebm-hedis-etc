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

    def _filter_claims_by_date(
        med_claims: DataFrame, performance_yearstart: datetime.date
    ) -> DataFrame:
        """ filter claims to only include in elig year"""
        filtered_med_claims = med_claims.where(
            spark_funcs.col("fromdate") >= performance_yearstart
        )

        return filtered_med_claims

    def _identify_eligible_events(
        self,
        med_claims: DataFrame,
        df_reference: DataFrame,
        df_reference_awv: DataFrame,
    ) -> DataFrame:
        """ Should identify all eligible claims that are part of the desired value-set defn"""
        ...

    def _calc_numerator_flag(self) -> DataFrame:
        """ Should output a df with member_id and numerator_flag """
        ...

    def _calc_denominator_flag(self):
        """ Should output a df with a member_id and numerator_flag"""
        ...

    def _calc_eligible_membership(self):
        """ Should output eligible membership to filter upon """
        ...

    def _identify_excluded_members(self):
        """ Exclude appropriate members"""
        ...

    def _calc_measure(
        self,
        dfs_input: typing.Mapping[str, DataFrame],
        performance_yearstart: datetime.date,
        **kwargs
    ) -> DataFrame:

        ...
