"""
### CODE OWNERS:

### OBJECTIVE:
    Calculate the percentage of patients who have had their Annual Wellness visit in the current plan year.

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

    def _get_reference_file(self, file_name: str):
        try:
            df = self.sparkapp.load_df(PATH_REF / "{}.parquet".format(file_name))
        except AssertionError:
            df = import_single_file(file_name)

        return df

    def get_reference_files(self):
        try:
            return self._references

        except AttributeError:
            self._references = {
                name: self._get_reference_file(file_name)
                for name, file_name in DICT_REFERENCES.items()
            }

            return self._references

    def _filter_claims_by_date(
        med_claims: DataFrame, performance_yearstart: datetime.date
    ) -> DataFrame:
        """ filter claims to only include in elig year"""
        ...

    def _identify_eligible_events(
        med_claims: DataFrame, df_reference: DataFrame, df_reference_awv: DataFrame
    ) -> DataFrame:
        """ Should identify all eligible claims that are part of the desired value-set defn"""
        ...

    def _calc_numerator_flag() -> DataFrame:
        """ Should output a df with member_id and numerator_flag """
        ...

    def _calc_denominator_flag():
        """ Should output a df with a member_id and numerator_flag"""
        ...

    def _calc_eligible_membership():
        """ Should output eligible membership to filter upon """
        ...

    def _identify_excluded_members():
        """ Exclude appropriate members"""
        ...

    def _calc_measure(
        self,
        dfs_input: typing.Mapping[str, DataFrame],
        performance_yearstart: datetime.date,
        **kwargs
    ) -> DataFrame:

        ...
