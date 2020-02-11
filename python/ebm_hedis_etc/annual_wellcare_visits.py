"""
### CODE OWNERS:

### OBJECTIVE:
    Calculate the percentage of patients who have had their Annual Wellness visit in the current plan year.

### DEVELOPER NOTES:
  <none>
"""
import datetime
import typing

import pyspark.sql.functions as spark_funcs
from ebm_hedis_etc.base_classes import QualityMeasure
from pyspark.sql import DataFrame
from pyspark.sql import Row


class AWV(QualityMeasure):  # pragma: no cover
    """ Object to house the logic to calculate AAB measure """

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
