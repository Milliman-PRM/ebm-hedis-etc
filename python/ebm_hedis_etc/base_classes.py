"""
### CODE OWNERS: Alexander Olivero, Chas Busenburg

### OBJECTIVE:
  Define how a quality measure should look

### DEVELOPER NOTES:
  <none>
"""
import logging
import datetime
import typing
import abc

from pyspark.sql import DataFrame

LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class QualityMeasure(metaclass=abc.ABCMeta):
    """Base class to validate schema of various quality measure results"""
    @staticmethod
    def validate_meausure_column_name(name: str) -> bool:
        """Defines naming convention for columns"""
        return name.startswith('comp_quality')

    def __init__(
            self,
    ) -> None:
        self.key_fields = ['member_id']

    @abc.abstractmethod
    def _calc_measure(
            self,
            dfs_input: typing.Mapping[str, DataFrame],
            performance_yearstart=datetime.date,
            **kwargs
    ) -> DataFrame:
        """Should capture business logic to make a measure"""

    def _validate_schema(
            self,
            df_: DataFrame,
    ) -> None:
        """Ensure the results are in the correct schema"""
        LOGGER.info("Validating %s measure result %s", self.__class__.__name__, df_)
        assert isinstance(df_, DataFrame), "Results should be presented as dataframe"
        for field in self.key_fields:
            assert field in df_.columns, "Result does not contain key field {}".format(field)
        for name in df_.columns:
            if name in self.key_fields:
                continue
            assert self.validate_meausure_column_name(name),\
                "Measure column {} failed to meet naming conventions".format(name)

    def calc_measure(
            self,
            dfs_input: typing.Mapping[str, DataFrame],
            performance_yearstart: datetime.date,
            **kwargs
    ) -> DataFrame:
        """Calculate a measure"""
        LOGGER.info("Beginning %s measure calculation", self.__class__.__name__)
        measure_results = self._calc_measure(
            dfs_input,
            performance_yearstart,
            **kwargs,
            )
        self._validate_schema(measure_results)
        return measure_results


if __name__ == '__main__':  # pragma: no cover
    pass
