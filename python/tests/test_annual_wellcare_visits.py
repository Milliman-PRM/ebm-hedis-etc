"""
### CODE OWNERS: Ben Copeland

### OBJECTIVE:
    Test the methods that calculate the Annual Wellcare Visits measure

### DEVELOPER NOTES:

"""
import datetime
from pathlib import Path

import ebm_hedis_etc.annual_wellcare_visits
import pytest
from prm.spark.io_txt import build_structtype_from_csv

try:
    _PATH_FILE = Path(__file__).parent
except NameError:  # Likely interactive development
    import os

    _PATH_FILE = Path(os.environ["ebm_hedis_etc_home"]) / "python" / "tests"

PATH_MOCK_SCHEMAS = _PATH_FILE / "mock_schemas" / "annual_wellcare_visits"
PATH_MOCK_DATA = _PATH_FILE / "mock_data" / "annual_wellcare_visits"

DATE_PY_START = datetime.date(2019, 1, 1)
DATE_PY_END = datetime.date(2019, 12, 31)
DATE_ROLLING_12_START = datetime.date(2018, 4, 1)
DATE_ROLLING_12_END = datetime.date(2019, 3, 31)

# pylint: disable=no-self-use, redefined-outer-name

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


@pytest.fixture
def mock_dataframes(spark_app):
    """Testing data"""
    dataframes = dict()
    for path_mock_datafile in PATH_MOCK_DATA.iterdir():
        dataframes[path_mock_datafile.stem] = spark_app.session.read.csv(
            str(path_mock_datafile),
            schema=build_structtype_from_csv(
                PATH_MOCK_SCHEMAS / path_mock_datafile.name
            ),
            sep=",",
            header=True,
            mode="FAILFAST",
        )

    return dataframes


def compare_actual_expected(result_actual, result_expected):
    """Helper function to compare results"""
    unexpected = result_actual.subtract(result_expected).collect()
    assert not unexpected, "Row(s) '{}' found in actual but not expected".format(
        unexpected
    )

    missing = result_expected.subtract(result_actual).collect()
    assert not missing, "Row(s) '{}' found in expected but not actual".format(missing)


class TestAWV:
    """Annual wellness visit test calculations"""

    def test_core_py(self, mock_dataframes, spark_app):
        """Test the performance year results for core reference set"""
        calc_obj = ebm_hedis_etc.annual_wellcare_visits.AWV(spark_app)
        results = calc_obj.calc_measure(
            mock_dataframes,
            performance_yearstart=DATE_PY_START,
            datetime_end=DATE_PY_END,
            filter_reference="refs_well_care_core",
        )
        compare_actual_expected(results, mock_dataframes["expected_core_py"])

    def test_awv_py(self, mock_dataframes, spark_app):
        """Test the performance year results for core reference set"""
        calc_obj = ebm_hedis_etc.annual_wellcare_visits.AWV(spark_app)
        results = calc_obj.calc_measure(
            mock_dataframes,
            performance_yearstart=DATE_PY_START,
            datetime_end=DATE_PY_END,
            filter_reference="reference_awv",
        )
        compare_actual_expected(results, mock_dataframes["expected_awv_py"])

    def test_combined_py(self, mock_dataframes, spark_app):
        """Test the performance year results for core reference set"""
        calc_obj = ebm_hedis_etc.annual_wellcare_visits.AWV(spark_app)
        results = calc_obj.calc_measure(
            mock_dataframes,
            performance_yearstart=DATE_PY_START,
            datetime_end=DATE_PY_END,
        )
        compare_actual_expected(results, mock_dataframes["expected_combined_py"])

    def test_core_rolling_12(self, mock_dataframes, spark_app):
        """Test the performance year results for core reference set"""
        calc_obj = ebm_hedis_etc.annual_wellcare_visits.AWV(spark_app)
        results = calc_obj.calc_measure(
            mock_dataframes,
            performance_yearstart=DATE_ROLLING_12_START,
            datetime_end=DATE_ROLLING_12_END,
            filter_reference="refs_well_care_core",
        )
        compare_actual_expected(results, mock_dataframes["expected_core_rolling_12"])


if __name__ == "__main__":
    pass
