"""
### CODE OWNERS: Demerrick Moton, Matthew Hawthorne

### OBJECTIVE:
    Test the persistent asthma adherence quality measure

### DEVELOPER NOTES:
    <none>
"""
# pragma: no cover
# pylint: disable=redefined-outer-name
import datetime
from pathlib import Path

import ebm_hedis_etc.mma
import pyspark.sql.functions as F
import pytest
from prm.spark.io_txt import build_structtype_from_csv

try:
    _PATH_FILE = Path(__file__).parent
except NameError:  # Likely interactive development
    _PATH_FILE = Path(ebm_hedis_etc.mma.__file__).parents[1] / "tests"

PATH_MOCK_SCHEMAS = _PATH_FILE / "mock_schemas"
PATH_SPECIFIC_SCHEMAS = PATH_MOCK_SCHEMAS / "specific_schemas"
PATH_MOCK_DATA = _PATH_FILE / "mock_data"

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


@pytest.fixture
def mock_dataframes(spark_app):
    """Testing data"""
    dataframes = dict()
    dataframes["reference"] = spark_app.session.read.csv(
        str(PATH_MOCK_DATA / "reference.csv"),
        schema=build_structtype_from_csv(PATH_MOCK_SCHEMAS / "reference.csv"),
        sep=",",
        header=True,
        mode="FAILFAST",
    )

    mma_df = spark_app.session.read.csv(
        str(PATH_MOCK_DATA / "mma.csv"),
        schema=build_structtype_from_csv(PATH_MOCK_SCHEMAS / "mma.csv"),
        sep=",",
        header=True,
        mode="FAILFAST",
    )

    for path in PATH_SPECIFIC_SCHEMAS.glob("*.csv"):
        specific_schema = build_structtype_from_csv(path)
        dataframes[path.stem] = mma_df.select(
            *[col.name for col in specific_schema.fields]
        )

    return dataframes


def compare_actual_expected(result_actual, result_expected):
    """Helper function to compare results"""
    assert result_actual.count() == result_expected.distinct().count()
    compare = result_expected.join(result_actual, "member_id", "left_outer")
    compare_columns = {
        expected_column: expected_column[expected_column.find("_") + 1 :]
        for expected_column in result_expected.columns
        if expected_column.startswith("expected_")
    }
    failures = list()
    for expected_column, actual_column in compare_columns.items():
        misses = compare.filter(compare[expected_column] != compare[actual_column])
        if misses.count() != 0:
            failures.append(actual_column)
    assert not failures, "Column(s) '{}' contains unexpected values".format(failures)


def test_mma(mock_dataframes):
    """Test the mma measure"""
    test_instance = ebm_hedis_etc.mma.MMA()
    result_actual = test_instance.calc_measure(
        mock_dataframes, datetime.date(2018, 1, 1)
    )
    result_actual.cache()
    result_expected = (
        mock_dataframes["expected"]
        .where(F.col("expected_comp_quality_denominator") > 0)
        .cache()
    )
    compare_actual_expected(result_actual, result_expected)
