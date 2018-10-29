"""
### CODE OWNERS: Alexander Olivero

### OBJECTIVE:
    Test the persistence of beta-blockers after heart attack quality measure

### DEVELOPER NOTES:
    <none>
"""
# pylint: disable=redefined-outer-name
import datetime
from pathlib import Path
import pytest


import ebm_hedis_etc.pbh
from prm.spark.io_txt import build_structtype_from_csv

try:
    _PATH_FILE = Path(__file__).parent
except NameError:  # Likely interactive development
    _PATH_FILE = Path(ebm_hedis_etc.pbh.__file__).parents[1] / 'tests'  # pylint: disable=redefined-variable-type

PATH_MOCK_SCHEMAS = _PATH_FILE / "mock_schemas"
PATH_MOCK_DATA = _PATH_FILE / "mock_data"

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _get_name_from_path(path_):
    """Get the data set name from the path"""
    return path_.stem[path_.stem.find('_') + 1:]


@pytest.fixture
def mock_dataframes(spark_app):
    """Testing data"""
    dataframes = dict()
    dataframes['reference'] = spark_app.session.read.csv(
        str(PATH_MOCK_DATA / 'reference.csv'),
        schema=build_structtype_from_csv(PATH_MOCK_SCHEMAS / 'reference.csv'),
        sep=',',
        header=True,
        mode='FAILFAST'
    )
    for path_ in PATH_MOCK_SCHEMAS.glob('pbh*.csv'):
        name = _get_name_from_path(path_)
        if name != 'input':
            specific_schema = build_structtype_from_csv(path_)
            dataframes[name] = spark_app.session.read.csv(
                str(PATH_MOCK_DATA / 'pbh_input.csv'),
                schema=build_structtype_from_csv(PATH_MOCK_SCHEMAS / 'pbh_input.csv'),
                sep=',',
                header=True,
                mode='FAILFAST'
            ).select(*[col.name for col in specific_schema.fields])
    return dataframes


def compare_actual_expected(
        result_actual,
        result_expected,
    ):
    """Helper function to compare results"""
    assert result_actual.count() == result_expected.distinct().count()
    compare = result_expected.join(
        result_actual,
        "member_id",
        "left_outer",
        )
    compare_columns = {
        expected_column: expected_column[expected_column.find("_") + 1:]
        for expected_column in result_expected.columns
        if expected_column.startswith("expected_")
        }
    failures = list()
    for expected_column, actual_column in compare_columns.items():
        misses = compare.filter(compare[expected_column] != compare[actual_column])
        if misses.count() != 0:
            failures.append(actual_column)
    assert not failures, "Column(s) '{}' contains unexpected values".format(failures)


def test_pbh(mock_dataframes):
    """Test the persistence of beta-blockers after heart attack logic"""
    test_instance = ebm_hedis_etc.pbh.PBH()
    result_actual = test_instance.calc_measure(mock_dataframes, datetime.date(2018, 1, 1))
    result_actual.cache()
    result_expected = mock_dataframes['expected'].cache()
    compare_actual_expected(
        result_actual,
        result_expected
    )
