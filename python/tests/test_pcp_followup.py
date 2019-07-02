"""
### CODE OWNERS: Austin Campbell

### OBJECTIVE:
    Test the 7-day and 14-day followup measures

### DEVELOPER NOTES:
    <none>
"""
# pragma: no cover
# pylint: disable=redefined-outer-name
import datetime
from pathlib import Path
import pytest


import ebm_hedis_etc.pcp_followup

try:
    _PATH_FILE = Path(__file__).parent
except NameError:  # Likely interactive development
    _PATH_FILE = Path(ebm_hedis_etc.pcp_followup.__file__).parents[1] / 'tests'  # pylint: disable=redefined-variable-type

PATH_MOCK_SCHEMAS = _PATH_FILE / "mock_schemas"
PATH_MOCK_DATA = _PATH_FILE / "mock_data"

CUTOFFS = ebm_hedis_etc.pcp_followup.CUTOFFS
EXPECTED_DROPPED_ROWS = 1

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _get_name_from_path(path_):
    """Get the data set name from the path"""
    return path_.stem[path_.stem.find('_') + 1:]


@pytest.fixture
def mock_dataframe(spark_app):
    """Testing data"""
    mock_df = spark_app.session.read.csv(
        str(PATH_MOCK_DATA / 'mock_outclaims_prm.csv'),
        sep=',',
        header=True,
        mode='FAILFAST'
    )
    return mock_df


def compare_actual_expected(
        result_actual,
        result_expected,
        cutoff
    ):
    """Helper function to compare results"""
    assert result_actual.count() == result_expected.count() - EXPECTED_DROPPED_ROWS
    compare = result_expected.join(
        result_actual,
        "member_id",
        how="left_outer",
        )
    compare_columns = {
        expected_column: expected_column[expected_column.find("expected_") + 9:]
        for expected_column in result_expected.columns
        if expected_column.find("expected_") >= 0
        }
    failures = list()
    for expected_column, actual_column in compare_columns.items():
        misses = compare.filter(compare[expected_column] != compare[actual_column])
        if misses.count() != 0:
            failures.append(actual_column)
    assert not failures, "Column(s) '{}' contains unexpected values".format(failures)


def test_pcp_followup(mock_dataframe):
    """Test the persistence of beta-blockers after heart attack logic"""
    test_instance = ebm_hedis_etc.pcp_followup.PCPFollowup()

    for cutoff in CUTOFFS:
        result_actual = test_instance.calc_measure(
            {'outclaims_prm': mock_dataframe},
            datetime.date(2018, 1, 1),
            cutoff=cutoff
        ).cache()
        result_expected = mock_dataframe.select(
            'member_id',
            '{}day_expected_comp_quality_numerator'.format(cutoff),
            'expected_comp_quality_denominator'
        ).cache()
        compare_actual_expected(
            result_actual,
            result_expected,
            cutoff
        )
