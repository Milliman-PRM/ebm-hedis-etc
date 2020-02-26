"""
### CODE OWNERS: Alexander Olivero, Matthew Hawthorne

### OBJECTIVE:
  Test common used functions among the HEDIS quality measures.

### DEVELOPER NOTES:
  <None>
"""
import logging
from pathlib import Path

import pyspark.sql.functions as spark_funcs
from ebm_hedis_etc.utils import find_elig_gaps
from ebm_hedis_etc.utils import flag_gap_exclusions

LOGGER = logging.getLogger(__name__)

_MOCK_DATA = Path(__file__).parent / 'mock_data'

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


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


def test_find_elig_gaps(spark_app):
    test_input = spark_app.session.read.csv(
        str(_MOCK_DATA / 'elig_gaps_input.csv'),
        header=True
    )
    expected_output = spark_app.session.read.csv(
        str(_MOCK_DATA / 'elig_gaps_output.csv'),
        header=True
    )
    function_output = find_elig_gaps(
        test_input,
        spark_funcs.lit('2017-01-01'),
        spark_funcs.lit('2018-12-31')
    )

    compare_actual_expected(function_output, expected_output)


def test_flag_gap_exclusions(spark_app):
    test_input = spark_app.session.read.csv(
        str(_MOCK_DATA / 'elig_gaps_output.csv'),
        header=True
    )
    test_input = test_input.select(
        [spark_funcs.col(col).alias(col.replace('expected_', '')) for col in test_input.columns]
    )
    expected_output = spark_app.session.read.csv(
        str(_MOCK_DATA / 'gap_exclusions_output.csv'),
        header=True
    )
    function_output = flag_gap_exclusions(
        test_input,
        45,
        1
    )

    compare_actual_expected(function_output, expected_output)
