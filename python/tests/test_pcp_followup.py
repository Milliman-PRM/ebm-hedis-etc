"""
### CODE OWNERS: Austin Campbell, Chas Busenburg

### OBJECTIVE:
    Test the 7-day and 14-day followup measures

### DEVELOPER NOTES:
    <none>
"""
# pragma: no cover
# pylint: disable=redefined-outer-name
# pylint: disable=no-member
import datetime
from pathlib import Path

import ebm_hedis_etc.pcp_followup
import pyspark.sql.functions as spark_funcs
import pytest

try:
    _PATH_FILE = Path(__file__).parent
except NameError:  # Likely interactive development
    _PATH_FILE = Path(ebm_hedis_etc.pcp_followup.__file__).parents[1] / "tests"

PATH_MOCK_SCHEMAS = _PATH_FILE / "mock_schemas"
PATH_MOCK_DATA = _PATH_FILE / "mock_data"

CUTOFFS = [7, 14]

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


@pytest.fixture
def mock_dataframe(spark_app):
    """Testing data"""
    mock_df = spark_app.session.read.csv(
        str(PATH_MOCK_DATA / "pcp_followup_input.csv"),
        sep=",",
        header=True,
        mode="FAILFAST",
    )
    return mock_df


@pytest.fixture
def expected_dataframe(spark_app):
    """Testing data"""
    expected_df = spark_app.session.read.csv(
        str(PATH_MOCK_DATA / "pcp_followup_output.csv"),
        sep=",",
        header=True,
        mode="FAILFAST",
    )
    return expected_df


def compare_actual_expected(result_actual, result_expected):
    """Helper function to compare results"""
    unexpected = result_actual.subtract(result_expected).collect()
    assert not unexpected, "Row(s) '{}' found in actual but not expected".format(
        unexpected
    )

    missing = result_expected.subtract(result_actual).collect()
    assert not missing, "Row(s) '{}' found in expected but not actual".format(missing)


def test_pcp_followup(mock_dataframe, expected_dataframe):
    """Test pcp followup output against expected output"""
    test_instance = ebm_hedis_etc.pcp_followup.PCPFollowup()

    for cutoff in CUTOFFS:
        result_actual = test_instance.calc_measure(
            {"outclaims_prm": mock_dataframe}, datetime.date(2018, 1, 1), cutoff=cutoff
        ).cache()
        result_expected = expected_dataframe.select(
            "member_id",
            spark_funcs.lit("PCP Followup: {}-Day".format(cutoff)).alias(
                "comp_quality_short"
            ),
            spark_funcs.col("{}day_comp_quality_numerator".format(cutoff)).alias(
                "comp_quality_numerator"
            ),
            "comp_quality_denominator",
            spark_funcs.lit(None).cast("string").alias("comp_quality_date_last"),
            spark_funcs.lit(None).cast("string").alias("comp_quality_date_actionable"),
            spark_funcs.lit(None).cast("string").alias("comp_quality_comments"),
        ).cache()
        compare_actual_expected(result_actual, result_expected)
