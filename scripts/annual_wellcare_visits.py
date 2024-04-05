"""
### CODE OWNERS: Chas Busenburg, Ben Copeland

### OBJECTIVE:
    Calculate the percentage of patients who have
    had their Annual Wellness visit in the current plan year.

### DEVELOPER NOTES:
  <none>
"""
import datetime
import logging
import os
from pathlib import Path

import pyspark.sql.functions as spark_funcs
from dateutil.relativedelta import relativedelta
from ebm_hedis_etc.annual_wellcare_visits import AWV
from prm.meta.project import parse_project_metadata
from prm.spark.app import SparkApp

PRM_META = parse_project_metadata()
LOGGER = logging.getLogger(__name__)

PATH_REF = Path(os.environ["EBM_HEDIS_ETC_PATHREF"])


def main() -> int:
    """A function to enclose the execution of business logic"""
    sparkapp = SparkApp(PRM_META["pipeline_signature"])

    dfs_input = {
        "member_time": sparkapp.load_df(PRM_META[35, "out"] / "member_time.parquet"),
        "member": sparkapp.load_df(PRM_META[35, "out"] / "member.parquet"),
        "med_claims": sparkapp.load_df(PRM_META[40, "out"] / "outclaims.parquet"),
        "reference": sparkapp.load_df(PATH_REF / "hedis_codes.parquet"),
    }

    measure = AWV(sparkapp)

    results_df_combined = (
        measure.calc_measure(
            dfs_input,
            PRM_META["date_performanceyearstart"],
            datetime_end=datetime.date(
                PRM_META["date_performanceyearstart"].year, 12, 31
            ),
            filter_reference="refs_well_care_whole",
        )
        .withColumn("comp_quality_short", spark_funcs.lit("AWV: Combined"))
        .select(
            "member_id",
            "comp_quality_short",
            "comp_quality_numerator",
            "comp_quality_denominator",
            "comp_quality_date_last",
            "comp_quality_date_actionable",
            "comp_quality_comments",
        )
    )

    results_df_combined_rolling = (
        measure.calc_measure(
            dfs_input,
            PRM_META["date_latestpaid"] - relativedelta(years = 1),
            datetime_end=PRM_META["date_latestpaid"],
            filter_reference="refs_well_care_whole",
            date_latestpaid=PRM_META["date_latestpaid"],
        )
        .withColumn("comp_quality_short", spark_funcs.lit("AWV: Combined Rolling"))
        .select(
            "member_id",
            "comp_quality_short",
            "comp_quality_numerator",
            "comp_quality_denominator",
            "comp_quality_date_last",
            "comp_quality_date_actionable",
            "comp_quality_comments",
        )
    )

    results_df_medicare = (
        measure.calc_measure(
            dfs_input,
            PRM_META["date_performanceyearstart"],
            datetime_end=datetime.date(
                PRM_META["date_performanceyearstart"].year, 12, 31
            ),
            filter_reference="reference_awv",
            date_latestpaid=PRM_META["date_latestpaid"],
        )
        .withColumn("comp_quality_short", spark_funcs.lit("AWV: Medicare"))
        .select(
            "member_id",
            "comp_quality_short",
            "comp_quality_numerator",
            "comp_quality_denominator",
            "comp_quality_date_last",
            "comp_quality_date_actionable",
            "comp_quality_comments",
        )
    )

    results_df_medicare_rolling = (
        measure.calc_measure(
            dfs_input,
            PRM_META["date_latestpaid"] - relativedelta(years = 1),
            datetime_end=PRM_META["date_latestpaid"],
            filter_reference="reference_awv",
            date_latestpaid=PRM_META["date_latestpaid"],
        )
        .withColumn("comp_quality_short", spark_funcs.lit("AWV: Medicare Rolling"))
        .select(
            "member_id",
            "comp_quality_short",
            "comp_quality_numerator",
            "comp_quality_denominator",
            "comp_quality_date_last",
            "comp_quality_date_actionable",
            "comp_quality_comments",
        )
    )
    sparkapp.save_df(
        results_df_combined,
        PRM_META[150, "out"] / "results_annual_wellcare_visits_combined.parquet",
    )
    sparkapp.save_df(
        results_df_combined_rolling,
        PRM_META[150, "out"]
        / "results_annual_wellcare_visits_combined_rolling.parquet",
    )
    sparkapp.save_df(
        results_df_medicare,
        PRM_META[150, "out"] / "results_annual_wellcare_visits_medicare.parquet",
    )
    sparkapp.save_df(
        results_df_medicare_rolling,
        PRM_META[150, "out"]
        / "results_annual_wellcare_visits_medicare_rolling.parquet",
    )

    return 0


if __name__ == "__main__":
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext
    import prm.spark.defaults_prm

    prm.utils.logging_ext.setup_logging_stdout_handler()
    SPARK_DEFAULTS_PRM = prm.spark.defaults_prm.get_spark_defaults(PRM_META)

    with SparkApp(PRM_META["pipeline_signature"], **SPARK_DEFAULTS_PRM):
        RETURN_CODE = main()

    sys.exit(RETURN_CODE)
