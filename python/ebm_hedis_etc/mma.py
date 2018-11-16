"""
### CODE OWNERS: Demerrick Moton
### OBJECTIVE:
    Implement MMA - Persistent Asthma Patients with >75% medication adherence
### DEVELOPER NOTES:
  <none>
"""
import logging
import datetime

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from prm.dates.windows import decouple_common_windows
from ebm_hedis_etc.base_classes import QualityMeasure

LOGGER = logging.getLogger(__name__)

# pylint does not recognize many of the spark functions
# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

class MMA(QualityMeasure):
    """Class for MMA implementation"""
    def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart=datetime.date,
        ):

        # find eligible population
            # exclude hospice
            # commercial, medicaid
            # ages 5-64 - stratfiy by 5-11, 12-18, 19-50, 51-64
            # use current measurement year
            # no more than one gap (<=45 days) in enrollment

        pass


if __name__ == '__main__':
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import prm.utils.logging_ext
    import prm.spark.defaults_prm
    import prm.meta.project
    from prm.spark.app import SparkApp

    PRM_META = prm.meta.project.parse_project_metadata()

    prm.utils.logging_ext.setup_logging_stdout_handler()
    SPARK_DEFAULTS_PRM = prm.spark.defaults_prm.get_spark_defaults(PRM_META)

    sparkapp = SparkApp(PRM_META['pipeline_signature'], **SPARK_DEFAULTS_PRM)
    sparkapp.session.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # ------------------------------------------------------------------
    from pathlib import Path
    from pyspark.sql import SQLContext
    sqlContext = SQLContext(sparkapp.session.sparkContext)
    input_dfs = {
        "claims": sparkapp.load_df(
            PRM_META[(40, "out")] / "outclaims.parquet"
            ),
        "member_time": sparkapp.load_df(
            PRM_META[(35, "out")] / "member_time.parquet"
            ),
        "member": sparkapp.load_df(
            PRM_META[(35, "out")] / "member.parquet"
            )
        }

    ref_claims_path = r"C:\Users\Demerrick.Moton\repos\ebm-hedis-etc\references\_data\hedis_codes.csv"
    ref_rx_path = r"C:\Users\Demerrick.Moton\repos\ebm-hedis-etc\references\_data\hedis_ndc_codes.csv"

    reference_dfs = {
        'claims': sqlContext.read.csv(ref_claims_path, header=True),
        'rx': sqlContext.read.csv(ref_rx_path, header=True)
        }
    # ------------------------------------------------------------------
    mma_decorator = MMA()
    result = mma_decorator.calc_decorator(DFS_INPUT_MED)
