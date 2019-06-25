"""
### CODE OWNERS: Austin Campbell

### OBJECTIVE:
    Calculate some Non-HEDIS Follow-Up Measures

### DEVELOPER NOTES:
  <none>
"""
import logging
import os
from pathlib import Path

import pyspark.sql.functions as spark_funcs

from prm.spark.app import SparkApp
from prm.meta.project import parse_project_metadata

PRM_META = parse_project_metadata()
LOGGER = logging.getLogger(__name__)

PATH_REF = Path(os.environ['EBM_HEDIS_ETC_PATHREF'])

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def main() -> int:
    """A function to enclose the execution of business logic."""
    sparkapp = SparkApp(PRM_META['pipeline_signature'])

    pcp_followup_df = sparkapp.load_df(
        PRM_META[PRM_META[70, 'out'] / 'med120_prm_pcp_followup.parquet']
    )

    numerator_7_day = pcp_followup_df.between(0, 7).count()
    numerator_14_day = pcp_followup_df.between(0, 14).count()

    if numerator_7_day > numerator_14_day:
        raise AssertionError('7-day followup success should not be greater than 14-day followup success')

    denom = pcp_followup_df.where(
        spark_funcs.col('prm_pcp_followup_potential_yn') == 'Y'
        ).count()

    ratio_7_day = numerator_7_day / denom
    ratio_14_day = numerator_14_day / denom

    LOGGER.info('The aggregate prm_pcp_followup_potential_yn is %s', denom)
    LOGGER.info('The 7-day PCP Followup ratio is %s', (ratio_7_day * 100))
    LOGGER.info('The 14-day PCP Followup ratio is %s', (ratio_14_day * 100))

    return 0


if __name__ == '__main__':
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext
    import prm.spark.defaults_prm

    prm.utils.logging_ext.setup_logging_stdout_handler()
    SPARK_DEFAULTS_PRM = prm.spark.defaults_prm.get_spark_defaults(PRM_META)

    with SparkApp(PRM_META['pipeline_signature'], **SPARK_DEFAULTS_PRM):
        RETURN_CODE = main()

    sys.exit(RETURN_CODE)