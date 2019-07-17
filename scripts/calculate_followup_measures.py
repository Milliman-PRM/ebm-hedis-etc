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

from prm.spark.app import SparkApp
from prm.meta.project import parse_project_metadata
from ebm_hedis_etc.pcp_followup import PCPFollowup

CUTOFFS = [7, 14]

PRM_META = parse_project_metadata()
LOGGER = logging.getLogger(__name__)

PATH_REF = Path(os.environ['EBM_HEDIS_ETC_PATHREF'])

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def main() -> int:
    """A function to enclose the execution of business logic."""
    sparkapp = SparkApp(PRM_META['pipeline_signature'])

    dfs_input = {
        'outclaims_prm': sparkapp.load_df(PRM_META[73, 'out'] / 'outclaims_prm.parquet'),
    }

    measure = PCPFollowup()
    for cutoff in CUTOFFS:
        results_df = measure.calc_measure(
            dfs_input,
            PRM_META['date_performanceyearstart'],
            cutoff=cutoff
        )

        sparkapp.save_df(
            results_df,
            PRM_META[150, 'out'] / 'results_pcp_followup_{}_day.parquet'.format(cutoff)
        )

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
