"""
### CODE OWNERS: Alexander Olivero, Ben Copeland

### OBJECTIVE:
    Calculate the Statin Therapy for Patients with Cardiovascular Disease (SPC-2) HEDIS measure.

### DEVELOPER NOTES:
  <none>
"""
import logging
import os
from pathlib import Path

from ebm_hedis_etc.spc import SPC
from prm.meta.project import parse_project_metadata
from prm.spark.app import SparkApp

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
        'member_time': sparkapp.load_df(PRM_META[35, 'out'] / 'member_time.parquet'),
        'member': sparkapp.load_df(PRM_META[35, 'out'] / 'member.parquet'),
        'claims': sparkapp.load_df(PRM_META[40, 'out'] / 'outclaims.parquet'),
        'rx_claims': sparkapp.load_df(PRM_META[40, 'out'] / 'outpharmacy.parquet'),
        'reference': sparkapp.load_df(PATH_REF / 'hedis_codes.parquet'),
        'ndc': sparkapp.load_df(PATH_REF / 'hedis_ndc_codes.parquet')
    }

    measure = SPC()
    results_df = measure.calc_measure(dfs_input, PRM_META['date_performanceyearstart'])

    sparkapp.save_df(results_df, PRM_META[150, 'out'] / 'results_spc.parquet')

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
