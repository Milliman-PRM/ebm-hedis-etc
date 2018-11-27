"""
### CODE OWNERS: Alexander Olivero, Demerrick Moton

### OBJECTIVE:
  Define tasks for ebm-hedis quality measures

### DEVELOPER NOTES:

"""
import os
from pathlib import Path

from indypy.nonstandard.ext_luigi import IndyPyLocalTarget, build_logfile_name
import prm.meta.project
from prm.ext_luigi.base_tasks import PRMPythonTask, RequirementsContainer

from prm.execute.definitions import (
    staging_membership,
    staging_claims,
)

PATH_SCRIPTS = Path(os.environ['EBM_HEDIS_ETC_MEASURES']) / 'scripts'
PATH_REFDATA = Path(os.environ['EBM_HEDIS_ETC_MEASURES_PATHREF'])
PRM_META = prm.meta.project.parse_project_metadata()

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class BetaBlockerHeartAttack(PRMPythonTask):
    """Run Prod01_beta_blockers_after_heartattack.py"""

    requirements = RequirementsContainer(
        staging_membership.DeriveParamsFromClaims,
        staging_claims.DeriveParamsFromClaims
    )

    def output(self):
        names_output = {
            'results_pbh.parquet'
        }
        return [
            IndyPyLocalTarget(PRM_META[150, 'out'] / name) for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / 'Prod01_beta_blockers_after_heartattack.py'
        super().run(
            program,
            path_log=build_logfile_name(program, PRM_META[150, 'out'] / 'EBM_HEDIS_ETC_Measures'),
            create_folder=True,
        )
    # pylint: enable=arguments-differ
