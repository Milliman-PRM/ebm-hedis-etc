"""
### CODE OWNERS: Alexander Olivero

### OBJECTIVE:
  Define tasks for ebm-hedis-etc meausres

### DEVELOPER NOTES:

"""
import os
from pathlib import Path

import ebm_hedis_etc.reference
from indypy.nonstandard.ext_luigi import IndyPyLocalTarget, build_logfile_name
import prm.meta.project
from prm.ext_luigi.base_tasks import PRMPythonTask, RequirementsContainer

from prm.execute.definitions import (
    staging_membership,
    staging_claims
)

PATH_SCRIPTS = Path(os.environ['EBM_HEDIS_ETC_MEASURES']) / 'scripts'
PATH_REFDATA = Path(os.environ['EMB_HEDIS_ETC_MEASURES_PATHREF'])
PRM_META = prm.meta.project.parse_project_metadata()


# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class ImportReferences(PRMPythonTask):  # pragma: no cover
    """Run reference.py"""

    requirements = RequirementsContainer()

    def output(self):
        names_output = {
            'hedis_codes.parquet',
            'hedis_ndc_codes.parquet',
            'hedis_codes.sas7bdat',
            'hedis_ndc_codes.sas7bdat',
            'hedis_ref_quality_measures.parquet',
            'hedis_ref_quality_measures.sas7bdat'
        }
        return [
            IndyPyLocalTarget(PATH_REFDATA / name)
            for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = Path(ebm_hedis_etc.reference.__file__)
        super().run(
            program,
            path_log=build_logfile_name(
                program,
                PATH_REFDATA,
            )
        )
        # pylint: enable=arguments-differ


