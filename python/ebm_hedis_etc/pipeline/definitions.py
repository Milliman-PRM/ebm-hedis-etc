"""
### CODE OWNERS: Alexander Olivero, Demerrick Moton, Ben Copeland

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
    hcg_grouper_validation
)

PATH_SCRIPTS = Path(os.environ['EBM_HEDIS_ETC_HOME']) / 'scripts'
PATH_REFDATA = Path(os.environ['EBM_HEDIS_ETC_PATHREF'])
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
            'hedis_ref_quality_measures.parquet',
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
                PATH_REFDATA
            )
        )
        # pylint: enable=arguments-differ


class BetaBlockerHeartAttack(PRMPythonTask):  # pragma: no cover
    """Run beta_blockers_after_heartattack.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {
            'results_pbh.parquet'
        }
        return [
            IndyPyLocalTarget(PRM_META[150, 'out'] / name)
            for name in names_output
        ]

    def run(self):
        """Run the Luigi job"""
        program = PATH_SCRIPTS / 'beta_blockers_after_heartattack.py'
        super().run(
            program,
            path_log=build_logfile_name(program, PRM_META[150, 'log'] / 'EBM_HEDIS_ETC'),
            create_folder=True
        )


class AllCauseReadmissions(PRMPythonTask): # pragma: no cover
    """Run plan_allcause_readmissions.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {
            'results_pcr.parquet'
        }
        return [
            IndyPyLocalTarget(PRM_META[150, 'out'] / name)
            for name in names_output
        ]

    def run(self):
        """Run the Luigi job"""
        program = PATH_SCRIPTS / 'plan_allcause_readmissions.py'
        super().run(
            program,
            path_log=build_logfile_name(program, PRM_META[150, 'log'] / 'EBM_HEDIS_ETC'),
            create_folder=True
        )


class ChildhoodImmunization(PRMPythonTask): # pragma: no cover
    """Run childhood_immunization_status.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {
            'results_cis.parquet'
        }
        return [
            IndyPyLocalTarget(PRM_META[150, 'out'] / name)
            for name in names_output
        ]

    def run(self):
        """Run the Luigi job"""
        program = PATH_SCRIPTS / 'childhood_immunization_status.py'
        super().run(
            program,
            path_log=build_logfile_name(program, PRM_META[150, 'log'] / 'EBM_HEDIS_ETC'),
            create_folder=True
        )


class StatinTherapyCardiovascular(PRMPythonTask):  # pragma: no cover
    """Run statin_therapy_with_cardiovascular.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {
            'results_spc.parquet'
        }
        return [
            IndyPyLocalTarget(PRM_META[150, 'out'] / name)
            for name in names_output
        ]

    def run(self):
        """Run the Luigi job"""
        program = PATH_SCRIPTS / 'statin_therapy_with_cardiovascular.py'
        super().run(
            program,
            path_log=build_logfile_name(program, PRM_META[150, 'log'] / 'EBM_HEDIS_ETC'),
            create_folder=True
        )


class StatinTherapyDiabetes(PRMPythonTask): # pragma: no cover
    """Run statin_therapy_with_diabetes.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {
            'results_spd.parquet'
        }
        return [
            IndyPyLocalTarget(PRM_META[150, 'out'] / name)
            for name in names_output
        ]

    def run(self):
        """Run the Luigi job"""
        program = PATH_SCRIPTS / 'statin_therapy_with_diabetes.py'
        super().run(
            program,
            path_log=build_logfile_name(program, PRM_META[150, 'log'] / 'EBM_HEDIS_ETC'),
            create_folder=True
        )


class MonitoringDiuretics(PRMPythonTask): # pragma: no cover
    """Run annual_monitoring_of_diuretics.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {
            'results_mpm3.parquet'
        }
        return [
            IndyPyLocalTarget(PRM_META[150, 'out'] / name)
            for name in names_output
        ]

    def run(self):
        """Run the Luigi job"""
        program = PATH_SCRIPTS / 'annual_monitoring_of_diuretics.py'
        super().run(
            program,
            path_log=build_logfile_name(program, PRM_META[150, 'log'] / 'EBM_HEDIS_ETC'),
            create_folder=True
        )


class ComprehensiveDiabetesCare(PRMPythonTask): # pragma: no cover
    """Run comprehensive_diabetes_care.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {
            'results_cdc.parquet'
        }
        return [
            IndyPyLocalTarget(PRM_META[150, 'out'] / name)
            for name in names_output
        ]

    def run(self):
        """Run the Luigi job"""
        program = PATH_SCRIPTS / 'comprehensive_diabetes_care.py'
        super().run(
            program,
            path_log=build_logfile_name(program, PRM_META[150, 'log'] / 'EBM_HEDIS_ETC'),
            create_folder=True
        )


class PersistentAsthmaAdherence(PRMPythonTask): # pragma: no cover
    """Run persistent_asthma_adherence.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {
            'results_mma.parquet'
        }
        return [
            IndyPyLocalTarget(PRM_META[150, 'out'] / name)
            for name in names_output
        ]

    def run(self):
        """Run the Luigi job"""
        program = PATH_SCRIPTS / 'persistent_asthma_adherence.py'
        super().run(
            program,
            path_log=build_logfile_name(program, PRM_META[150, 'log'] / 'EBM_HEDIS_ETC'),
            create_folder=True
        )


class CombineAll(PRMPythonTask): # pragma: no cover
    """Run combine_all.py"""

    requirements = RequirementsContainer(
        StatinTherapyCardiovascular,
        StatinTherapyDiabetes,
        BetaBlockerHeartAttack,
        MonitoringDiuretics,
        ChildhoodImmunization,
        AllCauseReadmissions,
        ComprehensiveDiabetesCare,
        PersistentAsthmaAdherence
    )

    def output(self):
        names_output = {
            'quality_measures.parquet',
            'quality_measures.sass7bdat',
            'ref_quality_measures.parquet',
            'ref_quality_measures.sas7bdat',
        }
        return [
            IndyPyLocalTarget(PRM_META[150, 'out'] / name)
            for name in names_output
        ]

    def run(self):
        """Run the Luigi job"""
        program = PATH_SCRIPTS / 'combine_all.py'
        super().run(
            program,
            path_log=build_logfile_name(program, PRM_META[150, 'log'] / 'EBM_HEDIS_ETC'),
            create_folder=True
        )
