"""
### CODE OWNERS: Alexander Olivero, Demerrick Moton, Ben Copeland

### OBJECTIVE:
  Define tasks for ebm-hedis-etc meausres

### DEVELOPER NOTES:

"""
import os
from pathlib import Path

import ebm_hedis_etc.reference
import prm.meta.project
from indypy.nonstandard.ext_luigi import build_logfile_name
from indypy.nonstandard.ext_luigi import IndyPyLocalTarget
from prm.execute.definitions import hcg_grouper_validation
from prm.execute.definitions import staging_membership
from prm.ext_luigi.base_tasks import PRMPythonTask
from prm.ext_luigi.base_tasks import RequirementsContainer

PATH_SCRIPTS = Path(os.environ["EBM_HEDIS_ETC_HOME"]) / "scripts"  # pragma: no cover
PATH_REFDATA = Path(os.environ["EBM_HEDIS_ETC_PATHREF"])  # pragma: no cover
PRM_META = prm.meta.project.parse_project_metadata()  # pragma: no cover

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class ImportReferences(PRMPythonTask):  # pragma: no cover
    """Run reference.py"""

    requirements = RequirementsContainer()

    def output(self):
        names_output = {
            "hedis_codes.parquet",
            "hedis_ndc_codes.parquet",
            "hedis_ref_quality_measures.parquet",
            "reference_awv.parquet",
        }
        return [IndyPyLocalTarget(PATH_REFDATA / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = Path(ebm_hedis_etc.reference.__file__)
        super().run(program, path_log=build_logfile_name(program, PATH_REFDATA))
        # pylint: enable=arguments-differ


class BetaBlockerHeartAttack(PRMPythonTask):  # pragma: no cover
    """Run beta_blockers_after_heartattack.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {"results_pbh.parquet"}
        return [IndyPyLocalTarget(PRM_META[150, "out"] / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / "beta_blockers_after_heartattack.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program, PRM_META[150, "log"] / "EBM_HEDIS_ETC"
            ),
            create_folder=True,
        )
        # pylint: enable=arguments-differ


class AllCauseReadmissions(PRMPythonTask):  # pragma: no cover
    """Run plan_allcause_readmissions.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {"results_pcr.parquet"}
        return [IndyPyLocalTarget(PRM_META[150, "out"] / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / "plan_allcause_readmissions.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program, PRM_META[150, "log"] / "EBM_HEDIS_ETC"
            ),
            create_folder=True,
        )
        # pylint: enable=arguments-differ


class ChildhoodImmunization(PRMPythonTask):  # pragma: no cover
    """Run childhood_immunization_status.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {"results_cis.parquet"}
        return [IndyPyLocalTarget(PRM_META[150, "out"] / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / "childhood_immunization_status.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program, PRM_META[150, "log"] / "EBM_HEDIS_ETC"
            ),
            create_folder=True,
        )
        # pylint: enable=arguments-differ


class StatinTherapyCardiovascular(PRMPythonTask):  # pragma: no cover
    """Run statin_therapy_with_cardiovascular.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {"results_spc.parquet"}
        return [IndyPyLocalTarget(PRM_META[150, "out"] / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / "statin_therapy_with_cardiovascular.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program, PRM_META[150, "log"] / "EBM_HEDIS_ETC"
            ),
            create_folder=True,
        )
        # pylint: enable=arguments-differ


class StatinTherapyDiabetes(PRMPythonTask):  # pragma: no cover
    """Run statin_therapy_with_diabetes.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {"results_spd.parquet"}
        return [IndyPyLocalTarget(PRM_META[150, "out"] / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / "statin_therapy_with_diabetes.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program, PRM_META[150, "log"] / "EBM_HEDIS_ETC"
            ),
            create_folder=True,
        )
        # pylint: enable=arguments-differ


class MonitoringDiuretics(PRMPythonTask):  # pragma: no cover
    """Run annual_monitoring_of_diuretics.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {"results_mpm3.parquet"}
        return [IndyPyLocalTarget(PRM_META[150, "out"] / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / "annual_monitoring_of_diuretics.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program, PRM_META[150, "log"] / "EBM_HEDIS_ETC"
            ),
            create_folder=True,
        )
        # pylint: enable=arguments-differ


class ComprehensiveDiabetesCare(PRMPythonTask):  # pragma: no cover
    """Run comprehensive_diabetes_care.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {"results_cdc.parquet"}
        return [IndyPyLocalTarget(PRM_META[150, "out"] / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / "comprehensive_diabetes_care.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program, PRM_META[150, "log"] / "EBM_HEDIS_ETC"
            ),
            create_folder=True,
        )
        # pylint: enable=arguments-differ


class PersistentAsthmaAdherence(PRMPythonTask):  # pragma: no cover
    """Run persistent_asthma_adherence.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {"results_mma.parquet"}
        return [IndyPyLocalTarget(PRM_META[150, "out"] / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / "persistent_asthma_adherence.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program, PRM_META[150, "log"] / "EBM_HEDIS_ETC"
            ),
            create_folder=True,
        )
        # pylint: enable=arguments-differ


class PCPFollowup(PRMPythonTask):  # pragma: no cover
    """Run calculate_followup_measures.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {
            "results_pcp_followup_7_day.parquet",
            "results_pcp_followup_14_day.parquet",
        }
        return [IndyPyLocalTarget(PRM_META[150, "out"] / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / "calculate_followup_measures.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program, PRM_META[150, "log"] / "EBM_HEDIS_ETC"
            ),
            create_folder=True,
        )
        # pylint: enable=arguments-differ


class AvoidanceAntibioticBronchitis(PRMPythonTask):  # pragma: no cover
    """Run avoidance_antibiotics_bronchitis.py"""

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {"results_aab.parquet"}
        return [IndyPyLocalTarget(PRM_META[150, "out"] / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / "avoidance_antibiotics_bronchitis.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program, PRM_META[150, "log"] / "EBM_HEDIS_ETC"
            ),
            create_folder=True,
        )


class AnnualWellnessVisits(PRMPythonTask):  # pragma: no cover
    """ Run annual_wellness_visits.py """

    requirements = RequirementsContainer(
        ImportReferences,
        staging_membership.DeriveParamsFromMembership,
        hcg_grouper_validation.Validations,
    )

    def output(self):
        names_output = {
            "results_annual_wellcare_visits_core.parquet",
            "results_annual_wellcare_visits_whole.parquet",
            "results_annual_wellcare_visits_medicare.parquet",
            "results_annual_wellcare_visits_rolling.parquet",
        }
        return [IndyPyLocalTarget(PRM_META[150, "out"] / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / "annual_wellcare_visits.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program, PRM_META[150, "log"] / "EBM_HEDIS_ETC"
            ),
            create_folder=True,
        )


class CombineAll(PRMPythonTask):  # pragma: no cover
    """Run combine_all.py"""

    requirements = RequirementsContainer(
        StatinTherapyCardiovascular,
        StatinTherapyDiabetes,
        BetaBlockerHeartAttack,
        MonitoringDiuretics,
        ChildhoodImmunization,
        AllCauseReadmissions,
        ComprehensiveDiabetesCare,
        PersistentAsthmaAdherence,
        AvoidanceAntibioticBronchitis,
        PCPFollowup,
        AnnualWellnessVisits,
    )

    def output(self):
        names_output = {
            "quality_measures.parquet",
            "quality_measures.sas7bdat",
            "ref_quality_measures.parquet",
            "ref_quality_measures.sas7bdat",
        }
        return [IndyPyLocalTarget(PRM_META[150, "out"] / name) for name in names_output]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / "combine_all.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program, PRM_META[150, "log"] / "EBM_HEDIS_ETC"
            ),
            create_folder=True,
        )
        # pylint: enable=arguments-differ


if __name__ == "__main__":
    pass
