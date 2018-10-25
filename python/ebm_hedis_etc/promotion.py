"""
### CODE OWNERS: Alexander Olivero

### OBJECTIVE:
  Tools to automated some of the manual steps of code promotion

### DEVELOPER NOTES:
  When run as a script, should do the code promotion process
"""
import logging
import os
import subprocess
from pathlib import Path

from indypy.nonstandard.ghapi_tools import repo
from indypy.nonstandard.ghapi_tools import conf
from indypy.nonstandard import promotion_tools

import ebm_hedis_etc.reference

LOGGER = logging.getLogger(__name__)

_PATH_REL_RELEASE_NOTES = Path("docs") / "release-notes.md"
PATH_PROMOTION = Path(r"S:\PRM\Pipeline_Components\emb_hedis_etc")

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def promote_reference_data(path_release: Path) -> None:  # pragma: no cover
    """Convert the reference data to convenient downstream formats"""

    _path_reference_output = path_release / '_compiled_reference_data'
    LOGGER.info("Compiling reference data into %s", _path_reference_output)
    _path_reference_output.mkdir(exist_ok=False)
    os.environ['EBM_HEDIS_ETC_HOME'] = str(path_release)
    os.environ['EMB_HEDIS_ETC_MEASURES_PATHREF'] = str(_path_reference_output)

    _path_import_script = Path(ebm_hedis_etc.reference.__file__)

    LOGGER.info("Running import script: %s", _path_import_script)
    subprocess.run(['python', str(_path_import_script)], check=True)

    return None


def main() -> int:  # pragma: no cover
    """Promotion process"""
    LOGGER.info("Beginning code promotion for product component")
    github_repository = repo.GithubRepository.from_parts("PRM", "ebm-hedis-etc")
    version = promotion_tools.LocalVersion(
        input("Please enter the version number for this release (e.g. v1.2.3): "),
        partial=True,
    )
    promotion_branch = input("Please select the branch to promote (default: master): ")
    if not promotion_branch:
        promotion_branch = "master"
    assert promotion_branch == "master" or version.prerelease,\
        "Releases can only be promoted from master. Pre-releases can be promoted from any branch"
    doc_info = promotion_tools.get_documentation_inputs(github_repository)
    release = promotion_tools.Release(github_repository, version, PATH_PROMOTION, doc_info)
    repository_clone = release.export_repo(branch=promotion_branch)
    release.make_release_json()
    promote_reference_data(release.path_version)
    if not version.prerelease:
        LOGGER.info('Doing final promotion steps for real release (e.g. tagging)')
        tag = release.make_tag(repository_clone)
        release.post_github_release(
            conf.get_github_oauth(prompt_if_no_file=True),
            tag,
            body=promotion_tools.get_release_notes(
                release.path_version / _PATH_REL_RELEASE_NOTES,
                version,
            ),
        )
    return 0


if __name__ == '__main__':  # pragma: no cover
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext

    prm.utils.logging_ext.setup_logging_stdout_handler()

    sys.exit(main())
