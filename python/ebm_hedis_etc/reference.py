"""
### CODE OWNERS: Alexander Olivero, Ben Copeland

### OBJECTIVE:
  Compile the ebm_hedis_etc reference data into convenient downstream formats

### DEVELOPER NOTES:
  This is not actually intended to be ran during a true production run.
  The value of os.environ['EBM_HEDIS_ETC_PATHREF`] guides where this writes to.
"""
import logging
import os
from pathlib import Path

from prm.spark.app import SparkApp
from prm.spark.io_txt import build_structtype_from_csv

LOGGER = logging.getLogger(__name__)
try:
    PATH_INPUT = Path(os.environ['EBM_HEDIS_ETC_HOME']) / 'references'
    PATH_OUTPUT = Path(os.environ['EBM_HEDIS_ETC_PATHREF'])
except KeyError:  # pragma: no cover
    PATH_INPUT = Path(__file__).parents[2] / 'references'
    PATH_OUTPUT = None

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def import_flatfile_references(sparkapp: SparkApp) -> "typing.Mapping[str, DataFrame]":
    """Import the reference data into parquet"""
    refs = dict()
    for _file in (PATH_INPUT / '_data').iterdir():
        name = _file.stem.lower()
        LOGGER.info("Loading %s and saving as '%s'", _file, name)
        schema_temp = build_structtype_from_csv(
            PATH_INPUT / '_schemas' / (_file.stem + '.csv'),
        )
        refs[name] = sparkapp.session.read.csv(
            str(_file),
            schema=schema_temp,
            sep=',',
            mode='FAILFAST',
            header=True,
            ignoreLeadingWhiteSpace=True,
            ignoreTrailingWhiteSpace=True,
        )

    return refs


def main() -> int:  # pragma: no cover
    """Import the EBM HEDIS measure reference files and write to parquet"""
    LOGGER.info('Initializing SparkApp...')
    sparkapp = SparkApp('ref_ebm_hedis_etc')

    LOGGER.info('Reading in reference data to dataframes')
    refs = import_flatfile_references(sparkapp)

    LOGGER.info('Writing compiled reference data to %s', PATH_OUTPUT)
    for name, dataframe in refs.items():
        sparkapp.save_df(
            dataframe,
            PATH_OUTPUT / '{}.parquet'.format(name)
        )

    return 0


if __name__ == '__main__':  # pragma: no cover
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext

    prm.utils.logging_ext.setup_logging_stdout_handler()

    with SparkApp('ref_ebm_hedis_etc'):
        RETURN_CODE = main()

    sys.exit(RETURN_CODE)
