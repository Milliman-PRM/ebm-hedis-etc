"""
### CODE OWNERS: Alexander Olivero, Ben Copeland

### OBJECTIVE:
    Stack all results tables and serialize quality_measures and ref_quality_measures tables.

### DEVELOPER NOTES:
  <none>
"""
import logging
import os
from pathlib import Path

import pyspark.sql.functions as spark_funcs
from pyspark.sql import DataFrame
from prm.spark.app import SparkApp
from prm.spark.io_sas import write_sas_data
from prm.meta.project import parse_project_metadata
from indypy.file_utils import IndyPyPath

PRM_META = parse_project_metadata()
LOGGER = logging.getLogger(__name__)

PATH_REF = Path(os.environ['EBM_HEDIS_ETC_PATHREF'])

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def create_quality_measures_output(
        sparkapp: SparkApp,
        quality_measure_dir: IndyPyPath
) -> DataFrame:
    """Stack individual quality measure output tables for export"""
    quality_measures_df = None
    for measure_path in quality_measure_dir.collect_files_regex('results'):
        measure_df = sparkapp.load_df(measure_path)
        if not quality_measures_df:
            quality_measures_df = measure_df
        else:
            quality_measures_df = quality_measures_df.union(measure_df)

    return quality_measures_df.where(
        spark_funcs.col('comp_quality_denominator') != 0
    ).withColumn(
        'comp_quality_date_actionable',
        spark_funcs.col('comp_quality_date_actionable').cast('date')
    )


def main() -> int:
    """A function to enclose the execution of business logic."""
    sparkapp = SparkApp(PRM_META['pipeline_signature'])

    quality_measures_df = create_quality_measures_output(
        sparkapp,
        PRM_META[150, 'out']
    )

    ref_quality_measures_df = sparkapp.load_df(PATH_REF / 'hedis_ref_quality_measures.parquet')

    sparkapp.save_df(quality_measures_df, PRM_META[150, 'out'] / 'quality_measures.parquet')
    write_sas_data(quality_measures_df, PRM_META[150, 'out'] / 'quality_measures.sas7bdat')

    sparkapp.save_df(ref_quality_measures_df, PRM_META[150, 'out'] / 'ref_quality_measures.parquet')
    write_sas_data(ref_quality_measures_df, PRM_META[150, 'out'] / 'ref_quality_measures.sas7bdat')

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
