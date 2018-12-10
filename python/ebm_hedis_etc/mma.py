"""
### CODE OWNERS: Demerrick Moton
### OBJECTIVE:
    Implement MMA - Persistent Asthma Patients with >75% medication adherence
### DEVELOPER NOTES:
  <none>
"""
import logging
import datetime
import dateutil.relativedelta

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame
from prm.dates.windows import decouple_common_windows
from ebm_hedis_etc.base_classes import QualityMeasure

LOGGER = logging.getLogger(__name__)

# pylint does not recognize many of the spark functions
# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================



class MMA(QualityMeasure):
    """Class for MMA implementation"""
    def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart=datetime.date,
        ):
        # set timeline variables
        measurement_date_end = datetime.date(
            performance_yearstart.year,
            12,
            31
        )

        # filter to relevant value sets and diagnoses

        visit_valueset_df = dfs_input['reference'].where(
            F.col('value_set_name').rlike(r'\bED\b') |
            F.col('value_set_name').rlike(r'Acute Inpatient') |
            F.col('value_set_name').rlike(r'Outpatient') |
            F.col('value_set_name').rlike(r'Observation')
            ).select(
                F.col('value_set_name').alias('visit_valueset_name'),
                F.col('code_system').alias('visit_codesystem'),
                F.col('code').alias('visit_code')
            )

        diagnosis_valueset_df = dfs_input['reference'].where(
            F.col('value_set_name').rlike('Asthma')
        ).select(
            F.col('value_set_name').alias('diagnosis_valueset_name'),
            F.col('code_system').alias('diagnosis_codesystem'),
            F.col('code').alias('diagnosis_code')
        )

        filtered_claims_df = dfs_input['claims'].join(
            diagnosis_valueset_df,
            [
                dfs_input['claims'].icdversion == F.regexp_extract(
                    diagnosis_valueset_df.diagnosis_codesystem,
                    '\d+',
                    0
                    ),
                dfs_input['claims'].icddiag1 == F.regexp_replace(
                    diagnosis_valueset_df.diagnosis_code,
                    '\.',
                    ''
                )
            ],
            'inner'
        ).join(
            visit_valueset_df.where(
                F.col('code_system') == 'UBREV'
            ),
            F.col('revcode') == visit_valueset_df.visit_code,
            'inner'
        )

        asthma_controller_meds = [
            'Antiasthmatic combinations',
            'Antibody inhibitor',
            'Inhaled steroid combinations',
            'Inhaled corticosteroids',
            'Leukotriene modifiers',
            'Mast cell stabilizers',
            'Methylxanthines'
        ]

        asthma_reliever_meds = [
            'Short-acting inhaled beta-2 agonists'
        ]

        # find eligible population
        population_df = dfs_input['member'].where(
            # select members aged 5 - 64
            F.abs(
                F.year(F.col('dob')) - F.year(F.lit(measurement_date_end))
            ).between(5,64)
        ).join(
            dfs_input['member_time'],
            ['member_id'],
            'left_outer'
        )



        member_time_window = Window.partitionBy('member_id').orderBy(
            ['member_id', 'date_start']
            )

        lag_df = dfs_input['member_time'].withColumn(
            'is_laggy',
            F.when(
                (F.datediff(
                    F.col('date_start'),
                    F.lag(F.col('date_end')).over(member_time_window)
                    ) - F.lit(1)) >= 45,
                F.lit(1)
                ).otherwise(
                    F.lit(0)
                )
            )

        member_no_gaps_df = lag_df.groupBy('member_id').agg(
            F.sum(F.col('is_laggy')).alias('too_laggy')
            ).where(
                F.col('too_laggy') <= 1
            )

        member_no_gaps_df.join(
            dfs_input['member_time'],
            ['member_id'],
            'inner'
            ).view()

        # exclusions
        no_hospice = reference_dfs['claims'].where(F.col('value_set_name') != 'Hospice')

        # no more than one gap (<=45 days) in enrollment

        pass


if __name__ == '__main__':
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import prm.utils.logging_ext
    import prm.spark.defaults_prm
    import prm.meta.project
    from prm.spark.app import SparkApp

    PRM_META = prm.meta.project.parse_project_metadata()

    prm.utils.logging_ext.setup_logging_stdout_handler()
    SPARK_DEFAULTS_PRM = prm.spark.defaults_prm.get_spark_defaults(PRM_META)

    sparkapp = SparkApp(PRM_META['pipeline_signature'], **SPARK_DEFAULTS_PRM)
    sparkapp.session.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # ------------------------------------------------------------------
    from pathlib import Path
    from pyspark.sql import SQLContext

    performance_yearstart = datetime.datetime(2018, 1, 1)

    ref_claims_path = r"C:\Users\Demerrick.Moton\repos\ebm-hedis-etc\references\_data\hedis_codes.csv"
    ref_rx_path = r"C:\Users\Demerrick.Moton\repos\ebm-hedis-etc\references\_data\hedis_ndc_codes.csv"

    sqlContext = SQLContext(sparkapp.session.sparkContext)
    dfs_input = {
        "claims": sparkapp.load_df(
            PRM_META[(40, "out")] / "outclaims.parquet"
            ),
        "member_time": sparkapp.load_df(
            PRM_META[(35, "out")] / "member_time.parquet"
            ),
        "member": sparkapp.load_df(
            PRM_META[(35, "out")] / "member.parquet"
            ),
        "reference": sqlContext.read.csv(ref_claims_path, header=True),
        "ndc": sqlContext.read.csv(ref_rx_path, header=True)
        }

    reference_dfs['claims'].groupBy('value_set_name').count().where(F.col('value_set_name').isin('Hospice')).view()
    dfs_input['member'].view()
    # ------------------------------------------------------------------
    mma_decorator = MMA()
    result = mma_decorator.calc_decorator(DFS_INPUT_MED)
