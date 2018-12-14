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

        # value set exclusion
        exclusion_valueset_df = dfs_input['reference'].where(
            F.col('value_set_name').rlike(r'Emphysema') |
            F.col('value_set_name').rlike(r'COPD') |
            F.col('value_set_name').rlike(r'Obstructive Chronic Bronchitis') |
            F.col('value_set_name').rlike(r'Chronic Respiratory Conditions') |
            F.col('value_set_name').rlike(r'Cystic Fibrosis') |
            F.col('value_set_name').rlike(r'Acute Respiratory Failure')
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

        filtered_med_claims_df = dfs_input['claims'].join(
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

        # age exclusion
        population_df = dfs_input['member'].where(
            F.abs(
                F.year(F.col('dob')) - F.year(F.lit(measurement_date_end))
            ).between(5,64)
        ).join(
            dfs_input['member_time'].where(
                F.col('cover_medical') == 'Y'
                ),
            ['member_id'],
            'left_outer'
        )

        # gap exclusions
        member_time_window = Window.partitionBy('member_id').orderBy(
            ['member_id', 'date_start']
        )

        gaps_df = population_df.withColumn(
            'gap_exists',
            F.when(
                (F.datediff(
                    F.col('date_start'),
                    F.lag(F.col('date_end')).over(member_time_window)
                    ) - F.lit(1)) >= 45,
                True
                ).otherwise(
                    False
                )
        )

        gap_exclusions_df = gaps_df.groupBy(['gap_exists', 'member_id']).count().where(
            F.col('gap_exists') & (F.col('count') > 1)
        ).join(
            gaps_df,
            ['gap_exists', 'member_id'],
            'left_outer'
        )

        filtered_members_df = population_df.join(
            gap_exclusions_df,
            'member_id',
            'left_anti'
        )

        filtered_claims_df = filtered_members_df.join(
            filtered_med_claims_df,
            'member_id',
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

        controller_meds_df = dfs_input['ndc'].where(
            F.col('description').isin(asthma_controller_meds)
        ).withColumn(
            'medication_type',
            F.lit('controller')
        )

        reliever_meds_df = dfs_input['ndc'].where(
            F.col('description').isin(asthma_reliever_meds)
        ).withColumn(
            'medication_type',
            F.lit('reliever')
        )

        filtered_rx_claims_df = dfs_input['rx_claims'].join(
            controller_meds_df,
            F.col('ndc') == controller_meds_df.ndc_code,
            'inner'
        ).union(
            dfs_input['rx_claims'].join(
                reliever_meds_df,
                F.col('ndc') == controller_meds_df.ndc_code,
                'inner'
            )
        )

        filtered_rx_df = filtered_members_df.join(
            filtered_rx_claims_df,
            'member_id',
            'inner'
        )

        # filter claims data by event
        event_med_df = filtered_claims_df.groupBy(
            ['member_id', 'visit_valueset_name',
             'diagnosis_valueset_name', 'fromdate']
        ).count()

        event_rx_df = filtered_rx_df.groupBy(
            ['member_id', 'ndc', 'fromdate', 'medication_type']
        ).count()

        ed_event_df = event_med_df.withColumn(
            'is_elig',
            F.when(
                F.col('visit_valueset_name').rlike(r'\bED\b') &
                F.col('diagnosis_valueset_name').rlike(r'Asthma') &
                (F.col('count') >= 1),
                True
            ).otherwise(False)
        ).select(
            F.col('member_id'),
            F.col('is_elig')
        )

        acute_inp_event_df = event_med_df.withColumn(
            'is_elig',
            F.when(
                F.col('visit_valueset_name').rlike(r'Acute Inpatient') &
                F.col('diagnosis_valueset_name').rlike(r'Asthma') &
                (F.col('count') >= 1),
                True
            ).otherwise(False)
        ).select(
            F.col('member_id'),
            F.col('is_elig')
        )

        out_obs_event_med_df = event_med_df.where(
            (F.col('visit_valueset_name').rlike(r'Outpatient') |
            F.col('visit_valueset_name').rlike(r'Observation')) &
            F.col('diagnosis_valueset_name').rlike(r'Asthma')
        ).groupBy('member_id').agg(
            F.count('*').alias('unique_service_dates')
        ).join(
            event_med_df,
            'member_id',
            'left_outer'
        ).withColumn(
            'is_elig',
            F.when(
                F.col('unique_service_dates') >= 4,
                True
            ).otherwise(False)
        ).select(
            F.col('member_id'),
            F.col('is_elig')
        )

        out_obs_event_rx_df = event_rx_df.groupBy('member_id').agg(
            F.count('*').alias('unique_disp_event')
        ).join(
            event_rx_df,
            'member_id',
            'left_outer'
        ).withColumn(
            'is_elig',
            F.when(
                F.col('unique_disp_event') >= 2,
                True
            ).otherwise(False)
        ).select(
            F.col('member_id'),
            F.col('is_elig')
        )

        asthma_disp_event_df = event_rx_df.groupBy('member_id').agg(
            F.count('*').alias('unique_disp_event')
        ).join(
            event_rx_df,
            'member_id',
            'left_outer'
        ).withColumn(
            'is_elig',
            F.when(
                F.col('unique_disp_event') >= 4,
                True
            ).otherwise(False)
        ).select(
            F.col('member_id'),
            F.col('is_elig')
        )

        elig_members_df = ed_event_df.union(
            acute_inp_event_df
        ).union(
            out_obs_event_med_df
        ).union(
            out_obs_event_rx_df
        ).union(
            asthma_disp_event_df
        ).distinct()

        included_year1_df = filtered_rx_df.withColumn(
            'year',
            F.year(F.col('fromdate'))
        ).where(
            F.col('year') == performance_yearstart.year
        ).groupBy('member_id').agg(
            F.collect_set(F.col('description')).alias('rx_types')
        ).withColumn(
            'is_included',
            F.when(
                (F.array_contains(F.col('rx_types'), 'Leukotriene modifiers') &
                F.array_contains(F.col('rx_types'), 'Antibody inhibitor')) &
                (F.size(F.col('rx_types')) == F.lit(2)),
                True
            ).otherwise(False)
        ).where(
            F.col('is_included') == True
        ).join(
            filtered_rx_df,
            'member_id'
        ).groupBy('member_id').count().where(
            F.col('count') >= 4
        ).select(
            F.col('member_id')
        )

        included_year2_df = filtered_rx_df.withColumn(
            'year',
            F.year(F.col('fromdate'))
        ).where(
            F.col('year') == performance_yearstart.year
        ).groupBy('member_id').agg(
            F.collect_set(F.col('description')).alias('rx_types')
        ).withColumn(
            'is_included',
            F.when(
                (F.array_contains(F.col('rx_types'), 'Leukotriene modifiers') &
                F.array_contains(F.col('rx_types'), 'Antibody inhibitor')) &
                (F.size(F.col('rx_types')) == F.lit(2)),
                True
            ).otherwise(False)
        ).where(
            F.col('is_included') == True
        ).join(
            filtered_rx_df,
            'member_id'
        ).groupBy('member_id').count().where(
            F.col('count') >= 4
        ).select(
            F.col('member_id')
        )

        included_df = included_year1_df.union(
            included_year2_df
        )

        included_diag_df = included_df.join(
            filtered_claims_df.select(
                F.col('member_id'),
                F.col('diagnosis_valueset_name')
            ),
            'member_id',
            'inner'
        ).where(
             F.col('diagnosis_valueset_name').rlike('Asthma')
        )


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
        'rx_claims': sparkapp.load_df(
            PRM_META[40, 'out'] / 'outpharmacy.parquet'
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
