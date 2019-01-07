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
from math import ceil, floor

import pyspark.sql.functions as F
import pyspark.sql.types as T
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

def _rx_route_events(rx_data, route):
    med_route_event_df = None
    if route is 'oral':
        oral_events_df = rx_data.where(
            F.col('route') == 'oral'
        ).groupBy(
            ['member_id', 'fromdate', 'dayssupply', 'drug_id', 'claimid']
        ).agg(
            F.count('*').alias('count'),
            F.when(
                F.col('dayssupply') <= 30,
                F.bround(F.col('dayssupply')/F.lit(30)).cast('int')
            ).otherwise(
                F.floor(F.col('dayssupply')/F.lit(30)).cast('int')
            ).alias('num_disp_events')
        )

        max_disp_events = oral_events_df.select(
            F.max('num_disp_events').alias('max_n')
        ).first()['max_n']

        exploded_oral_events_df = oral_events_df.withColumn(
            'disp_events',
            F.array(
                [F.lit(x) for x in range(max_disp_events)]
            )
        ).select(
            F.col('*'),
            F.explode('disp_events').alias('expl_disp_events')
        ).where(
            F.col('expl_disp_events') < F.col('num_disp_events')
        ).withColumn(
            'rx_event_number',
            F.row_number().over(
                Window.partitionBy(
                    'member_id', 'fromdate', 'dayssupply', 'drug_id', 'claimid'
                ).orderBy('member_id')
            )
        ).select(
            F.col('member_id'),
            F.col('claimid'),
            F.col('fromdate'),
            F.col('drug_id'),
            F.col('dayssupply'),
            F.col('rx_event_number')
        )

        med_route_event_df = exploded_oral_events_df.join(
            rx_data,
            ['member_id', 'fromdate', 'dayssupply', 'drug_id', 'claimid'],
            'left_outer'
        )
    elif route is 'inhaler':
        inhaler_events_df = rx_data.where(
            F.col('route') == 'inhalation'
        ).groupBy(
            ['member_id', 'fromdate', 'drug_id']
        ).agg(
            F.row_number().over(
                Window.partitionBy(
                    'member_id', 'fromdate', 'drug_id'
                ).orderBy('member_id')
            ).alias('rx_event_number')
        )

        med_route_event_df = inhaler_events_df.join(
            rx_data,
            ['member_id', 'fromdate', 'drug_id'],
            'left_outer'
        )
    elif route is 'injected':
        med_route_event_df = rx_data.distinct().where(
            F.col('route') == 'subcutaneous'
        ).withColumn(
            'rx_event_number',
            F.lit(1)
        )

    return med_route_event_df


def _initial_filtering(dfs_input, measurement_date_end):
    # filter down to relevant value sets and diagnoses
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

    med_df = dfs_input['claims'].join(
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

    rx_df = dfs_input['rx_claims'].join(
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

    members_df = dfs_input['member'].where(
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

    return {
        'med': med_df,
        'rx': rx_df,
        'members': members_df
    }


def _exclusionary_filtering(dfs_input, filtered_data_dict, measurement_date_end):
    # find members with at least more than 1 45 day gap in
    # eligibility during meas. year
    member_time_window = Window.partitionBy('member_id').orderBy(
        ['member_id', 'date_start']
    )

    gap_exclusions_df = filtered_data_dict['members'].withColumn(
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
    ).groupBy(['gap_exists', 'member_id']).count().where(
        F.col('gap_exists') & (F.col('count') > 1)
    ).select('member_id')

    # find members with any presense of certain diagnoses
    excluded_diags_df = dfs_input['reference'].where(
        F.col('value_set_name').rlike(r'Emphysema') |
        F.col('value_set_name').rlike(r'COPD') |
        F.col('value_set_name').rlike(r'Obstructive Chronic Bronchitis') |
        F.col('value_set_name').rlike(r'Chronic Respiratory Conditions') |
        F.col('value_set_name').rlike(r'Cystic Fibrosis') |
        F.col('value_set_name').rlike(r'Acute Respiratory Failure')
    )

    valueset_exclusions_df = dfs_input['claims'].join(
        excluded_diags_df,
        [
            dfs_input['claims'].icdversion == F.regexp_extract(
                excluded_diags_df.code_system,
                '\d+',
                0
            ),
            dfs_input['claims'].icddiag1 == F.regexp_replace(
                excluded_diags_df.code,
                '\.',
                ''
            )
        ],
        'inner'
    ).where(
        F.datediff(
            F.col('fromdate'),
            F.lit(measurement_date_end)
        ) <= 0
    ).select('member_id')

    # find members who had no asthma controller medications dispensed
    # during the measurement year.
    controller_exclusions_df = filtered_data_dict['rx'].groupBy(
        ['member_id', 'fromdate']
    ).agg(
        F.collect_set('medication_type').alias('med_types')
    ).where(
        ~F.array_contains(F.col('med_types'), 'controller') &
        F.col('fromdate').between(
            F.lit(
                datetime.date(
                    measurement_date_end.year - 1,
                    12,
                    31
                )
            ),
            F.lit(measurement_date_end)
        )
    ).select('member_id')

    exclusions_df = gap_exclusions_df.union(
        valueset_exclusions_df
    ).union(
        controller_exclusions_df
    ).distinct()

    members_df = filtered_data_dict['members'].join(
        exclusions_df,
        'member_id',
        'left_anti'
    )

    med_df = filtered_data_dict['med'].join(
        exclusions_df,
        'member_id',
        'left_anti'
    )

    rx_df = filtered_data_dict['rx'].join(
        exclusions_df,
        'member_id',
        'left_anti'
    )

    return {
        'med': med_df,
        'rx': rx_df,
        'members': members_df
    }


def _event_filtering(dfs_input, exc_filtered_data_dict, measurement_date_end):
    rx_route = ['oral', 'inhaler', 'injected']

    # filter claims data by event
    med_event_df = exc_filtered_data_dict['med'].groupBy(
        ['member_id', 'visit_valueset_name',
         'diagnosis_valueset_name', 'fromdate']
    ).count()

    rx_route_df = None
    for route in rx_route:
        if rx_route_df is None:
            rx_route_df = _rx_route_events(
                exc_filtered_data_dict['rx'],
                route
            ).select(
                F.col('member_id'),
                F.col('ndc'),
                F.col('fromdate'),
                F.col('medication_type'),
                F.col('route')
            )
        else:
            rx_route_df = rx_route_df.union(
                _rx_route_events(
                    exc_filtered_data_dict['rx'],
                    route
                ).select(
                    F.col('member_id'),
                    F.col('ndc'),
                    F.col('fromdate'),
                    F.col('medication_type'),
                    F.col('route')
                )
            )

    rx_event_df = rx_route_df.groupBy(
        ['member_id', 'ndc', 'fromdate', 'medication_type']
    ).count()

    ed_event_df = med_event_df.withColumn(
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

    acute_inp_event_df = med_event_df.withColumn(
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

    out_obs_event_med_df = med_event_df.where(
        (F.col('visit_valueset_name').rlike(r'Outpatient') |
        F.col('visit_valueset_name').rlike(r'Observation')) &
        F.col('diagnosis_valueset_name').rlike(r'Asthma')
    ).groupBy('member_id').agg(
        F.count('*').alias('unique_service_dates')
    ).join(
        med_event_df,
        'member_id',
        'inner'
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

    out_obs_event_rx_df = rx_event_df.groupBy('member_id').agg(
        F.count('*').alias('unique_disp_event')
    ).join(
        rx_event_df,
        'member_id',
        'inner'
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

    asthma_disp_event_df = rx_event_df.groupBy('member_id').agg(
        F.count('*').alias('unique_disp_event')
    ).join(
        rx_event_df,
        'member_id',
        'inner'
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

    included_year1_df = exc_filtered_data_dict['rx'].withColumn(
        'year',
        F.year(F.col('fromdate'))
    ).where(
        F.col('year') == measurement_date_end.year - 1
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
        exc_filtered_data_dict['rx'],
        'member_id',
        'left_outer'
    ).groupBy('member_id').count().where(
        F.col('count') >= 4
    ).select(
        F.col('member_id')
    )

    included_year2_df = exc_filtered_data_dict['rx'].withColumn(
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
        exc_filtered_data_dict['rx'],
        'member_id',
        'left_outer'
    ).groupBy('member_id').count().where(
        F.col('count') >= 4
    ).select(
        F.col('member_id')
    )

    included_df = included_year1_df.union(
        included_year2_df
    ).distinct()

    included_diag_df = included_df.join(
        exc_filtered_data_dict['med'].select(
            F.col('member_id'),
            F.col('diagnosis_valueset_name')
        ),
        'member_id',
        'inner'
    ).where(
        F.col('diagnosis_valueset_name').rlike('Asthma')
    ).select(
        F.col('member_id')
    ).withColumn(
        'is_elig',
        F.lit(True)
    )

    elig_members_df = ed_event_df.union(
        acute_inp_event_df
    ).union(
        out_obs_event_med_df
    ).union(
        out_obs_event_rx_df
    ).union(
        asthma_disp_event_df
    ).union(
        included_diag_df
    ).where(
        F.col('is_elig') == True
    ).select(
        F.col('member_id')
    ).distinct()

    members_df = exc_filtered_data_dict['members'].join(
        elig_members_df,
        'member_id',
        'left_semi'
    )

    med_df = exc_filtered_data_dict['med'].join(
        members_df,
        'member_id',
        'left_semi'
    )

    rx_df = exc_filtered_data_dict['rx'].join(
        members_df,
        'member_id',
        'left_semi'
    )

    return {
        'med': med_df,
        'rx': rx_df,
        'members': members_df
    }


def _calculate_rates(rx_data, measurement_date_end):
    controller_claims_df = rx_data.where(
        (F.col('medication_type') == 'controller') &
        F.col('fromdate').between(
            F.lit(
                datetime.date(
                    measurement_date_end.year - 1,
                    12,
                    31
                )
            ),
            F.lit(measurement_date_end)
        )
    ).select(
        F.col('member_id'),
        F.col('fromdate'),
        F.col('drug_id'),
        F.col('dayssupply'),
        F.expr('date_add(fromdate, dayssupply - 1)').alias('fromdate_to_dayssupply')
    ).where(
        F.col('dayssupply') > 0
    ).select(
        '*',
        F.create_map(
            F.lit('date_start'),
            F.col('fromdate'),
            F.lit('date_end'),
            F.col('fromdate_to_dayssupply'),
        ).alias('map_coverage_window')
    )

    collect_date_windows = controller_claims_df.groupby(
        'member_id',
        'drug_id',
    ).agg(
        F.collect_list(F.col('map_coverage_window')).alias('array_coverage_windows'),
    )

    def adjust_date_windows(
            array_coverage_windows: "typing.Iterable[typing.Mapping[str, datetime.date]]",
        ) -> "typing.Iterable[typing.Mapping[str, datetime.date]]":
        """Combine and extend overlapping windows"""

        sorted_array = sorted(
            array_coverage_windows,
            key=lambda x: (x['date_start'], x['date_end']),
            )
        output_array = []
        last_date_end = None
        for window_coverage in sorted_array:
            date_start = window_coverage['date_start']
            date_end = window_coverage['date_end']
            if last_date_end and last_date_end >= date_start:
                bump_factor = (last_date_end - date_start).days + 1
                adj_date_start = date_start + datetime.timedelta(days=bump_factor)
                adj_date_end = date_end + datetime.timedelta(days=bump_factor)
                last_date_end = adj_date_end
            else:
                last_date_end = date_end
                adj_date_start = date_start
                adj_date_end = date_end

            output_array.append({
                'date_start': adj_date_start,
                'date_end': adj_date_end,
                })
        return output_array

    udf_adjust_date_windows = F.udf(
        adjust_date_windows,
        T.ArrayType(
            T.MapType(
                T.StringType(),
                T.DateType(),
            )
        )
    )

    adjusted_date_windows = collect_date_windows.select(
        '*',
        F.explode(
            udf_adjust_date_windows(
                F.col('array_coverage_windows')
            )
        ).alias('map_adj_coverage_windows'),
    ).select(
        '*',
        F.col('map_adj_coverage_windows')['date_start'].alias('date_start'),
        F.col('map_adj_coverage_windows')['date_end'].alias('date_end'),
    )

    decouple_overlapping_coverage = decouple_common_windows(
        adjusted_date_windows,
        'member_id',
        'date_start',
        'date_end',
    ).select(
        'member_id',
        'date_start',
        F.least(
            F.col('date_end'),
            F.lit(
                datetime.date(performance_yearstart.year, 12, 31)
            )
        ).alias('date_end'),
    ).withColumn(
        'days_covered',
        F.datediff(
            F.col('date_end'),
            F.col('date_start'),
            ) + 1,
    )

    member_coverage_summ = decouple_overlapping_coverage.groupby(
        'member_id',
    ).agg(
        F.min('date_start').alias('ipsd'),
        F.sum('days_covered').alias('days_covered'),
    ).select(
        '*',
        spark_funcs.datediff(
            F.lit(datetime.date(performance_yearstart.year, 12, 31)),
            F.col('ipsd')
        ).alias('treatment_period'),
    ).withColumn(
        'days_covered',
        F.least(
            F.col('days_covered'),
            F.col('treatment_period')
        )
    ).withColumn(
        'pdc',
        F.round(
            (F.col('days_covered') / F.col('treatment_period')),
            2
        )
    )

    return member_coverage_summ


def calculate_denominator(dfs_input, measurement_date_end):
    # do some general filtering
    filtered_data_dict = _initial_filtering(
        dfs_input,
        measurement_date_end
    )

    # do some exclusionary filtering
    exc_filtered_data_dict = _exclusionary_filtering(
        dfs_input,
        filtered_data_dict,
        measurement_date_end
    )

    # do some filtering based on "events"
    event_filtered_data_dict = _event_filtering(
        dfs_input,
        exc_filtered_data_dict,
        measurement_date_end
    )

    return event_filtered_data_dict


def calculate_numerator(rx_data, measurement_date_end):
    return _calculate_rates(rx_data, measurement_date_end)


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

        denominator = calculate_denominator(dfs_input, measurement_date_end)

        numerator = calculate_numerator(denominator['rx'], measurement_date_end)

        result = numerator/denominator


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

    # ------------------------------------------------------------------
    mma_decorator = MMA()
    result = mma_decorator.calc_decorator(DFS_INPUT_MED)
