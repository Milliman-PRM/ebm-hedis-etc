"""
### CODE OWNERS: Demerrick Moton
### OBJECTIVE:
    Implement MMA - Persistent Asthma Patients with >75% medication adherence
### DEVELOPER NOTES:
  Remove "no cover" flag after unit testing is added.
"""
import logging
import datetime
from math import floor
import re

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame

from prm.dates.windows import decouple_common_windows
from ebm_hedis_etc.base_classes import QualityMeasure
from ebm_hedis_etc.utils import find_elig_gaps

LOGGER = logging.getLogger(__name__)

# pragma: no cover
# pylint does not recognize many of the spark functions
# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _check_dx_array(dx_array, test_values, num_dx_codes=15):
    test_array = [
        F.coalesce(
            dx_array[dx_num].isin(test_values),
            F.lit(False)
        ) for dx_num in range(num_dx_codes)
    ]
    return F.array_contains(F.array(*test_array), True)


def _build_dx_array(df_columns, dx_field_name):
    """Build dx column from dataframe column list"""
    dx_column_list = [
        F.col(field) for field in
        df_columns if re.search(dx_field_name, field)
    ]
    return F.array(*dx_column_list)


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
        F.regexp_extract(
            F.col('code_system'),
            '\d+',
            0
        ).alias('diagnosis_codesystem'),
        F.regexp_replace(
            F.col('code'),
            '\.',
            ''
        ).alias('diagnosis_code')
    )
    diagnosis_valueset_list = [dx_col[2] for dx_col \
                               in diagnosis_valueset_df.collect()]

    # compare with all diags to find valid claims
    med_anydiag_df = dfs_input['claims'].withColumn(
        'diag_array',
        _build_dx_array(dfs_input['claims'].columns, 'icddiag')
    ).withColumn(
        'is_valid',
        _check_dx_array(
            F.col('diag_array'),
            diagnosis_valueset_list
        )
    ).drop(
        'diag_array'
    ).where(
        F.col('is_valid') == True
    ).join(
        F.broadcast(
            visit_valueset_df.where(
                F.col('code_system') == 'UBREV'
            )
        ),
        F.col('revcode') == visit_valueset_df.visit_code,
        'inner'
    )

    # compare with principle diags
    med_prindiag_df = dfs_input['claims'].join(
        diagnosis_valueset_df,
        [
            dfs_input['claims'].icdversion == \
            diagnosis_valueset_df.diagnosis_codesystem,
            dfs_input['claims'].icddiag1 == \
            diagnosis_valueset_df.diagnosis_code
        ],
        'inner'
    ).join(
        F.broadcast(
            visit_valueset_df.where(
                F.col('code_system') == 'UBREV'
            )
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

    rx_filtered_df = dfs_input['rx_claims'].join(
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

    # modify rx claims data for the various intake routes
    rx_route = ['oral', 'inhaler', 'injected']
    rx_df = None
    for route in rx_route:
        if rx_df is None:
            rx_df = _rx_route_events(
                rx_filtered_df,
                route
            ).select(
                F.col('member_id'),
                F.col('ndc'),
                F.col('fromdate'),
                F.col('medication_type'),
                F.col('route'),
                F.col('drug_id'),
                F.col('description'),
                F.col('dayssupply')
            )
        else:
            rx_df = rx_df.union(
                _rx_route_events(
                    rx_filtered_df,
                    route
                ).select(
                    F.col('member_id'),
                    F.col('ndc'),
                    F.col('fromdate'),
                    F.col('medication_type'),
                    F.col('route'),
                    F.col('drug_id'),
                    F.col('description'),
                    F.col('dayssupply')
                )
            )

    unique_memtime_cols = list(
        set(dfs_input['member_time'].columns).difference(
            set(dfs_input['member'].columns)
        )
    )

    members_df = dfs_input['member'].where(
        F.abs(
            F.year(F.col('dob')) - F.year(F.lit(measurement_date_end))
        ).between(5,64)
    ).join(
        dfs_input['member_time'].select(
            F.col('member_id'),
            *unique_memtime_cols
        ).where(
            F.col('cover_medical') == 'Y'
            ),
        ['member_id'],
        'left_outer'
    )

    return {
        'med_anydiag': med_anydiag_df,
        'med_prindiag': med_prindiag_df,
        'rx': rx_df,
        'members': members_df
    }


def _exclusionary_filtering(
        dfs_input,
        filtered_data_dict,
        measurement_date_end
    ):
    # find members with more than one 45 day gap in
    # eligibility during meas. year
    gaps_df = find_elig_gaps(
        filtered_data_dict['members'],
        filtered_data_dict['members'].date_start,
        filtered_data_dict['members'].date_end
    )

    gap_exclusions_df = gaps_df.where(
        (F.col('largest_gap') >= 45) & (F.col('gap_count') > 1)
    ).select(F.col('member_id'))

    # find members with any presense of certain diagnoses
    excluded_diags_df = dfs_input['reference'].where(
        F.col('value_set_name').rlike(r'Emphysema') |
        F.col('value_set_name').rlike(r'COPD') |
        F.col('value_set_name').rlike(r'Obstructive Chronic Bronchitis') |
        F.col('value_set_name').rlike(r'Chronic Respiratory Conditions') |
        F.col('value_set_name').rlike(r'Cystic Fibrosis') |
        F.col('value_set_name').rlike(r'Acute Respiratory Failure')
    )
    excluded_diags_list = [x[2] for x in excluded_diags_df.collect()]

    valueset_exclusions_df = dfs_input['claims'].withColumn(
        'diag_array',
        _build_dx_array(dfs_input['claims'].columns, 'icddiag')
    ).withColumn(
        'is_valid',
        _check_dx_array(
            F.col('diag_array'),
            excluded_diags_list
        )
    ).drop(
        'diag_array'
    ).where(
        (F.col('is_valid') == True) & \
        (F.col('fromdate') <= F.lit(measurement_date_end))
    ).select('member_id')

    # find members who had no asthma controller medications dispensed
    # during the measurement year.
    controller_exclusions_df = filtered_data_dict['rx'].where(
        F.col('fromdate').between(
            F.lit(datetime.date(measurement_date_end.year - 1, 1, 1)),
            F.lit(measurement_date_end)
        )
    ).groupBy(
        'member_id'
    ).agg(
        F.collect_set('medication_type').alias('med_types')
    ).where(
        ~F.array_contains(F.col('med_types'), 'controller')
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

    med_any_df = filtered_data_dict['med_anydiag'].join(
        exclusions_df,
        'member_id',
        'left_anti'
    )

    med_principal_df = filtered_data_dict['med_prindiag'].join(
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
        'med_anydiag': med_any_df,
        'med_prindiag': med_principal_df,
        'rx': rx_df,
        'members': members_df
    }


def _event_filtering(dfs_input, exc_filtered_data_dict, measurement_date_end):
    """helper function that filters claims by medical and pharmacutical
       dispensing events of interest"""
    rx_event_mask_df = exc_filtered_data_dict['rx'].groupBy(
        ['member_id', 'ndc', 'fromdate', 'medication_type']
    ).count()

    med_eventmask_any_df = exc_filtered_data_dict['med_anydiag'].groupBy(
        ['member_id', 'visit_valueset_name', 'fromdate']
    ).agg(F.countDistinct(F.col('visit_valueset_name')).alias('count'))

    med_eventmask_prin_df = exc_filtered_data_dict['med_prindiag'].groupBy(
        ['member_id', 'visit_valueset_name',
         'diagnosis_valueset_name', 'fromdate']
    ).agg(F.countDistinct('visit_valueset_name').alias('count'))

    # filter data by various medical events
    ed_event_df = med_eventmask_prin_df.withColumn(
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

    acute_inp_event_df = med_eventmask_prin_df.withColumn(
        'is_elig',
        F.when(
            F.col('visit_valueset_name').rlike(r'Acute Inpatient') &
            (F.col('count') >= 1),
            True
        ).otherwise(False)
    ).select(
        F.col('member_id'),
        F.col('is_elig')
    )

    out_obs_event_med_df = med_eventmask_any_df.where(
        (F.col('visit_valueset_name').rlike(r'Outpatient') |
        F.col('visit_valueset_name').rlike(r'Observation'))
    ).groupBy('member_id', 'fromdate').agg(
        F.max('count').alias('obs_out_visit')
    ).groupBy('member_id').agg(
        F.sum('obs_out_visit').alias('unique_service_dates')
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

    # filter data by various rx dispensing events
    out_obs_event_rx_df = rx_event_mask_df.groupBy('member_id').agg(
        F.count('*').alias('unique_disp_event')
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

    asthma_disp_event_df = rx_event_mask_df.groupBy('member_id').agg(
        F.count('*').alias('unique_disp_event')
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

    included_year1_df = exc_filtered_data_dict['rx'].where(
        F.year(F.col('fromdate')) == measurement_date_end.year - 1
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
        F.col('is_included')
    ).join(
        exc_filtered_data_dict['rx'],
        'member_id',
        'left_outer'
    ).groupBy('member_id').count().where(
        F.col('count') >= 4
    ).select(
        F.col('member_id')
    )

    included_year2_df = exc_filtered_data_dict['rx'].where(
        F.year(F.col('fromdate')) == performance_yearstart.year
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
        F.col('is_included')
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
        med_eventmask_any_df.select(
            F.col('member_id')
        ),
        'member_id',
        'inner'
    ).withColumn(
        'is_elig',
        F.lit(True)
    )

    out_obs_event_df = out_obs_event_med_df.join(
        out_obs_event_rx_df,
        ['member_id', 'is_elig'],
        'inner'
    )

    elig_members_df = ed_event_df.union(
        acute_inp_event_df
    ).union(
        out_obs_event_df
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

    med_df = exc_filtered_data_dict['med_anydiag'].join(
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
        F.datediff(
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


def calculate_denominator(
        dfs_input: DataFrame,
        measurement_date_end: datetime.date
    ):
    """Calculate the numerator portion of MMA measure"""
    filtered_data_dict = _initial_filtering(
        dfs_input,
        measurement_date_end
    )

    exc_filtered_data_dict = _exclusionary_filtering(
        dfs_input,
        filtered_data_dict,
        measurement_date_end
    )

    event_filtered_data_dict = _event_filtering(
        dfs_input,
        exc_filtered_data_dict,
        measurement_date_end
    )

    return event_filtered_data_dict


def calculate_numerator(
        dfs_input: DataFrame,
        rx_data: dict,
        measurement_date_end: datetime.date
    ):
    """Calculate the denominator portion of MMA measure"""
    rx_data = rx_data.join(
        dfs_input['rx_claims'],
        ['member_id', 'ndc', 'fromdate', 'dayssupply'],
        'inner'
    )

    numer_rates = _calculate_rates(rx_data, measurement_date_end)

    return numer_rates


class MMA(QualityMeasure):
    """Class for MMA implementation"""
    def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart=datetime.date,
        ):

        measurement_date_end = datetime.date(
            performance_yearstart.year,
            12,
            31
        )

        denom_df = calculate_denominator(
            dfs_input,
            measurement_date_end
        )

        numer_df = calculate_numerator(
            dfs_input,
            denom_df['rx'],
            measurement_date_end
        )

        denom_final_df = denom_df['members'].select(
            F.col('member_id')
        ).distinct().join(
            denom_df['rx'].select(
                F.col('member_id')
            ),
            'member_id',
            'inner'
        ).join(
            denom_df['med'].select(
                F.col('member_id')
            ),
            'member_id',
            'inner'
        ).distinct().select(
            F.col('member_id'),
            F.when(
                F.col('member_id').isNotNull(),
                F.lit(1)
            ).otherwise(
                F.lit(0)
            ).alias('comp_quality_denominator'),
        )

        numer_final_df = numer_df.where(
            F.col('pdc') >= 0.75
        ).distinct().select(
            F.col('member_id'),
            F.when(
                F.col('member_id').isNotNull(),
                F.lit(1)
            ).otherwise(
                F.lit(0)
            ).alias('comp_quality_numerator'),

        )

        result_df = denom_final_df.join(
            numer_final_df,
            'member_id',
            'full'
        ).select(
            F.col('member_id'),
            F.coalesce(
                F.col('comp_quality_numerator'),
                F.lit(0)
            ).alias('comp_quality_numerator'),
            F.coalesce(
                F.col('comp_quality_denominator'),
                F.lit(0)
            ).alias('comp_quality_denominator'),
            F.lit(None).cast('string').alias('comp_quality_date_last'),
            F.lit(None).cast('string').alias('comp_quality_date_actionable'),
            F.lit('MMA').alias('comp_quality_short'),
            F.lit(None).cast('string').alias('comp_quality_comments')
        )

        return result_df
