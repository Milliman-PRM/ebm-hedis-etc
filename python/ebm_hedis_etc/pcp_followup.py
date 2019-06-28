"""
### CODE OWNERS: Austin Campbell

### OBJECTIVE:
     Calculate some Non-HEDIS Follow-Up Measures

### DEVELOPER NOTES:
  <none>
"""
import logging
import datetime

import pyspark.sql.functions as spark_funcs
from pyspark.sql import DataFrame
from ebm_hedis_etc.base_classes import QualityMeasure

LOGGER = logging.getLogger(__name__)

# pylint does not recognize many of the spark functions
# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class PCP_Followup(QualityMeasure):
    """Object to house logic to calculate pcp followup measures"""

    def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart=datetime.date,
            **kwargs
    ) -> DataFrame:

        cutoff = kwargs['cutoff']
        quality_metric_name = '{}_day_followup'.format(cutoff)

        results_df = dfs_input['outclaims_prm'].where(
            spark_funcs.lower(spark_funcs.col('prm_line')).isin('i11', 'i12', 'i13')
        ).select(
            'claimid',
            'member_id',
            'prm_pcp_followup_success_yn',
            'prm_pcp_followup_potential_yn',
            'prm_pcp_followup_days_since'
        ).distinct().select(
            'member_id',
            spark_funcs.lit(quality_metric_name).alias('comp_quality_short'),
            spark_funcs.when(
                (spark_funcs.col('prm_pcp_followup_success_yn') == 'Y')
                & (spark_funcs.col('prm_pcp_followup_days_since').between(0, cutoff)),
                spark_funcs.lit(1)
            ).otherwise(
                spark_funcs.lit(0)
            ).alias('comp_quality_numerator'),
            spark_funcs.when(
                spark_funcs.col('prm_pcp_followup_potential_yn') == 'Y',
                spark_funcs.lit(1)
            ).otherwise(
                spark_funcs.lit(0)
            ).alias('comp_quality_denominator'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_date_last'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_date_actionable'),
            spark_funcs.lit(None).cast('string').alias('comp_quality_comments')
        )

        return results_df
