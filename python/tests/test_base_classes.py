"""
### CODE OWNERS: Alexander Olivero

### OBJECTIVE:
  Test the methods of a QualityMeasure object

### DEVELOPER NOTES:
  <none>
"""
import pytest

from pyspark.sql import Row

from ebm_hedis_etc.base_classes import QualityMeasure

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def test_qualitymeasure():
    """"""
    class test_good_QM(QualityMeasure):
        def _calc_measure(
                self,
                dfs_input: None,
                performance_yearstart: None
        ):
            return spark_app.session.createDataFrame(
                [Row(**{'member_id': 'membera', 'comp_quality_short': 'quality measure 1',
                        'comp_quality_numerator': 1})]
            )

    class test_bad_QM(QualityMeasure):
        def _calc_measure(
                self,
                dfs_input: None,
                performance_yearstart: None
        ):
            return spark_app.session.createDataFrame(
                [Row(**{'member_id': 'membera', 'short': 'quality measure 1',
                        'comp_quality_numerator': 1})]
            )

    test_qm = test_good_QM()
    assert test_qm.key_fields == ['member_id']
    test_results = test_qm.calc_measure(None, None)

    bad_qm = test_bad_QM()
    assert bad_qm.key_fields == ['member_id']
    with pytest.raises(AssertionError):
        bad_results = bad_qm.calc_measure(None, None)
