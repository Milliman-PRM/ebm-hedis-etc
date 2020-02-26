"""
### CODE OWNERS: Alexander Olivero, Chas Busenburg

### OBJECTIVE:
  Test the methods of a QualityMeasure object

### DEVELOPER NOTES:
  <none>
"""
# pylint: disable=abstract-class-instantiated
# pylint: disable=abstract-method
import datetime

import pytest
from ebm_hedis_etc.base_classes import QualityMeasure
from pyspark.sql import Row

# pylint: disable=unused-variable

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def test_qualitymeasure(spark_app):
    """Tests the methods of a QualityMeasure object"""

    class DummyGoodQM(QualityMeasure):
        """Dummy quality measure to pass test"""

        def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart=datetime.date,
            **kwargs
        ):
            return spark_app.session.createDataFrame(
                [
                    Row(
                        **{
                            "member_id": "membera",
                            "comp_quality_short": "quality measure 1",
                            "comp_quality_numerator": 1,
                        }
                    )
                ]
            )

    class DummyBadQM(QualityMeasure):
        """Dummy quality measure to fail test"""

        def _calc_measure(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            performance_yearstart=datetime.date,
            **kwargs
        ):
            return spark_app.session.createDataFrame(
                [
                    Row(
                        **{
                            "member_id": "membera",
                            "short": "quality measure 1",
                            "comp_quality_numerator": 1,
                        }
                    )
                ]
            )

    class DummyNoMethodQM(QualityMeasure):
        """Dummy quality measure to fail from lack of method"""

    test_qm = DummyGoodQM()
    assert test_qm.key_fields == ["member_id"]
    test_results = test_qm.calc_measure(None, None)

    bad_qm = DummyBadQM()
    assert bad_qm.key_fields == ["member_id"]
    with pytest.raises(AssertionError):
        bad_results = bad_qm.calc_measure(None, None)

    with pytest.raises(TypeError):
        no_method_qm = DummyNoMethodQM()
