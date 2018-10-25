"""
### CODE OWNERS: Alexander Olivero
### OBJECTIVE:
  Test reference module
### DEVELOPER NOTES:
  <none>
"""
import ebm_hedis_etc.reference

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def test_import_flatfile_references(spark_app):
    """Test that the references import cleanly"""
    refs = ebm_hedis_etc.reference.import_flatfile_references(spark_app)
    for _df in refs.values():
        # Fully realize each dataframe import
        _df.cache()
        _df.count()
        _df.unpersist()
