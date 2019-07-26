"""
### CODE OWNERS: Shea Parkes, Kyle Baird

### OBJECTIVE:
    Customize unit testing environment.

### DEVELOPER NOTES:
    Will be auto-found by py.test.
    Used below to define session-scoped fixtures
"""
# pylint: disable=redefined-outer-name
import os
from pathlib import Path
from shutil import rmtree
import pytest


import prm.spark.cluster
from prm.spark.app import SparkApp
from prm.spark.shared import TESTING_APP_PARAMS

USER_PROFILE = Path(os.environ['USERPROFILE'])
PRM_LOCAL = USER_PROFILE / 'prm_local'

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


@pytest.fixture(scope='session')
def spark_cluster(request):
    """Create a local spark cluster to be shared among the tests in this directory."""
    superman = prm.spark.cluster.SparkCluster(**TESTING_APP_PARAMS)
    superman.start_cluster()

    def _cleanup_cluster():
        """Cleanup function for when we are finished."""
        superman.stop_cluster()
    request.addfinalizer(_cleanup_cluster)

    return superman


@pytest.fixture(scope='module')
def spark_app(request, spark_cluster):
    """Make a SparkApp instance for testing."""
    try:
        SparkApp.implant_cluster(spark_cluster)
    except AssertionError:
        # Testing cluster has likely already been implanted
        pass
    testing_app = SparkApp(
        request.module.__name__,
        bypass_instance_cache=True,
        spark_sql_shuffle_partitions=TESTING_APP_PARAMS["spark_sql_shuffle_partitions"],
        allow_local_io=TESTING_APP_PARAMS["allow_local_io"],
    )

    def _cleanup_app():
        """Cleanup function for when we are finished."""
        testing_app.stop_app()
    request.addfinalizer(_cleanup_app)

    return testing_app

def pytest_sessionstart():
    '''Mock prm_params_input.txt'''
    temp_path = Path(os.environ['TEMP'])

    params_input = PRM_LOCAL / 'prm_params_Input.txt'
    params_other = PRM_LOCAL / 'prm_params_other.txt'
    params_module = PRM_LOCAL / 'prm_params_modules.txt'
    temp_dir = PRM_LOCAL / 'mock_temp'

    temp_dir.mkdir(exist_ok=True)

    output_rows = [
        ('Parameter_Name|Parameter_Value_String|Parameter_Value_Int|'
         'Parameter_Value_Date|Note'),
        'project_id||||',
        'M010_Cde||||',
        'date_crediblestart||||',
        'path_project_logs|{}|||'.format(temp_dir),
        'path_local_root|{}|||'.format(temp_path),
        'default_spark_sql_partitions|2|||',
        'pipeline_signature|ref_unit_test|||',
        ('codegen_prca_model_format|model_name_short $32.        '
         'model_name_long $128.        modpathel_name_category $16.        '
         'model_name_order best12.        model_name_format $16.|||'),
    ]

    with params_input.open('w') as out:
        out.write('\n'.join(output_rows))

    with params_other.open('w') as out:
        out.write('Parameter_Name|Parameter_Value_String|Parameter_Value_Int|' \
            'Parameter_Value_Date|Notes\n')

    data_mart = USER_PROFILE / '002_Data_Mart_Defs'
    function_library = USER_PROFILE / '008_Function_Library'
    risk_scores = USER_PROFILE / '090_Risk_Scores'
    references_product = PRM_LOCAL / 'temp_ref_product'
    staging_claims = PRM_LOCAL / 'temp_staging_claims'
    staging_membership = PRM_LOCAL / 'temp_staging_membership'
    downloads = USER_PROFILE / 'Downloads'

    module_output = [
        'Module_Name|Module_Number|Mod_Log|Mod_Error|Mod_Out|Mod_Temp|Mod_Prior|Mod_Code',
        '002_Data_Mart_Defs|002||||||{}'.format(data_mart),
        '008_Function_Library|008||||||{}'.format(function_library),
        '090_Risk_Scores|090||||||{}'.format(risk_scores),
        '015_References_Product|015|||{}|||{}'.format(references_product, downloads),
        '030_Staging_Claims|030|||{}|||{}'.format(staging_claims, downloads),
        '035_Staging_Membership|035|||{}|||{}'.format(staging_membership, downloads)
    ]

    with params_module.open('w') as out:
        out.write('\n'.join(module_output))


def pytest_sessionfinish():
    '''Remove temporary directory after tests complete'''
    temp_dir = PRM_LOCAL / 'mock_temp'
    rmtree(str(temp_dir), ignore_errors=True)
