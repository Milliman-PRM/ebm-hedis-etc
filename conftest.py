"""
### CODE OWNERS: Shea Parkes, Kyle Baird

### OBJECTIVE:
    Customize unit testing environment.

### DEVELOPER NOTES:
    Will be auto-found by py.test.
    Used below to define session-scoped fixtures
"""
# pylint: disable=redefined-outer-name
import pytest
import os
from pathlib import Path
from shutil import rmtree

import prm.spark.cluster
from prm.spark.app import SparkApp
from prm.spark.shared import TESTING_APP_PARAMS

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
    prm_local = Path(os.environ['USERPROFILE']) / 'prm_local'
    temp_path = Path(os.environ['TEMP'])

    params_input = prm_local / 'prm_params_Input.txt'
    temp_dir = prm_local / 'mock_temp'

    temp_dir.mkdir()

    output_rows = ['Parameter_Name|Parameter_Value_String|Parameter_Value_Int|' \
    'Parameter_Value_Date|Note']
    output_rows.append('project_id||||')
    output_rows.append('M010_Cde||||')
    output_rows.append('date_crediblestart||||')
    output_rows.append('path_project_logs|' + str(temp_dir) + '|||')
    output_rows.append('path_local_root|' + str(temp_path) + '|||')
    output_rows.append('default_spark_sql_partitions|2|||')
    output_rows.append('pipeline_signature|ref_unit_test|||')
    output_rows.append('codegen_prca_model_format|model_name_short $32.' \
        '        model_name_long $128.        modpathel_name_category $16.        ' \
        'model_name_order best12.        model_name_format $16.|||')

    with params_input.open('w') as out:
        for row in output_rows:
            out.write(row + '\n')

def pytest_sessionfinish():
    '''Remove temporary directory after tests complete'''
    temp_dir = Path(os.environ['USERPROFILE']) / 'prm_local'/ 'mock_temp'
    rmtree(str(temp_dir), ignore_errors=True)
    