import findspark  # this needs to be the first import
import logging
import pytest
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession

findspark.find()


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object

    """
    conf = (SparkConf().setMaster("local[2]").setAppName("pytest-pyspark-local-testing"))
    sc = SparkContext.getOrCreate(conf=conf)
    request.addfinalizer(lambda: sc.stop())

    quiet_py4j()
    return sc


@pytest.fixture(scope="session")
def sql_context(spark_context):
    return SQLContext(spark_context)


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.appName('test').getOrCreate()


@pytest.fixture(scope="module")
def clean_session(spark_session):
    spark_session.catalog.clearCache()
