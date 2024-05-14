
from dbt.adapters.fabricsparknb.fabric_spark_credentials import SparkCredentials # noqa
from dbt.adapters.fabricsparknb.impl import SparkAdapter
from dbt.adapters.base import AdapterPlugin
from dbt.include import fabricsparknb

Plugin = AdapterPlugin(
    adapter=SparkAdapter, 
    credentials=SparkCredentials, 
    include_path=fabricsparknb.PACKAGE_PATH, # type: ignore
    dependencies=["fabricspark"]
)
