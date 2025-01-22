from dbt.adapters.base import AdapterPlugin
from dbt.adapters.databricksnb.credentials import DatabricksCredentials
from dbt.adapters.databricksnb.impl import DatabricksAdapter
from dbt.include import databricks

Plugin = AdapterPlugin(
    adapter=DatabricksAdapter,  # type: ignore[arg-type]
    credentials=DatabricksCredentials,
    include_path=databricks.PACKAGE_PATH,
    dependencies=["spark"],
)
