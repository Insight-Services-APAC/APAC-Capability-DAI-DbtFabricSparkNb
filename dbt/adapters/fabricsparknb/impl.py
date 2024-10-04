# Document this file including a listing of methods and classes, and a description of the file's purpose.

# This file contains the implementation of the SparkAdapter class, which is a subclass of the SQLAdapter class. 
# The SparkAdapter class is used to interact with Spark databases. 
# The SparkAdapter class provides methods for executing SQL queries, listing schemas, listing relations, getting columns in a relation, and getting a relation. The SparkAdapter class also provides methods for converting agate table types to Spark types, and for converting agate table columns to Spark columns. The SparkAdapter class also provides methods for parsing relation information, and for building Spark relations. The SparkAdapter class also provides methods for getting the catalog, and for checking if a schema exists. The SparkAdapter class also provides methods for getting rows different SQL, and for standardizing grants dictionaries. The SparkAdapter class also provides a method for debugging queries.

# Create a mermaid class diagram:
# classDiagram
#     class FabricSparkAdapter {
#         -COLUMN_NAMES
#         -INFORMATION_COLUMNS_REGEX
#         -INFORMATION_OWNER_REGEX
#         -INFORMATION_STATISTICS_REGEX
#         -HUDI_METADATA_COLUMNS
#         -CONSTRAINT_SUPPORT
#         -Relation
#         -RelationInfo
#         -Column
#         -ConnectionManager
#         -AdapterSpecificConfigs
#         +date_function()
#         +convert_text_type()

#         +convert_number_type()
#         +convert_integer_type()
#         +convert_date_type()
#         +convert_time_type()
#         +convert_datetime_type()
#         +quote()
#         +_get_relation_information()
#         +_get_relation_information_using_describe()
#         +_build_spark_relation_list()
#         +get_relation()
#         +parse_describe_extended()
#         +find_table_information_separator()
#         +get_columns_in_relation()
#         +parse_columns_from_information()
#         +_get_columns_for_catalog()
#         +get_catalog()
#         +execute()
#         +list_schemas()
#         +check_schema_exists()
#         +get_rows_different_sql()
#         +standardize_grants_dict()
#         +debug_query()
#     }









from gettext import Catalog
import dbt.adapters.fabricspark.connections as fs_connections
import dbt.adapters.fabricsparknb.catalog as catalog
from dbt.adapters.base.connections import  AdapterResponse
import re
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Union, Tuple, Callable, Set
from dbt.adapters.base.relation import InformationSchema
from dbt.contracts.graph.manifest import Manifest
from typing_extensions import TypeAlias
import agate
import dbt
import dbt.exceptions
from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.impl import catch_as_completed, ConstraintSupport
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.fabricsparknb.connections import SparkConnectionManager
from dbt.adapters.fabricspark.relation import SparkRelation
from dbt.adapters.fabricspark.column import SparkColumn
from dbt.adapters.base import BaseRelation
from dbt.clients.agate_helper import DEFAULT_TYPE_TESTER
from dbt.contracts.graph.nodes import ConstraintType
from dbt.contracts.relation import RelationType
from dbt.events import AdapterLogger
from dbt.utils import executor, AttrDict
import dbt.adapters.fabricsparknb.livysession as livysession
import json

logger = AdapterLogger("fabricsparknb")

GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME = "get_columns_in_relation_raw"
LIST_SCHEMAS_MACRO_NAME = "list_schemas"
LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"
LIST_RELATIONS_SHOW_TABLES_MACRO_NAME = "list_relations_show_tables_without_caching"
DESCRIBE_TABLE_EXTENDED_MACRO_NAME = "describe_table_extended_without_caching"

KEY_TABLE_OWNER = "Owner"
KEY_TABLE_STATISTICS = "Statistics"

TABLE_OR_VIEW_NOT_FOUND_MESSAGES = (
    "[TABLE_OR_VIEW_NOT_FOUND]",
    "Table or view not found",
    "NoSuchTableException",
)


@dataclass
class SparkConfig(AdapterConfig):
    file_format: str = "parquet"
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None


class SparkAdapter(SQLAdapter):
    
    #allow me to modify init method
    def __init__(self,config) -> None:
        super().__init__(config)
    
    COLUMN_NAMES = (
        "table_database",
        "table_schema",
        "table_name",
        "table_type",
        "table_comment",
        "table_owner",
        "column_name",
        "column_index",
        "column_type",
        "column_comment",
        "stats:bytes:label",
        "stats:bytes:value",
        "stats:bytes:description",
        "stats:bytes:include",
        "stats:rows:label",
        "stats:rows:value",
        "stats:rows:description",
        "stats:rows:include",
    )
    INFORMATION_COLUMNS_REGEX = re.compile(r"^ \|-- (.*): (.*) \(nullable = (.*)\b", re.MULTILINE)
    INFORMATION_OWNER_REGEX = re.compile(r"^Owner: (.*)$", re.MULTILINE)
    INFORMATION_STATISTICS_REGEX = re.compile(r"^Statistics: (.*)$", re.MULTILINE)

    HUDI_METADATA_COLUMNS = [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
    ]

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.not_null: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_ENFORCED,
    }

    Relation: TypeAlias = SparkRelation
    RelationInfo = Tuple[str, str, str]
    Column: TypeAlias = SparkColumn
    ConnectionManager: TypeAlias = SparkConnectionManager
    AdapterSpecificConfigs: TypeAlias = SparkConfig

    @classmethod
    def date_function(cls) -> str:
        return "current_timestamp()"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "bigint"

    @classmethod
    def convert_integer_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "bigint"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "time"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp"

    def quote(self, identifier: str) -> str:  # type: ignore
        return "`{}`".format(identifier)

    def _get_relation_information(self, row: agate.Row) -> RelationInfo:
        """relation info was fetched with SHOW TABLES EXTENDED"""
        try:
            _schema = row['namespace']
            name = row['tableName']
            information = row['information']
        except ValueError:
            msg:str = ""
            for r in row:
                    msg += str(r) + "; "
            raise dbt.exceptions.DbtRuntimeError(
                f'Invalid value from "show tables extended ...", got {len(row)} values, expected 4 {msg}'
            )

        return _schema, name, information

    def _get_relation_information_using_describe(self, row: agate.Row) -> RelationInfo:
        """Relation info fetched using SHOW TABLES and an auxiliary DESCRIBE statement"""
        try:
            _schema, name, _ = row
        except ValueError:
            raise dbt.exceptions.DbtRuntimeError(
                f'Invalid value from "show tables ...", got {len(row)} values, expected 3'
            )

        table_name = f"{_schema}.{name}"
        try:
            table_results = self.execute_macro(
                DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs={"table_name": table_name}
            )
        except dbt.exceptions.DbtRuntimeError as e:
            logger.debug(f"Error while retrieving information about {table_name}: {e.msg}")
            table_results = AttrDict()

        information = ""
        for info_row in table_results:
            info_type, info_value, _ = info_row
            if not info_type.startswith("#"):
                information += f"{info_type}: {info_value}\n"

        return _schema, name, information

    def _build_spark_relation_list(
        self,
        row_list: agate.Table,
        relation_info_func: Callable[[agate.Row], RelationInfo],
    ) -> List[BaseRelation]:
        """Aggregate relations with format metadata included."""
        relations = []
        for row in row_list:
            _schema, name, information = relation_info_func(row)

            rel_type: RelationType = (
                RelationType.View if "Type: VIEW" in information else RelationType.Table
            )
            is_delta: bool = "Provider: delta" in information
            is_hudi: bool = "Provider: hudi" in information
            is_iceberg: bool = "Provider: iceberg" in information

            relation: BaseRelation = self.Relation.create(
                schema=_schema,
                identifier=name,
                type=rel_type,
                information=information,
                is_delta=is_delta,
                is_iceberg=is_iceberg,
                is_hudi=is_hudi,
            )
            relations.append(relation)

        return relations

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        """Distinct Spark compute engines may not support the same SQL featureset. Thus, we must
        try different methods to fetch relation information."""

        kwargs = {"schema_relation": schema_relation}

        try:
            # Default compute engine behavior: show tables extended
            show_table_extended_rows = catalog.ListRelations(self.config) #self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)
            rlist = self._build_spark_relation_list(
                row_list=show_table_extended_rows,
                relation_info_func=self._get_relation_information,
            )
            return rlist
        except dbt.exceptions.DbtRuntimeError as e:
            errmsg = getattr(e, "msg", "")
            if f"Database '{schema_relation}' not found" in errmsg:
                return []
            # Iceberg compute engine behavior: show table
            elif "SHOW TABLE EXTENDED is not supported for v2 tables" in errmsg:
                # this happens with spark-iceberg with v2 iceberg tables
                # https://issues.apache.org/jira/browse/SPARK-33393
                try:
                    # Iceberg behavior: 3-row result of relations obtained
                    show_table_rows = self.execute_macro(
                        LIST_RELATIONS_SHOW_TABLES_MACRO_NAME, kwargs=kwargs
                    )
                    return self._build_spark_relation_list(
                        row_list=show_table_rows,
                        relation_info_func=self._get_relation_information_using_describe,
                    )
                except dbt.exceptions.DbtRuntimeError as e:
                    description = "Error while retrieving information about"
                    logger.debug(f"{description} {schema_relation}: {e.msg}")
                    return []
            else:
                logger.debug(
                    f"Error while retrieving information about {schema_relation}: {errmsg}"
                )
                return []


    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        if not self.Relation.get_default_include_policy().database:
            database = None  # type: ignore

        #return super().get_relation(database, schema, identifier)
    
        relations_list = self.list_relations(database, schema)

        matches = self._make_match(relations_list, database, schema, identifier)

        if len(matches) > 1:
            kwargs = {
                "identifier": identifier,
                "schema": schema,
                "database": database,
            }
            raise dbt.exceptions.RelationReturnedMultipleResultsError(kwargs, matches)

        elif matches:
            return matches[0]

        return None

    def parse_describe_extended(
        self, relation: BaseRelation, raw_rows: AttrDict
    ) -> List[SparkColumn]:
        # Convert the Row to a dict
        dict_rows = [dict(zip(row._keys, row._values)) for row in raw_rows]
        # Find the separator between the rows and the metadata provided
        # by the DESCRIBE TABLE EXTENDED statement
        pos = self.find_table_information_separator(dict_rows)

        # Remove rows that start with a hash, they are comments
        rows = [row for row in raw_rows[0:pos] if not row["col_name"].startswith("#")]
        metadata = {col["col_name"]: col["data_type"] for col in raw_rows[pos + 1 :]}

        raw_table_stats = metadata.get(KEY_TABLE_STATISTICS)
        table_stats = SparkColumn.convert_table_stats(raw_table_stats)
        return [
            SparkColumn(
                table_database=None,
                table_schema=relation.schema,
                table_name=relation.name,
                table_type=relation.type,
                table_owner=str(metadata.get(KEY_TABLE_OWNER)),
                table_stats=table_stats,
                column=column["col_name"],
                column_index=idx,
                dtype=column["data_type"],
            )
            for idx, column in enumerate(rows)
        ]

    @staticmethod
    def find_table_information_separator(rows: List[dict]) -> int:
        pos = 0
        for row in rows:
            if not row["col_name"] or row["col_name"].startswith("#"):
                break
            pos += 1
        return pos

    def get_columns_in_relation(self, relation: BaseRelation) -> List[SparkColumn]:
        
        columns = []        
        try:
            rows: AttrDict = catalog.GetColumnsInRelation(self.config, relation.schema, relation.identifier)
            columns = self.parse_describe_extended(relation, rows)
        except dbt.exceptions.DbtRuntimeError as e:
            # spark would throw error when table doesn't exist, where other
            # CDW would just return and empty list, normalizing the behavior here
            errmsg = getattr(e, "msg", "")
            found_msgs = (msg in errmsg for msg in TABLE_OR_VIEW_NOT_FOUND_MESSAGES)
            if any(found_msgs):
                pass
            else:
                raise e

        # strip hudi metadata columns.
        columns = [x for x in columns if x.name not in self.HUDI_METADATA_COLUMNS]
        return columns

    def parse_columns_from_information(self, relation: BaseRelation) -> List[SparkColumn]:
        if hasattr(relation, "information"):
            information = relation.information or ""
        else:
            information = ""
        owner_match = re.findall(self.INFORMATION_OWNER_REGEX, information)
        owner = owner_match[0] if owner_match else None
        matches = re.finditer(self.INFORMATION_COLUMNS_REGEX, information)
        columns = []
        stats_match = re.findall(self.INFORMATION_STATISTICS_REGEX, information)
        raw_table_stats = stats_match[0] if stats_match else None
        table_stats = SparkColumn.convert_table_stats(raw_table_stats)
        for match_num, match in enumerate(matches):
            column_name, column_type, nullable = match.groups()
            column = SparkColumn(
                table_database=None,
                table_schema=relation.schema,
                table_name=relation.table,
                table_type=relation.type,
                column_index=match_num,
                table_owner=owner,
                column=column_name,
                dtype=column_type,
                table_stats=table_stats,
            )
            columns.append(column)
        return columns

    def _get_columns_for_catalog(self, relation: BaseRelation) -> Iterable[Dict[str, Any]]:
        table_name = f"{relation.schema}.{relation.identifier}"
        columns = []
        try:
            rows: AttrDict = catalog.GetColumnsInRelation(self.config, relation.schema, relation.identifier)
            columns = self.parse_describe_extended(relation, rows)
        except dbt.exceptions.DbtRuntimeError as e:
            logger.debug(f"Error while retrieving information about {table_name}: {e.msg}")
            raise e

        for column in columns:
            # convert SparkColumns into catalog dicts
            as_dict = column.to_column_dict()
            as_dict["column_name"] = as_dict.pop("column", None)
            as_dict["column_type"] = as_dict.pop("dtype")
            as_dict["table_database"] = None
            yield as_dict

    def get_catalog(
        self, manifest: Manifest, selected_nodes: Optional[Set] = None
    ) -> Tuple[agate.Table, List[Exception]]:
        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            raise dbt.exceptions.CompilationError(
                f"Expected only one database in get_catalog, found " f"{list(schema_map)}"
            )

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(
                        tpe.submit_connected(
                            self,
                            schema,
                            self._get_one_catalog,
                            info,
                            [schema],
                            manifest,
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        manifest: Manifest,
    ) -> agate.Table:
        if len(schemas) != 1:
            raise dbt.exceptions.CompilationError(
                f"Expected only one schema in spark _get_one_catalog, found " f"{schemas}"
            )

        database = information_schema.database
        
        schema = list(schemas)[0]
        #logger.debug("Datalake name is ", schema)
        columns: List[Dict[str, Any]] = []
        for relation in self.list_relations(database, schema):
            logger.debug("Getting table schema for relation {}", str(relation))
            columns_to_add = self._get_columns_for_catalog(relation)
            columns.extend(columns_to_add)
        return agate.Table.from_object(columns, column_types=DEFAULT_TYPE_TESTER)


    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False, limit: Optional[int] = None
    ) -> Tuple[AdapterResponse, agate.Table]:
        """Execute the given SQL. This is a thin wrapper around
        ConnectionManager.execute.

        :param str sql: The sql to execute.
        :param bool auto_begin: If set, and dbt is not currently inside a
            transaction, automatically begin one.
        :param bool fetch: If set, fetch results.
        :param Optional[int] limit: If set, only fetch n number of rows
        :return: A tuple of the query status and results (empty if fetch=False).
        :rtype: Tuple[AdapterResponse, agate.Table]
        """
        # Convert self.config to a JSON string
        project_root = (self.config.project_root).replace('\\', '/')

        # Inject the JSON into the SQL as a comment
        sql = '/*{"project_root": "'+ project_root + '"}*/' + f'\n{sql}'
        
        #print("sql is ", sql)

        return self.connections.execute(sql=sql, auto_begin=auto_begin, fetch=fetch, limit=limit)
    
    def list_schemas(self, database: str) -> List[str]:
        results = catalog.ListSchemas(profile=self.config)

        return [row[0] for row in results]

    def check_schema_exists(self, database: str, schema: str) -> bool:
        #logger.debug("Datalake name is ", schema)
        schema = schema.lower()
        results = catalog.ListSchema(profile=self.config, schema=schema)

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql

    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        grants_dict: Dict[str, List[str]] = {}
        for row in grants_table:
            grantee = row["Principal"]
            privilege = row["ActionType"]
            object_type = row["ObjectType"]

            # we only want to consider grants on this object
            # (view or table both appear as 'TABLE')
            # and we don't want to consider the OWN privilege
            if object_type == "TABLE" and privilege != "OWN":
                if privilege in grants_dict.keys():
                    grants_dict[privilege].append(grantee)
                else:
                    grants_dict.update({privilege: [grantee]})
        return grants_dict

    def debug_query(self) -> None:
        """Override for DebugTask method"""
        self.execute("select 1 as id")

    def __exit__(self, exc_type, exc_value, traceback):
        pass

# spark does something interesting with joins when both tables have the same
# static values for the join condition and complains that the join condition is
# "trivial". Which is true, though it seems like an unreasonable cause for
# failure! It also doesn't like the `from foo, bar` syntax as opposed to
# `from foo cross join bar`.
COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} EXCEPT
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} EXCEPT
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a
    cross join table_b
)
select
    INT(row_count_diff.difference) as row_count_difference,
    INT(diff_count.num_missing) as num_mismatched
from row_count_diff
cross join diff_count
""".strip()
