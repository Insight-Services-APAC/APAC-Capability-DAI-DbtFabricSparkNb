from colorama import init
import dbt.adapters.fabricspark
import dbt.utils
import dbt.adapters
import dbt.adapters.fabricsparknb.mock as mock
import dbt.adapters.fabricspark.connections as fs_connections
from contextlib import contextmanager
import os
import dbt.exceptions

from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import ConnectionState, AdapterResponse
from dbt.events import AdapterLogger
from dbt.events.functions import fire_event
from dbt.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.utils import DECIMALS
from dbt.adapters.fabricsparknb.livysession import LivySessionConnectionWrapper, LivySessionManager

from dbt.contracts.connection import Connection
from dbt.dataclass_schema import StrEnum
from typing import Any, Optional, Union, Tuple, List, Generator, Iterable, Sequence
from abc import ABC, abstractmethod
import time
import json 
import re

logger = AdapterLogger("Microsoft Fabric-Spark")
for logger_name in [
    "fabricsparknb.connector",
    "botocore",
    "boto3",
    "Microsoft Fabric-Spark.connector",
]:
    logger.debug(f"Setting {logger_name} to DEBUG")
    logger.set_adapter_dependency_log_level(logger_name, "DEBUG")

NUMBERS = DECIMALS + (int, float)


class SparkConnectionMethod(StrEnum):
    LIVY = "livy"


class SparkConnectionWrapper(ABC):
    @abstractmethod
    def cursor(self) -> "SparkConnectionWrapper":
        pass

    @abstractmethod
    def cancel(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def rollback(self) -> None:
        pass

    @abstractmethod
    def fetchall(self) -> Optional[List]:
        pass

    @abstractmethod
    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        pass

    @property
    @abstractmethod
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        pass


class SparkConnectionManager(fs_connections.SparkConnectionManager):
    TYPE = "fabricsparknb"
    connection_managers = {}
    spark_version = None
    

    @classmethod
    def open(cls, connection: Connection) -> Connection:        
        """Need to override the SparkConnectionManager class to use fabric-sparknb instead of fabric-spark"""
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds = connection.credentials
        exc = None
        handle: SparkConnectionWrapper = None

        for i in range(1 + creds.connect_retries):
            try:
                if creds.method == SparkConnectionMethod.LIVY:
                    try:
                        thread_id = cls.get_thread_identifier()
                        if thread_id not in cls.connection_managers:
                            cls.connection_managers[thread_id] = LivySessionManager()
                        handle = LivySessionConnectionWrapper(
                            cls.connection_managers[thread_id].connect(creds)
                        )
                        connection.state = ConnectionState.OPEN
                        # SparkConnectionManager.fetch_spark_version(handle)
                    except Exception as ex:
                        logger.debug("Connection error: {}".format(ex))
                        connection.state = ConnectionState.FAIL
                else:
                    raise dbt.exceptions.DbtProfileError(
                        f"invalid credential method: {creds.method}"
                    )
                break
            except Exception as e:
                exc = e
                if isinstance(e, EOFError):
                    # The user almost certainly has invalid credentials.
                    # Perhaps a token expired, or something
                    msg = "Failed to connect"
                    if creds.token is not None:
                        msg += ", is your token valid?"
                    raise dbt.exceptions.FailedToConnectError(msg) from e
                retryable_message = _is_retryable_error(e)
                if retryable_message and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {retryable_message}\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                elif creds.retry_all and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {getattr(exc, 'message', 'No message')}, "
                        f"retrying due to 'retry_all' configuration "
                        f"set to true.\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                else:
                    raise dbt.exceptions.FailedToConnectError("failed to connect") from e
        else:
            raise exc  # type: ignore

        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection


    

    @classmethod
    def release(self) -> None:
        pass

    @classmethod
    def cleanup_all(self) -> None:
        for thread_id in self.connection_managers:
            livySession = self.connection_managers[thread_id]
            livySession.disconnect()

            # garbage collect these connections
        self.connection_managers.clear()

    @classmethod
    def close(cls, connection) -> None:
        try:
            # if the connection is in closed or init, there's nothing to do
            if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
                return connection

            #connection = super().close(connection)
            return connection
        except Exception as err:
            logger.debug(f"Error closing connection {err}")

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[type, str]) -> str:  # type: ignore
        """
        :param Union[type, str] type_code: The sql to execute.
            * type_code is a python type (!) in pyodbc https://github.com/mkleehammer/pyodbc/wiki/Cursor#description, and a string for other spark runtimes.
            * ignoring the type annotation on the signature for this adapter instead of updating the base class because this feels like a really special case.
        :return: stringified the cursor type_code
        :rtype: str
        """
        if isinstance(type_code, str):
            return type_code
        return type_code.__name__.upper()

    @classmethod
    def fetch_spark_version(cls, connection) -> None:
        if SparkConnectionManager.spark_version:
            return SparkConnectionManager.spark_version

        try:
            sql = "split(version(), ' ')[0] as version"
            cursor = connection.handle.cursor()
            cursor.execute(sql)
            res = cursor.fetchall()
            SparkConnectionManager.spark_version = res[0][0]

        except Exception as ex:
            # we couldn't get the spark warehouse version, default to version 2
            logger.debug(f"Cannot get spark version, defaulting to version 2. Error: {ex}")
            SparkConnectionManager.spark_version = "2"

        os.environ["DBT_SPARK_VERSION"] = SparkConnectionManager.spark_version
        logger.debug(f"SPARK VERSION {os.getenv('DBT_SPARK_VERSION')}")

    def CheckSqlForModelCommentBlock(self, sql) -> bool:
        # Extract the comments from the SQL
        comments = re.findall(r'/\*(.*?)\*/', sql, re.DOTALL)

        # Convert each comment to a JSON object
        merged_json = {}
        for comment in comments:
            try:
                json_object = json.loads(comment)
                merged_json.update(json_object)
            except json.JSONDecodeError:
                #logger.error('Could not parse comment as JSON')
                #logger.error(comment)
                pass

        if 'node_id' in merged_json.keys():
            return True
        else:
            return False

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        
        if (sql.__contains__('/*FABRICSPARKNB_ALERT:')):
            raise dbt.exceptions.DbtRuntimeError(sql)            

        connection = self.get_thread_connection()
        if (self.CheckSqlForModelCommentBlock(sql) == False):
            sql = self._add_query_comment(sql)
            sql = '/*{"project_root": "' + self.profile.project_root + '"}*/' + f'\n{sql}'

        if auto_begin and connection.transaction_open is False:
            self.begin()
        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = "{}...".format(sql[:512])
            else:
                log_sql = sql

            fire_event(SQLQuery(conn_name=connection.name, sql=log_sql))
            pre = time.time()
            query_exception = None
            cursor = connection.handle.cursor()
            #cursor.SetProfile(self.profile)

            try:
                cursor.execute(sql, bindings)
            except Exception as ex:
                query_exception = ex

            elapsed_time = time.time() - pre

            # re-raise query exception so that it propogates to dbt
            if query_exception:
                raise query_exception

            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round(elapsed_time, 2),
                )
            )

            return connection, cursor
        
def _is_retryable_error(exc: Exception) -> str:
    message = str(exc).lower()
    if "pending" in message or "temporarily_unavailable" in message:
        return str(exc)
    else:
        return ""
