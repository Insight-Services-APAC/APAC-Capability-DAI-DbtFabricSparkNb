from colorama import init
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

def _is_retryable_error(exc: Exception) -> str:
    message = str(exc).lower()
    if "pending" in message or "temporarily_unavailable" in message:
        return str(exc)
    else:
        return ""
