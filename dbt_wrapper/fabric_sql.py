import struct
from itertools import chain, repeat
import pyodbc
from azure.identity import AzureCliCredential
from dbt_wrapper.stage_executor import ProgressConsoleWrapper
from rich.table import Table
from rich.console import Console

class FabricApiSql:
    def __init__(self, console, server, database):
        self.console = console
        self.server = server
        self.database = database    

    def ExecuteSQL(self, sql, progress: ProgressConsoleWrapper, task_id):
        credential = AzureCliCredential()  # use your authentication mechanism of choice
        sql_endpoint = f"{self.server}.datawarehouse.fabric.microsoft.com"  # copy and paste the SQL endpoint from any of the Lakehouses or Warehouses in your Fabric Workspace
     
        database = f"{self.database}"  # copy and paste the name of the Lakehouse or Warehouse you want to connect to

        connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_endpoint},1433;Database=f{database};Encrypt=Yes;TrustServerCertificate=No"

        token_object = credential.get_token("https://database.windows.net//.default")  # Retrieve an access token valid to connect to SQL databases
        token_as_bytes = bytes(token_object.token, "UTF-8") # Convert the token to a UTF-8 byte string
        encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))  # Encode the bytes to a Windows byte string
        token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes  # Package the token into a bytes object
        attrs_before = {1256: token_bytes}  # Attribute pointing to SQL_COPT_SS_ACCESS_TOKEN to pass access token to the driver

        connection = pyodbc.connect(connection_string, attrs_before=attrs_before)
        cursor = connection.cursor()
       
        cursor.execute(sql)
        rows = cursor.fetchall()
        
        # Create a table
        table = Table(title="Results")

        # Add columns to the table
        for column in cursor.description:
            table.add_column(column[0])

        # Add rows to the table
        for row in rows:
            cells = [str(cell) for cell in row]            
            table.add_row(*cells)

        # Print the table using the progress console
        progress.console.print(table)

        cursor.close()
        connection.close()