
import os
import re
import io 
import nbformat as nbf
from jinja2 import Environment, FileSystemLoader
from sysconfig import get_paths
from pathlib import Path



@staticmethod
def GetIncludeDir():
    ChkPath = Path(get_paths()['purelib']) / Path(f'dbt/include/fabricsparknb/')
    # print(ChkPath)
    # Does Check for the path
    if os.path.exists(ChkPath):
        return ChkPath
    else:        
        path = Path(os.getcwd()) / Path('dbt/include/fabricsparknb/')
        # print(str(path))
        return (path)

class ModelNotebook:
    def __init__(self, nb : nbf.NotebookNode = None, node_type='model'):
        
        if nb is None:
            filename = str((GetIncludeDir()) / Path(f'notebooks/{node_type}_notebook.ipynb'))            
            if os.path.exists(filename):
                with io.open(file=filename, mode='r', encoding='utf-8') as f:
                    file_str = f.read()
                    nb = nbf.reads(file_str, as_version=4)
            else: 
                raise Exception(f"Notebook file {filename} does not exist")

        self.nb: nbf.NotebookNode = nb
        self.sql: list[str] = []

    def AddSql(self, sql):
        self.sql.append(sql)

    def AddCell(self, cell):
        # Add the cell to the notebook
        self.nb.cells.append(cell)

    def GatherSql(self):
        # Concatenate all the SQL cells in the notebook
        self.sql = []
        for cell in self.GetSparkSqlCells():
            self.sql.append(cell.source.replace("%%sql", ""))

    def SetTheSqlVariable(self):
        # Find the first code cell and set the sql variable
        for i, cell in enumerate(self.nb.cells):
            if cell.cell_type == 'markdown' and "# Declare and Execute SQL Statements" in cell.source:
                x = 1
                for sql in self.sql:
                    # Create a new code cell
                    new_cell = nbf.v4.new_code_cell(source="sql = '''" + sql + "'''\n" + "spark.sql(sql)")
                    # Insert the new cell into the middle of the notebook
                    self.nb.cells.insert((i + x), new_cell)
                    x += 1
                break

    def GetSparkSqlCells(self):
        # Get the existing SQL Cell from the notebook. It will be the code cell following the markdown cell containing "# SPARK SQL Cell for Debugging"
        spark_sql_cells = []
        for i, cell in enumerate(self.nb.cells):
            if cell.cell_type == 'markdown' and "# SPARK SQL Cells for Debugging" in cell.source:
                spark_sql_cells = self.nb.cells[(i + 1):len(self.nb.cells)]

        return spark_sql_cells

    def Render(self):        
        # Define the directory containing the Jinja templates
        template_dir = str((GetIncludeDir()) / Path('notebooks/'))
        if os.path.exists(template_dir):
            # Create a Jinja environment
            env = Environment(loader=FileSystemLoader(template_dir))

            # Load the template
            template = env.get_template('model_notebook.ipynb')

            # Render the template with the notebook_file variable
            rendered_template = template.render()

            # Parse the rendered template as a notebook
            self.nb = nbf.reads(rendered_template, as_version=4)
        else:
            raise Exception(f"Directory {template_dir} does not exist")
