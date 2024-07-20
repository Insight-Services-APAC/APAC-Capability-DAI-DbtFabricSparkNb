import typer
from typing_extensions import Annotated
from rich.console import Console
from rich.theme import Theme
import os
import shutil
from dbt.adapters.fabricsparknb.wrapper import RunDbtProject

app = typer.Typer(no_args_is_help=True)

custom_theme = Theme({"info": "dim cyan", "warning": "magenta", "danger": "bold red"})

console = Console(theme=custom_theme)


def docs_options():
    return ["generate", "serve"]


@app.command()
def build(
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory. If left blank it will use the current directory"
        ),
    ],
    dbt_profiles_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_profile directory. If left blank it will use the users home directory followed by .dbt."
        ),
    ] = None,
    pre_install: Annotated[
        bool,
        typer.Option(
            help="The option to run the dbt adapter using source code and not the installed package."
        ),
    ] = False,
    upload_notebooks_via_api: Annotated[
        bool,
        typer.Option(
            help="The option to upload your notebooks directly via the powerbi api."
        ),
    ] = False,
):  
    """
    This command will build the dbt project and run the dbt adapter. It will also run the dbt-fabric-sparknb adapter's wrapper functions to allow generation of notebooks based on the dbt project artifacts.
    """
    if len(dbt_project_dir.replace("\\", "/").split("/")) > 1:
        console.print(
            "Warning: :file_folder: The dbt_project_dir provided is nested and not a valid dbt project directory in windows. Copying the dbt_project_dir to the samples_tests directory.",
            style="warning",
        )
        old_dbt_project_dir = dbt_project_dir.replace("\\", "/")
        dbt_project_dir = "samples_tests"
        if os.path.exists(dbt_project_dir):
            shutil.rmtree(dbt_project_dir)
        shutil.copytree(old_dbt_project_dir, dbt_project_dir)
    os.environ["DBT_PROJECT_DIR"] = dbt_project_dir
    if (dbt_profiles_dir is not None):
        os.environ["DBT_PROFILES_DIR"] = dbt_profiles_dir
    RunDbtProject(PreInstall=pre_install, Upload=upload_notebooks_via_api)


@app.command()
def docs():
    """
    This command will generate the documentation for the dbt project.
    """
    print(f"Goodbye")


if __name__ == "__main__":
    app()
