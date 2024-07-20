import typer
from typing_extensions import Annotated
from rich.console import Console
from rich.theme import Theme
import os
import shutil
from dbt_wrapper.wrapper import Commands

app = typer.Typer(no_args_is_help=True)

custom_theme = Theme({"info": "dim cyan", "warning": "magenta", "danger": "bold red"})

console = Console(theme=custom_theme)

wrapper_commands = Commands(console=console)


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
    wrapper_commands.BuildDbtProject(PreInstall=pre_install)


@app.command()
def docs():
    """
    This command will generate the documentation for the dbt project.
    """
    print(f"Goodbye")


@app.command()
def run_all(
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
    clean_target_dir: Annotated[
        bool,
        typer.Option(
            help="The option to automatically refresh metadata before dbt project build."
        ),
    ] = True,
    generate_pre_dbt_scripts: Annotated[
        bool,
        typer.Option(
            help="The option to automatically refresh metadata before dbt project build."
        ),
    ] = True,
    generate_post_dbt_scripts: Annotated[
        bool,
        typer.Option(
            help="The option to automatically refresh metadata before dbt project build."
        ),
    ] = True,
    auto_execute_metadata_extract: Annotated[
        bool,
        typer.Option(
            help="The option to automatically refresh metadata before dbt project build."
        ),
    ] = True,
    download_metadata: Annotated[
        bool,
        typer.Option(
            help="The option to automatically download metadata before dbt project build."
        ),
    ] = True,
    build_dbt_project: Annotated[
        bool,
        typer.Option(
            help="The option to suppress build the dbt project."
        ),
    ] = True,
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
    ] = True,
):
    """
    This tba.
    """
    wrapper_commands.GetDbtConfigs(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir)
    if (clean_target_dir):
        wrapper_commands.CleanProjectTargetDirectory()
    if (generate_pre_dbt_scripts):
        wrapper_commands.GeneratePreDbtScripts(PreInstall=pre_install)
        wrapper_commands.ConvertNotebooksToFabricFormat()
    if (auto_execute_metadata_extract):
        wrapper_commands.RunMetadataExtract()        
    if (download_metadata):
        wrapper_commands.DownloadMetadata()
    if (build_dbt_project):
        wrapper_commands.BuildDbtProject(PreInstall=pre_install)
    if (generate_post_dbt_scripts):
        wrapper_commands.GeneratePostDbtScripts(PreInstall=pre_install)
        wrapper_commands.ConvertNotebooksToFabricFormat()
    if (upload_notebooks_via_api):
        wrapper_commands.AutoUploadNotebooksViaApi()

if __name__ == "__main__":
    app()
