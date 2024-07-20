import time
import typer
from typing_extensions import Annotated
from rich.console import Console
from rich.theme import Theme
import os
import shutil
from dbt_wrapper.wrapper import Commands
from rich.progress import Progress, SpinnerColumn, TextColumn

app = typer.Typer(no_args_is_help=True)

custom_theme = Theme({"info": "dim cyan", "warning": "magenta", "danger": "bold red", "debug": "grey82"})

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
    ] = True
):
    """
    This tba.
    """
    wrapper_commands.GetDbtConfigs(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir)
    perform_stage(option=clean_target_dir, action_callables=[wrapper_commands.CleanProjectTargetDirectory], stage_name="Clean Target")

    action_callables = [
        lambda **kwargs: wrapper_commands.GeneratePreDbtScripts(PreInstall=pre_install, **kwargs),
        lambda **kwargs: wrapper_commands.ConvertNotebooksToFabricFormat(**kwargs),
    ]
    perform_stage(option=generate_pre_dbt_scripts, action_callables=action_callables, stage_name="Generate Pre-DBT Scripts")

    perform_stage(option=auto_execute_metadata_extract, action_callables=[wrapper_commands.RunMetadataExtract], stage_name="Auto Execute Metadata Extract")

    perform_stage(option=download_metadata, action_callables=[wrapper_commands.DownloadMetadata], stage_name="Download Metadata")

    if (build_dbt_project):
        wrapper_commands.BuildDbtProject(PreInstall=pre_install)

    action_callables = [
        lambda **kwargs: wrapper_commands.GeneratePostDbtScripts(PreInstall=pre_install, **kwargs),
        lambda **kwargs: wrapper_commands.ConvertNotebooksToFabricFormat(**kwargs)
    ]
    perform_stage(option=generate_post_dbt_scripts, action_callables=action_callables, stage_name="Generate Post-DBT Scripts")    

    perform_stage(option=upload_notebooks_via_api, action_callables=[wrapper_commands.AutoUploadNotebooksViaApi], stage_name="Upload Notebooks via API")


if __name__ == "__main__":
    app()


def perform_stage(option, action_callables, stage_name):    
    with Progress(
            SpinnerColumn(spinner_name="dots", style="progress.spinner", finished_text="✅"),
            TextColumn("[progress.description]{task.description}"), transient=False,
            console=console
    ) as progress:
        stage_status = "Initiated"
        ptid = progress.add_task(description=f"Stage: {stage_name} - {stage_status}", total=1)
        start = time.time()
        if option:
            stage_status = "Running"
            progress.update(task_id=ptid, description=f"Stage: {stage_name} - {stage_status}")
            for action_callable in action_callables:
                action_callable(progress=progress, task_id=ptid)
            stage_status = "Completed"
            progress.update(task_id=ptid, description=f"Stage: {stage_name} - {stage_status}")
        else:
            stage_status = "Skipped"
            progress.update(task_id=ptid, description=f"Stage: {stage_name} - {stage_status}")
        runtime = time.time() - start
        runtime_str = time.strftime("%H:%M:%S", time.gmtime(runtime))
        time.sleep(1)  # Simulate some delay
        progress.update(task_id=ptid, description=f"Stage: {stage_name} - {stage_status} - Total Runtime: {runtime_str}", completed=1)
