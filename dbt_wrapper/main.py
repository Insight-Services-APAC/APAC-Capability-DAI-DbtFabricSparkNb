from typing import Optional, List
import typer
from typing_extensions import Annotated
from rich.console import Console
from rich.theme import Theme
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm
from pathlib import Path
from dbt_wrapper.wrapper import Commands
from rich import print
from dbt_wrapper.log_levels import LogLevel
from dbt_wrapper.hashcheck_levels import HashCheckLevel
from dbt_wrapper.stage_executor import stage_executor
from dbt_wrapper.workflows import WorkflowManager, WorkflowType, InteractiveMode, StageType
from dbt_wrapper.pipeline_config import ConfigLoader, PipelineConfig
import json
import os
from datetime import datetime


app = typer.Typer(no_args_is_help=True, rich_markup_mode="rich")

# Create subcommands for better organization
stage_app = typer.Typer(help="Manage and run individual pipeline stages")
env_app = typer.Typer(help="Manage environments and configurations")
config_app = typer.Typer(help="Manage pipeline configuration files")

app.add_typer(stage_app, name="stage")
app.add_typer(env_app, name="env")
app.add_typer(config_app, name="config")

custom_theme = Theme({"info": "dim cyan", "warning": "dark_orange", "danger": "bold red", "error": "bold red", "debug": "khaki1"})

console = Console(theme=custom_theme)

wrapper_commands = Commands(console=console)

_log_level: LogLevel = None

if (_log_level is None):
    _log_level = LogLevel.WARNING

#JM issues61 adding _hashcheck_level
_hashcheck_level: HashCheckLevel = None
if (_hashcheck_level is None):
    _hashcheck_level = HashCheckLevel.BYPASS

def docs_options():
    return ["generate", "serve"]

def log_levels():
    return ["DEBUG", "INFO", "WARNING", "ERROR"]

#JM issues61 adding _hashcheck_level
def hashcheck_levels():
    return ["BYPASS", "WARNING", "ERROR"]

# Initialize workflow manager
workflow_manager = WorkflowManager(console=console)
config_loader = ConfigLoader(console=console)

# ============================================
# NEW WORKFLOW COMMANDS - Primary Interface
# ============================================

@app.command()
def dev(
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory"
        ),
    ] = ".",
    dbt_profiles_dir: Annotated[
        Optional[str],
        typer.Option(
            "--profiles-dir",
            help="The path to the dbt_profiles directory"
        ),
    ] = None,
    skip: Annotated[
        Optional[str],
        typer.Option(
            help="Comma-separated list of stages to skip"
        ),
    ] = None,
    only: Annotated[
        Optional[str],
        typer.Option(
            help="Comma-separated list of stages to run exclusively"
        ),
    ] = None,
    select: Annotated[
        str,
        typer.Option(
            help="dbt resource selection syntax"
        ),
    ] = "",
    exclude: Annotated[
        str,
        typer.Option(
            help="dbt resource exclude syntax"
        ),
    ] = "",
):
    """
    🚀 [bold cyan]Development workflow[/bold cyan] - Quick iteration for local development
    
    Runs: clean → pre-scripts → metadata → build → post-scripts
    
    Perfect for rapid development and testing cycles.
    """
    skip_stages = skip.split(",") if skip else None
    only_stages = only.split(",") if only else None
    
    workflow_manager.run_workflow(
        workflow_type=WorkflowType.DEV,
        dbt_project_dir=dbt_project_dir,
        dbt_profiles_dir=dbt_profiles_dir,
        skip_stages=skip_stages,
        only_stages=only_stages,
        select=select,
        exclude=exclude
    )

@app.command()
def deploy(
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory"
        ),
    ] = ".",
    dbt_profiles_dir: Annotated[
        Optional[str],
        typer.Option(
            "--profiles-dir",
            help="The path to the dbt_profiles directory"
        ),
    ] = None,
    skip: Annotated[
        Optional[str],
        typer.Option(
            help="Comma-separated list of stages to skip"
        ),
    ] = None,
    only: Annotated[
        Optional[str],
        typer.Option(
            help="Comma-separated list of stages to run exclusively"
        ),
    ] = None,
    select: Annotated[
        str,
        typer.Option(
            help="dbt resource selection syntax"
        ),
    ] = "",
    exclude: Annotated[
        str,
        typer.Option(
            help="dbt resource exclude syntax"
        ),
    ] = "",
):
    """
    🚢 [bold green]Deploy workflow[/bold green] - Full deployment pipeline
    
    Runs: clean → pre-scripts → metadata → build → post-scripts → upload → execute
    
    Complete pipeline with Fabric deployment and execution.
    """
    skip_stages = skip.split(",") if skip else None
    only_stages = only.split(",") if only else None
    
    workflow_manager.run_workflow(
        workflow_type=WorkflowType.DEPLOY,
        dbt_project_dir=dbt_project_dir,
        dbt_profiles_dir=dbt_profiles_dir,
        skip_stages=skip_stages,
        only_stages=only_stages,
        select=select,
        exclude=exclude
    )

@app.command()
def build(
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory"
        ),
    ] = ".",
    dbt_profiles_dir: Annotated[
        Optional[str],
        typer.Option(
            "--profiles-dir",
            help="The path to the dbt_profiles directory"
        ),
    ] = None,
    select: Annotated[
        str,
        typer.Option(
            help="dbt resource selection syntax"
        ),
    ] = "",
    exclude: Annotated[
        str,
        typer.Option(
            help="dbt resource exclude syntax"
        ),
    ] = "",
):
    """
    🔨 [bold yellow]Build workflow[/bold yellow] - Minimal build only
    
    Runs: metadata-download → build
    
    Just builds the dbt project with minimal overhead.
    """
    workflow_manager.run_workflow(
        workflow_type=WorkflowType.BUILD,
        dbt_project_dir=dbt_project_dir,
        dbt_profiles_dir=dbt_profiles_dir,
        select=select,
        exclude=exclude
    )


@app.command()
def build_local(
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory"
        ),
    ] = ".",
    dbt_profiles_dir: Annotated[
        Optional[str],
        typer.Option(
            "--profiles-dir",
            help="The path to the dbt_profiles directory"
        ),
    ] = None,
    select: Annotated[
        str,
        typer.Option(
            help="dbt resource selection syntax"
        ),
    ] = "",
    exclude: Annotated[
        str,
        typer.Option(
            help="dbt resource exclude syntax"
        ),
    ] = "",
):
    """
    🔨 [bold yellow]Build workflow[/bold yellow] - Minimal build only
    
    Runs: metadata-download → build
    
    Just builds the dbt project with minimal overhead.
    """
    workflow_manager.run_workflow(
        workflow_type=WorkflowType.BUILD_LOCAL,
        dbt_project_dir=dbt_project_dir,
        dbt_profiles_dir=dbt_profiles_dir,
        select=select,
        exclude=exclude
    )

@app.command()
def test(
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory"
        ),
    ] = ".",
    dbt_profiles_dir: Annotated[
        Optional[str],
        typer.Option(
            "--profiles-dir",
            help="The path to the dbt_profiles directory"
        ),
    ] = None,
    skip: Annotated[
        Optional[str],
        typer.Option(
            help="Comma-separated list of stages to skip"
        ),
    ] = None,
    select: Annotated[
        str,
        typer.Option(
            help="dbt resource selection syntax"
        ),
    ] = "",
    exclude: Annotated[
        str,
        typer.Option(
            help="dbt resource exclude syntax"
        ),
    ] = "",
):
    """
    🧪 [bold magenta]Test workflow[/bold magenta] - Validation-focused pipeline
    
    Runs: clean → metadata → build → validation
    
    Ensures quality without deployment.
    """
    skip_stages = skip.split(",") if skip else None
    
    workflow_manager.run_workflow(
        workflow_type=WorkflowType.TEST,
        dbt_project_dir=dbt_project_dir,
        dbt_profiles_dir=dbt_profiles_dir,
        skip_stages=skip_stages,
        select=select,
        exclude=exclude
    )

@app.command()
def run(
    workflow: Annotated[
        str,
        typer.Option(
            "--workflow", "-w",
            help="Workflow name from configuration file"
        ),
    ] = None,
    config_file: Annotated[
        Optional[str],
        typer.Option(
            "--config", "-c",
            help="Path to configuration file"
        ),
    ] = None,
    dbt_project_dir: Annotated[
        str,
        typer.Option(
            "--project-dir", "-p",
            help="The path to the dbt_project directory"
        ),
    ] = ".",
    dbt_profiles_dir: Annotated[
        Optional[str],
        typer.Option(
            "--profiles-dir",
            help="The path to the dbt_profiles directory"
        ),
    ] = None,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive", "-i",
            help="Run in interactive mode"
        ),
    ] = False,
    pre_install: Annotated[
        bool,
        typer.Option(
            help="The option to run the dbt adapter using source code and not the installed package."
        ),
    ] = False
):
    """
    🎯 Run a workflow from configuration file or interactively
    
    Examples:
      dbt_wrapper run --workflow ci
      dbt_wrapper run --interactive
      dbt_wrapper run -w production -c ./my-config.yml
    """
    if interactive:
        interactive_mode = InteractiveMode(console, workflow_manager)
        interactive_mode.run_interactive_workflow()
        return
    
    if not workflow:
        console.print("[error]Please specify --workflow or use --interactive mode[/error]")
        raise typer.Exit(1)
    
    # Load configuration
    config = config_loader.load_config(config_file)
    pipeline_workflow = config_loader.get_workflow(workflow)
    
    if not pipeline_workflow:
        console.print(f"[error]Workflow '{workflow}' not found in configuration[/error]")
        available = config_loader.list_workflows()
        if available:
            console.print(f"Available workflows: {', '.join(available)}")
        raise typer.Exit(1)
    
    # Convert pipeline stages to StageType enum
    stages = []
    for stage_name in pipeline_workflow.stages:
        try:
            stage = StageType(stage_name)
            stages.append(stage)
        except ValueError:
            console.print(f"[warning]Unknown stage: {stage_name}[/warning]")
    
    # Display workflow info
    console.print(f"\n[bold cyan]Running '{workflow}' Workflow[/bold cyan]")
    console.print(f"[dim]{pipeline_workflow.description}[/dim]\n")
    
    # Display stages plan
    workflow_manager._display_stages_plan(stages)
    
    # Initialize and execute
    workflow_manager.wrapper_commands.GetDbtConfigs(
        dbt_project_dir=dbt_project_dir,
        dbt_profiles_dir=dbt_profiles_dir
    )
    
    workflow_manager._execute_stages(
        stages,
        pipeline_workflow.options,
        select="",
        exclude="",
        pre_install=pre_install
    )

# ============================================
# STAGE MANAGEMENT COMMANDS
# ============================================

@stage_app.command("list")
def stage_list():
    """List all available pipeline stages"""
    workflow_manager.list_stages()

@stage_app.command("describe")
def stage_describe(
    stage_name: Annotated[
        str,
        typer.Argument(help="Name of the stage to describe")
    ]
):
    """Get detailed information about a specific stage"""
    workflow_manager.describe_stage(stage_name)

@stage_app.command("run")
def stage_run(
    stages: Annotated[
        List[str],
        typer.Argument(help="Stage names to run")
    ],
    dbt_project_dir: Annotated[
        str,
        typer.Option(
            "--project-dir", "-p",
            help="The path to the dbt_project directory"
        ),
    ] = ".",
    dbt_profiles_dir: Annotated[
        Optional[str],
        typer.Option(
            "--profiles-dir",
            help="The path to the dbt_profiles directory"
        ),
    ] = None,
    pre_install: Annotated[
        bool,
        typer.Option(
            help="The option to run the dbt adapter using source code and not the installed package."
        ),
    ] = False,
    option: Annotated[
        List[str],
        typer.Option(
            "--option",
            help="Additional key=value options to pass to the stage executor. Can be specified multiple times."
        ),
    ] = [],
):
    """Run specific pipeline stages with additional options"""
    # Convert stage names to StageType
    stage_types = []
    for stage_name in stages:
        try:
            stage = StageType(stage_name)
            stage_types.append(stage)
        except ValueError:
            console.print(f"[error]Unknown stage: {stage_name}[/error]")
            console.print("Run 'dbt_wrapper stage list' to see available stages")
            raise typer.Exit(1)

    # Parse additional options into a dictionary
    options_dict = {"log_level": "INFO", "hashcheck_level": "BYPASS", "notebook_timeout": 1800}
    for opt in option:
        if "=" in opt:
            k, v = opt.split("=", 1)
            options_dict[k.strip()] = v.strip()
        else:
            console.print(f"[warning]Ignoring option '{opt}' (expected key=value format)[/warning]")

    # Initialize and execute
    workflow_manager.wrapper_commands.GetDbtConfigs(
        dbt_project_dir=dbt_project_dir,
        dbt_profiles_dir=dbt_profiles_dir
    )

    workflow_manager._display_stages_plan(stage_types)

    if Confirm.ask("\nProceed with execution?", default=True):
        workflow_manager._execute_stages(
            stage_types,
            options_dict,
            select="",
            exclude="",
            pre_install=pre_install
        )

# ============================================
# CONFIGURATION MANAGEMENT COMMANDS
# ============================================

@config_app.command("init")
def config_init(
    path: Annotated[
        Optional[str],
        typer.Option(
            "--path", "-p",
            help="Path where to create the configuration file"
        ),
    ] = None,
):
    """Create an example configuration file"""
    config_path = config_loader.create_example_config(path)
    console.print(f"\n[dim]Edit this file to customize your workflows[/dim]")

@config_app.command("validate")
def config_validate(
    path: Annotated[
        Optional[str],
        typer.Argument(help="Path to configuration file")
    ] = None,
):
    """Validate a configuration file"""
    config = config_loader.load_config(path)
    issues = config_loader.validate_config(config)
    
    if issues:
        console.print("[bold red]Configuration issues found:[/bold red]")
        for issue in issues:
            console.print(f"  • {issue}")
    else:
        console.print("[bold green]✓ Configuration is valid[/bold green]")

@config_app.command("list")
def config_list(
    path: Annotated[
        Optional[str],
        typer.Option(
            "--config", "-c",
            help="Path to configuration file"
        ),
    ] = None,
):
    """List workflows in configuration file"""
    config = config_loader.load_config(path)
    workflows = config_loader.list_workflows()
    
    if not workflows:
        console.print("[dim]No workflows found in configuration[/dim]")
        return
    
    table = Table(title="Available Workflows", show_header=True)
    table.add_column("Workflow", style="cyan")
    table.add_column("Description", style="white")
    table.add_column("Stages", style="dim")
    
    for workflow_name in workflows:
        workflow = config.workflows[workflow_name]
        stages_str = " → ".join(workflow.stages[:3])
        if len(workflow.stages) > 3:
            stages_str += f" ... (+{len(workflow.stages)-3})"
        table.add_row(
            workflow_name,
            workflow.description or "[dim italic]No description[/dim italic]",
            stages_str
        )
    
    console.print(table)

# ============================================
# ENVIRONMENT MANAGEMENT COMMANDS  
# ============================================

@env_app.command("list")
def env_list(
    dbt_profiles_dir: Annotated[
        Optional[str],
        typer.Option(
            "--profiles-dir",
            help="The path to the dbt_profiles directory"
        ),
    ] = None,
):
    """List available environments from profiles.yml"""
    # This would need to parse profiles.yml
    console.print("[dim]Environment listing coming soon...[/dim]")

@env_app.command("validate")
def env_validate(
    env_name: Annotated[
        str,
        typer.Argument(help="Environment name to validate")
    ],
    dbt_profiles_dir: Annotated[
        Optional[str],
        typer.Option(
            "--profiles-dir",
            help="The path to the dbt_profiles directory"
        ),
    ] = None,
):
    """Validate environment configuration"""
    console.print(f"[dim]Validating environment: {env_name}...[/dim]")

# ============================================
# STATUS AND MONITORING COMMANDS
# ============================================

@app.command()
def status(
    dbt_project_dir: Annotated[
        str,
        typer.Option(
            "--project-dir", "-p",
            help="The path to the dbt_project directory"
        ),
    ] = ".",
    watch: Annotated[
        bool,
        typer.Option(
            "--watch", "-w",
            help="Watch for status updates"
        ),
    ] = False,
):
    """Show status of last pipeline run"""
    console.print("[dim]Status monitoring coming soon...[/dim]")

@app.command()
def history(
    dbt_project_dir: Annotated[
        str,
        typer.Option(
            "--project-dir", "-p",
            help="The path to the dbt_project directory"
        ),
    ] = ".",
    limit: Annotated[
        int,
        typer.Option(
            "--limit", "-n",
            help="Number of entries to show"
        ),
    ] = 10,
):
    """Show pipeline run history"""
    console.print("[dim]History tracking coming soon...[/dim]")

# ============================================
# ORIGINAL COMMANDS (kept for backwards compatibility)
# ============================================

@app.command()
def docs():
    """
    This command will generate the documentation for the dbt project.
    """
    print(f"Goodbye")


@app.command()
def buildcomparemetadata(  
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory. If left blank it will use the current directory"
        ),
    ],
    source: Annotated[
        str,
        typer.Argument(
            help="Source environment name from profile.yml"
        ),
    ],
    target: Annotated[
        str,
        typer.Argument(
            help="Target environment name from profile.yml"
        ),
    ],
    dbt_profiles_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_profile directory. If left blank it will use the users home directory followed by .dbt."
        ),
    ] = None
    ):
    """
    This command will execute the 'build metadata' notebooks in the two specified environments within Fabric. This notebook will create a `comparemetadata` table in their respective environment that will store the Lakehouse schema information.
    """
    log_level = "WARNING"

    _log_level: LogLevel = LogLevel.from_string(log_level)    
    wrapper_commands.GetDbtConfigs(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir, source_env=source, target_env=target)

    se: stage_executor = stage_executor(log_level=_log_level, console=console)
    se.perform_stage(option=True, action_callables=[wrapper_commands.RunBuildMetadataNotebook_Source], stage_name=f"Run Build Metadata Notebook (Source: {source})")
    se.perform_stage(option=True, action_callables=[wrapper_commands.RunBuildMetadataNotebook_Target], stage_name=f"Run Build Metadata Notebook (Target: {target})")


    print(f"Goodbye")


@app.command()
def compare(  
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory. If left blank it will use the current directory"
        ),
    ],
    source: Annotated[
        str,
        typer.Argument(
            help="Source environment name from profile.yml"
        ),
    ],
    target: Annotated[
        str,
        typer.Argument(
            help="Target environment name from profile.yml"
        ),
    ],
    dbt_profiles_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_profile directory. If left blank it will use the users home directory followed by .dbt."
        ),
    ] = None):
    """
    This command will compare two environments Lakehouse's. Generated notebooks will be uploaded to the 'target' 
    environment configured in the 'profile.yml' file which will contain the sql commands that can be used to 
    create and alter tables, from one environment to the next.
    """
    log_level = "WARNING"

    _log_level: LogLevel = LogLevel.from_string(log_level)    
    wrapper_commands.GetDbtConfigs(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir, source_env=source, target_env=target)
    se: stage_executor = stage_executor(log_level=_log_level, console=console)

    se.perform_stage(option=True, action_callables=[wrapper_commands.GenerateCompareNotebook], stage_name="Generate Compare Notebook")
    se.perform_stage(option=True, action_callables=[wrapper_commands.ConvertNotebooksToFabricFormat], stage_name="Convert to Fabric Notebook")
    se.perform_stage(option=True, action_callables=[wrapper_commands.UploadCompareNotebookViaApi], stage_name="Upload Compare Notebook")
    se.perform_stage(option=True, action_callables=[wrapper_commands.RunCompareNotebook], stage_name="Run Compare Notebook")


    # #download the metadata
    se.perform_stage(option=True, action_callables=[wrapper_commands.DownloadMetadata], stage_name="Download Metadata")

    se.perform_stage(option=True, action_callables=[wrapper_commands.GenerateMissingObjectsNotebook], stage_name="Generate Missing Objects Notebook")
    se.perform_stage(option=True, action_callables=[wrapper_commands.ConvertNotebooksToFabricFormat], stage_name="Convert to Fabric Notebook")
    se.perform_stage(option=True, action_callables=[wrapper_commands.UploadMissingObjectsNotebookViaApi], stage_name="Upload Missing Objects Notebook to Target Workspace")




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
            help="The option to clear out the target folder before dbt project build."
        ),
    ] = True,
    generate_pre_dbt_scripts: Annotated[
        bool,
        typer.Option(
            help="The option to generate the pre dbt scripts before dbt project build."
        ),
    ] = True,
    generate_post_dbt_scripts: Annotated[
        bool,
        typer.Option(
            help="The option to generate the post dbt scripts before dbt project build."
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
    auto_run_master_notebook: Annotated[
        bool,
        typer.Option(
            help="The option to automatically execute your transformation pipeline by executing the master orchestration notebook after the build and publish stages."
        ),
    ] = True,
    log_level: Annotated[
        Optional[str],
        typer.Option(
            help="The option to set the log level. This controls the verbosity of the output. Allowed values are `DEBUG`, `INFO`, `WARNING`, `ERROR`. Default is `WARNING`.",
        ),
    ] = "WARNING",
    #JM issues61 adding _hashcheck_level
    hashcheck_level: Annotated[
        Optional[str],
        typer.Option(
            help="The option to set the hash check level. This controls the verbosity of the output. Allowed values are `BYPASS`, `WARNING`, `ERROR`. Default is `BYPASS`.",
        ),
    ] = "BYPASS",
    notebook_timeout: Annotated[
        int,
        typer.Option(
            help="Use this option to change the default notebook execution timeout setting.",
        ),
    ] = 1800
    ,
    select: Annotated[
        str,
        typer.Option(
            help="Use this option to provide a dbt resource selection syntax.Default is ``",
        ),
    ] = ""
    ,
    exclude: Annotated[
        str,
        typer.Option(
            help="Use this option to provide a dbt resource exclude syntax.Default is ``",
        ),
    ] = ""
    ,
    lakehouse_config: Annotated[
        Optional[str],
        typer.Option(
            help="Use this option to set the default lakehouse in code or metadata. Allowed values are `CODE` or `METADATA`. Default is `METADATA`.",
        ),
    ] = "METADATA"
):
    """
    This command will run all elements of the project. For more granular control you can use the options provided to suppress certain stages or use a different command.
    """    
    
    _log_level: LogLevel = LogLevel.from_string(log_level)    
    #JM issues61 adding _hashcheck_level
    _hashcheck_level: HashCheckLevel = HashCheckLevel.from_string(hashcheck_level)

    wrapper_commands.GetDbtConfigs(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir)
    se: stage_executor = stage_executor(log_level=_log_level, console=console)
    se.perform_stage(option=clean_target_dir, action_callables=[wrapper_commands.CleanProjectTargetDirectory], stage_name="Clean Target")

    action_callables = [
        lambda **kwargs: wrapper_commands.GeneratePreDbtScripts(PreInstall=pre_install, notebook_timeout=notebook_timeout, lakehouse_config=lakehouse_config, **kwargs),
        lambda **kwargs: wrapper_commands.ConvertNotebooksToFabricFormat(lakehouse_config=lakehouse_config, **kwargs),
    ]
    se.perform_stage(option=generate_pre_dbt_scripts, action_callables=action_callables, stage_name="Generate Pre-DBT Scripts")

    se.perform_stage(option=auto_execute_metadata_extract, action_callables=[wrapper_commands.RunMetadataExtract], stage_name="Auto Execute Metadata Extract")

    se.perform_stage(option=download_metadata, action_callables=[wrapper_commands.DownloadMetadata], stage_name="Download Metadata")

    if (build_dbt_project):
        wrapper_commands.BuildDbtProject(PreInstall=pre_install, select=select, exclude=exclude)

#JM issues61 adding _hashcheck_level
    action_callables = [
        lambda **kwargs: wrapper_commands.GeneratePostDbtScripts(PreInstall=pre_install, notebook_timeout=notebook_timeout, notebook_hashcheck=_hashcheck_level, lakehouse_config=lakehouse_config, **kwargs),
        lambda **kwargs: wrapper_commands.ConvertNotebooksToFabricFormat(lakehouse_config=lakehouse_config, **kwargs)
    ]
    se.perform_stage(option=generate_post_dbt_scripts, action_callables=action_callables, stage_name="Generate Post-DBT Scripts")    

    se.perform_stage(option=upload_notebooks_via_api, action_callables=[wrapper_commands.AutoUploadNotebooksViaApi], stage_name="Upload Notebooks via API")

    se.perform_stage(option=auto_run_master_notebook, action_callables=[wrapper_commands.RunMasterNotebook], stage_name="Run Master Notebook")
    se.perform_stage(option=auto_run_master_notebook, action_callables=[wrapper_commands.GetExecutionResults], stage_name="Get Execution Results")


@app.command()
def download_metadata(
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
    log_level: Annotated[
        Optional[str],
        typer.Option(
            help="The option to set the log level. This controls the verbosity of the output. Allowed values are `DEBUG`, `INFO`, `WARNING`, `ERROR`. Default is `WARNING`.",
        ),
    ] = "WARNING"
):
    """
    This command will run just the metadata download.
    """    
    
    _log_level: LogLevel = LogLevel.from_string(log_level)    
    
    wrapper_commands.GetDbtConfigs(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir)
    se: stage_executor = stage_executor(log_level=_log_level, console=console)
    
    se.perform_stage(option=download_metadata, action_callables=[wrapper_commands.DownloadMetadata], stage_name="Download Metadata")


@app.command()
def get_execution_results(
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
    log_level: Annotated[
        Optional[str],
        typer.Option(
            help="The option to set the log level. This controls the verbosity of the output. Allowed values are `DEBUG`, `INFO`, `WARNING`, `ERROR`. Default is `WARNING`.",
        ),
    ] = "WARNING"
):
    """
    This command will run just the extract of the last execution results.
    """    
    
    _log_level: LogLevel = LogLevel.from_string(log_level)    
    
    wrapper_commands.GetDbtConfigs(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir)
    se: stage_executor = stage_executor(log_level=_log_level, console=console)
    
    se.perform_stage(option=True, action_callables=[wrapper_commands.GetExecutionResults], stage_name="Get Execution Results")


if __name__ == "__main__":
    app()
