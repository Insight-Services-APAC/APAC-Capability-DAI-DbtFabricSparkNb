"""
Workflow-based commands for dbt_wrapper CLI.
Provides intuitive, task-oriented commands for common workflows.
"""
from typing import List, Optional, Dict, Any
from enum import Enum
from dataclasses import dataclass
from pathlib import Path
import typer
from rich.console import Console
from rich.table import Table
from rich.prompt import Confirm, Prompt
from rich import print
from dbt_wrapper.log_levels import LogLevel
from dbt_wrapper.hashcheck_levels import HashCheckLevel
from dbt_wrapper.stage_executor import stage_executor
from dbt_wrapper.wrapper import Commands


class WorkflowType(str, Enum):
    """Predefined workflow types"""
    DEV = "dev"
    DEPLOY = "deploy"
    BUILD = "build"
    BUILD_LOCAL = "build-local"
    TEST = "test"
    CI = "ci"
    PRODUCTION = "production"
    CUSTOM = "custom"


class StageType(str, Enum):
    """Available pipeline stages"""
    CLEAN = "clean"
    PRE_SCRIPTS = "pre-scripts"
    METADATA_EXTRACT = "metadata-extract"
    METADATA_DOWNLOAD = "metadata-download"
    BUILD = "build"
    POST_SCRIPTS = "post-scripts"
    UPLOAD_ARTIFACTS = "upload-artifacts"
    UPLOAD_NOTEBOOKS = "upload-notebooks"
    EXECUTE = "execute"
    VALIDATE = "validate"
    RESULTS = "results"


@dataclass
class WorkflowConfig:
    """Configuration for a workflow"""
    name: str
    stages: List[StageType]
    options: Dict[str, Any]
    description: str = ""


class WorkflowManager:
    """Manages workflow execution and configuration"""
    
    def __init__(self, console: Console):
        self.console = console
        self.wrapper_commands = Commands(console=console)
        self.predefined_workflows = self._init_predefined_workflows()
        
    def _init_predefined_workflows(self) -> Dict[WorkflowType, WorkflowConfig]:
        """Initialize predefined workflow configurations"""
        return {
            WorkflowType.DEV: WorkflowConfig(
                name="Development",
                description="Quick iteration workflow for local development",
                stages=[
                    StageType.CLEAN,
                    StageType.PRE_SCRIPTS,
                    StageType.METADATA_EXTRACT,
                    StageType.BUILD,
                    StageType.POST_SCRIPTS,
                ],
                options={
                    "log_level": "INFO",
                    "hashcheck_level": "BYPASS",
                    "notebook_timeout": 1800,
                    "lakehouse_config": "CODE",
                    "upload_notebooks": False,
                    "auto_run_master": False,
                }
            ),
            WorkflowType.DEPLOY: WorkflowConfig(
                name="Deploy",
                description="Full deployment pipeline with upload and execution",
                stages=[
                    StageType.CLEAN,
                    StageType.PRE_SCRIPTS,
                    StageType.METADATA_EXTRACT,
                    StageType.METADATA_DOWNLOAD,
                    StageType.BUILD,
                    StageType.POST_SCRIPTS,
                    StageType.UPLOAD_ARTIFACTS,
                    StageType.UPLOAD_NOTEBOOKS,
                    StageType.EXECUTE,
                    StageType.RESULTS,
                ],
                options={
                    "log_level": "WARNING",
                    "hashcheck_level": "BYPASS",
                    "lakehouse_config": "CODE",
                    "notebook_timeout": 1800,
                    "upload_notebooks": True,
                    "auto_run_master": True,
                }
            ),
            WorkflowType.BUILD: WorkflowConfig(
                name="Build Only",
                description="Minimal workflow - just build the dbt project",
                stages=[
                    StageType.METADATA_DOWNLOAD,
                    StageType.BUILD,
                ],
                options={
                    "log_level": "WARNING",
                    "hashcheck_level": "BYPASS",
                    "lakehouse_config": "CODE",
                    "notebook_timeout": 1800,
                    "upload_notebooks": False,
                    "auto_run_master": False,
                }
            ),
            WorkflowType.BUILD_LOCAL: WorkflowConfig(
                name="Build Only",
                description="Minimal workflow - just build the dbt project",
                stages=[
                    StageType.BUILD,
                    StageType.POST_SCRIPTS,
                ],
                options={
                    "log_level": "WARNING",
                    "hashcheck_level": "BYPASS",
                    "lakehouse_config": "CODE",
                    "notebook_timeout": 1800,
                    "upload_notebooks": False,
                    "auto_run_master": False,
                }
            ),
            WorkflowType.TEST: WorkflowConfig(
                name="Test",
                description="Validation-focused workflow without deployment",
                stages=[
                    StageType.CLEAN,
                    StageType.PRE_SCRIPTS,
                    StageType.METADATA_EXTRACT,
                    StageType.METADATA_DOWNLOAD,
                    StageType.BUILD,
                    StageType.POST_SCRIPTS,
                    StageType.VALIDATE,
                ],
                options={
                    "log_level": "INFO",
                    "hashcheck_level": "BYPASS",
                    "lakehouse_config": "CODE",
                    "notebook_timeout": 1800,
                    "upload_notebooks": False,
                    "auto_run_master": False,
                }
            ),
            WorkflowType.CI: WorkflowConfig(
                name="Continuous Integration",
                description="CI pipeline for automated testing",
                stages=[
                    StageType.CLEAN,
                    StageType.METADATA_EXTRACT,
                    StageType.BUILD,
                    StageType.VALIDATE,
                ],
                options={
                    "log_level": "INFO",
                    "hashcheck_level": "BYPASS",
                    "lakehouse_config": "CODE",
                    "notebook_timeout": 900,
                    "upload_notebooks": False,
                    "auto_run_master": False,
                }
            ),
        }
    
    def run_workflow(
        self,
        workflow_type: WorkflowType,
        dbt_project_dir: str,
        dbt_profiles_dir: Optional[str] = None,
        dbt_target: Optional[str] = None,
        skip_stages: Optional[List[str]] = None,
        only_stages: Optional[List[str]] = None,
        select: str = "",
        exclude: str = "",
        pre_install: bool = False,
        **override_options,

    ):
        """Execute a predefined workflow"""
        workflow = self.predefined_workflows.get(workflow_type)
        if not workflow:
            self.console.print(f"[error]Unknown workflow type: {workflow_type}[/error]")
            return

        # Display workflow info
        self.console.print(f"\n[bold cyan]Running {workflow.name} Workflow[/bold cyan]")
        self.console.print(f"[dim]{workflow.description}[/dim]\n")

        # Merge options with overrides
        options = {**workflow.options, **override_options}

        # Filter stages based on skip/only parameters
        stages_to_run = self._filter_stages(
            workflow.stages,
            skip_stages,
            only_stages
        )

        # Display stages to run
        self._display_stages_plan(stages_to_run)

        # Initialize configurations
        self.wrapper_commands.GetDbtConfigs(
            dbt_project_dir=dbt_project_dir,
            dbt_profiles_dir=dbt_profiles_dir,
            dbt_target=dbt_target
        )

        # Execute stages
        self._execute_stages(stages_to_run, options, select, exclude, pre_install)
    
    def _filter_stages(
        self,
        stages: List[StageType],
        skip_stages: Optional[List[str]] = None,
        only_stages: Optional[List[str]] = None
    ) -> List[StageType]:
        """Filter stages based on skip/only parameters"""
        if only_stages:
            return [s for s in stages if s.value in only_stages]
        if skip_stages:
            return [s for s in stages if s.value not in skip_stages]
        return stages
    
    def _display_stages_plan(self, stages: List[StageType]):
        """Display the execution plan"""
        table = Table(title="Execution Plan", show_header=True)
        table.add_column("Stage", style="cyan")
        table.add_column("Description", style="white")
        
        stage_descriptions = {
            StageType.CLEAN: "Clean target directory",
            StageType.PRE_SCRIPTS: "Generate pre-dbt scripts",
            StageType.METADATA_EXTRACT: "Extract metadata from Fabric",
            StageType.METADATA_DOWNLOAD: "Download metadata locally",
            StageType.BUILD: "Build dbt project",
            StageType.POST_SCRIPTS: "Generate post-dbt scripts",
            StageType.UPLOAD_ARTIFACTS: "Upload artifacts to lakehouse",
            StageType.UPLOAD_NOTEBOOKS: "Upload notebooks to workspace",
            StageType.EXECUTE: "Execute master notebook",
            StageType.VALIDATE: "Run validation checks",
            StageType.RESULTS: "Retrieve execution results",
        }
        
        for stage in stages:
            table.add_row(
                stage.value,
                stage_descriptions.get(stage, "")
            )
        
        self.console.print(table)
        self.console.print()
    
    def _execute_stages(
        self,
        stages: List[StageType],
        options: Dict[str, Any],
        select: str,
        exclude: str,
        pre_install: bool 
    ):
        """Execute the workflow stages"""
        log_level = LogLevel.from_string(options.get("log_level", "WARNING"))
        hashcheck_level = HashCheckLevel.from_string(options.get("hashcheck_level", "BYPASS"))
        notebook_timeout = options.get("notebook_timeout", 1800)
        lakehouse_config = options.get("lakehouse_config", "CODE")

        # Prepare extra kwargs to pass through
        reserved_keys = {"log_level", "hashcheck_level", "notebook_timeout", "upload_notebooks", "auto_run_master"}
        extra_kwargs = {k: v for k, v in options.items() if k not in reserved_keys}

        se = stage_executor(log_level=log_level, console=self.console)

        # Map stages to execution functions
        stage_map = {
            StageType.CLEAN: lambda: se.perform_stage(
                option=True,
                action_callables=[self.wrapper_commands.CleanProjectTargetDirectory],
                stage_name="Clean Target"
            ),
            StageType.PRE_SCRIPTS: lambda: se.perform_stage(
                option=True,
                action_callables=[
                    lambda **kwargs: self.wrapper_commands.GeneratePreDbtScripts(
                        PreInstall=pre_install,
                        notebook_timeout=notebook_timeout,
                        lakehouse_config=lakehouse_config,
                        **kwargs
                    ),
                    lambda **kwargs: self.wrapper_commands.ConvertNotebooksToFabricFormat(
                        lakehouse_config=lakehouse_config,
                        **kwargs
                    ),
                ],
                stage_name="Generate Pre-DBT Scripts"
            ),
            StageType.METADATA_EXTRACT: lambda: se.perform_stage(
                option=True,
                action_callables=[self.wrapper_commands.RunMetadataExtract],
                stage_name="Auto Execute Metadata Extract"
            ),
            StageType.METADATA_DOWNLOAD: lambda: se.perform_stage(
                option=True,
                action_callables=[self.wrapper_commands.DownloadMetadata],
                stage_name="Download Metadata"
            ),
            StageType.BUILD: lambda: self.wrapper_commands.BuildDbtProject(
                PreInstall=pre_install,
                select=select,
                exclude=exclude
            ),
            StageType.POST_SCRIPTS: lambda: se.perform_stage(
                option=True,
                action_callables=[
                    lambda **kwargs: self.wrapper_commands.GeneratePostDbtScripts(
                        PreInstall=pre_install,
                        notebook_timeout=notebook_timeout,
                        notebook_hashcheck=hashcheck_level,
                        lakehouse_config=lakehouse_config,
                        **kwargs
                    ),
                    lambda **kwargs: self.wrapper_commands.ConvertNotebooksToFabricFormat(
                        lakehouse_config=lakehouse_config,
                        **kwargs
                    ),
                ],
                stage_name="Generate Post-DBT Scripts"
            ),
            StageType.UPLOAD_ARTIFACTS: lambda: se.perform_stage(
                option=True,
                action_callables=[self.wrapper_commands.UploadArtifacts],
                stage_name="Upload Artifacts to Lakehouse"
            ),
            StageType.UPLOAD_NOTEBOOKS: lambda: se.perform_stage(
                option=options.get("upload_notebooks", False),
                action_callables=[self.wrapper_commands.AutoUploadNotebooksViaApi],
                stage_name="Upload Notebooks to Workspace"
            ),
            StageType.EXECUTE: lambda: se.perform_stage(
                option=options.get("auto_run_master", False),
                action_callables=[
                    lambda **kwargs: self.wrapper_commands.RunMasterNotebook(
                        select=select,
                        exclude=exclude,
                        **kwargs
                    )
                ],
                stage_name="Run Master Notebook"
            ),
            StageType.RESULTS: lambda: se.perform_stage(
                option=options.get("auto_run_master", False),
                action_callables=[self.wrapper_commands.GetExecutionResults],
                stage_name="Get Execution Results"
            ),
        }

        # Execute each stage
        for stage in stages:
            if stage in stage_map:
                stage_map[stage]()
    
    def list_stages(self):
        """List all available stages"""
        table = Table(title="Available Stages", show_header=True)
        table.add_column("Stage", style="cyan", no_wrap=True)
        table.add_column("Description", style="white")
        table.add_column("Used In", style="dim")
        
        stage_info = [
            (StageType.CLEAN, "Clean target directory", "dev, deploy, test, ci"),
            (StageType.PRE_SCRIPTS, "Generate pre-dbt scripts", "dev, deploy, test"),
            (StageType.METADATA_EXTRACT, "Extract metadata from Fabric", "dev, deploy, test, ci"),
            (StageType.METADATA_DOWNLOAD, "Download metadata locally", "deploy, build, test"),
            (StageType.BUILD, "Build dbt project", "all workflows"),
            (StageType.POST_SCRIPTS, "Generate post-dbt scripts", "dev, deploy, test"),
            (StageType.UPLOAD_ARTIFACTS, "Upload artifacts to lakehouse", "deploy"),
            (StageType.UPLOAD_NOTEBOOKS, "Upload notebooks to workspace", "deploy"),
            (StageType.EXECUTE, "Execute master notebook", "deploy"),
            (StageType.VALIDATE, "Run validation checks", "test, ci"),
            (StageType.RESULTS, "Retrieve execution results", "deploy"),
        ]
        
        for stage, desc, used_in in stage_info:
            table.add_row(stage.value, desc, used_in)
        
        self.console.print(table)
    
    def describe_stage(self, stage_name: str):
        """Describe what a specific stage does"""
        stage_details = {
            "clean": {
                "name": "Clean Target",
                "description": "Removes all files from the dbt target directory",
                "purpose": "Ensures a clean build without cached artifacts",
                "when_to_skip": "When doing incremental builds or debugging",
            },
            "pre-scripts": {
                "name": "Generate Pre-DBT Scripts",
                "description": "Creates metadata extraction and setup notebooks",
                "purpose": "Prepares the Fabric environment before dbt execution",
                "when_to_skip": "When metadata is already up-to-date",
            },
            "metadata-extract": {
                "name": "Metadata Extract",
                "description": "Executes notebooks to extract lakehouse metadata",
                "purpose": "Gathers schema information from Fabric lakehouses",
                "when_to_skip": "When working with static schemas",
            },
            "metadata-download": {
                "name": "Metadata Download",
                "description": "Downloads extracted metadata to local environment",
                "purpose": "Makes Fabric metadata available for dbt compilation",
                "when_to_skip": "When metadata hasn't changed",
            },
            "build": {
                "name": "Build DBT Project",
                "description": "Runs dbt build command to compile and test models",
                "purpose": "Core transformation logic execution",
                "when_to_skip": "Never - this is the core functionality",
            },
            "post-scripts": {
                "name": "Generate Post-DBT Scripts",
                "description": "Creates orchestration and validation notebooks",
                "purpose": "Prepares notebooks for Fabric execution",
                "when_to_skip": "When only doing local development",
            },
            "upload-artifacts": {
                "name": "Upload Artifacts",
                "description": "Uploads manifest.json and runtime artifacts to lakehouse",
                "purpose": "Makes build artifacts available for notebook execution",
                "when_to_skip": "When not using runtime model selection",
            },
            "upload-notebooks": {
                "name": "Upload Notebooks",
                "description": "Uploads generated notebooks to Fabric workspace",
                "purpose": "Deploys transformation logic to Fabric",
                "when_to_skip": "During local development and testing",
            },
            "execute": {
                "name": "Execute Master Notebook",
                "description": "Runs the master orchestration notebook in Fabric",
                "purpose": "Triggers the full transformation pipeline",
                "when_to_skip": "When doing manual or scheduled execution",
            },
            "validate": {
                "name": "Validation",
                "description": "Runs tests and quality checks",
                "purpose": "Ensures data quality and transformation correctness",
                "when_to_skip": "During rapid prototyping",
            },
            "results": {
                "name": "Get Results",
                "description": "Retrieves execution logs and metrics",
                "purpose": "Provides visibility into pipeline execution",
                "when_to_skip": "When not interested in execution details",
            },
        }
        
        details = stage_details.get(stage_name)
        if not details:
            self.console.print(f"[error]Unknown stage: {stage_name}[/error]")
            return
        
        self.console.print(f"\n[bold cyan]{details['name']}[/bold cyan]")
        self.console.print(f"[white]{details['description']}[/white]\n")
        self.console.print(f"[bold]Purpose:[/bold] {details['purpose']}")
        self.console.print(f"[bold]When to skip:[/bold] {details['when_to_skip']}\n")


class InteractiveMode:
    """Provides interactive wizard functionality"""
    
    def __init__(self, console: Console, workflow_manager: WorkflowManager):
        self.console = console
        self.workflow_manager = workflow_manager
    
    def run_interactive_workflow(self):
        """Run an interactive workflow selection wizard"""
        self.console.print("\n[bold cyan]Welcome to dbt_wrapper Interactive Mode[/bold cyan]\n")
        
        # Select workflow type
        workflow_options = {
            "1": ("Development", WorkflowType.DEV),
            "2": ("Deploy", WorkflowType.DEPLOY),
            "3": ("Build Only", WorkflowType.BUILD),
            "4": ("Test", WorkflowType.TEST),
            "5": ("CI Pipeline", WorkflowType.CI),
            "6": ("Custom", WorkflowType.CUSTOM),
        }
        
        self.console.print("[bold]Select a workflow:[/bold]")
        for key, (name, _) in workflow_options.items():
            self.console.print(f"  {key}. {name}")
        
        choice = Prompt.ask("Enter your choice", choices=list(workflow_options.keys()))
        workflow_name, workflow_type = workflow_options[choice]
        
        if workflow_type == WorkflowType.CUSTOM:
            stages = self._select_custom_stages()
        else:
            workflow = self.workflow_manager.predefined_workflows[workflow_type]
            stages = workflow.stages
            self.console.print(f"\n[dim]Using {workflow_name} workflow with predefined stages[/dim]")
        
        # Get project directory
        dbt_project_dir = Prompt.ask(
            "Enter dbt project directory",
            default="."
        )
        
        # Get profiles directory
        use_custom_profiles = Confirm.ask(
            "Use custom profiles directory?",
            default=False
        )
        dbt_profiles_dir = None
        if use_custom_profiles:
            dbt_profiles_dir = Prompt.ask("Enter profiles directory")
        
        # Confirm execution
        self.workflow_manager._display_stages_plan(stages)
        if Confirm.ask("\nProceed with execution?", default=True):
            if workflow_type == WorkflowType.CUSTOM:
                # For custom workflow, create a temporary config
                custom_config = WorkflowConfig(
                    name="Custom",
                    description="User-defined custom workflow",
                    stages=stages,
                    options={
                        "log_level": "INFO",
                        "hashcheck_level": "BYPASS",
                        "notebook_timeout": 1800,
                        "upload_notebooks": StageType.UPLOAD_NOTEBOOKS in stages,
                        "auto_run_master": StageType.EXECUTE in stages,
                    }
                )
                self.workflow_manager.predefined_workflows[WorkflowType.CUSTOM] = custom_config
            
            self.workflow_manager.run_workflow(
                workflow_type=workflow_type,
                dbt_project_dir=dbt_project_dir,
                dbt_profiles_dir=dbt_profiles_dir
            )
    
    def _select_custom_stages(self) -> List[StageType]:
        """Allow user to select custom stages"""
        self.console.print("\n[bold]Select stages to run:[/bold]")
        
        available_stages = [
            (StageType.CLEAN, "Clean target directory"),
            (StageType.PRE_SCRIPTS, "Generate pre-dbt scripts"),
            (StageType.METADATA_EXTRACT, "Extract metadata"),
            (StageType.METADATA_DOWNLOAD, "Download metadata"),
            (StageType.BUILD, "Build dbt project"),
            (StageType.POST_SCRIPTS, "Generate post-dbt scripts"),
            (StageType.UPLOAD_ARTIFACTS, "Upload artifacts to lakehouse"),
            (StageType.UPLOAD_NOTEBOOKS, "Upload notebooks to workspace"),
            (StageType.EXECUTE, "Execute master notebook"),
            (StageType.VALIDATE, "Run validation"),
            (StageType.RESULTS, "Get execution results"),
        ]
        
        selected_stages = []
        for stage, description in available_stages:
            if Confirm.ask(f"  Include '{description}'?", default=False):
                selected_stages.append(stage)
        
        if not selected_stages:
            self.console.print("[warning]No stages selected![/warning]")
            return []
        
        return selected_stages