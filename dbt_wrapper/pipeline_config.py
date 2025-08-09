"""
Pipeline configuration management for dbt_wrapper.
Handles YAML-based workflow definitions and configuration loading.
"""
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from rich.console import Console
from rich import print


@dataclass
class PipelineStage:
    """Represents a single stage in a pipeline"""
    name: str
    enabled: bool = True
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineWorkflow:
    """Represents a complete workflow configuration"""
    name: str
    description: str
    stages: List[str]
    options: Dict[str, Any] = field(default_factory=dict)
    environment: Optional[str] = None


@dataclass
class PipelineConfig:
    """Complete pipeline configuration from YAML"""
    workflows: Dict[str, PipelineWorkflow] = field(default_factory=dict)
    defaults: Dict[str, Any] = field(default_factory=dict)
    environments: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    version: str = "1.0"


class ConfigLoader:
    """Loads and manages pipeline configurations"""
    
    DEFAULT_CONFIG_NAMES = [".dbt-wrapper.yml", ".dbt-wrapper.yaml", "dbt-wrapper.yml", "dbt-wrapper.yaml"]
    
    def __init__(self, console: Optional[Console] = None):
        self.console = console or Console()
        self.config: Optional[PipelineConfig] = None
        self.config_path: Optional[Path] = None
    
    def find_config_file(self, start_dir: Optional[str] = None) -> Optional[Path]:
        """
        Search for configuration file starting from given directory.
        Searches current dir, then parent directories up to root.
        """
        start_path = Path(start_dir) if start_dir else Path.cwd()
        
        # Search in current and parent directories
        current = start_path.resolve()
        while current != current.parent:
            for config_name in self.DEFAULT_CONFIG_NAMES:
                config_path = current / config_name
                if config_path.exists():
                    return config_path
            current = current.parent
        
        return None
    
    def load_config(self, config_path: Optional[str] = None) -> PipelineConfig:
        """Load configuration from YAML file"""
        if config_path:
            path = Path(config_path)
        else:
            path = self.find_config_file()
        
        if not path or not path.exists():
            # Return default empty config if no file found
            return PipelineConfig()
        
        self.config_path = path
        
        try:
            with open(path, 'r') as f:
                data = yaml.safe_load(f) or {}
            
            config = PipelineConfig()
            
            # Load version
            config.version = data.get('version', '1.0')
            
            # Load defaults
            config.defaults = data.get('defaults', {})
            
            # Load environments
            config.environments = data.get('environments', {})
            
            # Load workflows
            workflows_data = data.get('workflows', {})
            for workflow_name, workflow_data in workflows_data.items():
                workflow = PipelineWorkflow(
                    name=workflow_name,
                    description=workflow_data.get('description', ''),
                    stages=workflow_data.get('stages', []),
                    options=workflow_data.get('options', {}),
                    environment=workflow_data.get('environment')
                )
                config.workflows[workflow_name] = workflow
            
            self.config = config
            
            if self.console:
                self.console.print(f"[dim]Loaded configuration from: {path}[/dim]")
            
            return config
            
        except Exception as e:
            if self.console:
                self.console.print(f"[warning]Failed to load config from {path}: {e}[/warning]")
            return PipelineConfig()
    
    def save_config(self, config: PipelineConfig, path: Optional[str] = None):
        """Save configuration to YAML file"""
        save_path = Path(path) if path else self.config_path
        if not save_path:
            save_path = Path.cwd() / ".dbt-wrapper.yml"
        
        data = {
            'version': config.version,
            'defaults': config.defaults,
            'environments': config.environments,
            'workflows': {}
        }
        
        for workflow_name, workflow in config.workflows.items():
            data['workflows'][workflow_name] = {
                'description': workflow.description,
                'stages': workflow.stages,
                'options': workflow.options,
            }
            if workflow.environment:
                data['workflows'][workflow_name]['environment'] = workflow.environment
        
        with open(save_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        if self.console:
            self.console.print(f"[success]Configuration saved to: {save_path}[/success]")
    
    def create_example_config(self, path: Optional[str] = None) -> Path:
        """Create an example configuration file"""
        example_config = """# dbt_wrapper Pipeline Configuration
version: '1.0'

# Default options applied to all workflows unless overridden
defaults:
  log_level: WARNING
  notebook_timeout: 1800
  lakehouse_config: METADATA

# Environment-specific configurations
environments:
  development:
    workspace_id: dev-workspace-id
    lakehouse_id: dev-lakehouse-id
    
  staging:
    workspace_id: staging-workspace-id
    lakehouse_id: staging-lakehouse-id
    
  production:
    workspace_id: prod-workspace-id
    lakehouse_id: prod-lakehouse-id

# Workflow definitions
workflows:
  # Development workflow - for local development
  dev:
    description: Quick iteration workflow for local development
    stages:
      - clean
      - pre-scripts
      - metadata-extract
      - build
      - post-scripts
    options:
      log_level: INFO
      hashcheck_level: WARNING
      upload_notebooks: false
      auto_run_master: false
  
  # Full deployment workflow
  deploy:
    description: Complete deployment pipeline with upload and execution
    stages:
      - clean
      - pre-scripts
      - metadata-extract
      - metadata-download
      - build
      - post-scripts
      - upload
      - execute
      - results
    options:
      log_level: WARNING
      hashcheck_level: ERROR
      upload_notebooks: true
      auto_run_master: true
  
  # CI/CD workflow for automated testing
  ci:
    description: Continuous integration pipeline
    stages:
      - clean
      - metadata-extract
      - build
      - validate
    options:
      log_level: INFO
      hashcheck_level: ERROR
      notebook_timeout: 900
      upload_notebooks: false
      auto_run_master: false
  
  # Production deployment with specific environment
  production:
    description: Production deployment workflow
    environment: production
    stages:
      - clean
      - pre-scripts
      - metadata-extract
      - metadata-download
      - build
      - post-scripts
      - upload
      - execute
      - results
    options:
      log_level: ERROR
      hashcheck_level: ERROR
      notebook_timeout: 3600
      upload_notebooks: true
      auto_run_master: true
  
  # Custom example - only specific stages
  quick-test:
    description: Quick test without cleaning or uploading
    stages:
      - metadata-download
      - build
    options:
      log_level: DEBUG
      hashcheck_level: BYPASS
"""
        
        save_path = Path(path) if path else Path.cwd() / ".dbt-wrapper.yml"
        
        with open(save_path, 'w') as f:
            f.write(example_config)
        
        if self.console:
            self.console.print(f"[success]Example configuration created at: {save_path}[/success]")
        
        return save_path
    
    def get_workflow(self, workflow_name: str) -> Optional[PipelineWorkflow]:
        """Get a specific workflow configuration"""
        if not self.config:
            self.load_config()
        
        if self.config and workflow_name in self.config.workflows:
            workflow = self.config.workflows[workflow_name]
            
            # Merge with defaults
            merged_options = {**self.config.defaults, **workflow.options}
            workflow.options = merged_options
            
            # Apply environment-specific settings if specified
            if workflow.environment and workflow.environment in self.config.environments:
                env_config = self.config.environments[workflow.environment]
                workflow.options.update(env_config)
            
            return workflow
        
        return None
    
    def list_workflows(self) -> List[str]:
        """List all available workflow names"""
        if not self.config:
            self.load_config()
        
        if self.config:
            return list(self.config.workflows.keys())
        return []
    
    def validate_config(self, config: PipelineConfig) -> List[str]:
        """Validate configuration and return list of issues"""
        issues = []
        
        # Check version
        if not config.version:
            issues.append("Missing version field")
        
        # Validate workflows
        for workflow_name, workflow in config.workflows.items():
            if not workflow.stages:
                issues.append(f"Workflow '{workflow_name}' has no stages defined")
            
            # Check for valid stage names
            valid_stages = [
                'clean', 'pre-scripts', 'metadata-extract', 'metadata-download',
                'build', 'post-scripts', 'upload', 'execute', 'validate', 'results'
            ]
            for stage in workflow.stages:
                if stage not in valid_stages:
                    issues.append(f"Workflow '{workflow_name}' has invalid stage: {stage}")
            
            # Check environment reference
            if workflow.environment and workflow.environment not in config.environments:
                issues.append(f"Workflow '{workflow_name}' references undefined environment: {workflow.environment}")
        
        # Validate options
        valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR']
        valid_hashcheck_levels = ['BYPASS', 'WARNING', 'ERROR']
        
        for workflow_name, workflow in config.workflows.items():
            if 'log_level' in workflow.options:
                if workflow.options['log_level'] not in valid_log_levels:
                    issues.append(f"Workflow '{workflow_name}' has invalid log_level: {workflow.options['log_level']}")
            
            if 'hashcheck_level' in workflow.options:
                if workflow.options['hashcheck_level'] not in valid_hashcheck_levels:
                    issues.append(f"Workflow '{workflow_name}' has invalid hashcheck_level: {workflow.options['hashcheck_level']}")
        
        return issues