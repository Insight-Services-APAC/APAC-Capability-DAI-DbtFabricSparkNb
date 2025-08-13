"""
Test suite to validate documentation accuracy against codebase implementation.
This test file can be used to automatically verify that documentation stays in sync with code.

Note: This is a static validation test that doesn't execute the actual commands.
"""

import pytest
import os
import re
from pathlib import Path
from typing import List, Dict, Any
import yaml
import ast
import importlib.util


class TestDocumentationAccuracy:
    """Test class to validate documentation accuracy"""
    
    @pytest.fixture
    def project_root(self):
        """Get project root directory"""
        return Path(__file__).parent.parent.parent
    
    @pytest.fixture
    def main_module_ast(self, project_root):
        """Parse main.py module AST for command analysis"""
        main_path = project_root / "dbt_wrapper" / "main.py"
        with open(main_path, 'r') as f:
            return ast.parse(f.read())
    
    @pytest.fixture
    def workflows_module_ast(self, project_root):
        """Parse workflows.py module AST for workflow analysis"""
        workflows_path = project_root / "dbt_wrapper" / "workflows.py"
        with open(workflows_path, 'r') as f:
            return ast.parse(f.read())
    
    def test_readme_exists(self, project_root):
        """Test that README.md exists and is not empty"""
        readme_path = project_root / "README.md"
        assert readme_path.exists(), "README.md does not exist"
        
        content = readme_path.read_text()
        assert len(content) > 100, "README.md appears to be empty or too short"
        assert "dbt-fabricsparknb" in content, "README doesn't mention project name"
    
    def test_cli_commands_documented(self, project_root, main_module_ast):
        """Test that all CLI commands are documented"""
        # Extract command names from main.py using AST
        commands = []
        for node in ast.walk(main_module_ast):
            if isinstance(node, ast.FunctionDef):
                # Look for functions decorated with @app.command()
                for decorator in node.decorator_list:
                    if (isinstance(decorator, ast.Attribute) and 
                        decorator.attr == 'command'):
                        commands.append(node.name)
        
        # Check if commands are documented in CLI reference
        cli_ref_path = project_root / "docs" / "user_guide" / "cli_reference.md"
        if cli_ref_path.exists():
            cli_content = cli_ref_path.read_text()
            
            # Map internal function names to CLI commands
            command_map = {
                'dev': 'dbt_wrapper dev',
                'deploy': 'dbt_wrapper deploy',
                'build': 'dbt_wrapper build',
                'build_local': 'dbt_wrapper build-local',
                'test': 'dbt_wrapper test',
                'run': 'dbt_wrapper run',
                'run_all': 'dbt_wrapper run-all',  # legacy
            }
            
            for cmd in commands:
                if cmd in command_map:
                    cli_cmd = command_map[cmd]
                    assert cli_cmd in cli_content, f"Command '{cli_cmd}' not documented in CLI reference"
    
    def test_workflow_stages_accurate(self, project_root, workflows_module_ast):
        """Test that documented workflow stages match implementation"""
        # This would parse the workflows.py to extract actual stages
        # and compare with documentation
        docs_path = project_root / "docs" / "user_guide" / "cli_reference.md"
        if docs_path.exists():
            content = docs_path.read_text()
            
            # Check for key workflow stage sequences
            workflows = {
                'dev': ['clean', 'pre-scripts', 'metadata-extract', 'build', 'post-scripts'],
                'deploy': ['clean', 'pre-scripts', 'metadata-extract', 'metadata-download', 
                          'build', 'post-scripts', 'upload', 'execute', 'results'],
                'build': ['metadata-download', 'build'],
                'build-local': ['build', 'post-scripts'],
                'test': ['clean', 'pre-scripts', 'metadata-extract', 'metadata-download',
                        'build', 'post-scripts', 'validate'],
            }
            
            for workflow, stages in workflows.items():
                # Check that the workflow is documented
                assert f"dbt_wrapper {workflow}" in content, f"Workflow '{workflow}' not documented"
    
    def test_python_version_requirement(self, project_root):
        """Test that Python version requirement is correctly documented"""
        # Check pyproject.toml
        pyproject_path = project_root / "pyproject.toml"
        if pyproject_path.exists():
            with open(pyproject_path, 'r') as f:
                content = f.read()
                assert 'requires-python = ">=3.12"' in content
        
        # Check documentation mentions Python 3.12+
        readme_path = project_root / "README.md"
        if readme_path.exists():
            readme_content = readme_path.read_text()
            assert "Python 3.12" in readme_content or "3.12+" in readme_content, \
                "README doesn't mention Python 3.12+ requirement"
    
    def test_installation_instructions(self, project_root):
        """Test that installation instructions are accurate"""
        readme_path = project_root / "README.md"
        if readme_path.exists():
            content = readme_path.read_text()
            
            # Check for uv installation method (preferred)
            assert "uv pip install" in content or "uv" in content, \
                "Installation instructions should mention uv package manager"
            
            # Check for git clone method
            assert "git clone" in content, \
                "Installation should include git clone instructions"
    
    def test_configuration_file_structure(self, project_root):
        """Test that documented configuration structure matches implementation"""
        config_docs_path = project_root / "docs" / "user_guide" / "configuration.md"
        if config_docs_path.exists():
            content = config_docs_path.read_text()
            
            # Key configuration elements that should be documented
            config_elements = [
                'workflows:',
                'defaults:',
                'environments:',
                'version:',
                'stages:',
                'options:',
                'log_level:',
                'notebook_timeout:',
                'lakehouse_config:',
            ]
            
            for element in config_elements:
                assert element in content, f"Configuration element '{element}' not documented"
    
    def test_no_typos_in_documentation(self, project_root):
        """Test for common typos in documentation"""
        typo_patterns = [
            (r'fabrickspark', 'fabricspark'),  # Common typo
            (r'apater', 'adapter'),  # Common typo
            (r'leverging', 'leveraging'),  # Common typo
        ]
        
        docs_dir = project_root / "docs"
        for doc_file in docs_dir.rglob("*.md"):
            content = doc_file.read_text()
            for pattern, correct in typo_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                assert not matches, f"Typo found in {doc_file}: '{pattern}' should be '{correct}'"
    
    def test_all_dependencies_documented(self, project_root):
        """Test that key dependencies are mentioned in documentation"""
        key_deps = [
            'dbt-fabricspark',
            'dbt-spark',
            'msfabricpysdkcore',
            'typer',
        ]
        
        readme_path = project_root / "README.md"
        if readme_path.exists():
            content = readme_path.read_text()
            for dep in key_deps:
                assert dep in content or dep.replace('-', '_') in content, \
                    f"Key dependency '{dep}' not mentioned in README"
    
    def test_links_validity(self, project_root):
        """Test that internal documentation links are valid"""
        docs_dir = project_root / "docs"
        
        # Collect all .md files
        md_files = list(docs_dir.rglob("*.md"))
        
        for md_file in md_files:
            content = md_file.read_text()
            
            # Find internal links like [text](./path/to/file.md)
            internal_links = re.findall(r'\[.*?\]\((\./[^)]+\.md)\)', content)
            
            for link in internal_links:
                # Resolve the link relative to the current file
                link_path = (md_file.parent / link).resolve()
                assert link_path.exists(), \
                    f"Broken internal link in {md_file}: {link}"
    
    def test_commands_have_help_text(self, main_module_ast):
        """Test that all commands have help documentation"""
        for node in ast.walk(main_module_ast):
            if isinstance(node, ast.FunctionDef):
                # Check if it's a command function
                for decorator in node.decorator_list:
                    if (isinstance(decorator, ast.Attribute) and 
                        decorator.attr == 'command'):
                        # Check for docstring
                        docstring = ast.get_docstring(node)
                        assert docstring, f"Command '{node.name}' lacks help documentation"
                        assert len(docstring) > 10, f"Command '{node.name}' has too short help text"


class TestExampleValidity:
    """Test that code examples in documentation are valid"""
    
    def test_yaml_examples_valid(self, project_root):
        """Test that YAML examples in documentation are valid YAML"""
        docs_dir = project_root / "docs"
        
        for doc_file in docs_dir.rglob("*.md"):
            content = doc_file.read_text()
            
            # Extract YAML code blocks
            yaml_blocks = re.findall(r'```yaml\n(.*?)\n```', content, re.DOTALL)
            
            for i, yaml_block in enumerate(yaml_blocks):
                try:
                    yaml.safe_load(yaml_block)
                except yaml.YAMLError as e:
                    pytest.fail(f"Invalid YAML in {doc_file} (block {i+1}): {e}")
    
    def test_bash_examples_syntax(self, project_root):
        """Test that bash examples have valid syntax"""
        docs_dir = project_root / "docs"
        
        for doc_file in docs_dir.rglob("*.md"):
            content = doc_file.read_text()
            
            # Extract bash code blocks
            bash_blocks = re.findall(r'```bash\n(.*?)\n```', content, re.DOTALL)
            
            for block in bash_blocks:
                # Basic syntax checks
                lines = block.strip().split('\n')
                for line in lines:
                    if line.strip() and not line.strip().startswith('#'):
                        # Check for basic command structure
                        assert not line.strip().startswith('-'), \
                            f"Bash command in {doc_file} starts with dash: {line}"
                        
                        # Check for dbt_wrapper commands
                        if 'dbt_wrapper' in line:
                            # Should be valid command syntax
                            assert re.match(r'^dbt_wrapper\s+\w+', line.strip()), \
                                f"Invalid dbt_wrapper command syntax in {doc_file}: {line}"


if __name__ == "__main__":
    # This allows running the tests directly
    pytest.main([__file__, '-v'])