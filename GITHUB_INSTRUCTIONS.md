# GitHub Instructions for dbt-fabricsparknb

This document provides comprehensive instructions for using GitHub to interact with the dbt-fabricsparknb project.

## Table of Contents

- [Getting Started](#getting-started)
- [Contributing to the Project](#contributing-to-the-project)
- [Reporting Issues](#reporting-issues)
- [Working with Pull Requests](#working-with-pull-requests)
- [GitHub Workflows and CI/CD](#github-workflows-and-cicd)
- [Getting Help](#getting-help)

## Getting Started

### Repository Information
- **Repository**: [Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb](https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb)
- **Main Branch**: `main`
- **Documentation**: [GitHub Pages](https://insight-services-apac.github.io/APAC-Capability-DAI-DbtFabricSparkNb/)

### Prerequisites
- Git installed on your machine ([Installation Guide](https://github.com/git-guides/install-git))
- GitHub account
- Basic familiarity with Git and GitHub workflows

## Contributing to the Project

### For External Contributors (Non-Organization Members)

1. **Fork the Repository**
   ```bash
   # Click the "Fork" button on GitHub, then clone your fork
   git clone https://github.com/YOUR-USERNAME/APAC-Capability-DAI-DbtFabricSparkNb.git
   cd APAC-Capability-DAI-DbtFabricSparkNb
   ```

2. **Set up Development Environment**
   ```bash
   # Create and activate virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   uv pip install -e . -r requirements.txt
   uv pip install -e ".[dev]"
   ```

3. **Create a Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

4. **Make Your Changes**
   - Follow the coding standards in the project
   - Add tests for new functionality
   - Update documentation as needed

5. **Test Your Changes**
   ```bash
   # Run tests
   pytest
   
   # Run linting
   ruff check .
   ruff format .
   
   # Run pre-commit checks
   pre-commit run --all-files
   ```

6. **Commit and Push**
   ```bash
   git add .
   git commit -m "feat: your descriptive commit message"
   git push origin feature/your-feature-name
   ```

7. **Create Pull Request**
   - Go to your fork on GitHub
   - Click "Compare & pull request"
   - Fill out the PR template with detailed information
   - Submit the pull request to the main repository

### For Organization Members (Insight-Services-APAC)

1. **Clone the Repository Directly**
   ```bash
   git clone https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb.git
   cd APAC-Capability-DAI-DbtFabricSparkNb
   ```

2. **Create Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Follow steps 4-6 from External Contributors**

4. **Push Branch and Create PR**
   ```bash
   git push origin feature/your-feature-name
   # Then create PR through GitHub interface
   ```

## Reporting Issues

### Bug Reports
1. **Search Existing Issues**: Check if the bug has already been reported
2. **Use Bug Report Template**: Go to [Issues](https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb/issues/new?template=bug_report.md)
3. **Provide Detailed Information**:
   - Clear description of the bug
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (OS, Python version, etc.)
   - Screenshots or logs if applicable

### Feature Requests
1. **Use Feature Request Template**: Go to [Feature Requests](https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb/issues/new?template=feature_request.md)
2. **Provide Clear Description**:
   - What problem does this solve?
   - Proposed solution
   - Alternative solutions considered
   - Additional context

### General Questions
- Search existing [issues](https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb/issues)
- Check the [documentation](https://insight-services-apac.github.io/APAC-Capability-DAI-DbtFabricSparkNb/)
- Create a new issue with the "question" label

## Working with Pull Requests

### Pull Request Guidelines
1. **Use Descriptive Titles**: Clearly describe what the PR does
2. **Follow Conventional Commits**: Use prefixes like `feat:`, `fix:`, `docs:`, etc.
3. **Fill Out Template**: Provide detailed description of changes
4. **Link Related Issues**: Reference any related issues using `#issue-number`
5. **Keep PRs Focused**: One feature or fix per PR
6. **Update Documentation**: Include relevant documentation updates

### PR Review Process
1. **Automated Checks**: CI/CD workflows will run automatically
2. **Code Review**: Maintainers will review your code
3. **Address Feedback**: Make requested changes if any
4. **Approval and Merge**: Once approved, maintainers will merge

### Common PR Commands
```bash
# Update your branch with latest main
git checkout main
git pull origin main
git checkout your-feature-branch
git rebase main

# Push updates after addressing feedback
git add .
git commit -m "fix: address review feedback"
git push origin your-feature-branch
```

## GitHub Workflows and CI/CD

### Automated Workflows
The repository includes several GitHub Actions workflows:

1. **Testing Workflow**: Runs tests on multiple Python versions
2. **Deployment Workflow**: Deploys to workspace environments
3. **Documentation**: Builds and deploys documentation to GitHub Pages

### CI/CD Integration Example
For projects using this adapter, you can set up GitHub Actions:

```yaml
# .github/workflows/dbt.yml
name: dbt CI/CD

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
          
      - name: Install dependencies
        run: |
          pip install -e .
          
      - name: Run tests
        run: |
          dbt_wrapper test my_project
        env:
          DBT_FABRIC_WORKSPACE_ID: ${{ secrets.TEST_WORKSPACE_ID }}
          DBT_FABRIC_LAKEHOUSE_ID: ${{ secrets.TEST_LAKEHOUSE_ID }}
```

### Branch Protection Rules
The main branch is protected with the following rules:
- Require pull request reviews
- Require status checks to pass
- Restrict pushes to certain users/teams
- Require branches to be up to date before merging

## Getting Help

### Support Channels
1. **Documentation**: [GitHub Pages](https://insight-services-apac.github.io/APAC-Capability-DAI-DbtFabricSparkNb/)
2. **GitHub Issues**: [Create an issue](https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb/issues/new/choose)
3. **Support Guide**: See [SUPPORT.md](SUPPORT.md) for detailed support options

### Best Practices
- **Search Before Asking**: Check existing issues and documentation
- **Be Specific**: Provide detailed information when reporting issues
- **Follow Templates**: Use the provided issue and PR templates
- **Be Patient**: Maintainers review contributions as time allows
- **Be Respectful**: Follow the [Code of Conduct](CODE_OF_CONDUCT.md)

### Useful GitHub Features
- **Watch Repository**: Get notifications for new issues and PRs
- **Star Repository**: Show your support and bookmark the project
- **Fork Repository**: Create your own copy for contributions
- **Releases**: Track version updates and changelogs

## Additional Resources

- [GitHub Documentation](https://docs.github.com/)
- [Git Handbook](https://guides.github.com/introduction/git-handbook/)
- [Understanding the GitHub Flow](https://guides.github.com/introduction/flow/)
- [Forking Projects](https://guides.github.com/activities/forking/)
- [Contributing to Open Source](https://opensource.guide/how-to-contribute/)

---

For more detailed development instructions, see [CONTRIBUTING.md](CONTRIBUTING.md).