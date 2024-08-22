---

    weight: 3

---
# Documentation Guide

## Building you environment 
Documentation for this project is built using [mkdocs-material](https://squidfunk.github.io/mkdocs-material/). To contribute to the documentation you will need to create a separate python environment. I suggest that you call this `.env_mkdocs` to avoid confusion with the dbt environment. 

!!! important 
    The commands below assume that you have already performed the `Core Tools Installation` steps in the [User Guide](../user_guide/initial_setup/#core-tools-installation). If you have not done this yet, please do so before proceeding. Note you **ONLY** have to install `core tools` it is not necessary to move on to the `other tools` section. 


Before creating the environment you will need to clone the repository. You can do this by running the command below:

``` powershell title="clone the repository"
git clone https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb.git MyDocsProject

```
This will clone the repository into a directory called ==MyDocsProject==. You can rename this directory to whatever you like. Navigate into this new directory and then run the commands below.

``` powershell title="Create and activate the Python environment"
# Create the Python environment
python -m venv .env_mkdocs

# Activate the Python environment
.\.env_mkdocs\Scripts\activate.ps1

#Install the mkdocs packages
pip install -r ./requirements_mkdocs.txt

```

These commands will create a new python environment and install the required packages for building the documentation. 

## Updating the documentation
The documentation source is held in the `docs` directory. To update the documentation you will need to edit the markdown files in this directory. In order to understand the syntax used for the markdown be sure to review the reference section for [mkdocs-material](https://squidfunk.github.io/mkdocs-material/reference/). Once you have made your changes you can build the documentation using the command below:

``` powershell title="Build the documentation"
mkdocs build
```

To view the documentation locally you can use the command below:

``` powershell title="View the documentation locally"
mkdocs serve
```

!!! tip
    The `mkdocs serve` command will start a local web server that will allow you to view the documentation in your browser. The server will also automatically rebuild the documentation when you make changes to the source files.


Before publishing the documentation you should ensure that the documentation is up to date and that the changes are correct. You should also pull the latest from the repository to ensure that you are not overwriting someone else's changes. Do this by running the command below:

``` powershell title="Pull the latest changes from the repository"
git pull
```

You can now publish the documentation to the repository by running the command below:

``` powershell title="Publish the documentation"
mkdocs gh-deploy
```
