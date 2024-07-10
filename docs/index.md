---
    title: Home
---
# Dbt Fabric Spark Notebook Generator (Dbt-FabricSparkNb)

The first and only dbt adapter for a true, modern, software-as-a-service (SAAS) Lakehouse.

## What is it

...

## Why did we build it?
As a team of data specialists we have been working with dbt for a number of years. We have found that dbt is a powerful tool for data transformation, but it has some limitations. We have built this adapter to address some of these limitations and to make it easier to work with dbt in a modern, software-as-a-service (SAAS) lakehouse environment. 

## How does it work?
Dbt-FabricSparkNb works by leverging the power of the dbt-core, and the [dbt-fabrickspark](https://github.com/microsoft/dbt-fabricspark) apater to create a new adapter. As such, it can be described as a "child apater" of [dbt-fabrickspark](https://github.com/microsoft/dbt-fabricspark). 

The adapter inherits all of the functionality of the [dbt-fabrickspark](https://github.com/microsoft/dbt-fabricspark) adapter and simply extends it to meet the unique requirements of our project.

Consequently, to use this adapter, you will need to install the [dbt-fabrickspark](https://github.com/microsoft/dbt-fabricspark) adapter and then install the [dbt-fabricksparknb](https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb) adapter.

## Key Features

- Support for SAAS only lakehouse architecture (No PAAS components requried)
- Support for lightweight, disconnected local development workflow
- Fully featured with modern data transformation capabilities such as data lineage, data catalog, data quality checks and templated data transformation activities
- Opensource and free to use
- Extensible and customisable

## Adapter User Guide
For data engineers looking to use the adapter, the user guide can be found [here](./user_guide/initial_setup.md)

## Adapter Developer Guides
For advanced users looking to extend or customise the adapter the adapter developer guide can be found [here]()

#### Branching
When creating a branch to work on from please use the branch name of "feature/YourBranchName". The case on "feature/" matters so please make sure to keep it lower case. Pull requests are to be made into the "dev" branch only. Any pull requests made into "Main" will be removed and not merged.


## Community

### Logging to Delta

Logging was previously done to a log file saved in the lakehouse and in json format. This has been changed to now log to a delta table in the lakehouse.

It works using 2 tables *"batch"* and *"execution_log"*. At the start of the ETL the Prepare step will check if the tables exist and if they done they will be created. This is followed by a check for an *"open"* batch and where the batch is still open it will fail. 

If you need to close the batch manually, this code is available at the end of the master notebook. 

If this check passes, a batch will be opened. There are steps in each master numbered notebook to check for failures in previousn notebook runs and this is done using the open batch so previous ETL executions with failures are not picked up and return false stops on the current execution.
