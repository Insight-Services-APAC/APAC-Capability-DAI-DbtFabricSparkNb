# Dbt Fabric Spark Notebook Generator (Dbt-FabricSparkNb)

> The first and only dbt adapter for a true, modern, software-as-a-service (SAAS) Lakehouse.

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


## Developer Guides

Developer guides have been created to assist with the setup of applications required, the installation of dbt and framework setup.

- [Windows Application Setup](developer_guide/applications_setup.md)

- [dbt Setup](developer_guide/dbt_setup.md)

- [Framework Setup](developer_guide/framework_setup.md)


## Community

**TBA**
