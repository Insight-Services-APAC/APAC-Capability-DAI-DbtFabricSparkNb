---
    title: Home
---
# Dbt Fabric Spark Notebook Generator (Dbt-FabricSparkNb)

<em><p style="text-align: center;"><b>"The first and only dbt adapter for a true, modern, software-as-a-service (SAAS) Lakehouse."</b></p></em>

## What is it
The `dbt-fabricsparknb` package contains all of the code enabling dbt to work with Microsoft Fabric **WITHOUT** a connection endpoint like livy or the Datawarehouse SQL endpoint. 

## Why did we build it?
As a team of data specialists we have been working with dbt for a number of years. We have found that dbt is a powerful tool for data transformation, but it has some limitations. We have built this adapter to address some of these limitations and to make it easier to work with dbt in a modern, software-as-a-service (SAAS) lakehouse environment. 

## How does it work?
Dbt-FabricSparkNb works by leverging the power of the dbt-core, and the [dbt-fabrickspark](https://github.com/microsoft/dbt-fabricspark) apater to create a new adapter. As such, it can be described as a "child apater" of [dbt-fabrickspark](https://github.com/microsoft/dbt-fabricspark). 

The adapter inherits all of the functionality of the [dbt-fabrickspark](https://github.com/microsoft/dbt-fabricspark) adapter and simply extends it to meet the unique requirements of our project.

Consequently, to use this adapter, you will need to install the [dbt-fabrickspark](https://github.com/microsoft/dbt-fabricspark) adapter and then install the [dbt-fabricksparknb](https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb) adapter.

## Key Features

- [x] Support for SAAS only lakehouse architecture (No PAAS components requried)
- [x] Support for lightweight, disconnected local development workflow
- [x] Fully featured with modern data transformation capabilities such as data lineage, data catalog, data quality checks and templated data transformation activities
- [x] Opensource and free to use
- [x] Extensible and customisable

## Unsupported Features

- [x] High Concurrency Sessions is currently not supported

## Getting Started



<div class="grid cards" markdown>

-   :material-clock-fast:{ .lg .middle } __User Guide__

    ---

    For data engineers looking to use the adapter to create data transformation projects

    [:octicons-arrow-right-24: User Guide](./user_guide/index.md)

    !!! note
        This is the guide appropriate for **MOST** users.

-   :octicons-tools-16:{ .lg .middle } __Developer Guide__

    ---

    For advanced users looking to extend or customise the adapter

    [:octicons-arrow-right-24: Developer Guide](./developer_guide/index.md)

    !!! danger
        This is the guide for advanced users only.

-   :octicons-file-code-16:{ .lg .middle } __Documentation Guide__

    ---

    For users who are looking to contribute to the adapter documentation

    [:octicons-arrow-right-24: Documentation Guide](./documentation_guide/index.md)

    !!! warning
        This guide is still under construction
    



</div>


## Contributing
### Branching
When creating a branch to work on from please use the branch name of `feature/YourBranchName`. The case on `feature/` matters so please make sure to keep it lower case. Pull requests are to be made into the "dev" branch only. Any pull requests made into "Main" will be removed and not merged.

