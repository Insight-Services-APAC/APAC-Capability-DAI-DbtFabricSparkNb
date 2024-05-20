
# Objective of this project

This project is the result of a search to find a tool that would accelerate the development and ongoing maintenance processes relating to data transformation activities within a software-as-a-service (SAAS) aligend lakehouse architecture and technology stack.

When designing this project the core, **"must have"** requirements were to:

1) Minimise the effort required to **`track lineage and dependencies`** between data transformation activities

2) Minimise the effort required to create, maintain and update **`data transformation activities`**

3) Minimise the effort required to **`document data transformation activities`**

4) Minimise the effort required to **`create and maintain a data catalog`** of the entities created by data transformation activities

5) Be compatable with a **`Software-As-A-Service (SAAS) technology stack`** and avoid the neeed for platform as a Service (PaaS) components

6) **`Avoid locking-in transformation logic`** into specific vendor proprietry language or technology stack.

7) Minimise the effort required to **`test data transformation activities`**

Secondary **"nice to have"** requirements were:

1) Minimise the effort required to create, maintain, update and execute **`data quality checks`**

2) **`Automate the generation of data transformation activities`** from a data lineage and dependencies

# Discovering our approach

After reviewing the available tools and technologies and considering the requirements it soon became clear to us that [Dbt-Core ](https://github.com/dbt-labs/dbt-core) appeared to be good fit for our needs. However, there were some limitations that we needed to address:

1) Dbt-Core is a command line tool and all of its adapters appeared to require some Platform as a Service (PaaS) comoponents to be introduced into the technology stack in order to host a secure "python" runtime environement. This was not compatable with our requirement for a Software-As-A-Service (SAAS) technology stack.

2) Dbt-Core combined code build, test and deployment with re-occuring data pipeline execution. This was not compatable with our requirement to separate the build, test and deployment processes from the data pipeline execution.

3) Dbt-Core rose to popularity as a tool for data transformation at a time when data warehouses and relational dataabses were the predominant transformation engines. There seemed to be some uncertainty as to how it would  perform in the context of a SAAS, lakehouse architecture. Dbt-Core can be quite "chatty" and create multiple connections to the transformation engine. This could be a problem in a SAAS environment where the number of connections may be limited. In addition, code execution against a lakehouse architecture tend to incur higher latency than traditional data warehouses. It is plausible to expect that Dbt-Core's deafult behaviour of creating multiple connections and executing a large number of separate and distinct code blocks would result in relatively long project run times and a degraded developer experience.
   
4) Dbt-core might be considered to be a "heavy" tool for some use cases. It is a large codebase with a lot of functionality. It is possible that some users may find it difficult to understand and use.




