**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.


## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

## Running locally
### Installs 
#### Windows 
```bash
winget install microsoft.azd

```

#### OSX
```bash
brew tap azure/azd && brew install azd

```

### Profile
Create a profile like this one:

```yaml
fabric-spark-test:
  target: fabricspark-dev
    fabricspark-dev:
        authentication: CLI
        method: livy
        connect_retries: 0
        connect_timeout: 10
        endpoint: https://api.fabric.microsoft.com/v1
        workspaceid: bab084ca-748d-438e-94ad-405428bd5694
        workspacename: myworkspace
        lakehouseid: ccb45a7d-60fc-447b-b1d3-713e05f55e9a
        lakehouse: test
        schema: test
        threads: 1
        type: fabricspark
        retry_all: true
```