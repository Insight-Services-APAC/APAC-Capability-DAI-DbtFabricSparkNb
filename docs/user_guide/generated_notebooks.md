---
    weight: 4
---

# Understanding the Generated Notebooks

## Understanding the Notebooks Generated

When you run the build script successfully, you will see a series of notebooks generated in your my_project/target/notebooks directory. This is the `special sauce` of this dbt-adapter that allows your to run your dbt project natively as notebooks in a Fabric workspace. The image below shows a sample listing of generated notebooks. Your specific notebooks will be contain the name of your dbt project and may be different depending on the models and tests that you have defined in your dbt project.

**Sample listing of Generated Notebooks**

![notebooks](../assets/images/notebooks.png)

If you study the files shown above you will notice that there is a naming convention and that the notebooks are prefixed with a specific string. The following table explains at a high level the naming convention and the purpose of each type of notebook.

| Notebook Prefix | Description               |
| --------------- | --------------------------|
|  model.         |  These are dbt **model** notebooks. A notebook will be generated for each dbt **model** that you define. You will be able to run, debug and monitor execution of these notebooks directly in the Fabric portal independently of dbt.|
|  test.          |  These are dbt **test** notebooks. A notebook will be generated for each dbt **test** that you define. You will be able to run, debug and monitor execution of these notebooks directly in the Fabric portal independently of dbt. |
|  seed.          |  These are dbt **seed** notebooks. A notebook will be generated for each dbt **seed** that you define. You will be able to run, debug and monitor execution of these notebooks directly in the Fabric portal independently of dbt.|
|  master_        |  These are **execution orchestration** notebooks. They allow the running of your models, tests and seeds in parallel and in the correct order. They are what allow you to run your transformation pipelines independently of dbt as an orchestrator. In order to run your project simply schedule master.{project_name}.notebook.iypnb using Fabric's native scheduling functionality |
|  import_        |  This is a helper notebook that facilitate import of generated notebooks into workspace.  |
|  metadata_      |  This is a helper notebook to facilitate generation of workspace metadata json files.    |


!!! important
    The green panels below provide a more detailed discussion of each type of notebook. Take a moment to expand each panel by clicking on it and read the detailed explanation of each type of notebook.

??? Question "Notebooks with the Prefix `"model."`"
    These are dbt **model** notebooks. A notebook will be generated for each dbt **model** that you define. You will be able to run, debug and monitor execution of these notebooks directly in the Fabric portal independently of dbt.

    ![alt text](../assets/images/model_notebook0.png)

    ![alt text](../assets/images/model_notebook1.png)

??? Question "Notebooks with the Prefix `"test."`"
    These are dbt **test** notebooks. A notebook will be generated for each dbt **test** that you define. You will be able to run, debug and monitor execution of these notebooks directly in the Fabric portal independently of dbt.

??? Question "Notebooks with the Prefix `"seed."`"
    These are dbt **seed** notebooks. A notebook will be generated for each dbt **seed** that you define. You will be able to run, debug and monitor execution of these notebooks directly in the Fabric portal independently of dbt.

??? Question "Notebooks with the Prefix `"master_"`"
    These are **execution orchestration** notebooks. They allow the running of your models, tests and seeds in parallel and in the correct order. They are what allow you to run your transformation pipelines independently of dbt as an orchestrator. In order to run your project simply schedule master.{project_name}.notebook.iypnb using Fabric's native scheduling functionality.

??? Question "Notebooks with the Prefix `"import_"`"
    This is a helper notebook that facilitates import of generated notebooks into workspace.

??? Question "Notebooks with the Prefix `"metadata_"`"
    This is a helper notebook to facilitates the generation of workspace metadata json files.

## Notebooks in your Fabric Workspace
If you login to your fabric workspace and navigate to the notebooks section you will see that the generated notebooks have been uploaded to your workspace.

!!! tip
    I suggest that you move your notebooks into a folder that matches the name of your dbt project.

