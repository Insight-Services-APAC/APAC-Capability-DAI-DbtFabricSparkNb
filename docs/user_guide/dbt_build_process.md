---

    title: "Dbt Build Process"
    excerpt: "This guide will walk you through the process of building your dbt project using the dbt-fabricsparknb package."
    sidebar_label: "Dbt Build Process"
    slug: /user_guide/dbt_build_process
    weight: 3

---

# Dbt Build Process

## Run the build script
Run the build script using the code below in the terminal.

!!! Important
    
    Prerequisites: Make sure you're logged into your tenant in the PowerShell terminal using the command `az login` and your fabric tentant id.
    ```powershell
    az login --tenant 73738727-cfc1-4875-90c2-2a7a1149ed3d 
    ```
    Be sure to replace ==my_project== with the name of your dbt project folder. 

```powershell
dbt_wrapper run-all my_project 
```

When you run this build script successfully, you will see a series of notebooks generated in your ==my_project==/target/notebooks directory. 
After a successful execution you can now see your notebooks to your Fabric Workspace.  In order to organise your workspace efficiently I suggest that you move them all into a folder called the matches the name of your dbt project. The image below shows a sample listing of generated notebooks. Your specific notebooks will be contain the name of your dbt project and may be different depending on the models and tests that you have defined in your dbt project. 

#### Sample listing of Generated Notebooks
![notebooks](/assets/images/notebooks.png)

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

    ![alt text](./assets/images/model_notebook0.png)

    ![alt text](./assets/images/model_notebook1.png)

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

### Other useful dbt_wrapper options
 | Options|Description|   
 | --------------- | --------------------------|
 |dbt_wrapper --help|Displays all the userful commands|
 |--install-completion|Install completion for the current shell.|
 |--show-completion|how completion for the current shell, to copy it or customize the installation.|

### Other useful dbt_wrapper commands

 | Commands|Description|
 | --------------- | --------------------------|
 |build-dbt-project |This command will just build the dbt project. It assumes all other stages have been completed.|
 |docs|This command will generate the documentation for the dbt project.|
 |execute-master-notebook|This command will just execute the final orchestrator notebook in Fabric. Assumes that the notebook has been uploaded.|
 |run-all|This command will run all elements of the project. For more granular control you can use the options provided to suppress certain stages or use a different command.|
 |run-all-local|This command will just execute the final orchestrator notebook in Fabric. Assumes that the notebook has been uploaded.|

