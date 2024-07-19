---

    title: "Dbt Build Process"
    excerpt: "This guide will walk you through the process of building your dbt project using the dbt-fabricsparknb package."
    sidebar_label: "Dbt Build Process"
    slug: /user_guide/dbt_build_process
    weight: 3

---

# Dbt Build Process

## Run the build script
Run the build script that you created in the previous step using the code below in the terminal.

!!! Important
    Be sure to replace ==my_project== with the name of your dbt project folder.

```powershell
python post_install.py my_project 
```

## Additional Steps for the First Run
The first time that you run this script you will get a warning that the metadata json files are not present. You will need to follow the steps in the warning message to download the metadata json files from your Fabric Lakehouse. These steps are as follows: 

- [x] In a pwsh terminal run the script below. This will set an environment variable that will be used by steps later on. Be sure to replace ==my_project== with the name of your dbt project folder.
    ```powershell           
    $env:DBT_PROJECT_DIR = "my_project"
    ```
- [x] In the same terminal now run the script below. This will upload a series of notebooks to a folder in your Fabric workspace
    ```powershell           
    Invoke-Expression -Command $env:DBT_PROJECT_DIR/target/pwsh/upload.ps1
    ```
- [x] Now run the following code to output the path to the import notebook. This will be used to import the notebooks into your workspace:
    ```powershell           
    Get-Childitem "$env:DBT_PROJECT_DIR/target/notebooks/import_*" | % {Write-Host $_.FullName}
    ```
- [x] Now login to the Fabric Portal and navigate to the workspace and lakehouse you are using and import the notebook using the path from the previous step.
!!! tip "How to manually upload a notebook"
    See [https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook#import-existing-notebooks](https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook#import-existing-notebooks)
- [x] Open the notebook in the workspace and run all cells. This will upload the generated notebooks to your workspace.
- [x] A new notebook should appear in the workspace called metadata_==my_project==_extract.ipynb where the text ==my_project== is replaced with the name of your dbt_project. Open this notebook and run all cells. This will generate the metadata extract json files and place them in the metaextracts sub-directory of your lakehouse.
- [x] Now, back in your powershell terminal. Run the script below. This will download the metadata extract json files to a subfolder in your dbt project directory called metaextracts.
    ```powershell
    Invoke-Expression -Command $env:DBT_PROJECT_DIR/target/pwsh/download.ps1
    ```
- [x] Now re-run the dbt build script.
    ```powershell
    python post_install.py my_project 
    ```
!!! Important
    Be sure to replace ==my_project== with the name of your dbt project folder.

When you run this build script successfully, you will see a series of notebooks generated in your ==my_project==/target/notebooks directory. This is the `"special sauce"` of this dbt-adapter that allows your to run your dbt project natively as notebooks in a Fabric workspace. The image below shows a sample listing of generated notebooks. Your specific notebooks will be contain the name of your dbt project and may be different depending on the models and tests that you have defined in your dbt project. 

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

    

### Post Build Steps & Checks

After a successful execution you can now upload your notebooks to your Fabric Workspace and run your dbt transformations against your lakehouse/s. To do this follow the steps below: 


- [x] In a pwsh terminal run the script below. This will set an environment variable that will be used by steps later on. Be sure to replace ==my_project== with the name of your dbt project folder.
    ```powershell           
    $env:DBT_PROJECT_DIR = "my_project"
    ```
- [x] In the same terminal now run the script below. This will upload a the generated notebooks to a folder in your Fabric workspace
    ```powershell           
    Invoke-Expression -Command $env:DBT_PROJECT_DIR/target/pwsh/upload.ps1
    ```
- [x] In your Fabric workspace open the import_==my_project==_notebook and run all cells. This will upload the generated notebooks to your workspace. If they already exist they will be updated. In order to organise your workspace efficiently I suggest that you move them all into a folder called the matches the name of your dbt project. Don't worry, even if you upload a new version of the notebooks the folder structure will be mantained.