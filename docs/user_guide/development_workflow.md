---
  weight: 5
---

# Development Workflow

## Development and Deployment Flow Using Fabric Spark Notebook Adapter
![FabricSparkNb_Developer_Flow](../assets/diagrams/modern_saas_lakehouse.drawio)

<div class="grid" markdown>
!!! success "Advantages"
    - [x] Available today
    - [x] Native Fabric Notebooks Generated and Deployed
          1. Non dbt users able to view notebooks and business logic
          2. Monitoring and debugging of loads directly in Fabric without the need for a separate tool
    - [x] Re-occurring loads achieved using native Fabric scheduling 
    - [x] Simplified code promotion process using native Fabric Git integration
    - [X] No need for dbt hosted in a virtual machine 
          1. No need for service account
          2. **No need for Azure Landing Zone**
          3. No need for secure network connectivity between Azure VM and Fabric   
    - [x] Allows for disconnected development environment providing
          1. Faster DBT build times
          2. Greater developer flexibility
    - [x] Simplified code promotion Process using native Fabric Git integration
          1. Single, native promotion process for all Fabric artifacts including non-dbt ones

!!! failure "Disadvantages"
    -  Requires Additional Steps
        1. Meta Data Extract
        2. Notebook Upload 
        3. Notebook Import


</div>


## Development and Deployment Flow Using Original Fabric Spark Adapter 

![FabricSpark](../assets/diagrams/modern_saas_lakehouse.drawio)


## Detailed Workflow

![](../diagrams/drawio/development_flow.drawio)

**Inital Setup**
1. Provision Workspace
   - **Development Environment:** Fabric Portal
   - **Re-occurence:** Do once per development environment set-up
   - **Instructions:** Create a new workspace in the Power BI Portal, or use an existing workspace.

2. Get Workspace Connection Details
   - **Development Environment:** Fabric Portal
   - **Re-occurence:** Do once per development environment set-up
   - **Instructions:** Get the workspace connection details from the Power BI Portal.
   
3. Create or Update `profiles.yml`
   - **Development Environment:** VS Code on local, developemnt machine
   - 

4. Create or Update `dbt_project.yml`
   ![](./assets/dbt_project.yml.png)
5. Build Project
6. Manually Upload Notebooks 
7. Run Meta Data Extract

**Ongoing Development Cycle**

8. Download Metadata: 
   
9.  Update Dbt Project 
10. Build Dbt Project 
11. Verify Outputs 
12. Update Notebooks
    <ol type="a">
        <li>Upload to Onelake</li>
        <li>Update to GIT repo</li>   
    </ol>    
13. Promote to Workspace
    <ol type="a">
        <li>Run Import Notebook</li>
        <li>Promote GIT branch</li>   
    </ol>   
14. Run Master Notebook 
15. Validate Results 
16. Run Metadata Extract