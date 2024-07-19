---
  weight: 4
---

# Fabric CI/CD with Git Deployment

## Git Based Deployment

The initial setup is based on a Git branch that is linked to all workspaces. As illustrated in the given example, we have described three stages: Development, Test, and Production. It also employs feature branches for individual developments within isolated workspaces using branch out functionality.

The successful operation of this scenario depends on branching, merging, and pull requests.

1. `Each workspace is assigned its own branch.`
2. `The introduction of new features is facilitated by raising pull requests.`
3. `All deployments are initiated from the repository.`
4. `To transition from Development to Test, and subsequently from Test to Production, a pull request must be initiated from the originating stage.`

The synchronization between the Git branch and the workspace can be automated. This is achieved by invoking the [Git Sync API](https://learn.microsoft.com/en-us/rest/api/fabric/core/git/update-from-git) as part of a build pipelines, which is automatically triggered following the approval of a pull request.

![Git and Workspace setup](../assets/diagrams/Fabric-CI-CD.drawio)

## Git and Build Environment Based Deployment

Git is exclusively linked to the Development workspace. The deployment to other stages is executed based on Build environments. This implies that the [Fabric Item APIs](https://learn.microsoft.com/en-us/rest/api/fabric/core/items) are utilized to perform Create, Read, Update or Delete operations.

Key points of this setup are:

1. `The Git repository serves as the foundation for creating, updating, or deleting items in the workspace.`
2. `Git is solely connected to the Development workspace.`
2. `Following a pull request, a Build pipeline is activated.`
3. `The Build pipeline executes operations to the workspace.`

!!! Note
        This approach is code-intensive and for each future item to be supported, modifications may be required in the Build pipelines.

![Git and Workspace setup](../assets/diagrams/Fabric-CI-Cd-2.drawio)

## Git and Fabric Deployment Pipeline Based Deployment

This is based on Fabric Deployment pipelines. This user-friendly interface simplifies the deployment process from one stage to another and is less code-intensive.

Git is solely connected to the Development workspace, and feature branches continue to exist in separate workspaces. However, the Test, Production, and any additional workspaces are not linked to Git.

Key aspects of this setup include:


1. `The release process to other stages, such as Test and Production, is managed via Deployment Pipelines in the Fabric.`
2. `The Development workspace is the only one connected to Git.`
3. `Triggers for the Fabric deployment pipeline can be automated. This is achieved by using Build Pipelines, which are automatically activated following the approval of a pull request.`

 These pipelines can call the [Fabric REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/deploy-stage-content) and can also be integrated with [Git Sync API](https://learn.microsoft.com/en-us/rest/api/fabric/core/git/update-from-git) for synchronizing the development workspace.

![Git and Workspace setup](../assets/diagrams/Fabric-Ci-Cd-3.drawio)
