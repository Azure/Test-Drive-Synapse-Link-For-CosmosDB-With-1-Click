## Azure Synapse Link For CosmosDB 1-click Environment
This 1-click deployment allows the user to deploy an environment with Synapse Link, you can directly connect to your Azure Cosmos DB containers from Azure Synapse Analytics and access the analytical store with no separate connectors. This scenario is to Ingest data into Cosmos DB containers, Setup Spark tables, Join & aggregate operational data across Cosmos DB containers.Pyspark Notebooks to Perform Sales Forecasting and Anomaly Detection using Azure Synapse Link,Azure Automated Machine Learning and Azure Cognitive Services on Synapse Spark (MMLSpark).

![SynapseCosmosDB](https://github.com/Azure/Test-Drive-Synapse-Link-For-CosmosDB-With-1-Click/blob/main/images/synapse-cosmosdb.png)

## Prerequisites

Owner role (or Contributor roles) for the Azure Subscription the template being deployed in. This is for the creation of a separate Resource Group and to delegate roles necessary for this proof of concept. Refer to this [official documentation](https://docs.microsoft.com/en-us/azure/role-based-access-control/role-assignments-steps) for RBAC role-assignments.

## Deployment Steps
1. Fork out [This GitHub Repository](https://github.com/Azure/Test-Drive-Synapse-Link-For-CosmosDB-With-1-Click) into your GitHub account. 
    
   **If you don't fork repo:** 
   + **Notebook will not be deployed**
   + **You will get a Github publishing error**
   
   
  <!--  ![Fork](https://raw.githubusercontent.com/Azure/Test-Drive-Synapse-Link-For-CosmosDB-With-1-Click/main/images/4.gif) -->
 
2. While in your forked repo,Click 'Deploy To Azure' button given below to deploy all the resources.

    [![Deploy To Azure](https://raw.githubusercontent.com/Azure/azure-quickstart-templates/master/1-CONTRIBUTION-GUIDE/images/deploytoazure.svg?sanitize=true)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FAzure%2FTest-Drive-Synapse-Link-For-CosmosDB-With-1-Click%2Fmain%2Fazuredeploy.json)

   - Provide the values for:

     - Resource group (create new)
     - Region
     - Company Tla
     - Option (true or false) for Allow All Connections
     - Option (true or false) for Spark Deployment
     - Spark Node Size (Small, Medium, large) if Spark Deployment is set to true
     - Sql Administrator Login
     - Sql Administrator Login Password
     - Sku
     - Option (true or false) for Metadata Sync
     - Frequency
     - Time Zone
     - Resume Time
     - Pause Time
     - CosmosDB Account Name
     - CosmosDB Throughput Policy
     - CosmosDB Manual Provisioned Throughput
     - CosmosDB Autoscale Max Throughput
     - CosmosDB Analytical Store TTL (Default -1)
     - Github Username (username for the account where [This GitHub repository](https://github.com/Azure/Test-Drive-Synapse-Link-For-CosmosDB-With-1-Click) was forked out into)

   - Click 'Review + Create'.
   - On successful validation, click 'Create'.

## Azure Services being Deployed
This template deploys necessary resources to support an Azure Synapse link for CosmosDB which includes the following resources along with some RBAC role assignments:

- An Azure Synapse Workspace 
- An Azure Synapse SQL Pool
- An optional Apache Spark Pool
- Azure Data Lake Storage Gen2 account
- A new File System inside the Storage Account to be used by Azure Synapse
- A Logic App to Pause the SQL Pool at a defined schedule
- A Logic App to Resume the SQL Pool at a defined schedule
- A key vault to store the secrets
- CosmosDB Database (CosmosDemoDB)
- CosmosDB Containers with Analytical Store Enabled
  - Products
  - StoreDemoGraphics
  - RetailSales
  - IoTDeviceInfo
  - IoTSignals
- AML workspace
- Pyspark Notebook to ingest batch data into CosmosDB containers, Fetch data from CosmosDB,Join dataset together,Perform Sales Forecasting using Azure Synapse Link and Azure Automated Machine Learning on Synapse Spark 
- Pyspark Notebook to ingest stream and batch data into CosmosDB containers, Fetch data from CosmosDB,Join dataset together,Perform Anomaly Detection using Azure Synapse Link and Azure Cognitive Services on Synapse Spark (MMLSpark)

<!-- The data pipeline inside the Synapse Workspace gets New York Taxi trip and fare data, joins them and perform aggregations on them to give the final aggregated results. Other resources include datasets, linked services and dataflows. All resources are completely parameterized and all the secrets are stored in the key vault. These secrets are fetched inside the linked services using key vault linked service. The Logic App will check for Active Queries. If there are active queries, it will wait 5 minutes and check again until there are none before pausing -->

## Post Deployment
- Current Azure user needs to have ["Storage Blob Data Contributor" role access](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-add-admin#azure-rbac-role-assignments-on-the-workspaces-primary-storage-account) to recently created Azure Data Lake Storage Gen2 account to avoid 403 type permission errors.
- After the deployment is complete, click 'Go to resource group'.
- You'll see all the resources deployed in the resource group.
- Click on the newly deployed Synapse workspace.
- Click on the link 'Open' inside the box labeled as 'Open Synapse Studio'.
- Click on 'Log into Github' after the workspace is opened. Provide your credentials for the Github account holding the forked-out repository.
- After logging to your GitHub account, click on the 'Notebook' icon in the left panel. A blade will appear from the right side of the screen.
- Make sure that the 'main' branch is selected as 'Working branch' and click 'Save'.

![Start Workspace](https://github.com/Azure/Test-Drive-Synapse-Link-For-CosmosDB-With-1-Click/blob/main/images/Start_Workspace2.gif)

#### Configuring Synapse Link for CosmosDB
- In Synapse Studio click on the 'Manage' icon in the left panel and navigate to 'Linked Services' menu option.
- In Synapse Studio click on 'CosmosDBLink' linked service to open up configuration settings.
- Select 'Connection String' under Authentication Method.
- Select 'From Azure subscription' under 'Account Selection Method'.
- Select Subscription under which the package is deployed.
- Select CosmosDB account
- Select CosmosDB Databasename 'CosmosDemoDB'
- Click 'Apply' to save the changes

![Configure Comsos Link](https://github.com/Azure/Test-Drive-Synapse-Link-For-CosmosDB-With-1-Click/blob/main/images/Configure_CosmosLink3.gif)

#### Navigating Synapse Link for CosmosDB
- To verify CosmosDB Analytical store and containers
  - In Cosmos DB account navigate to 'Features' tab. 
  - Synapse Link will appear as Enabled option.
  - To check the pre-created database and containers with Analytical store navigate to 'Data Explorer' tab.
- In Synapse Studio you can see the same CosmosDB containers configured 
  - Navigate to 'Data' section in the left panel and then to the 'Linked' menu option.
  - Expand 'Azure CosmosDB', There will be five containers listed with 'Analytical Store' enabled.

![Navigate Synapse Link](https://github.com/Azure/Test-Drive-Synapse-Link-For-CosmosDB-With-1-Click/blob/main/images/Navigate_Synapse_Link3.gif)

#### Notebook Execution Using Synapse Link for CosmosDB
- In Synapse Studio click on the 'Develop' icon in the left panel and navigate to the 'Notebooks' section.
- There are two Notebooks available '1-SalesForecastingWithAML' and '2-AnomalyDetectionWithMML'.
- Follow the instructions in each cell of Notebook to execute:
  - Ingest Data into CosmosDB containers using Synapse Link for CosmosDB
  - Create Spark tables out of CosmosDB using Synapse link for CosmosDB
  - Join spark tables using Synapse spark serverless
  - Execute Machine learning model on this dataset using Azure ML

![Navigate Notebook](https://github.com/Azure/Test-Drive-Synapse-Link-For-CosmosDB-With-1-Click/blob/main/images/Navigate_Notebook4.gif)

#### Verify Data in CosmosDB Containers
- On completion of Notebook CosmosDB containers will be populated with a sample dataset.
- Navigate to CosmosDB account,Under 'Data Explorer' tab you can see containers populated with data.

![CosmosDB Populated Containers](https://github.com/Azure/Test-Drive-Synapse-Link-For-CosmosDB-With-1-Click/blob/main/images/CosmosDB_Containers_Data2.gif)

#### Read and Explore CosmosDB data within Synapse studio Using Synapse Link for CosmosDB
- In Synapse Studio navigate to 'Data' tab in the left menu,Once you are here click on 'Linked' tab.
- Expand the 'Azure CosmosDB' option and right click container you want to load the dataframe for.
- On-Right click section select 'New Notebook' then 'Load to DataFrame'.
- Attach the already created Spark pool to Notebook and execute.
- On successful completion,You should be able to see the resultset in Synapse Studio.

![Read and Explore](https://github.com/Azure/Test-Drive-Synapse-Link-For-CosmosDB-With-1-Click/blob/main/images/Read_Container2.gif)

#### Publish Changes
- Once published all the resources will now be available in the live mode.
- To switch to the live mode from git mode, click the drop-down at the top left corner and select 'Switch to live mode'.

![Publish](https://github.com/Azure/Test-Drive-Synapse-Link-For-CosmosDB-With-1-Click/blob/main/images/Publish3.gif)

