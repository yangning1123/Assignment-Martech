# Assignment-Martech

## Overview
At first sight, the data has a typical hive-style format, so I developed an application based on Spark SQL([version v1](https://github.com/yangning1123/Assignment-Martech/blob/main/Martech-Assignment-V1-Prod.ipynb)), generating a wide table to fulfill this task. The core principles were: (1) Create external table from gcs. (2) Create tables for 2g, 3g, 4g and site, respectively. (3) Based on site table left join 2g, 3g and 5g tables to calculate site_$TECHNOLOGY_cnt and frequency_band_is_exist. We can make this code run every day if we set the datekey parameter(default: yesterday like '2025-03-27'). It also can be triggered just once to backfill the historical data.

But after I finished it, I found there were a lot of hard-coded blocks. It was not elegant at all. What's more, it also lacks flexibility. There will be 5G or 6G added in the future, it will be a disater to maintain this code block. 
So I developed the second version([version v2](https://github.com/yangning1123/Assignment-Martech/blob/main/Martech-Assignment-V2-Prod.ipynb)), fully took advantage of python and SQL pivot functionalities. The core principles were as follows: 
(1) Create a dimensional table, which contains technology's basic info, like alias and frequency, only the dimensional table should be maintained in the future. (2) Dynamically aquire technology data from gcs based on the dimensional table info. (3) Dynamically produce the result data, automatically enriching the result data based on the dimensional table. In a nut shell, this version is easy to maintain, adaptive when the technology is expanding.


## Step by Step
1. Setup GCP. we are using GCP, so we need to create a project in GCP and a buckets in Cloud Storage, then upload the Archive data into the buckets.
2. Setup Databricks. In GCP, search the product named Databricks and enable this service.
3. Setup Credential and Permissions. When we are in Databricks Workspace, we need to create a Credential and Permissions to connect buckets in GCP. [Docs](https://docs.databricks.com/gcp/en/connect/unity-catalog/cloud-storage/storage-credentials)
4. Setup Github. Create a git folder in Databricks, make sure the git folder point to a github repo. [Docs](https://docs.databricks.com/gcp/en/repos/)
5. Git clone this repo to your workspace in Databricks.
6. Specify result data to external location. Change the variable named `gcs_output_path` to specify external location if needed.
7. Setup Workflows. Create a job refering V2-Prod notebooks running everyday to transform data. It's better to use the repo prod branch to trigger the daily job. 


## Future Plan
+ Data Quality Check. I noticed that there is some error data in the Archive data. LTE shouldn't contain 900mhz frequency band, but there is a record, it sould be rectified or droped before transformation.
+ Table Lifecycle Management. Setup table lifecycle or archive out-dated data.
+ Completely Automation. Directly connect business tables or get data from REST API into our dimensional table to get acknowledge whether the technology is expanding. Thus we don't even need to maintain the dimensional table by manpower.

