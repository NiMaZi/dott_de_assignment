# dott_de_assignment
This is the code repository for dott de assignment.  
The whole application consists of the following parts:  
1. ETL and preprocessing
1. WebApp
1. Functional and pressure tests modules

# ETL and preprocessing
This part of the application does the following things:
1. Loading raw data (*.csv files) from a designated Cloud Storage bucket into Dataflow,
1. Performing record counting and deduplication using Beam pipeline on Dataflow,
1. Writing the deduplicated data (*.csv files) back to a designated Cloud Storage bucket,
1. Loading the deduplicated data into Bigquery tables using scheduled Data Transfer,
1. Preprocessing the BigQuery tables into more efficient data models for the downstream querying application.

## Dataflow Pipelines
ETL and preprocessing steps 1, 2 and 3 are included in this section.  
Corresponding code is in ```/etl/dataflow_etl.py```.  

The pipeline starts with scanning all raw data files with certain prefixes. For example, it use pattern ```*some_folder*/pickups*.csv``` to find all files about pickups. Then it use Beam built-in ```Distinct()``` function to deduplicate the records. In this case, only identical records are going to be considered. If there are two records with the same key (e.g., task_id or vehicle_id) but different values on other fields, they will both be considered as valid records.  
After deduplication, the pipeline writes the final records back to a bucket. By default the same bucket as the input files will be used, and the final files will have the "final_" prefix.  
A Cloud VM holds the code and automatically submit the abovementioned pipeline as Dataflow jobs on daily basis. The scheduling is managed by crontab.  
  
Comments:
1. At the beginning, I wanted to implement a "new file checking" logic, so the pipeline will not run if there is not any new file in the input bucket. Then I thought since the application has to serve 5000 requests per minute, the data refresh rate must be high (I would assume more than once a day) in the real situation. So this idea was discarded. We can add this back if the data won't be updated so often.
1. I chose Dataflow because I guess the real situation will be streaming data. If that's the case, I can easily use the PubSub programming model to connect Dataflow and BigQuery.
1. I don't expect that there are really two records with the same key but different values on other fields. If it happened, there must be something wrong in the data collection part. I guess it is kind of "out of scope" for this assignment, so I decided to do nothing about it.

## BigQuery preprocessing
ETL and preprocessing steps 4 and 5 are included in this section.  
Corresponding code is in ```/etl/bigquery_etl.py```

Initially, the input files will end up in 3 tables. However, to get the required results of the assignment, some transformations have to be performed:
1. Calculating the geographic distance between start and end points of each ride,
1. Finding the last deployment-pickup cycle for each vehicle,
1. Using the cycles to index the rides, so that only the rides related to the last cycles can be selected.

The results of these transformations are pre-computed in the ETL and preprocessing phase to speed up the downstream querying application.  
BigQuery Data Transfer is used to load the data files (result of ETL) into the 3 initial tables on daily basis.  
The same cloud VM as the ETL phase holds the code and automatically run the BigQuery API functions. The scheduling is also managed by crontab.

Comments:
1. To find the last deployment-pickup cycles, I considered these 2 scenarios: "...-dep-pick" and "...-dep-pick-dep". I assume if a vehicle is still on the street and there is no plan to pick it up yet, the latter will happen. However, I found that there are actually "...-dep-pick-pick" cases in the data. For simplicity, I always take the very last "pick". That is to say, if we see "...-dep-pick1-pick2", it will be treated as "...-dep-pick2".
1. I don't have much experience of web programming, so 5000 requests per minute sounds really like a lot to me. That's why I decided to pre-compute as much as I can.
1. I chose BigQuery mostly because it is in the job description and I want to get familiar with it. I think if there is actually large volume data stream, then BigQuery can be efficient. For the data size of this assignment (3 csv files, ~110K row in total), in-memory processing (e.g., Pandas) or simply Cloud SQL is probably faster.

# WebApp

The WebApp is hosted on App Engine. It is implemented with Flask. It has the following logic:
1. Getting a *key* from the GET request URL,
1. Identifying whether the key is a vehicle_id or qr_code by string length (assuming a qr_code always has length 6),
1. Performing the query accordingly to get the required results,
1. Returning the query results as simple HTML.