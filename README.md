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

The pipeline starts with scanning all raw data files with certain prefixes. For example, it use pattern ```*some_folder*/pickups*.csv``` to find all files about pickups. Then it group the records by the primary keys. To deduplicate the records, it only returns the last record in terms of timestamp for each primary key. For deployments and pickups, the field ```time_task_resolved``` is used for deduplication and for rides ```time_ride_end``` is used.
After deduplication, the pipeline writes the final records back to a bucket. By default the same bucket as the input files will be used, and the final files will have the "final_" prefix.  
A Cloud VM holds the code and automatically submit the abovementioned pipeline as Dataflow jobs on daily basis. The scheduling is managed by crontab.  
  
### Comments
1. At the beginning, I wanted to implement a "new file checking" logic, so the pipeline will not run if there is not any new file in the input bucket. Then I thought since the application has to serve 5000 requests per minute, the data refresh rate must be high (I would assume more than once a day) in the real situation. So this idea was discarded. We can add this back if the data won't be updated so often.
1. I chose Dataflow because I guess the real situation will be streaming data. If that's the case, I can easily use the PubSub programming model to connect Dataflow and BigQuery.

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

### Comments
1. To find the last deployment-pickup cycles, I considered these 2 scenarios: "...-dep-pick" and "...-dep-pick-dep". I assume if a vehicle is still on the street and there is no plan to pick it up yet, the latter will happen. However, I found that there are actually "...-dep-pick-pick" cases in the data. For simplicity, I always take the very last "pick". That is to say, if we see "...-dep-pick1-pick2", it will be treated as "...-dep-pick2".
1. I don't have much experience of web programming, so 5000 requests per minute sounds really like a lot to me. That's why I decided to pre-compute as much as I can.
1. I chose BigQuery mostly because it is in the job description and I want to get familiar with it. I think if there actually is a large volume data stream, then BigQuery can be efficient. For the data size of this assignment (3 csv files, ~110K row in total), in-memory processing (e.g., Pandas) is probably faster.

# WebApp

The WebApp is hosted on App Engine. It is implemented with Flask. It has the following logic:
1. Getting a *key* from the GET request URL,
1. Returning the cached results if the corresponding value of the *key* is cached,
1. If not cached, identifying whether the key is a vehicle_id or qr_code by string length (assuming a qr_code always has length 6),
1. Performing the query accordingly to get the required results,
1. Caching the query results,
1. Returning the query results as simple HTML.

The WebApp backend has the following architecture:
1. BigQuery as primary DB,
1. Redis as caching DB,
1. Flask as app framework,
1. Gunicorn as HTTP server.

### Comments
1. BigQuery has a concurrent query limit on its API, which is 300 per user. This limit makes it hard to reach the "5000 response per minute" requirement if the requests are cluttered. This is the reason I introduced an extra caching DB.
2. With the pre-computed final tables, the queries in this part are fast. The execution time (measured at BigQuery side) is averagely 15 - 20 ms.

# Functional and pressure tests

This part does the following things:
1. Performing the unit test for some core functions,
1. Performing the pressure test on the WebApp according to the requirement of the assignment.

## Unit test

The following functions are tested:
1. The function that performs user query to get vehicle information (code in ```app/test_app.py```),
1. The function that loads csv files using Dataflow and logs the deduplication information (code in ```etl/test_etl.py```).
The tests are performed using Python built-in unittest package.

For testing ```app.bigquery_handling.get_results```, 4 cases are constructed. They are: using Vehicle ID, using QRCODE, using an unrecognizable key and using a non-string value as key.  
For testing ```etl.main.dataflow_etl_core```, 2 cases are constructed. They are: duplicated csv files and no duplicated csv files.  

To execute the tests, please use ```python3 -m unittest discover``` at the root of the repository.

### Comments
1. I couldn't come up with any meaningful test case for my functions. So here I am with those stupid ones. However, I still spotted and fixed several bugs with these cases.

## Pressure test

As required by the assignment, the WebApp needs to serve at least 5000 requests per minute. So a simulated test case is constructed as: There are 1000 users in total. Each user randomly select a QRCODE to query the vehicle information 10 ~ 20 times per minute. That in the end will result in 10000 ~ 20000 requests per minute. The result from a 5-minute-lasting test shows:  
```Aggregated                                                     63546     0(0.00%)      64      10    4917  |      18  211.55    0.00```  
The result means that the WebApp can serve 211 requests per minute in average and throw no failure. Extended tests can be performed, for example if users are using both QRCODEs and Vehicle IDs. The worst case scenario should happen when the ratio between QRCODE and Vehicle ID is 1 : 1.   
The test is performed using locust package: ```locust -f pressure_test.py --host http://dott-de-assignment.ew.r.appspot.com --headless -u 1000 -r 50 -t 5m --csv pressure_test```

# Other information

The job graph of the Dataflow ETL pipeline is in:  
https://storage.googleapis.com/dott_de_assignment_shares/job_graph.PNG  
This is an example of loading rides. The other 2 are identical in terms of pipeline structure.  
  
The WebApp can be accessed via http://dott-de-assignment.ew.r.appspot.com/vehicles/{qr_code} or http://dott-de-assignment.ew.r.appspot.com/vehicles/{vehicle_id}.  
  
The pressure test results are saved in the following files:
1. https://storage.googleapis.com/dott_de_assignment_shares/pressure_test_stats.csv
2. https://storage.googleapis.com/dott_de_assignment_shares/pressure_test_stats_history.csv