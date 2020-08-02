import os
import sys
run_spec = int(sys.argv[1])

try:
    from .bigquery_etl import *
    from .dataflow_etl import *
except:
    from bigquery_etl import *
    from dataflow_etl import *

BUCKET_NAME = "dott_de_assignment_bucket"

def dataflow_etl_core(bucket_name):
    os.system("python3 dataflow_etl.py") 
    dataflow_report = count_duplicates(bucket_name)
    return dataflow_report # This return value is for unit testing.

def bigquery_etl_core():
    bigquery_preprocess()

def main():
    # Here the run_spec is for the two parts of ETL to run separately, but also together for debugging.
    # They are separated because they have to be scheduled at different times. The corret time line should be:
    # Dataflow ETL -> BigQuery Data Transfer -> BigQuery Preprocessing
    # On the VM, the scheduling is enabled by crontab

    if run_spec == 0 or run_spec == 1:
        dataflow_report = dataflow_etl_core(BUCKET_NAME)
    elif run_spec == 0 or run_spec == 2:
        bigquery_preprocess()

if __name__ == '__main__':
    main()