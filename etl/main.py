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
    return dataflow_report

def bigquery_etl_core():
    bigquery_preprocess()

def main():
    if run_spec == 0 or run_spec == 1:
        dataflow_report = dataflow_etl_core(BUCKET_NAME)
    elif run_spec == 0 or run_spec == 2:
        bigquery_preprocess()

if __name__ == '__main__':
    main()