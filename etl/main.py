from bigquery_etl import *
from dataflow_etl import *

BUCKET_NAME = "dott_test"
RAW_DATA_PREFIX = ""

def etl_core(bucket_name):
    dataflow_pipeline_options = get_pipeline_options()
    dataflow_pipeline_run(bucket_name, dataflow_pipeline_options)
    dataflow_report = count_duplicates(bucket_name)
    bigquery_preprocess()
    return dataflow_report

def main():
    dataflow_report = etl_core(BUCKET_NAME)

if __name__ == '__main__':
    main()