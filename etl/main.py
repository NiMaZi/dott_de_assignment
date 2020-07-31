from bigquery_etl import *
from dataflow_etl import *
from gs_utils import *

BUCKET_NAME = "dott_test"
RAW_DATA_PREFIX = ""

def etl_core(bucket_name):
    dataflow_pipeline_options = get_pipeline_options()
    dataflow_pipeline_run(bucket_name, dataflow_pipeline_options)
    dataflow_report = count_duplicates(bucket_name)
    bigquery_preprocess()
    return dataflow_report

def main():

    new_file = check_new_file(BUCKET_NAME, RAW_DATA_PREFIX)

    if new_file:
        dataflow_report = etl_core(BUCKET_NAME)
        check_log()
    else:
        pass


if __name__ == '__main__':
    main()