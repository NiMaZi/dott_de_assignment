from bigquery_etl import *
from dataflow_etl import *
from gs_utils import *

BUCKET_NAME = "dott_de_assignment"
RAW_DATA_PREFIX = "raw_data_landing/"

def etl_core():
    dataflow_pipeline_run()

def main():
    new_file = check_new_file(BUCKET_NAME, RAW_DATA_PREFIX)

    if new_file:
        etl_core()


if __name__ == '__main__':
    main()