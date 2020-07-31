# import sys
# RUNNER_OPTION = sys.argv[1]
RUNNER_OPTION = 0

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from google.cloud import storage

def get_pipeline_options():
    options = PipelineOptions()

    if RUNNER_OPTION == "0":
        options.view_as(StandardOptions).runner = 'DirectRunner'
    elif RUNNER_OPTION == "1":
        gcp_options = options.view_as(GoogleCloudOptions)
        gcp_options.job_name = "dott-de-assignment"
        gcp_options.project = "peaceful-tide-284813"
        gcp_options.region = "europe-west1"
        gcp_options.temp_location = "gs://dott_test/dataflow_temps"
        gcp_options.service_account_email = "dott-test-local@peaceful-tide-284813.iam.gserviceaccount.com"
        options.view_as(StandardOptions).runner = 'DataflowRunner'
    else:
        raise Exception("invalid arg")

    return options

def dataflow_pipeline_run(BUCKET_NAME, options):

    with beam.Pipeline(options = options) as p:
        picks = p | 'ReadPickups' >> beam.io.ReadFromText('gs://{}/pickups*.csv'.format(BUCKET_NAME))
        depls = p | 'ReadDeployments' >> beam.io.ReadFromText('gs://{}/deployments*.csv'.format(BUCKET_NAME))
        rides = p | 'ReadRides' >> beam.io.ReadFromText('gs://{}/rides*.csv'.format(BUCKET_NAME))

        picks | 'PickupsCountBeforeDedup' >> beam.combiners.Count.Globally() | 'PickupsNameIt1' >> beam.Map(lambda x: (x, 'PickupsBeforeDedup')) | 'PickupsPrintCount1' >> beam.io.WriteToText('gs://{}/etl_logs/pickups_before_dedup.txt'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")
        depls | 'DeploymentsCountBeforeDedup' >> beam.combiners.Count.Globally() | 'DeploymentsNameIt1' >> beam.Map(lambda x: (x, 'DeploymentsBeforeDedup')) | 'DeploymentsPrintCount1' >> beam.io.WriteToText('gs://{}/etl_logs/deployments_before_dedup.txt'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")
        rides | 'RidesCountBeforeDedup' >> beam.combiners.Count.Globally() | 'RidesNameIt1' >> beam.Map(lambda x: (x, 'RidesBeforeDedup')) | 'RidesPrintCount1' >> beam.io.WriteToText('gs://{}/etl_logs/rides_before_dedup.txt'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")

        picks_dedup = picks | 'DeDupPickups' >> beam.Distinct()
        depls_dedup = depls | 'DeDupDeployments' >> beam.Distinct()
        rides_dedup = rides | 'DeDupRides' >> beam.Distinct()

        picks_dedup | 'PickupsCountAfterDedup' >> beam.combiners.Count.Globally() | 'PickupsNameIt2' >> beam.Map(lambda x: (x, 'PickupsAfterDedup')) | 'PickupsPrintCount2' >> beam.io.WriteToText('gs://{}/etl_logs/pickups_after_dedup.txt'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")
        depls_dedup | 'DeploymentsCountAfterDedup' >> beam.combiners.Count.Globally() | 'DeploymentsNameIt2' >> beam.Map(lambda x: (x, 'DeploymentsAfterDedup')) | 'DeploymentsPrintCount2' >> beam.io.WriteToText('gs://{}/etl_logs/deployments_after_dedup.txt'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")
        rides_dedup | 'RidesCountAfterDedup' >> beam.combiners.Count.Globally() | 'RidesNameIt2' >> beam.Map(lambda x: (x, 'RidesAfterDedup')) | 'RidesPrintCount2' >> beam.io.WriteToText('gs://{}/etl_logs/rides_after_dedup.txt'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")

        picks_dedup | 'WritePickups' >> beam.io.WriteToText('gs://{}/pickups_final.csv'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")
        depls_dedup | 'WriteDeployments' >> beam.io.WriteToText('gs://{}/deployments_final.csv'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")
        rides_dedup | 'WriteRides' >> beam.io.WriteToText('gs://{}/rides_final.csv'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")

def count_duplicates(bucket_name):
    passclient = storage.Client()
    bucket = client.bucket(bucket_name)

    total_num_record_before_dedup = (
        list(bucket.list_blobs(prefix = 'etl_logs/pickups_before_dedup'))[0].download_as_string() +\
        list(bucket.list_blobs(prefix = 'etl_logs/deployments_before_dedup'))[0].download_as_string() +\
        list(bucket.list_blobs(prefix = 'etl_logs/rides_before_dedup'))[0].download_as_string()
    )

    total_num_record_after_dedup = (
        list(bucket.list_blobs(prefix = 'etl_logs/pickups_after_dedup'))[0].download_as_string() +\
        list(bucket.list_blobs(prefix = 'etl_logs/deployments_after_dedup'))[0].download_as_string() +\
        list(bucket.list_blobs(prefix = 'etl_logs/rides_after_dedup'))[0].download_as_string()
    )

    print(total_num_record_before_dedup, total_num_record_after_dedup)

if __name__ == '__main__':
    dataflow_pipeline_run('dott_test', get_pipeline_options())