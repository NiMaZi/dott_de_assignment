import sys
RUNNER_OPTION = sys.argv[1]

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions

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

def dataflow_pipeline_run(options):

    with beam.Pipeline(options = options) as p:
        picks = p | 'ReadPickups' >> beam.io.ReadFromText('gs://dott_test/pickups*.csv')
        depls = p | 'ReadDeployments' >> beam.io.ReadFromText('gs://dott_test/deployments*.csv')
        rides = p | 'ReadRides' >> beam.io.ReadFromText('gs://dott_test/rides*.csv')

        picks | 'PickupsCountBeforeDedup' >> beam.combiners.Count.Globally() | 'PickupsNameIt1' >> beam.Map(lambda x: (x, 'PickupsBeforeDedup')) | 'PickupsPrintCount1' >> beam.io.WriteToText('gs://dott_test/etl_logs/pickups_before_dedup.txt', num_shards = 1, shard_name_template = "")
        depls | 'DeploymentsCountBeforeDedup' >> beam.combiners.Count.Globally() | 'DeploymentsNameIt1' >> beam.Map(lambda x: (x, 'DeploymentsBeforeDedup')) | 'DeploymentsPrintCount1' >> beam.io.WriteToText('gs://dott_test/etl_logs/deployments_before_dedup.txt', num_shards = 1, shard_name_template = "")
        rides | 'RidesCountBeforeDedup' >> beam.combiners.Count.Globally() | 'RidesNameIt1' >> beam.Map(lambda x: (x, 'RidesBeforeDedup')) | 'RidesPrintCount1' >> beam.io.WriteToText('gs://dott_test/etl_logs/rides_before_dedup.txt', num_shards = 1, shard_name_template = "")

        picks_dedup = picks | 'DeDupPickups' >> beam.Distinct()
        depls_dedup = depls | 'DeDupDeployments' >> beam.Distinct()
        rides_dedup = rides | 'DeDupRides' >> beam.Distinct()

        picks_dedup | 'PickupsCountAfterDedup' >> beam.combiners.Count.Globally() | 'PickupsNameIt2' >> beam.Map(lambda x: (x, 'PickupsAfterDedup')) | 'PickupsPrintCount2' >> beam.io.WriteToText('gs://dott_test/etl_logs/pickups_after_dedup.txt', num_shards = 1, shard_name_template = "")
        depls_dedup | 'DeploymentsCountAfterDedup' >> beam.combiners.Count.Globally() | 'DeploymentsNameIt2' >> beam.Map(lambda x: (x, 'DeploymentsAfterDedup')) | 'DeploymentsPrintCount2' >> beam.io.WriteToText('gs://dott_test/etl_logs/deployments_after_dedup.txt', num_shards = 1, shard_name_template = "")
        rides_dedup | 'RidesCountAfterDedup' >> beam.combiners.Count.Globally() | 'RidesNameIt2' >> beam.Map(lambda x: (x, 'RidesAfterDedup')) | 'RidesPrintCount2' >> beam.io.WriteToText('gs://dott_test/etl_logs/rides_after_dedup.txt', num_shards = 1, shard_name_template = "")

        picks_dedup | 'WritePickups' >> beam.io.WriteToText('gs://dott_test/pickups_final.csv', num_shards = 1, shard_name_template = "")
        depls_dedup | 'WriteDeployments' >> beam.io.WriteToText('gs://dott_test/deployments_final.csv', num_shards = 1, shard_name_template = "")
        rides_dedup | 'WriteRides' >> beam.io.WriteToText('gs://dott_test/rides_final.csv', num_shards = 1, shard_name_template = "")

if __name__ == '__main__':
    dataflow_pipeline_run(get_pipeline_options())