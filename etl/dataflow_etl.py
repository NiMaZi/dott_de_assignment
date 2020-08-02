# import sys
# RUNNER_OPTION = sys.argv[1]
RUNNER_OPTION = "1"

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from google.cloud import storage

def gen_parse_kv(mode = 'depl'):

    # This function parses a line in the csv file into a key-value pair.

    temp_rschemas = {
        'depl': ['vid', 't_create', 't_resolve'],
        'pick': ['vid', 'qrcode', 't_create', 't_resolve'],
        'ride': ['vid', 't_create', 't_resolve', 's_lat', 's_lng', 'e_lat', 'e_lng', 'gm'],
    }

    def parse_kv(line):
        vs = line.split(',')
        return (vs[0], dict(zip(temp_rschemas[mode], vs[1:])))
    
    return parse_kv

def gen_get_last(mode = 'depl', sortkey = 't_resolve'):

    # This function takes a GroupByKey result and select the max based on sortkey (in this case the latest time_task_resolved or time_ride_end), 
    # and returns in the original comma-separated line format.

    temp_rschemas = {
        'depl': ['vid', 't_create', 't_resolve'],
        'pick': ['vid', 'qrcode', 't_create', 't_resolve'],
        'ride': ['vid', 't_create', 't_resolve', 's_lat', 's_lng', 'e_lat', 'e_lng', 'gm'],
    }

    def get_last(kv):
        k, v = kv
        last = max(v, key = lambda e: e[sortkey])
        return ','.join([k] + [last[n] for n in last])

    return get_last

def get_pipeline_options():
    options = PipelineOptions()

    if RUNNER_OPTION == "0":
        options.view_as(StandardOptions).runner = 'DirectRunner'
    elif RUNNER_OPTION == "1":
        gcp_options = options.view_as(GoogleCloudOptions)
        gcp_options.job_name = "dott-de-assignment"
        gcp_options.project = "dott-de-assignment"
        gcp_options.region = "us-central1"
        gcp_options.temp_location = "gs://dott_de_assignment_bucket/dataflow_temps/"
        options.view_as(StandardOptions).runner = 'DataflowRunner'
    else:
        raise Exception("invalid arg")

    return options

def dataflow_pipeline_run(BUCKET_NAME, options):

    with beam.Pipeline(options = options) as p:

        # Read in raw data files
        picks = p | 'ReadPickups' >> beam.io.ReadFromText('gs://{}/pickups*.csv'.format(BUCKET_NAME))
        depls = p | 'ReadDeployments' >> beam.io.ReadFromText('gs://{}/deployments*.csv'.format(BUCKET_NAME))
        rides = p | 'ReadRides' >> beam.io.ReadFromText('gs://{}/rides*.csv'.format(BUCKET_NAME))

        # Count total number of records
        picks | 'PickupsCountBeforeDedup' >> beam.combiners.Count.Globally() | 'PickupsNameIt1' >> beam.Map(lambda x: (x, 'PickupsBeforeDedup')) | 'PickupsPrintCount1' >> beam.io.WriteToText('gs://{}/etl_logs/pickups_before_dedup.txt'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")
        depls | 'DeploymentsCountBeforeDedup' >> beam.combiners.Count.Globally() | 'DeploymentsNameIt1' >> beam.Map(lambda x: (x, 'DeploymentsBeforeDedup')) | 'DeploymentsPrintCount1' >> beam.io.WriteToText('gs://{}/etl_logs/deployments_before_dedup.txt'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")
        rides | 'RidesCountBeforeDedup' >> beam.combiners.Count.Globally() | 'RidesNameIt1' >> beam.Map(lambda x: (x, 'RidesBeforeDedup')) | 'RidesPrintCount1' >> beam.io.WriteToText('gs://{}/etl_logs/rides_before_dedup.txt'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")

        # Deduplication
        picks_dedup = picks | 'ParseKeyPickups' >> beam.Map(gen_parse_kv("pick")) | "GroupByKeyPickups" >> beam.GroupByKey() | "DeDupPickups" >> beam.Map(gen_get_last("pick", "t_resolve"))
        depls_dedup = depls | 'ParseKeyDeployments' >> beam.Map(gen_parse_kv("depl")) | "GroupByKeyDeployments" >> beam.GroupByKey() | "DeDupDeployments" >> beam.Map(gen_get_last("depl", "t_resolve"))
        rides_dedup = rides | 'ParseKeyRides' >> beam.Map(gen_parse_kv("ride")) | "GroupByKeyRides" >> beam.GroupByKey() | "DeDupRides" >> beam.Map(gen_get_last("ride", "t_resolve"))

        # Count total number of records again
        picks_dedup | 'PickupsCountAfterDedup' >> beam.combiners.Count.Globally() | 'PickupsNameIt2' >> beam.Map(lambda x: (x, 'PickupsAfterDedup')) | 'PickupsPrintCount2' >> beam.io.WriteToText('gs://{}/etl_logs/pickups_after_dedup.txt'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")
        depls_dedup | 'DeploymentsCountAfterDedup' >> beam.combiners.Count.Globally() | 'DeploymentsNameIt2' >> beam.Map(lambda x: (x, 'DeploymentsAfterDedup')) | 'DeploymentsPrintCount2' >> beam.io.WriteToText('gs://{}/etl_logs/deployments_after_dedup.txt'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")
        rides_dedup | 'RidesCountAfterDedup' >> beam.combiners.Count.Globally() | 'RidesNameIt2' >> beam.Map(lambda x: (x, 'RidesAfterDedup')) | 'RidesPrintCount2' >> beam.io.WriteToText('gs://{}/etl_logs/rides_after_dedup.txt'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")

        # Write to final data files
        picks_dedup | 'WritePickups' >> beam.io.WriteToText('gs://{}/final_pickups.csv'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")
        depls_dedup | 'WriteDeployments' >> beam.io.WriteToText('gs://{}/final_deployments.csv'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")
        rides_dedup | 'WriteRides' >> beam.io.WriteToText('gs://{}/final_rides.csv'.format(BUCKET_NAME), num_shards = 1, shard_name_template = "")

def count_duplicates(bucket_name):
    # This function runs after the Dataflow ETL pipeline, to check if there are duplicates.
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    total_num_record_before_dedup = (
        eval(list(bucket.list_blobs(prefix = 'etl_logs/pickups_before_dedup'))[0].download_as_string())[0] +\
        eval(list(bucket.list_blobs(prefix = 'etl_logs/deployments_before_dedup'))[0].download_as_string())[0] +\
        eval(list(bucket.list_blobs(prefix = 'etl_logs/rides_before_dedup'))[0].download_as_string())[0]
    )

    total_num_record_after_dedup = (
        eval(list(bucket.list_blobs(prefix = 'etl_logs/pickups_after_dedup'))[0].download_as_string())[0] +\
        eval(list(bucket.list_blobs(prefix = 'etl_logs/deployments_after_dedup'))[0].download_as_string())[0] +\
        eval(list(bucket.list_blobs(prefix = 'etl_logs/rides_after_dedup'))[0].download_as_string())[0]
    )

    return "{} records loaded, {} duplicated records discarded.".format(total_num_record_after_dedup, total_num_record_before_dedup - total_num_record_after_dedup)

if __name__ == '__main__':
    dataflow_pipeline_run('dott_de_assignment_bucket', get_pipeline_options())