import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def dataflow_pipeline_run():
    with beam.Pipeline(options = PipelineOptions()) as p:
        picks = p | 'ReadPickups' >> beam.io.ReadFromText('gs://dott_test/pickups*.csv')
        depls = p | 'ReadDeployments' >> beam.io.ReadFromText('gs://dott_test/deployments*.csv')
        rides = p | 'ReadRides' >> beam.io.ReadFromText('gs://dott_test/rides*.csv')

        
        picks | 'PickupsCountBeforeDedup' >> beam.combiners.Count.Globally() | 'PickupsNameIt1' >> beam.Map(lambda x: f"{x} pickup records before deduplication.") | 'PickupsPrintCount1' >> beam.Map(logging.error)
        depls | 'DeploymentsCountBeforeDedup' >> beam.combiners.Count.Globally() | 'DeploymentsNameIt1' >> beam.Map(lambda x: f"{x} deployment records before deduplication.") | 'DeploymentsPrintCount1' >> beam.Map(logging.error)
        rides | 'RidesCountBeforeDedup' >> beam.combiners.Count.Globally() | 'RidesNameIt1' >> beam.Map(lambda x: f"{x} ride records before deduplication.") | 'RidesPrintCount1' >> beam.Map(logging.error)

        picks_dedup = picks | 'DeDupPickups' >> beam.Distinct()
        depls_dedup = depls | 'DeDupDeployments' >> beam.Distinct()
        rides_dedup = rides | 'DeDupRides' >> beam.Distinct()

        picks_dedup | 'PickupsCountAfterDedup' >> beam.combiners.Count.Globally() | 'PickupsNameIt2' >> beam.Map(lambda x: f"{x} pickup records after deduplication.") | 'PickupsPrintCount2' >> beam.Map(logging.error)
        picks_dedup | 'DeploymentsCountAfterDedup' >> beam.combiners.Count.Globally() | 'DeploymentsNameIt2' >> beam.Map(lambda x: f"{x} deployment records after deduplication.") | 'DeploymentsPrintCount2' >> beam.Map(logging.error)
        picks_dedup | 'RidesCountAfterDedup' >> beam.combiners.Count.Globally() | 'RidesNameIt2' >> beam.Map(lambda x: f"{x} ride records after deduplication.") | 'RidesPrintCount2' >> beam.Map(logging.error)

        picks_dedup | 'WritePickups' >> beam.io.WriteToText('gs://dott_test/pickups_final.csv', num_shards = 1, shard_name_template = "")
        depls_dedup | 'WriteDeployments' >> beam.io.WriteToText('gs://dott_test/deployments_final.csv', num_shards = 1, shard_name_template = "")
        rides_dedup | 'WriteRides' >> beam.io.WriteToText('gs://dott_test/rides_final.csv', num_shards = 1, shard_name_template = "")

if __name__ == '__main__':
    dataflow_pipeline_run()