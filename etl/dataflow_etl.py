import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def dataflow_pipeline_run():
    with beam.Pipeline(options = PipelineOptions()) as p:
        picks = p | 'ReadPickups' >> beam.io.ReadFromText('gs://dott_test/pickups*.csv')
        depls = p | 'ReadDeployments' >> beam.io.ReadFromText('gs://dott_test/deployments*.csv')
        rides = p | 'ReadRides' >> beam.io.ReadFromText('gs://dott_test/rides*.csv')

        (
            picks 
            | 'CountBeforeDedup' >> beam.combiners.Count.Globally()
            | 'NameIt1' >> beam.Map(lambda x: (x, 'BeforeDedup'))
            | 'PrintCount1' >> beam.Map(logging.error)
        )

        picks_dedup = picks | 'DeDupPickups' >> beam.Distinct()
        depls_dedup = depls | 'DeDupDeployments' >> beam.Distinct()
        rides_dedup = rides | 'DeDupRides' >> beam.Distinct()

        (
            picks_dedup 
            | 'CountAfterDedup' >> beam.combiners.Count.Globally()
            | 'NameIt2' >> beam.Map(lambda x: (x, 'AfterDedup'))
            | 'PrintCount2' >> beam.Map(logging.error)
        )

        picks_dedup | 'WritePickups' >> beam.io.WriteToText('gs://dott_test/pickups_final.csv')

        # table_schema = {
        #     'fields': [
        #         {'name': 'task_id', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        #         {'name': 'vehicle_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        #         {'name': 'qr_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        #         {'name': 'time_task_created', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        #         {'name': 'time_task_resolved', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        #     ]
        # }

        # picks_dedup | beam.io.gcp.bigquery.WriteToBigQuery(
        #     'peaceful-tide-284813:dott_test.pickups',
        #     schema = table_schema
        # )


if __name__ == '__main__':
    dataflow_pipeline_run()