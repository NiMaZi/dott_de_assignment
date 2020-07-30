from google.cloud import bigquery_datatransfer
from google.cloud import bigquery

project = 'peaceful-tide-284813'
dataset = 'dott_test'

def start_bigquery_transfer():

    client = bigquery_datatransfer.DataTransferServiceClient()

    # Get the full path to your project.
    parent = client.project_path(project)

    print('Supported Data Sources:')

    # Iterate over all possible data sources.
    for data_source in client.list_data_sources(parent):
        print('{}:'.format(data_source.display_name))
        print('\tID: {}'.format(data_source.data_source_id))
        print('\tFull path: {}'.format(data_source.name))
        print('\tDescription: {}'.format(data_source.description))

def bq_keymap(client):
    
    table = 'keymapped_deployments'

    job_config = bigquery.QueryJobConfig(destination = "{}.{}.{}".format(project, dataset, table))
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    query = """
        select
            dep.task_id,
            dep.vehicle_id,
            keymap.qr_code,
            dep.time_task_created,
            dep.time_task_resolved 
        from `dott_test.deployment` as dep left join
        (select
            vehicle_id,
            qr_code
        from `dott_test.pickups` 
        group by vehicle_id, qr_code) as keymap
        on (dep.vehicle_id = keymap.vehicle_id)
        order by vehicle_id, time_task_resolved desc
    """

    job = client.query(query, job_config = job_config)
    _ = job.result()

def bq_cycle_index(client):

    table = 'last_dep_pick_cycle_indexed_rides'

    job_config = bigquery.QueryJobConfig(destination = "{}.{}.{}".format(project, dataset, table))
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    query = """
        select
            dep.task_id,
            dep.vehicle_id,
            keymap.qr_code,
            dep.time_task_created,
            dep.time_task_resolved 
        from `dott_test.deployment` as dep left join
        (select
            vehicle_id,
            qr_code
        from `dott_test.pickups` 
        group by vehicle_id, qr_code) as keymap
        on (dep.vehicle_id = keymap.vehicle_id)
        order by vehicle_id, time_task_resolved desc
    """

    # job = client.query(query, job_config = job_config)
    # _ = job.result()

def bigquery_preprocess():

    bq_client = bigquery.Client(project = project)

    bq_keymap(bq_client)
    bq_cycle_index(bq_client)