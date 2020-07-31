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
            cycle.vehicle_id as vehicle_id,
            cycle.qr_code as qr_code,
            rides.gross_amount as gross_amount,
            rides.ride_distance as ride_distance,
            rides.start_lat as start_lat,
            rides.start_lng as start_lng,
            rides.end_lat as end_lat,
            rides.end_lng as end_lng 
        from
        (select
            last_pick.vehicle_id as vehicle_id,
            last_pick.qr_code as qr_code,
            last_dep.time_last_task_resolved as time_last_dep_resolved,
            last_pick.time_last_task_created as time_last_pick_created
        from
        (select
            dep.vehicle_id as vehicle_id,
            max(dep.time_task_resolved) as time_last_task_resolved
        from `dott_test.deployment` as dep inner join
        (select
            a.task_id,
            a.vehicle_id,
            a.qr_code,
            a.time_task_created,
            a.time_task_resolved
        from `dott_test.pickups` as a
        left outer join `dott_test.pickups` as b on (a.vehicle_id = b.vehicle_id and a.time_task_created < b.time_task_created)
        where b.vehicle_id is null) as last_pick
        on (dep.vehicle_id = last_pick.vehicle_id and dep.time_task_resolved < last_pick.time_task_created )
        group by dep.vehicle_id) as last_dep inner join
        (select
            a.vehicle_id as vehicle_id,
            a.qr_code as qr_code,
            a.time_task_created as time_last_task_created
        from `dott_test.pickups` as a
        left outer join `dott_test.pickups` as b on (a.vehicle_id = b.vehicle_id and a.time_task_created < b.time_task_created)
        where b.vehicle_id is null) as last_pick
        on last_dep.vehicle_id = last_pick.vehicle_id) as cycle inner join
        (select
            ride_id,
            vehicle_id,
            time_ride_start,
            time_ride_end,
            start_lat,
            start_lng,
            end_lat,
            end_lng,
            gross_amount,
            ST_DISTANCE(ST_GEOGPOINT(start_lat,start_lng),ST_GEOGPOINT(end_lat,end_lng)) as ride_distance
        from `dott_test.rides`) as rides
        on (cycle.vehicle_id = rides.vehicle_id and cycle.time_last_dep_resolved < rides.time_ride_start and cycle.time_last_pick_created > rides.time_ride_end )
    """

    job = client.query(query, job_config = job_config)
    _ = job.result()

def bigquery_preprocess():

    bq_client = bigquery.Client(project = project)

    bq_keymap(bq_client)
    bq_cycle_index(bq_client)