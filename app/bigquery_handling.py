from google.cloud import bigquery

DATASET = "dott_de_assignment_dataset"
TABLE_MAPPED_DEPS = "keymapped_deployments"
TABLE_INDEXED_RIDES = "last_dep_pick_cycle_indexed_rides"

def get_rides(key, bq_client, qmode='vehicle_id'):
    query = """
        select
            gross_amount,
            ride_distance,
            start_lat,
            start_lng,
            end_lat,
            end_lng
        from `{}.{}` 
        where {} = '{}'
        limit 5
    """.format(DATASET, TABLE_INDEXED_RIDES, qmode, key)
    job = bq_client.query(query)
    return ["Gross amount: {}, Ride distance: {}, Start: ({}, {}), End: ({}, {}).".format(
        row['gross_amount'],
        row['ride_distance'],
        row['start_lng'],
        row['start_lat'],
        row['end_lng'],
        row['end_lng']
    ) for row in job]

def get_n_deployments(n, key, bq_client, qmode='vehicle_id'):
    query = """
        select
            task_id,
            vehicle_id,
            qr_code,
            time_task_created,
            time_task_resolved
        from `{}.{}` 
        where {} = "{}"
        limit {}
    """.format(DATASET, TABLE_MAPPED_DEPS, qmode, key, n)
    job = bq_client.query(query)
    return ["Task ID: {}, Vehicle ID: {}, QR Code: {}, Task timeline: {} -> {}".format(
        row['task_id'],
        row['vehicle_id'],
        row['qr_code'],
        row['time_task_created'],
        row['time_task_resolved']
    ) for row in job]

def get_results(key, bq_client):

    if len(key) == 6:
        qmode = 'qr_code'
    else:
        qmode = 'vehicle_id'

    results = get_rides(key, bq_client, qmode)

    if len(results) < 5:
        results += get_n_deployments(5 - len(results), key, bq_client, qmode)
    
    if len(results) > 0:
        str_results = "<br>".join(results)
    else:
        str_results = "This query didn't hit any record."

    return str_results