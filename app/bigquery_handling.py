from google.cloud import bigquery

def get_last_pickup_with_vehicle_id(vehicle_id, bq_client):
    query = """
        select max(time_task_created) as time_last_task_created
        from `dott_test.pickups`
        where vehicle_id = "{}"
    """.format(vehicle_id)
    job = bq_client.query(query)
    for row in job:
        return str(row['time_last_task_created'])
    return False

def get_last_deployment(vehicle_id, pickup_time, bq_client):
    query = """
        select max(time_task_resolved) as time_last_task_resolved
        from `dott_test.deployment`
        where (vehicle_id = "{}" and time_task_resolved < "{}")
    """.format(vehicle_id, pickup_time)
    job = bq_client.query(query)
    for row in job:
        return str(row['time_last_task_resolved'])
    return False

def get_rides(vehicle_id, deploy_time, pickup_time, bq_client):
    query = """
        select
            gross_amount,
            ride_distance,
            start_lat,
            start_lng,
            end_lat,
            end_lng
        from `dott_test.rides_with_distance`
        where (vehicle_id = "{}" and time_ride_start > "{}" and time_ride_end < "{}")
        order by gross_amount desc, time_ride_start
    """.format(vehicle_id, deploy_time, pickup_time)
    job = bq_client.query(query)
    return [{
        "gross_amount": str(row['gross_amount']),
        "ride_distance": str(row['ride_distance']),
        "start_point": f"Lng: {row['start_lng']}, Lat: {row['start_lat']}",
        "end_point": f"Lng: {row['end_lng']}, Lat: {row['end_lat']}",
    } for row in job]

def get_n_deployments(n, vehicle_id, bq_client):
    query = """
        select
            task_id,
            vehicle_id,
            time_task_created,
            time_task_resolved
        from `dott_test.deployment` 
        where vehicle_id = "{}"
        order by time_task_created desc
        limit {}
    """.format(vehicle_id, n)
    job = bq_client.query(query)
    return [{
        "task_id": str(row['task_id']),
        "vehicle_id": str(row['vehicle_id']),
        "time_task_created": str(row['time_task_created']),
        "time_task_resolved": str(row['time_task_resolved']),
    } for row in job]

def get_results(key, bq_client):

    vehicle_id = key

    results = []

    pickup_time = get_last_pickup_with_vehicle_id(vehicle_id, bq_client)
    deploy_time = get_last_deployment(vehicle_id, pickup_time, bq_client)
    rides = get_rides(vehicle_id, deploy_time, pickup_time, bq_client)

    results += rides[:5]
    
    if not len(rides) > 5:
        results += get_n_deployments(5 - len(rides), vehicle_id, bq_client)
    
    return results