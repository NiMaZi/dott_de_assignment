from google.cloud import bigquery_datatransfer

def start_bigquery_transfer():

    client = bigquery_datatransfer.DataTransferServiceClient()

    project = 'peaceful-tide-284813'  # TODO: Update to your project ID.

    # Get the full path to your project.
    parent = client.project_path(project)

    print('Supported Data Sources:')

    # Iterate over all possible data sources.
    for data_source in client.list_data_sources(parent):
        print('{}:'.format(data_source.display_name))
        print('\tID: {}'.format(data_source.data_source_id))
        print('\tFull path: {}'.format(data_source.name))
        print('\tDescription: {}'.format(data_source.description))