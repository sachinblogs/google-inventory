import datetime
import sys
from google.cloud import asset_v1
from google.cloud.asset_v1.proto import asset_service_pb2
from google.oauth2 import service_account
from google.cloud import resource_manager

if len(sys.argv) < 2:
    print("The script needs service file as parameter")
    sys.exit()
else:
    service_account_file = sys.argv[1]

client = resource_manager.Client.from_service_account_json(service_account_file)
for project in client.list_projects():
    project_id = project.project_id
    cur_dt = datetime.datetime.now()
    dt = '{}-{}-{}'.format(cur_dt.year,cur_dt.month,cur_dt.day)
    dump_file_path = 'gs://[bucket nm]/{}/{}/object_name_prefix'.format(dt,project_id)
    credentials = service_account.Credentials.from_service_account_file(service_account_file)
    client = asset_v1.AssetServiceClient(credentials=credentials)
    parent = client.project_path(project_id)
    output_config = asset_service_pb2.OutputConfig()
    output_config.gcs_destination.uri_prefix = dump_file_path
    response = client.export_assets(parent,output_config,content_type='RESOURCE')
    print(response.result())
