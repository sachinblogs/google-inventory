import json
import os
import time
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import monitoring_v3

def instance_record_process(blob_refe,bq_refe,ds_nm,ds_tbl_nm,inv_dt):
    dataset = bq_refe.dataset(ds_nm)
    try:
        table = bq_refe.get_table(ds_tbl_nm)
    except NotFound:
        print('Table {} not found'.format(ds_tbl_nm))
        return False
    fl_records = blob_refe.download_as_string()
    records = fl_records.decode('utf8').split('\n')
    for rec in records:
        print(rec)
        if rec != "":
            j_rec = json.loads(rec)
            nm_attr = j_rec['name'].split('/')
            project = nm_attr[4]
            instance_nm = nm_attr[8]
            zone = nm_attr[6]
            cr_dt = j_rec['resource']['data']['creationTimestamp']
            boot_disk_sz =  j_rec['resource']['data']['disks'][0]['diskSizeGb']
            boot_disk_nm = j_rec['resource']['data']['disks'][0]['deviceName']
            os_ver_attr = j_rec['resource']['data']['disks'][0]['licenses'][0].split('/')
            os_ver = os_ver_attr[9]
            instance_id = j_rec['resource']['data']['id']
            mach_attr = j_rec['resource']['data']['machineType'].split('/')
            mach_type = mach_attr[10]
            lbl_str = ''
            try:
                lbl_attr = j_rec['resource']['data']['labels']
                i = 0
                for lbl in lbl_attr:
                    if i == 0:
                        lbl_str = lbl+"="+lbl_attr[lbl]
                        i = i + 1
                    else:
                        lbl_str = lbl_str+","+lbl+"="+lbl_attr[lbl]
            except KeyError:
                print('No labels')
            instance_ip = j_rec['resource']['data']['networkInterfaces'][0]['networkIP']
            vpc_attr = j_rec['resource']['data']['networkInterfaces'][0]['network'].split('/')
            vpc = vpc_attr[9]
            subnet_attr = j_rec['resource']['data']['networkInterfaces'][0]['subnetwork'].split('/')
            subnet = subnet_attr[10]
            if 'serviceAccounts' in j_rec['resource']['data'].keys():
                srv_act =  j_rec['resource']['data']['serviceAccounts'][0]['email']
            else:
                srv_act = 'None'
            status = j_rec['resource']['data']['status']
            rows_insert = [(project,instance_nm,zone,cr_dt,boot_disk_nm,boot_disk_sz,os_ver,instance_id,mach_type,instance_ip,vpc,subnet,srv_act,lbl_str,status,inv_dt)]
            errors = bq_refe.insert_rows(table,rows_insert)
            if errors == []:
                print("Compute Instance Record added")
                print(rows_insert)
            else:
                print(errors)
                
    def gcs_inventory_trigger(data, context):
        storage_client = storage.Client()
        bucket_nm = data['bucket']
        blob_nm = data['name']
        blob_attr = blob_nm.split('/')
        #Skip the run until we have required file created 
        #example gs://[bucknm]/[date]/[prjnm]/object_name_prefix/compute.googleapis.com/Instance/0
        if len(blob_attr) < 6:
            print("Skipping run")
            return True
        inv_dt = blob_attr[0]
        inv_project = blob_attr[1]
        DS_PROJECT = os.environ.get('DATASET_PROJECT')
        NAME = os.environ.get('DATASET_NM')
        DS_NAME = DS_PROJECT+'.'+NAME
        bq_client = bigquery.Client(project=DS_PROJECT)
        if blob_attr[2] == "object_name_prefix":
            bob_post_fix_nm = blob_attr[3]+"/"+blob_attr[4]+"/"+blob_attr[5]
            if bob_post_fix_nm == "compute.googleapis.com/Instance/0":
                print("Processing Instance Inventory")
                bucket = storage_client.get_bucket(bucket_nm)
                blob = bucket.blob(blob_nm)
                DS_TABLE = DS_NAME+'.Instances'
                instance_record_process(blob,bq_client,DS_NAME,DS_TABLE,inv_dt)
