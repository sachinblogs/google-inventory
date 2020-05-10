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
    
def bucket_record_process(blob_refe,bq_refe,ds_nm,ds_tbl_nm,project_nm,inv_dt):
    dataset = bq_refe.dataset(ds_nm)
    try:
        table = bq_refe.get_table(ds_tbl_nm)
    except NotFound:
        print('Table {} not found'.format(ds_tbl_nm))
        return False
    fl_records = blob_refe.download_as_string()
    records = fl_records.decode('utf8').split('\n')
    for rec in records:
        if rec != "":
            j_rec = json.loads(rec)
            prj_nm =  project_nm
            buck_nm_attr = j_rec['name'].split('/')
            buck_nm = buck_nm_attr[3]
            buck_loc = j_rec['resource']['data']['location'].lower()
            buck_loc_type = j_rec['resource']['data']['locationType']
            buck_prj_num = j_rec['resource']['data']['projectNumber']
            buck_stg_class = j_rec['resource']['data']['storageClass']
            buck_cr_dt = j_rec['resource']['data']['timeCreated']
            buck_iam_buckpolicy = False
            if 'enabled' in j_rec['resource']['data']['iamConfiguration']['bucketPolicyOnly']:
                buck_iam_buckpolicy = j_rec['resource']['data']['iamConfiguration']['bucketPolicyOnly']['enabled']
            buck_rule_str = ''
            i = 0
            for rl in j_rec['resource']['data']['lifecycle']['rule']:
                if i == 0:
                    buck_rule_str = str(rl)
                    i = i + 1
                else:
                    buck_rule_str = buck_rule_str+'|'+str(rl)
            buck_iam_buckpolicy_lck_tm = time.time()
            buck_iam_uniform_lck_tm = time.time()
            if buck_iam_buckpolicy:
                  buck_iam_buckpolicy_lck_tm = j_rec['resource']['data']['iamConfiguration']['bucketPolicyOnly']['lockedTime']
            buck_iam_uniform = False
            if 'enabled' in j_rec['resource']['data']['iamConfiguration']['uniformBucketLevelAccess']:
                buck_iam_uniform = j_rec['resource']['data']['iamConfiguration']['uniformBucketLevelAccess']['enabled']
            if buck_iam_uniform:
                 buck_iam_uniform_lck_tm = j_rec['resource']['data']['iamConfiguration']['uniformBucketLevelAccess']['lockedTime']
            try:
                buck_retn_policy_period = j_rec['resource']['data']['retentionPolicy']['retentionPeriod']
            except KeyError:
                buck_retn_policy_period = -1
            try:
                buck_retn_policy_locked = j_rec['resource']['data']['retentionPolicy']['isLocked']
            except KeyError:
                buck_retn_policy_locked = "False"
            try:
                buck_retn_policy_eff_tm = j_rec['resource']['data']['retentionPolicy']['effectiveTime']
            except KeyError:
                buck_retn_policy_eff_tm = time.time()
            buck_sz = 0
            client = monitoring_v3.MetricServiceClient()
            project_name = client.project_path(project_nm)
            interval = monitoring_v3.types.TimeInterval()
            now = time.time()
            interval.end_time.seconds = int(now)
            interval.end_time.nanos = int(
                    (now - interval.end_time.seconds) * 10**9)
            interval.start_time.seconds = int(now - 3600)
            interval.start_time.nanos = interval.end_time.nanos
            aggregation = monitoring_v3.types.Aggregation()
            aggregation.alignment_period.seconds = 3600  # 20 minutes
            aggregation.per_series_aligner = (
                    monitoring_v3.enums.Aggregation.Aligner.ALIGN_MEAN)
            filter = 'resource.type = "gcs_bucket" AND resource.labels.bucket_name = \"'+buck_nm+'\" AND resource.labels.location = '+buck_loc+' AND resource.labels.project_id = '+project_nm+' AND metric.type = "storage.googleapis.com/storage/total_bytes"'
            results = client.list_time_series(
                    project_name,
                    filter,
                    interval,
                    monitoring_v3.enums.ListTimeSeriesRequest.TimeSeriesView.FULL,
                    aggregation)
            for result in results:
                buck_sz = result.points[0].value.double_value/(1024*1024)
            rows_insert = [(prj_nm,buck_nm,buck_loc,buck_loc_type,buck_prj_num,buck_stg_class,buck_cr_dt,buck_iam_buckpolicy,buck_iam_buckpolicy_lck_tm,buck_iam_uniform,buck_iam_uniform_lck_tm,buck_rule_str,buck_retn_policy_period,buck_retn_policy_locked,buck_retn_policy_eff_tm,buck_sz,inv_dt)]
            errors = bq_refe.insert_rows(table,rows_insert)
            if errors == []:
                print("Bucket Record added")
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
            elif bob_post_fix_nm == "storage.googleapis.com/Bucket/0":
                print("Processing Bucket Inventory")
                bucket = storage_client.get_bucket(bucket_nm)
                blob = bucket.blob(blob_nm)
                DS_TABLE = DS_NAME+'.Buckets'
                bucket_record_process(blob,bq_client,DS_NAME,DS_TABLE,inv_project,inv_dt
