# Set variables
prj=[project name]  #Replace with your project name
bucket=[buckect name] #Replace with your bucket name
asset_srv_act=[asset export srv act name] #Replace with your service account name use in pipeline or cloud function

# Enable asset API on project#
gcloud services enable cloudasset.googleapis.com --project ${prj}

# Provide Google managed asset service account access on bucket
srv_act=`gcloud projects get-iam-policy ${prj}|grep gcp-sa-cloudasset.iam.gserviceaccount.com|awk '{print $2}'`;echo $srv_act
gsutil iam ch ${srv_act}:objectAdmin gs://${bucket}
gsutil iam get gs://${bucket} | grep ${srv_act}

# Provide access to pipeline/cloud function service acccount on project 
for i in cloudasset.viewer browser monitoring.viewer
do
  echo "gcloud projects add-iam-policy-binding ${prj} --role roles/${i} --member serviceAccount:${asset_srv_act}"
  gcloud projects add-iam-policy-binding ${prj} --role roles/${i} --member serviceAccount:${asset_srv_act}
done

gcloud projects get-iam-policy ${prj} |grep -A 2 ${asset_srv_act}
