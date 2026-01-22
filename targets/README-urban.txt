
------- S3 Buckets ------- 

To Log into RedHat OpenShift Console:

Link: https://console.apps.shift.nerc.mghpcc.org/k8s/cluster/projects

1. Select mss-keycloak
2. Select Google
3. Log in with BU credentials

To Create S3 Bucket:

Redhat Openshift Console
1. Click on the plus in the top right corner
2. Import YAML (ensure you are in the right project)
3. Copy and Paste the YAML from here: https://nerc-project.github.io/nerc-docs/openshift-ai/other-projects/fraud-detection-predictive-ai-app/#12-using-a-script-to-set-up-local-s3-storage-minio

To Log into Minio:

1. Go to RedHat OpenShift AI (9 squares at the top right of the RedHat OpenShift Console)
2. Select Data Science Projets, the right project, connections
3. Click the 3 dots next to your S3 Bucket
4. Copy the access key as your username and the secret key as your password
5. Go to: https://minio-console.apps.shift.nerc.mghpcc.org/login
5. Paste your username and password

To Upload: Navigate to the right folder and click the upload button in the top right

------- EPA Data ------- 

Currently our data only downloads for PM2.5, PM10, O3, NO2 in Suffolk, Essex, 
and Norfolk county. To expand the targets, you first have to run urban_initial_download
and upload the files from there to the Minio bucket. Only then can you run urban_targets.
If you expand the pollutants you will have to update the site metadata in both urban_initial_download
and target_helper_fucntions so that all of the columns get created.

To Get API Credentials:

1. Copy and Paste this link (with your email) into a web browser:
https://aqs.epa.gov/data/api/signup?email=myemail@example.com
2. Once you recieve the email, you now have your email and password to pass to the api call
3. Use this in the urban_inital_download or under secret keys in urban_targets


