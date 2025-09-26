## Example Targets file for the BU Forecasting Challenges
## Created: 9/26/2025

# This template will walk you through each step that needs to be done in a generic format
# The target files should only contain the variable(s) you want the students to predict
# If you have other data to help predict those variable(s), that will go in the driver files

## Step 1: Download Data
# If you need a key to access the data, create a secret key
# Github Repo -> Settings -> Secrets and Variables -> Actions -> New Repository Secret
# Access the variable (i.e MY_VAR) in your script using Sys.getenv("MY_VAR")

# Download the list of sites you want to work with


# Download data for target variable(s)


## Step 2: Combine data together for each of your sites

data = data.frame(matrix(ncol = 6, nrow = 0))

# This is the order and names you must use for the data when writing to the S3 Bucket
colnames(data) = c('project_id', 'site_id', 'datetime', 'duration', 'variable', 'observation')



## Column Explanations
# project_id = bu4cast
# site_id
# datetime
# observation = actual value of variable
# duration = how often the data is collected
  # "P1M" = monthly
  # "P1D" = daily
  # "PT30M" = 30 minute
  # General Formula found here: https://courses.ischool.berkeley.edu/i290-14/s05/lecture-15/slide31.html
# variable = which type of variable we're using (since we don't have a different column for each variable)

## Step 3: Prep data for storage
# Depending on your process for step 2, this may not be necessary


## Step 4: Write to S3 Bucket

# Set Up S3 Connection
s3_read <- arrow::s3_bucket('bu4cast-ci-read',
                            endpoint_override = 'https://minio-s3.apps.shift.nerc.mghpcc.org',
                            access_key = Sys.getenv("OSN_KEY"),
                            secret_key = Sys.getenv("OSN_SECRET"),
                            scheme = "https")
# Create file name/folder
duration_type = '' # Fill in string with duration type (i.e P1M, P1D, etc.)
challenge_name = '' # Fill in string with name of challenge (i.e Disease, Urban, Coastal)
filename = paste("challenges/targets/project_id=bu4cast/duration=", duration_type,
                 "/", challenge_name, "-targets.csv.gz")

# Write to S3 bucket
# Change data if you changed the name of the cleaned data in the script
arrow::write_csv_arrow(data, sink = s3_read$path(filename))

## Step 5: Clean Up and Health Check

# Remove file from working directory
csv_filename = paste(challenge_name, "-targets.csv.gz")
unlink(csv_filename)

# Health Check
# Created at www.healthchecks.io
# Currently set to bu4cast-ci-example
RCurl::getURL("https://hc-ping.com/9836911f-f0e5-485b-91a0-f1d98f11dd7a")
