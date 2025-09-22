## Set Up

# Load Libraries
library(arrow)
library(readxl)
library(fs)


# Define link and endpoint override
OUR_LINK_read = 'bu4cast-ci-read'
OUR_LINK_write = 'bu4cast-ci-write'
OUR_ENDPOINT_OVERRIDE = 'https://minio-s3.apps.shift.nerc.mghpcc.org'


if (Sys.getenv("GITHUB_ACTIONS") == "true") {
  # Set Up S3 Connection (Git Hub)
  # Keys saved in Settings -> Secrets + Variables -> Actions -> Repository secrets
  s3_read <- arrow::s3_bucket(OUR_LINK_read,
                         endpoint_override = OUR_ENDPOINT_OVERRIDE,
                         access_key = Sys.getenv("OSN_KEY"),
                         secret_key = Sys.getenv("OSN_SECRET"))
  
  s3_write <- arrow::s3_bucket(OUR_LINK_write,
                         endpoint_override = OUR_ENDPOINT_OVERRIDE,
                         access_key = Sys.getenv("OSN_KEY"),
                         secret_key = Sys.getenv("OSN_SECRET"))
  
} else {
  # Read in Access Key and Secret Key (R Studio)
  # Found in Red Hat OpenShift AI -> software-application-innovation-lab-sail-projects-fcd6dfa
  # -> Connections
  keys = read_excel('S3Bucket_Keys.xlsx', col_names = FALSE)
  OUR_ACCESS_KEY = keys[[1,2]]
  OUR_SECRET_KEY = keys[[2,2]]
  
  # Set Up S3 Connection (R Studio)
  s3_read <- arrow::s3_bucket(OUR_LINK_read,
                         endpoint_override = OUR_ENDPOINT_OVERRIDE,
                         access_key = OUR_ACCESS_KEY,
                         secret_key = OUR_SECRET_KEY,
                         scheme = "https")
  
  s3_write <- arrow::s3_bucket(OUR_LINK_write,
                         endpoint_override = OUR_ENDPOINT_OVERRIDE,
                         access_key = OUR_ACCESS_KEY,
                         secret_key = OUR_SECRET_KEY,
                         scheme = "https")
}

## Write

test_data = data.frame(
  id = 1:100,                        
  age = sample(18:80, 100, replace = TRUE),  
  height = rnorm(100, mean = 170, sd = 10),  
  weight = rnorm(100, mean = 70, sd = 15)
)

arrow::write_csv_arrow(test_data, sink = s3_read$path("test/test_data2.csv"))
arrow::write_csv_arrow(test_data, sink = s3_write$path("test/test_data2.csv"))

## Read

df_read <- arrow::read_csv_arrow(s3_read$path("test/test_data2.csv"))
df_write <- arrow::read_csv_arrow(s3_write$path("test/test_data2.csv"))
