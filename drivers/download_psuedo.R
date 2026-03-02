## setup
library(gdalcubes)
library(gefs4cast)
library(arrow)
library(dplyr)
print(sessioninfo::package_info())
gdalcubes::gdalcubes_options(parallel = 20)

# Read sites from bu4cast bucket
s3_read <- arrow::s3_bucket(
  "bu4cast-ci-read",
  endpoint_override = "https://minio-s3.apps.shift.nerc.mghpcc.org",
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)

sites <- arrow::read_csv_arrow(
  s3_read$path("challenges/targets/project_id=bu4cast/field_sites.csv")
) %>%
  as.data.frame() %>%
  transmute(
    site_id   = as.character(field_site_id),
    latitude  = as.numeric(latitude),
    longitude = as.numeric(longitude)
  )

message("Sites loaded: ", nrow(sites))

Sys.setenv("GEFS_VERSION" = "v12")
dates        <- seq(as.Date("2020-09-24"), Sys.Date() - 1, by = 1)
dates_pseudo <- seq(as.Date("2020-09-24"), Sys.Date(),     by = 1)

message("GEFS v12 pseudo")
s3 <- gefs_s3_dir("pseudo")
have_dates    <- gsub("reference_datetime=", "", s3$ls())
missing_dates <- dates_pseudo[!(as.character(dates_pseudo) %in% have_dates)]
gefs4cast:::gefs_pseudo_measures(missing_dates, path = s3, sites = sites)
