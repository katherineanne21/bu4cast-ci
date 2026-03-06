# Setup
library(gdalcubes)
library(gefs4cast)
library(arrow)
library(dplyr)
library(yaml)

config <- yaml::read_yaml("challenge_configuration.yaml")

gdalcubes::gdalcubes_options(parallel = 2 * parallel::detectCores())

# Read bucket
s3 <- arrow::s3_bucket(
  config$s3_bucket_read,
  endpoint_override = config$endpoint,
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)

sites <- arrow::read_csv_arrow(
  s3$path(paste0(config$target_metadata_bucket, "/field_sites.csv"))
) %>%
  as.data.frame() %>%
  transmute(
    site_id   = as.character(field_site_id),
    latitude  = as.numeric(latitude),
    longitude = as.numeric(longitude)
  )
message("Sites loaded: ", nrow(sites))

Sys.setenv("GEFS_VERSION" = "v12")

dates_pseudo <- seq(as.Date(config$gefs_start_date), Sys.Date(), by = 1)

message("GEFS v12 pseudo")
s3_path    <- s3$path(paste0(config$drivers_bucket, "/pseudo"))
have_dates <- tryCatch(
  gsub("reference_datetime=", "", s3_path$ls()),
  error = function(e) character(0)
)
missing_dates <- dates_pseudo[!(as.character(dates_pseudo) %in% have_dates)]
gefs4cast:::gefs_pseudo_measures(missing_dates, path = s3_path, sites = sites)
