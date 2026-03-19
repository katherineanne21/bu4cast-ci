# Setup
library(gdalcubes)
library(gefs4cast)
library(arrow)
library(dplyr)
library(yaml)

config <- yaml::read_yaml("challenge_configuration.yaml")

gdalcubes::gdalcubes_options(parallel = 4)

# Read bucket
s3 <- arrow::s3_bucket(
  config$s3_bucket_read,
  endpoint_override = config$endpoint,
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)

metadata_path <- gsub(paste0("^", config$s3_bucket_read, "/"), "", config$target_metadata_bucket)
drivers_path  <- gsub(paste0("^", config$s3_bucket_read, "/"), "", config$drivers_bucket)

sites <- arrow::read_csv_arrow(
  s3$path(paste0(metadata_path, "/field_sites.csv"))
) %>%
  as.data.frame() %>%
  transmute(
    site_id   = as.character(field_site_id),
    latitude  = as.numeric(latitude),
    longitude = as.numeric(longitude)
  )
Sys.setenv("GEFS_VERSION" = "v12")

dates_pseudo <- seq(as.Date(config$gefs_start_date), Sys.Date(), by = 1)

message("GEFS v12 pseudo")
s3_path    <- s3$path(paste0(drivers_path, "/pseudo"))
have_dates <- tryCatch(
    gsub("reference_datetime=", "", s3_path$ls()),
    error = function(e) character(0)
  )
  missing_dates <- purrr::keep(as.character(dates), function(d) {
    if (!(d %in% have_dates)) return(TRUE)
    have_sites <- tryCatch(
      gsub("site_id=", "", s3_path$path(paste0("reference_datetime=", d))$ls()),
      error = function(e) character(0)
    )
    !all(as.character(sites$site_id) %in% have_sites)
  })
gefs4cast:::gefs_pseudo_measures(missing_dates, path = s3_path, sites = sites)
