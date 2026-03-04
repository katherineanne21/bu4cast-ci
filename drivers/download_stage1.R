## setup
library(gdalcubes)
library(gefs4cast)
library(arrow)
library(dplyr)
print(sessioninfo::package_info())
gdalcubes::gdalcubes_options(parallel = 2 * parallel::detectCores())

# Read bucket for sites and driver storage
s3_read <- arrow::s3_bucket(
  "bu4cast-ci-read",
  endpoint_override = "https://minio-s3.apps.shift.nerc.mghpcc.org",
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)

sites <- arrow::read_csv_arrow(
  s3_read$path("challenges/project_id=bu4cast/metadata/field_sites.csv")
) %>%
  as.data.frame() %>%
  transmute(
    site_id   = as.character(field_site_id),
    latitude  = as.numeric(latitude),
    longitude = as.numeric(longitude)
  )

message("Sites loaded: ", nrow(sites))

Sys.setenv("GEFS_VERSION" = "v12")
#dates        <- seq(as.Date("2020-09-24"), Sys.Date() - 1, by = 1)
#dates_pseudo <- seq(as.Date("2020-09-24"), Sys.Date(),     by = 1)

dates        <- seq(as.Date("2020-09-24"), as.Date("2021-01-01") - 1, by = 1)
dates_pseudo <- seq(as.Date("2020-09-24"), as.Date("2021-01-01"),     by = 1)

message("GEFS v12 stage1-stats")
bench::bench_time({
  s3 <- s3_read$path("challenges/project_id=bu4cast/drivers/stage1-stats")
  have_dates <- tryCatch(
    gsub("reference_datetime=", "", s3$ls()),
    error = function(e) character(0)
  )
  missing_dates <- dates[!(as.character(dates) %in% have_dates)]
  gefs_to_parquet(missing_dates,
                  ensemble = c("geavg", "gespr"),
                  path     = s3,
                  sites    = sites)
})

message("GEFS v12 stage1")
bench::bench_time({
  s3 <- s3_read$path("challenges/project_id=bu4cast/drivers/stage1")
  have_dates <- tryCatch(
    gsub("reference_datetime=", "", s3$ls()),
    error = function(e) character(0)
  )
  missing_dates <- dates[!(as.character(dates) %in% have_dates)]
  gefs_to_parquet(missing_dates, path = s3, sites = sites)
})
