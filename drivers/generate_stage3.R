library(minioclient)
source("drivers/to_hourly.R")
library(arrow)
library(dplyr)
library(yaml)

config <- yaml::read_yaml("challenge_configuration.yaml")

s3 <- arrow::s3_bucket(
  config$s3_bucket_read,
  endpoint_override = config$endpoint,
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)

metadata_path <- gsub(paste0("^", config$s3_bucket_read, "/"), "", config$target_metadata_bucket)
drivers_path  <- gsub(paste0("^", config$s3_bucket_read, "/"), "", config$drivers_bucket)

# read site coords for to_hourly patch
site_coords <- arrow::read_csv_arrow(
  s3$path(paste0(metadata_path, "/field_sites.csv"))
) %>%
  as.data.frame() %>%
  dplyr::rename(site_id = field_site_id)

site_list <- site_coords$site_id

mc_alias_set("osn", "minio-s3.apps.shift.nerc.mghpcc.org", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))
mc_mirror("osn/bu4cast-ci-read/challenges/project_id=bu4cast/drivers/pseudo", "pseudo")

s3_stage3 <- arrow::s3_bucket(
  config$s3_bucket_read,
  endpoint_override = config$endpoint,
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)
s3_stage3 <- s3_stage3$path(paste0(drivers_path, "/stage3"))

future::plan("future::multisession", workers = 8)

# have to pass config to workers
furrr::future_walk(site_list, function(curr_site_id) {
  source("drivers/to_hourly.R")
  
  config        <- yaml::read_yaml("challenge_configuration.yaml")
  metadata_path <- gsub(paste0("^", config$s3_bucket_read, "/"), "", config$target_metadata_bucket)
  drivers_path  <- gsub(paste0("^", config$s3_bucket_read, "/"), "", config$drivers_bucket)
  site_coords   <- arrow::read_csv_arrow(
    arrow::s3_bucket(
      config$s3_bucket_read,
      endpoint_override = config$endpoint,
      access_key = Sys.getenv("OSN_KEY"),
      secret_key = Sys.getenv("OSN_SECRET"),
      scheme = "https"
    )$path(paste0(metadata_path, "/field_sites.csv"))
  ) %>%
    as.data.frame() %>%
    dplyr::rename(site_id = field_site_id)

  df <- arrow::open_dataset("pseudo") |>
    dplyr::filter(variable %in% c("PRES", "TMP", "RH", "UGRD", "VGRD", "APCP", "DSWRF", "DLWRF")) |>
    dplyr::filter(site_id == curr_site_id) |>
    dplyr::collect() |>
    dplyr::mutate(date         = lubridate::as_date(reference_datetime),
                  new_datetime = date + lubridate::hours(as.numeric(horizon)) + lubridate::hours(as.numeric(cycle)),
                  datetime     = ifelse(datetime != new_datetime, new_datetime, datetime),
                  datetime     = lubridate::as_datetime(datetime)) |>
    dplyr::select(-date, -new_datetime)

  s3_w <- arrow::s3_bucket(
    config$s3_bucket_read,
    endpoint_override = config$endpoint,
    access_key = Sys.getenv("OSN_KEY"),
    secret_key = Sys.getenv("OSN_SECRET"),
    scheme = "https"
  )

  print(curr_site_id)
  df |>
    to_hourly(use_solar_geom = TRUE, psuedo = TRUE) |>
    dplyr::mutate(ensemble = as.numeric(stringr::str_sub(ensemble, start = 4, end = 5))) |>
    dplyr::rename(parameter = ensemble) |>
    arrow::write_dataset(path = s3_w$path(paste0(drivers_path, "/stage3")),
                         partitioning = "site_id")
})
