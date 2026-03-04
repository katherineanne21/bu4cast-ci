library(arrow)
library(dplyr)
source("https://raw.githubusercontent.com/eco4cast/neon4cast/main/R/to_hourly.R")
print(sessioninfo::package_info())

# Read bucket for sites and driver storage
s3_read <- arrow::s3_bucket(
  "bu4cast-ci-read",
  endpoint_override = "https://minio-s3.apps.shift.nerc.mghpcc.org",
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)

site_list <- arrow::read_csv_arrow(
  s3_read$path("challenges/project_id=bu4cast/metadata/field_sites.csv")
) %>%
  as.data.frame() %>%
  dplyr::pull(field_site_id)

message("Sites loaded: ", length(site_list))

purrr::map(site_list, function(curr_site_id) {
  print(curr_site_id)

  s3_stage3 <- s3_read$path("challenges/project_id=bu4cast/drivers/stage3")
  s3_pseudo  <- s3_read$path("challenges/project_id=bu4cast/drivers/pseudo")

  stage3_df <- arrow::open_dataset(s3_stage3) %>%
    dplyr::filter(site_id == curr_site_id) %>%
    dplyr::collect()

  max_date <- stage3_df %>%
    dplyr::summarise(max = as.character(lubridate::as_date(max(datetime)))) %>%
    dplyr::pull(max)

  cut_off <- as.character(lubridate::as_date(max_date) - lubridate::days(3))

  df <- arrow::open_dataset(s3_pseudo) %>%
    dplyr::filter(variable %in% c("PRES", "TMP", "RH", "UGRD", "VGRD", "APCP", "DSWRF", "DLWRF")) %>%
    dplyr::filter(site_id == curr_site_id,
                  reference_datetime >= cut_off) %>%
    dplyr::collect() %>%
    dplyr::mutate(
      date         = lubridate::as_date(reference_datetime),
      new_datetime = date + lubridate::hours(as.numeric(horizon)) + lubridate::hours(as.numeric(cycle)),
      datetime     = ifelse(datetime != new_datetime, new_datetime, datetime),
      datetime     = lubridate::as_datetime(datetime)
    ) %>%
    dplyr::select(-date, -new_datetime)

  if (nrow(df) > 0) {
    df2 <- df %>%
      to_hourly(use_solar_geom = TRUE, psuedo = TRUE) %>%
      dplyr::mutate(ensemble = as.numeric(stringr::str_sub(ensemble, start = 4, end = 5))) %>%
      dplyr::rename(parameter = ensemble)

    stage3_df %>%
      dplyr::filter(datetime < min(df2$datetime)) %>%
      dplyr::bind_rows(df2) %>%
      dplyr::arrange(variable, datetime, parameter) %>%
      arrow::write_dataset(path = s3_stage3, partitioning = "site_id")
  }
})
