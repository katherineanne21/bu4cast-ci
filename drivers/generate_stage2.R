source("https://raw.githubusercontent.com/eco4cast/neon4cast/ci_upgrade/R/to_hourly.R")
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

site_list <- arrow::read_csv_arrow(
  s3$path(paste0(metadata_path, "/field_sites.csv"))
) %>%
  as.data.frame() %>%
  dplyr::rename(site_id = field_site_id)

message("Sites loaded: ", nrow(site_list))

s3_stage2 <- s3$path(paste0(drivers_path, "/stage2"))

have_dates <- dplyr::tibble(
  reference_datetime = tryCatch(
    gsub("reference_datetime=", "", s3_stage2$ls()),
    error = function(e) character(0)
  )
)
curr_date     <- Sys.Date()
last_week     <- dplyr::tibble(reference_datetime = as.character(seq(curr_date - lubridate::days(14), curr_date - lubridate::days(1), by = "1 day")))
missing_dates <- dplyr::anti_join(last_week, have_dates, by = "reference_datetime") %>%
  dplyr::pull(reference_datetime)

if (length(missing_dates) > 0) {
  for (i in seq_along(missing_dates)) {
    print(missing_dates[i])

    s3_stage1 <- s3$path(
      paste0(drivers_path, "/stage1/reference_datetime=", missing_dates[i])
    )
    
    site_df <- arrow::open_dataset(s3_stage1) %>%
      dplyr::filter(variable %in% c("PRES", "TMP", "RH", "UGRD", "VGRD", "APCP", "DSWRF", "DLWRF")) %>%
      dplyr::filter(site_id %in% site_list$site_id) %>%
      dplyr::collect() %>%
      dplyr::mutate(reference_datetime = missing_dates[i]) %>%
      dplyr::left_join(
        site_list %>% dplyr::select(site_id, latitude, longitude),
        by = "site_id"
      )
    
    site_df <- site_df %>%
      dplyr::mutate(horizon = as.numeric(horizon, units = "hours"))
    
    message("longitude type: ", typeof(site_df$longitude))
    message("longitude class: ", class(site_df$longitude))
            
    hourly_df <- to_hourly(site_df, use_solar_geom = TRUE, psuedo = FALSE) %>%
      dplyr::mutate(
        ensemble           = as.numeric(stringr::str_sub(ensemble, start = 4, end = 5)),
        reference_datetime = lubridate::as_date(reference_datetime)
      ) %>%
      dplyr::rename(parameter = ensemble)

    arrow::write_dataset(hourly_df, path = s3_stage2, partitioning = c("reference_datetime", "site_id"))
  }
}
