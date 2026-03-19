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

site_coords <- arrow::read_csv_arrow(
  s3$path(paste0(metadata_path, "/field_sites.csv"))
) %>%
  as.data.frame() %>%
  dplyr::rename(site_id = field_site_id)

message("Sites loaded: ", nrow(site_coords))

# split sites by type (disease site ids have 6 digits)
site_coords_standard <- site_coords %>% dplyr::filter(nchar(site_id) != 6)
site_coords_disease  <- site_coords %>% dplyr::filter(nchar(site_id) == 6)

s3_stage2 <- s3$path(paste0(drivers_path, "/stage2"))

have_dates <- tryCatch(
  gsub("reference_datetime=", "", s3_stage2$ls()),
  error = function(e) character(0)
)

curr_date <- Sys.Date()
dates     <- as.character(seq(as.Date(config$gefs_start_date), curr_date - lubridate::days(1), by = "1 day"))

missing_dates <- purrr::keep(dates, function(d) {
  if (!(d %in% have_dates)) return(TRUE)
  have_sites <- tryCatch(
    gsub("site_id=", "", s3_stage2$path(paste0("reference_datetime=", d))$ls()),
    error = function(e) character(0)
  )
  !all(as.character(site_coords$site_id) %in% have_sites)
})

message("Missing or incomplete dates: ", length(missing_dates))

if (length(missing_dates) > 0) {
  for (i in seq_along(missing_dates)) {
    print(missing_dates[i])
    s3_stage1 <- s3$path(
      paste0(drivers_path, "/stage1/reference_datetime=", missing_dates[i])
    )

    # coastal + urban downscaled to hourly 
    if (nrow(site_coords_standard) > 0) {
      site_df <- arrow::open_dataset(s3_stage1) %>%
        dplyr::filter(variable %in% c("PRES", "TMP", "RH", "UGRD", "VGRD", "APCP", "DSWRF", "DLWRF")) %>%
        dplyr::filter(site_id %in% site_coords_standard$site_id) %>%
        dplyr::collect() %>%
        dplyr::mutate(reference_datetime = missing_dates[i]) %>%
        dplyr::left_join(
          site_coords_standard %>% dplyr::select(site_id, latitude, longitude),
          by = "site_id"
        ) %>%
        dplyr::mutate(horizon = as.numeric(horizon, units = "hours"))

      hourly_df <- to_hourly(site_df, use_solar_geom = TRUE, psuedo = FALSE) %>%
        dplyr::mutate(
          ensemble           = as.numeric(stringr::str_sub(ensemble, start = 4, end = 5)),
          reference_datetime = lubridate::as_date(reference_datetime)
        ) %>%
        dplyr::rename(parameter = ensemble)

      arrow::write_dataset(hourly_df, path = s3_stage2,
                           partitioning = c("reference_datetime", "site_id"))
    }

    # disease sites are aggregated to weekly (sum for precipitation, mean for everything else)
    if (nrow(site_coords_disease) > 0) {
      disease_df <- arrow::open_dataset(s3_stage1) %>%
        dplyr::filter(variable %in% c("PRES", "TMP", "RH", "UGRD", "VGRD", "APCP", "DSWRF", "DLWRF")) %>%
        dplyr::filter(site_id %in% site_coords_disease$site_id) %>%
        dplyr::collect() %>%
        dplyr::mutate(
          reference_datetime = lubridate::as_date(missing_dates[i]),
          datetime           = lubridate::as_datetime(datetime),
          days_from_ref      = as.numeric(difftime(lubridate::as_date(datetime),
                                                    reference_datetime, units = "days")),
          week               = floor(days_from_ref / 7),
          ensemble           = as.numeric(stringr::str_sub(as.character(ensemble), start = 4, end = 5))
        ) %>%
        dplyr::group_by(site_id, ensemble, variable, reference_datetime, week) %>%
        dplyr::summarise(
          prediction = ifelse(variable[1] == "APCP",
                              sum(prediction, na.rm = TRUE),
                              mean(prediction, na.rm = TRUE)),
          datetime   = min(datetime),  # first datetime of the week
          .groups    = "drop"
        ) %>%
        dplyr::select(-week) %>%
        dplyr::rename(parameter = ensemble)

      arrow::write_dataset(disease_df, path = s3_stage2,
                           partitioning = c("reference_datetime", "site_id"))
    }
  }
}
