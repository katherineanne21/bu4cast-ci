source("https://raw.githubusercontent.com/eco4cast/neon4cast/ci_upgrade/R/to_hourly.R")
library(arrow)
library(dplyr)
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
  s3_read$path("challenges/targets/project_id=bu4cast/field_sites.csv")
) %>%
  as.data.frame() %>%
  dplyr::rename(site_id = field_site_id)

message("Sites loaded: ", nrow(site_list))

s3_stage2 <- s3_read$path("challenges/targets/project_id=bu4cast/drivers/stage2")

have_dates    <- dplyr::tibble(reference_datetime = gsub("reference_datetime=", "", s3_stage2$ls()))
curr_date     <- Sys.Date()
last_week     <- dplyr::tibble(reference_datetime = as.character(seq(curr_date - lubridate::days(14), curr_date - lubridate::days(1), by = "1 day")))
missing_dates <- dplyr::anti_join(last_week, have_dates, by = "reference_datetime") %>%
  dplyr::pull(reference_datetime)

if (length(missing_dates) > 0) {
  for (i in seq_along(missing_dates)) {
    print(missing_dates[i])

    # Read stage1 from bu4cast bucket
    s3_stage1 <- s3_read$path(
      paste0("challenges/targets/project_id=bu4cast/drivers/stage1/reference_datetime=", missing_dates[i])
    )

    site_df <- arrow::open_dataset(s3_stage1) %>%
      dplyr::filter(variable %in% c("PRES", "TMP", "RH", "UGRD", "VGRD", "APCP", "DSWRF", "DLWRF")) %>%
      dplyr::filter(site_id %in% site_list$site_id) %>%
      dplyr::collect() %>%
      dplyr::mutate(reference_datetime = missing_dates[i])

    hourly_df <- to_hourly(site_df, use_solar_geom = TRUE, psuedo = FALSE) %>%
      dplyr::mutate(
        ensemble           = as.numeric(stringr::str_sub(ensemble, start = 4, end = 5)),
        reference_datetime = lubridate::as_date(reference_datetime)
      ) %>%
      dplyr::rename(parameter = ensemble)

    arrow::write_dataset(hourly_df, path = s3_stage2, partitioning = c("reference_datetime", "site_id"))
  }
}
