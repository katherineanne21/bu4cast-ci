source("drivers/download_ensemble_forecast.R")

download_ensemble_forecast("gfs_seamless")

s3 <- arrow::s3_bucket("bio230121-bucket01/flare/drivers/met/ensemble_forecast/model_id=gfs_seamless",
                      endpoint_override = "renc.osn.xsede.org",
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))

df <- arrow::open_dataset(s3) |> filter(site_id == "fcre") |> collect()

max_reference_date <- max(df$reference_date)

library(tidyverse)
filename <- paste0("drivers/gfs_seamless-",max_reference_date,".csv.gz")
df |> filter(reference_date == max_reference_date) |>
  mutate(date = lubridate::as_date(datetime)) |>
  select(-unit) |>
  pivot_wider(names_from = variable, values_from = prediction) |>
  summarize(RH_percent_mean = mean(relativehumidity_2m, na.rm = TRUE),
            Rain_mm_sum = sum(precipitation, na.rm = TRUE),
            WindSpeed_ms_mean = mean(windspeed_10m, na.rm = TRUE),
            AirTemp_C_mean = mean(temperature_2m, na.rm = TRUE),
            ShortwaveRadiationUp_Wm2_mean = mean(shortwave_radiation, na.rm = TRUE),
            BP_kPa_mean = mean(surface_pressure * 0.1, na.rm = TRUE),
            .by = c("date","ensemble")) |>
  pivot_longer(-c(date, ensemble), names_to = "variable", values_to = "prediction") |>
  mutate(datetime = lubridate::as_datetime(date),
         reference_datetime = lubridate::as_datetime(max_reference_date),
         site_id = "fcre",
         model_id = "gfs_seamless",
         duration = "P1D",
         project_id = "vera4cast",
         depth_m = NA,
         family = "ensemble",
         ensemble = as.numeric(ensemble)) |>
  rename(parameter = ensemble) |>
  select(c("project_id", "site_id","model_id", "reference_datetime", "datetime","duration", "depth_m","variable", "family", "parameter", "prediction")) |>
  readr::write_csv(filename)

vera4castHelpers::submit(filename, first_submission = FALSE)



