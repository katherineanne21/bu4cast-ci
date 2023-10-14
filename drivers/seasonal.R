source("drivers/download_seasonal_forecast.R")
library(tidyverse)

download_seasonal_forecast()

s3 <- arrow::s3_bucket("bio230121-bucket01/flare/drivers/met/seasonal_forecast/model_id=cfs",
                       endpoint_override = "renc.osn.xsede.org",
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))

df <- arrow::open_dataset(s3) |> filter(site_id == "fcre") |> collect()



max_reference_date <- max(df$reference_date)

filename <- paste0("drivers/cfs-",max_reference_date,".csv.gz")
dates <- df |> filter(reference_date == max_reference_date &
               datetime > reference_datetime,
             variable == "temperature_2m") |>
  na.omit() |>
  mutate(date = as_date(datetime)) |>
  distinct(date, ensemble) |>
  group_by(date) |>
  count() |>
  filter(n > 1) |>
  pull(date)


df |> filter(reference_date == max_reference_date &
               datetime > reference_datetime) |>
  mutate(date = lubridate::as_date(datetime)) |>
  filter(date %in% dates) |>
  select(-unit) |>
  pivot_wider(names_from = variable, values_from = prediction) |>
  na.omit() |>
  summarize(RH_percent_mean = mean(relativehumidity_2m, na.rm = TRUE),
            Rain_mm_sum = sum(precipitation, na.rm = TRUE),
            WindSpeed_ms_mean = mean(windspeed_10m, na.rm = TRUE),
            AirTemp_C_mean = mean(temperature_2m, na.rm = TRUE),
            ShortwaveRadiationUp_Wm2_mean = mean(shortwave_radiation, na.rm = TRUE),
            .by = c("date","ensemble")) |>
  pivot_longer(-c(date, ensemble), names_to = "variable", values_to = "prediction") |>
  mutate(datetime = lubridate::as_datetime(date),
         reference_datetime = lubridate::as_datetime(max_reference_date),
         site_id = "fcre",
         model_id = "cfs",
         duration = "P1D",
         project_id = "vera4cast",
         depth_m = NA,
         family = "ensemble",
         ensemble = as.numeric(ensemble)) |>
  rename(parameter = ensemble) |>
  select(c("project_id", "site_id","model_id", "reference_datetime", "datetime","duration", "depth_m","variable", "family", "parameter", "prediction")) |>
  readr::write_csv(filename)

vera4castHelpers::submit(filename, first_submission = FALSE)
