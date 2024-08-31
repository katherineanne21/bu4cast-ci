library(dplyr)
library(duckdbfs)

config <- yaml::read_yaml("challenge_configuration.yaml")
sites <- open_dataset(paste0("https://raw.githubusercontent.com/eco4cast/neon4cast-ci/main/",config$site_table)) |>
  rename(site_id = field_site_id)

# FORECASTS

s3_summaries_P1D <- open_dataset(paste0("s3://", config$forecasts_bucket,"/bundled-summaries/project_id=",  config$project_id,"/duration=P1D/"), s3_endpoint = config$endpoint, anonymous = TRUE)
cutoff <- Sys.Date() - lubridate::days(2)


reference_datetimes_P1D <- s3_summaries_P1D |>
  select(reference_datetime, variable) |>
  summarize(reference_datetime = max(reference_datetime), .by = "variable")

df_P1D <- s3_summaries_P1D |>
  select(-project_id, -family, -sd, -duration, -pub_datetime) |>
  filter(datetime > reference_datetime) |>
  inner_join(reference_datetimes_P1D) |>
  inner_join(sites, by = "site_id") |>
  write_dataset("forecasts_P1D.parquet")

s3_summaries_P1W <- open_dataset(paste0("s3://", config$forecasts_bucket,"/bundled-summaries/project_id=",  config$project_id,"/duration=P1W/"), s3_endpoint = config$endpoint, anonymous = TRUE)

reference_datetimes_P1W <- s3_summaries_P1W |>
  select(reference_datetime, variable) |>
  summarize(reference_datetime = max(reference_datetime), .by = "variable")

s3_summaries_P1W |>
  select(-project_id, -family, -sd, -duration, -pub_datetime) |>
  filter(datetime > reference_datetime) |>
  inner_join(reference_datetimes_P1W) |>
  inner_join(sites, by = "site_id") |>
  write_dataset("forecasts_P1W.parquet")

reference_datetimes <- arrow::open_dataset(s3_summaries_P1D) |>
  select(reference_datetime, variable) |>
  dplyr::summarize(reference_datetime_max = max(reference_datetime), .by = "variable") |>
  dplyr::collect() |>
  group_by(variable) |>
  dplyr::mutate(reference_datetime_max = min(c(reference_datetime_max, Sys.Date() - lubridate::days(2))))

#SCORES

s3_scores_P1D <- open_dataset(paste0("s3://", config$scores_bucket,"/bundled-parquets/project_id=",  config$project_id,"/duration=P1D/"), s3_endpoint = config$endpoint, anonymous = TRUE)

cutoff <- Sys.Date() - lubridate::days(30)

s3_scores_P1D |>
  select(-project_id, -family, -sd, -duration, -pub_datetime) |>
  filter(reference_datetime > cutoff) |>
  inner_join(sites, by = "site_id") |>
  mutate(reference_datetime = lubridate::as_datetime(reference_datetime),
         datetime = lubridate::as_datetime(datetime)) |>
  write_dataset("scores_P1D.parquet")


cutoff <- Sys.Date() - lubridate::days(365)

s3_scores_P1W <- open_dataset(paste0("s3://", config$scores_bucket,"/bundled-parquets/project_id=",  config$project_id,"/duration=P1W/"), s3_endpoint = config$endpoint, anonymous = TRUE)

s3_scores_P1W |>
  select(-project_id, -family, -sd, -duration, -pub_datetime) |>
  filter(reference_datetime > cutoff) |>
  inner_join(sites, by = "site_id") |>
  mutate(reference_datetime = lubridate::as_datetime(reference_datetime),
         datetime = lubridate::as_datetime(datetime)) |>
  write_dataset("scores_P1W.parquet")

## STATS

s3_forecasts_all <- open_dataset(paste0("s3://", config$scores_bucket,"/scores/bundled-forecasts/project_id=",  config$project_id), s3_endpoint = config$endpoint, anonymous = TRUE)

s3_forecasts_all |>
  select(model_id, reference_date) |>
  distinct(model_id, reference_date) |>
  write_dataset("stats_all.parquet")



