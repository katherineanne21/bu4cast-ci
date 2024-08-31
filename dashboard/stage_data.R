library(dplyr)
library(duckdbfs)

config <- yaml::read_yaml("challenge_configuration.yaml")
sites <- open_dataset(paste0("https://raw.githubusercontent.com/eco4cast/neon4cast-ci/main/",config$site_table)) |>
  rename(site_id = field_site_id)

message("P1D forecast summaries")

s3_summaries_P1D <- open_dataset(paste0("s3://", config$forecasts_bucket,"/bundled-summaries/project_id=",  config$project_id,"/duration=P1D/"), s3_endpoint = config$endpoint, anonymous = TRUE)
cutoff <- Sys.Date() - lubridate::days(2)


reference_datetimes_P1D <- s3_summaries_P1D |>
  select(reference_datetime, variable) |>
  summarize(reference_datetime = max(reference_datetime), .by = "variable")

df_P1D <- s3_summaries_P1D |>
  select(-project_id, -family, -sd, -duration, -pub_datetime) |>
  filter(datetime > reference_datetime) |>
  inner_join(reference_datetimes_P1D, by = join_by(reference_datetime, variable)) |>
  inner_join(sites, by = "site_id") |>
  write_dataset("forecasts_P1D.parquet")

message("P1W forecast summaries")

s3_summaries_P1W <- open_dataset(paste0("s3://", config$forecasts_bucket,"/bundled-summaries/project_id=",  config$project_id,"/duration=P1W/"), s3_endpoint = config$endpoint, anonymous = TRUE)

reference_datetimes_P1W <- s3_summaries_P1W |>
  select(reference_datetime, variable) |>
  summarize(reference_datetime = max(reference_datetime), .by = "variable")

s3_summaries_P1W |>
  select(-project_id, -family, -sd, -duration, -pub_datetime) |>
  filter(datetime > reference_datetime) |>
  inner_join(reference_datetimes_P1W, by = join_by(reference_datetime, variable)) |>
  inner_join(sites, by = "site_id") |>
  write_dataset("forecasts_P1W.parquet")

message("P1D scores")

s3_scores_P1D <- open_dataset(paste0("s3://", config$scores_bucket,"/bundled-parquet/project_id=",  config$project_id,"/duration=P1D/"), s3_endpoint = config$endpoint, anonymous = TRUE)

cutoff <- Sys.Date() - lubridate::days(30)

s3_scores_P1D |>
  select(-project_id, -family, -sd, -duration, -pub_datetime) |>
  filter(reference_datetime > cutoff) |>
  inner_join(sites, by = "site_id") |>
  mutate(reference_datetime = lubridate::as_datetime(reference_datetime),
         datetime = lubridate::as_datetime(datetime)) |>
  write_dataset("scores_P1D.parquet")

message("P1W forecast summaries")

cutoff <- Sys.Date() - lubridate::days(365)

s3_scores_P1W <- open_dataset(paste0("s3://", config$scores_bucket,"/bundled-parquet/project_id=",  config$project_id,"/duration=P1W/"), s3_endpoint = config$endpoint, anonymous = TRUE)

s3_scores_P1W |>
  select(-project_id, -family, -sd, -duration, -pub_datetime) |>
  filter(reference_datetime > cutoff) |>
  inner_join(sites, by = "site_id") |>
  mutate(reference_datetime = lubridate::as_datetime(reference_datetime),
         datetime = lubridate::as_datetime(datetime)) |>
  write_dataset("scores_P1W.parquet")

message("high level stats")

s3_forecasts_all <- open_dataset(paste0("s3://", config$forecasts_bucket,"/scores/bundled-parquet/project_id=",  config$project_id), s3_endpoint = config$endpoint, anonymous = TRUE)

s3_forecasts_all |>
  select(model_id, reference_date) |>
  distinct(model_id, reference_date) |>
  write_dataset("stats_all.parquet")



