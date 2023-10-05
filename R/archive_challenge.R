start_date
end_date

library(arrow)
library(tidyverse)

curr_dir <- here::here()

config <- yaml::read_yaml("challenge_configuration.yaml")

fs::dir_create(file.path(curr_dir, "archive"))


start_date <- "2023-01-01"
end_date <- "2024-01-01"

######
message("Archiving forecast parquets")

s3_forecasts <- arrow::s3_bucket(file.path(config$forecasts_bucket,"parquet"),
                                 endpoint_override = config$endpoint,
                                 anonymous = TRUE)


df <- open_dataset(s3_forecasts) |>
  filter(datetime >= lubridate::as_datetime(start_date), datetime < lubridate::as_datetime(end_date)) |> collect()

write_dataset(df, path = file.path(curr_dir, "archive/forecasts/forecasts/"),
              hive_style = TRUE,
              partitioning = c("duration","variable", "model_id"))

setwd(file.path(curr_dir, "archive/forecasts"))
files2zip <- fs::dir_ls(recurse = TRUE)
files2zip <- files2zip[stringr::str_detect(files2zip, pattern = "DS_Store", negate = TRUE)][-1]
utils::zip(zipfile = file.path(curr_dir, "archive/forecasts"), files = files2zip)

#######
message("Archiving score parquets")

s3_scores <- arrow::s3_bucket(config$scores_bucket,
                                 endpoint_override = config$endpoint,
                                 anonymous = TRUE)

df_scores <- open_dataset(s3_scores) |>
  filter(datetime >= lubridate::as_datetime(start_date), datetime < lubridate::as_datetime(start_date))

write_dataset(df_scores, path = file.path("archive/scores/scores"), hive_style = TRUE, partitioning = c("duration","variable","model_id"))

setwd(file.path(lake_directory, "archive/scores"))
files2zip <- fs::dir_ls(recurse = TRUE)
files2zip <- files2zip[stringr::str_detect(files2zip, pattern = "DS_Store", negate = TRUE)][-1]
utils::zip(zipfile = file.path(lake_directory, "archive/scores"), files = files2zip)

#######
message("Archiving targets")

minioclient::mc_alias_set("osn",
                          config$endpoint,
                          Sys.getenv("OSN_KEY"),
                          Sys.getenv("OSN_SECRET"))

local_dir <- file.path(curr_dir, "archive/targets")
fs::dir_create(local_dir)

minioclient::mc_mirror(from = paste0("osn/",config$targets_bucket), to = local_dir)


setwd(file.path(curr_dir, "archive/targets"))
files2zip <- fs::dir_ls(recurse = TRUE)
files2zip <- files2zip[stringr::str_detect(files2zip, pattern = "DS_Store", negate = TRUE)][-1]
utils::zip(zipfile = file.path(lake_directory, "archive/targets"), files = files2zip)

######
message("Catalog")

setwd(here::here())
jsons <- fs::dir_ls(path ="catalog", glob="*.json", recurse=TRUE)

fs::dir_copy()
