library(arrow)
library(tidyverse)

start_date <- "2023-01-01"
end_date <- "2024-01-01"

curr_dir <- here::here()

config <- yaml::read_yaml("challenge_configuration.yaml")

fs::dir_create(file.path(curr_dir, "archive/scores"), recurse = TRUE)
fs::dir_create(file.path(curr_dir, "archive/forecasts"), recurse = TRUE)
fs::dir_create(file.path(curr_dir, "archive/targets"), recurse = TRUE)
fs::dir_create(file.path(curr_dir, "archive/catalog"), recurse = TRUE)
#######
message("Archiving forecasts")

s3_forecasts <- arrow::s3_bucket(file.path(config$forecasts_bucket,"parquet"),
                                 endpoint_override = config$endpoint,
                                 anonymous = TRUE)


df <- open_dataset(s3_forecasts) |>
  filter(datetime >= lubridate::as_datetime(start_date), datetime < lubridate::as_datetime(end_date)) |> collect()

write_dataset(df, path = file.path(curr_dir, "archive/forecasts"),
              hive_style = TRUE,
              partitioning = c("duration","variable", "model_id"))

#######
message("Archiving scores")

s3_scores <- arrow::s3_bucket(config$scores_bucket,
                                 endpoint_override = config$endpoint,
                                 anonymous = TRUE)

df_scores <- open_dataset(s3_scores) |>
  filter(datetime >= lubridate::as_datetime(start_date), datetime < lubridate::as_datetime(end_date))

write_dataset(df_scores, path = file.path("archive/scores"), hive_style = TRUE, partitioning = c("duration","variable","model_id"))

#######
message("Archiving targets")

minioclient::mc_alias_set("osn",
                          config$endpoint,
                          Sys.getenv("OSN_KEY"),
                          Sys.getenv("OSN_SECRET"))

minioclient::mc_mirror(from = paste0("osn/",config$targets_bucket), to = "archive/targets")

######
message("Archive catalog and metadata")

setwd(here::here())
jsons <- fs::dir_ls(path ="catalog", glob="*.json", recurse=TRUE)

for(i in 1:length(jsons)){
  dir.create(file.path(curr_dir, "archive/catalog",dirname(jsons[i])),recursive = TRUE, showWarnings = FALSE)
  fs::file_copy(file.path(curr_dir, jsons[i]), dirname(file.path(curr_dir, "archive/catalog",jsons[i])), overwrite = TRUE)
}

# Archive variable descriptions
googlesheets4::gs4_deauth()
target_metadata <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/1fOWo6zlcWA8F6PmRS9AD6n1pf-dTWSsmGKNpaX3yHNE/edit?usp=sharing")

readr::write_csv(target_metadata, file.path(curr_dir, "archive/catalog/target_metadata.csv"))

###
setwd(file.path(curr_dir, "archive"))
files2zip <- fs::dir_ls(recurse = TRUE)
files2zip <- files2zip[stringr::str_detect(files2zip, pattern = "DS_Store", negate = TRUE)][-1]
utils::zip(zipfile = file.path(curr_dir, paste0("archive_", Sys.Date())), files = files2zip)
