library(tidyverse)
config <- yaml::read_yaml("challenge_configuration.yaml")
inventory_df <- NULL
for(i in 1:length(config$themes)){
  theme <- config$themes[i]
  s3 <- arrow::s3_bucket(paste0(config$forecasts_bucket, "/parquet/",theme), endpoint_override = config$endpoint, anonymous = TRUE)


  df <- arrow::open_dataset(s3) |>
    mutate(theme = theme,
           reference_date = lubridate::as_date(reference_datetime),
           date = lubridate::as_date(datetime)) |>
    distinct(theme, model_id, site_id, reference_date, variable, date) |>
    collect() |>
    mutate(path = glue::glue("{bucket}/parquet/{theme}/variable={variable}"),
           endpoint =config$endpoint)

  inventory_df <- bind_rows(inventory_df, df)
}

s3_inventory <- arrow::s3_bucket(config$inventory_bucket,
                                 endpoint_override = config$endpoint,
                                 access_key = Sys.getenv("OSN_KEY"),
                                 secret_key = Sys.getenv("OSN_SECRET"))

arrow::write_dataset(inventory_df, path = s3_inventory$path("catalog"))

s3_inventory <- arrow::s3_bucket(config$inventory_bucket,
                                 endpoint_override = config$endpoint,
                                 access_key = Sys.getenv("OSN_KEY"),
                                 secret_key = Sys.getenv("OSN_SECRET"))

inventory_df |> distinct(model_id, theme) |>
  arrow::write_csv_arrow(s3_inventory$path("model_id/model_id-theme-inventory.csv"))
