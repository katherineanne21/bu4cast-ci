config <- yaml::read_yaml("../challenge_configuration.yaml")

s3_inventory <- arrow::s3_bucket(paste0(config$inventory_bucket,"/catalog/forecasts/project_id=", config$project_id),
                                 endpoint_override = config$endpoint,
                                 access_key = Sys.getenv("OSN_KEY"),
                                 secret_key = Sys.getenv("OSN_SECRET"))

inventory_df <- arrow::open_dataset(s3_inventory) |> dplyr::collect()


df <- inventory_df |> dplyr::distinct(duration, model_id, variable, project_id, path, endpoint)


s3 <- arrow::s3_bucket(config$forecasts_bucket,
                       endpoint_override = config$endpoint,
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))


for(i in 1:nrow(df)){

  print(i)

  arrow::open_dataset(paste0("s3://anonymous@",df$path[i],"/model_id=",df$model_id[i],"?endpoint_override=",df$endpoint[i])) |>
    dplyr::mutate(model_id = df$model_id[i],
                  variable =  df$variable[i],
                  duration = df$duration[i],
                  project_id = df$project_id[i]) |>
    dplyr::collect() |>
    dplyr::summarise(prediction = mean(prediction), .by = dplyr::any_of(c("site_id", "datetime", "reference_datetime", "family", "duration", "model_id",
                                                                          "parameter", "pub_datetime", "reference_date", "variable", "project_id"))) |>
    score4cast::summarize_forecast(extra_groups = c("duration", "project_id")) |>
    dplyr::mutate(reference_date = lubridate::as_date(reference_datetime)) |>
    arrow::write_dataset(s3$path("summaries"), format = 'parquet',
                         partitioning = c("project_id",
                                          "duration",
                                          "variable",
                                          "model_id",
                                          "reference_date"))

}
