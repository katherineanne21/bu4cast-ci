s3 <- arrow::s3_bucket(bucket = "bio230121-bucket01/vera4cast/forecasts/parquet",
                       endpoint_override = "renc.osn.xsede.org",
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))

arrow::open_dataset(s3) |>
  arrow::write_dataset(path = ".")

df <- aws.s3::get_bucket_df(bucket = "bio230121-bucket01",
                            prefix = "vera4cast/forecasts/parquet",
                            region =  "renc",
                            base_url = "osn.xsede.org",
                            key = Sys.getenv("OSN_KEY"),
                            secret = Sys.getenv("OSN_SECRET"))

for(i in 1:nrow(df)){

  aws.s3::delete_object(object = df$Key[i],
                        bucket = "bio230121-bucket01",
                        region = "renc",
                        base_url = "osn.xsede.org",
                        key = Sys.getenv("OSN_KEY"),
                        secret = Sys.getenv("OSN_SECRET"))
}

arrow::open_dataset("part-0.parquet") |>
  collect() |>
  dplyr::mutate(depth_m = ifelse(depth_m == 1.5 & site_id == "fcre", 1.6, depth_m)) |>
  arrow::write_dataset(s3, partitioning = c("project_id", "duration","variable","model_id","reference_date"))

###

s3 <- arrow::s3_bucket(bucket = "bio230121-bucket01/vera4cast/scores/parquet",
                       endpoint_override = "renc.osn.xsede.org",
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))

arrow::open_dataset(s3) |> arrow::write_dataset(path = ".")

df <- aws.s3::get_bucket_df(bucket = "bio230121-bucket01",
                            prefix = "vera4cast/scores/parquet",
                            region =  "renc",
                            base_url = "osn.xsede.org",
                            key = Sys.getenv("OSN_KEY"),
                            secret = Sys.getenv("OSN_SECRET"))

for(i in 1:nrow(df)){

  aws.s3::delete_object(object = df$Key[i],
                        bucket = "bio230121-bucket01",
                        region = "renc",
                        base_url = "osn.xsede.org",
                        key = Sys.getenv("OSN_KEY"),
                        secret = Sys.getenv("OSN_SECRET"))
}

arrow::open_dataset("part-0.parquet") |>
  collect() |>
  mutate(reference_datetime = stringr::str_sub(reference_datetime, start = 1, end = 10),
         reference_datetime = lubridate::as_datetime(reference_datetime)) |>
  arrow::write_dataset(s3, partitioning = c("duration", "variable","model_id","date"))



