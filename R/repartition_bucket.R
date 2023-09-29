s3 <- arrow::s3_bucket(bucket = "bio230121-bucket01/vera4cast/forecasts/parquet",
                       endpoint_override = "renc.osn.xsede.org",
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))

arrow::open_dataset(s3) |> arrow::write_dataset(path = ".")

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
  mutate(depth_m = ifelse(site_id == "bvre", 1.5, depth_m)) |>
  arrow::write_dataset(s3, partitioning = c("duration","variable","model_id","reference_date"))


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
  mutate(depth_m = ifelse(site_id == "bvre", 1.5, depth_m)) |>
  arrow::write_dataset(s3, partitioning = c("duration", "variable","model_id","date"))



