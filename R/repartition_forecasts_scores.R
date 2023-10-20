minioclient::mc_alias_set("s3_store",
                          "renc.osn.xsede.org",
                          Sys.getenv("OSN_KEY"),
                          Sys.getenv("OSN_SECRET"))

minioclient::mc_mirror("s3_store/bio230121-bucket01/vera4cast/forecasts/parquet", "temp_forecasts")



s3 <- arrow::s3_bucket("bio230121-bucket01/vera4cast/forecasts/parquet",
                       endpoint_override = "renc.osn.xsede.org",
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))

s <- arrow::schema(
  project_id = arrow::string(),
  site_id = arrow::string(),
  reference_datetime = arrow::timestamp(unit = "us"),
  datetime = arrow::timestamp(unit = "us"),
  depth_m = arrow::float(),
  family = arrow::string(),
  parameter  = arrow::string(),
  prediction = arrow::float(),
  pub_datetime = arrow::timestamp(unit = "us"),
  duration  = arrow::string(),
  variable  = arrow::string(),
  model_id  = arrow::string(),
  reference_date  = arrow::string()
)

d <- arrow::open_dataset(s3,schema = s)


s3_2 <- arrow::s3_bucket("bio230121-bucket01/vera4cast/forecasts",
                       endpoint_override = "renc.osn.xsede.org",
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))

s3_2 <- arrow::s3_bucket("bio230121-bucket01/vera4cast/forecasts/parquet",
                         endpoint_override = "renc.osn.xsede.org",
                         access_key = Sys.getenv("OSN_KEY"),
                         secret_key = Sys.getenv("OSN_SECRET"))

d <- arrow::open_dataset("temp_forecasts",schema = s)

d |> arrow::write_dataset(s3_2, format = 'parquet',
                     partitioning = c("project_id",
                                      "duration",
                                      "variable",
                                      "model_id",
                                      "reference_date"))

s3_2 <- arrow::s3_bucket("bio230121-bucket01/vera4cast/forecasts/parquet2",
                         endpoint_override = "renc.osn.xsede.org",
                         access_key = Sys.getenv("OSN_KEY"),
                         secret_key = Sys.getenv("OSN_SECRET"))

d <- arrow::open_dataset(s3_2)

minioclient::mc_rm("s3_store/bio230121-bucket01/vera4cast/forecasts/parquet", recursive = TRUE)

minioclient::mc_mv("s3_store/bio230121-bucket01/vera4cast/forecasts/parquet2", "s3_store/bio230121-bucket01/vera4cast/forecasts/parquet", recursive = TRUE)



#####

minioclient::mc_alias_set("s3_store",
                          "renc.osn.xsede.org",
                          Sys.getenv("OSN_KEY"),
                          Sys.getenv("OSN_SECRET"))

minioclient::mc_mirror("s3_store/bio230121-bucket01/vera4cast/scores/parquet", "temp_scores")

minioclient::mc_rm("s3_store/bio230121-bucket01/vera4cast/scores/parquet", recursive = TRUE)

s3 <- arrow::s3_bucket("temp_scores",
                       endpoint_override = "renc.osn.xsede.org",
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))

s <- arrow::schema(
  project_id = arrow::string(),
  site_id = arrow::string(),
  reference_datetime = arrow::timestamp(unit = "us"),
  datetime = arrow::timestamp(unit = "us"),
  depth_m = arrow::float(),
  family = arrow::string(),
  parameter  = arrow::string(),
  prediction = arrow::float(),
  pub_datetime = arrow::timestamp(unit = "us"),
  duration  = arrow::string(),
  variable  = arrow::string(),
  model_id  = arrow::string(),
  reference_date  = arrow::string()
)

d <- arrow::open_dataset("temp_scores") |>
  dplyr::mutate(pub_datetime = lubridate::as_datetime(pub_datetime),
                project_id = "vera4cast")

s3 <- arrow::s3_bucket("bio230121-bucket01/vera4cast/scores",
                       endpoint_override = "renc.osn.xsede.org",
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))

s3$CreateDir("parquet")

s3 <- arrow::s3_bucket("bio230121-bucket01/vera4cast/scores/parquet",
                       endpoint_override = "renc.osn.xsede.org",
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))


d |> dplyr::filter(variable == "WindSpeed_ms_mean") |>
  arrow::write_dataset(s3, format = 'parquet',
                          partitioning = c("project_id",
                                           "duration",
                                           "variable",
                                           "model_id",
                                           "date"))
