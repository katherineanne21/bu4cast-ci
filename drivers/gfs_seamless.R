source("drivers/download_ensemble_forecast.R")

download_ensemble_forecast("gfs_seamless")


#s3 <- arrow::s3_bucket("bio230121-bucket01/flare/drivers/met/ensemble_forecast/model_id=gfs_seamless",
#                       endpoint_override = "renc.osn.xsede.org",
#                       access_key = Sys.getenv("OSN_KEY"),
#                       secret_key = Sys.getenv("OSN_SECRET"))

#df <- arrow::open_dataset(s3) |> filter(site_id == "BARC") |> collect()

#max_reference_date <- max(df$reference_date)

#df |> distinct(variable)
