s3 <- arrow::s3_bucket(bucket = "bio230121-bucket01/vera4cast/metadata/model_id/",
                       endpoint_override = "renc.osn.xsede.org", anonymous = TRUE)

d1 <- arrow::open_dataset(s3, format = "json") |> dplyr::collect()

unnest(d1[[3]], cols = names(d1[[3]]))
