df <- aws.s3::get_bucket_df(bucket = "bio230121-bucket01",
                            prefix = "vera4cast/drivers/met/model_id=gfs_seamless",
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
