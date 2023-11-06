df <- aws.s3::get_bucket_df(bucket = "bio230014-bucket01",
                            prefix = "challenges/forecasts/parquet/project_id=neon4cast/duration=PT30M/variable=le/model_id=climatology",
                            region =  "sdsc",
                            base_url = "osn.xsede.org",
                            key =  "3V0QE2X34IYY0FFNPBHC",
                            secret = "jIWNAp753sFdd0J1oiEKnwal5Gg/lD")

for(i in 1:nrow(df)){

  aws.s3::delete_object(object = df$Key[i],
                     bucket = "bio230121-bucket01",
                     region = "renc",
                     base_url = "osn.xsede.org",
                     key = Sys.getenv("OSN_KEY"),
                     secret = Sys.getenv("OSN_SECRET"))
}

df <- aws.s3::get_bucket_df(bucket = "bio230121-bucket01",
                            prefix = "vera4cast/prov/",
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

