library(tidyverse)
library(arrow)
library(glue)
library(vera4castHelpers)
library(here)

config <- yaml::read_yaml("challenge_configuration.yaml")

AWS_DEFAULT_REGION_submissions <- stringr::str_split_fixed(config$submissions_endpoint,"\\.", 2)[,1]
region_submissions <- stringr::str_split_fixed(config$submissions_endpoint,"\\.", 2)[,1]
AWS_S3_ENDPOINT_submissions <- stringr::str_split_fixed(config$submissions_endpoint,"\\.", 2)[,2]

AWS_DEFAULT_REGION_forecasts <- stringr::str_split_fixed(config$endpoint,"\\.", 2)[,1]
region_forecasts <- stringr::str_split_fixed(config$endpoint,"\\.", 2)[,1]
AWS_S3_ENDPOINT_forecasts <- stringr::str_split_fixed(config$endpoint,"\\.", 2)[,2]
endpoint_override_forecasts <- config$endpoint


message(paste0("Starting Processing Submissions ", Sys.time()))

local_dir <- file.path(here::here(), "submissions")
unlink(local_dir, recursive = TRUE)
fs::dir_create(local_dir)

#Sys.unsetenv("AWS_SECRET_ACCESS_KEY")

message("Downloading forecasts ...")

## Note: s3sync stupidly also requires auth credentials even to download from public bucket

aws.s3::s3sync(local_dir, bucket = config$submissions_bucket,
               direction= "download",
               verbose = FALSE,
               base_url = AWS_S3_ENDPOINT_submissions,
               region = region_submissions)

submissions <- fs::dir_ls(local_dir, recurse = TRUE, type = "file")
submissions_bucket <- basename(submissions)

themes <- config$themes

if(length(submissions) > 0){

  Sys.unsetenv("AWS_DEFAULT_REGION")
  Sys.unsetenv("AWS_S3_ENDPOINT")
  Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")

  s3 <- arrow::s3_bucket(config$forecasts_bucket,
                         endpoint_override = endpoint_override_forecasts,
                         access_key = Sys.getenv("OSN_KEY"),
                         secret_key = Sys.getenv("OSN_SECRET"))

  s3_inventory <- arrow::s3_bucket("bio230121-bucket01/vera4cast",
                                   endpoint_override = endpoint_override_forecasts,
                                   access_key = Sys.getenv("OSN_KEY"),
                                   secret_key = Sys.getenv("OSN_SECRET"))

  s3_inventory$CreateDir("inventory")


  s3_inventory <- arrow::s3_bucket(config$inventory_bucket,
                         endpoint_override = endpoint_override_forecasts,
                         access_key = Sys.getenv("OSN_KEY"),
                         secret_key = Sys.getenv("OSN_SECRET"))

  inventory_df <- arrow::open_dataset(s3_inventory) |> collect()

  for(i in 1:length(submissions)){

    curr_submission <- basename(submissions[i])
    theme <-  stringr::str_split(curr_submission, "-")[[1]][1]
    submission_date <- lubridate::as_date(paste(stringr::str_split(curr_submission, "-")[[1]][2:4],
                                                collapse = "-"))

    print(curr_submission)
    print(theme)

    example <- stringr::str_detect(curr_submission, pattern = config$example_model_id)

    if((tools::file_ext(curr_submission) %in% c("gz", "csv")) & !is.na(submission_date) & !example){

      if(theme %in% themes){

        valid <- forecast_output_validator(file.path(local_dir, curr_submission))

        if(valid){

            fc <- readr::read_csv(submissions[i], show_col_types = FALSE)

            reference_datetime_format <- config$theme_datetime_format[which(themes == theme)]

            pub_datetime <- strftime(Sys.time(), format = "%Y-%m-%d %H:%M:%S", tz = "UTC")

            fc <- fc |>
              mutate(pub_datetime = pub_datetime,
                     reference_date = lubridate::as_date(reference_datetime),
                     reference_datetime = strftime(lubridate::as_datetime(reference_datetime),
                                                           format = reference_datetime_format, tz = "UTC"))
            print(head(fc))
            s3$CreateDir(paste0("parquet/", theme))
            path <- s3$path(paste0("parquet/", theme))
            fc |> write_dataset(path, format = 'parquet',
                                partitioning=c("model_id", "reference_date"))

            model_id <- fc$model_id[1]
            bucket <- config$forecasts_bucket
            reference_date <- fc$reference_date[1]
            endpoint <- config$endpoint
            curr_inventory <- fc |>
              mutate(theme = theme,
                     date = lubridate::as_date(datetime),
                     path = glue::glue("{bucket}/parquet/{theme}"),
                     endpoint =config$endpoint) |>
              distinct(theme, model_id, site_id, reference_date, variable, date, path, endpoint)

            inventory_df <- bind_rows(inventory_df, curr_inventory)

            aws.s3::put_object(submissions_bucket[i],
                              object = paste0("raw/", theme,"/",basename(submissions[i])),
                              bucket = config$forecasts_bucket,
                              region= region_forecasts,
                              base_url = AWS_S3_ENDPOINT_forecasts,
                              key = Sys.getenv("OSN_KEY"),
                              secret = Sys.getenv("OSN_SECRET"))

          if(aws.s3::object_exists(object = paste0("raw/", theme,"/",basename(submissions[i])),
                                   bucket = config$forecasts_bucket,
                                   region = region_forecasts,
                                   base_url = AWS_S3_ENDPOINT_forecasts,
                                   key = Sys.getenv("OSN_KEY"),
                                   secret = Sys.getenv("OSN_SECRET"))){

            aws.s3::delete_object(object = submissions_bucket[i],
                                  bucket = config$submissions_bucket,
                                  region=AWS_DEFAULT_REGION_submissions,
                                  base_url = AWS_S3_ENDPOINT_submissions,
                                  key = Sys.getenv("AWS_ACCESS_KEY_SUBMISSIONS"),
                                  secret = Sys.getenv("AWS_SECRET_ACCESS_KEY_SUBMISSIONS"))

          }
        } else {

          aws.s3::put_object(submissions_bucket[i],
                             object = paste0("not_in_standard/", theme,"/",basename(submissions[i])),
                             bucket = config$forecasts_bucket,
                             region = region_forecasts,
                             base_url = AWS_S3_ENDPOINT_forecasts,
                             key = Sys.getenv("OSN_KEY"),
                             secret = Sys.getenv("OSN_SECRET"))

          if(aws.s3::object_exists(object = paste0("not_in_standard/",basename(submissions[i])),
                                   bucket = config$forecasts_bucket,
                                   region = region_forecasts,
                                   base_url = AWS_S3_ENDPOINT_forecasts,
                                   key = Sys.getenv("OSN_KEY"),
                                   secret = Sys.getenv("OSN_SECRET"))){

            aws.s3::delete_object(object = submissions_bucket[i],
                                  bucket = config$submissions_bucket,
                                  region=AWS_DEFAULT_REGION_submissions,
                                  base_url = AWS_S3_ENDPOINT_submissions,
                                  key = Sys.getenv("AWS_ACCESS_KEY_SUBMISSIONS"),
                                  secret = Sys.getenv("AWS_SECRET_ACCESS_KEY_SUBMISSIONS"))

          }

        }

      } else if(!(theme %in% themes)){


        aws.s3::put_object(submissions_bucket[i],
                           object = paste0("not_in_standard/", theme,"/",basename(submissions[i])),
                           bucket = config$forecasts_bucket,
                           region = region_forecasts,
                           base_url = AWS_S3_ENDPOINT_forecasts,
                           key = Sys.getenv("OSN_KEY"),
                           secret = Sys.getenv("OSN_SECRET"))

        if(aws.s3::object_exists(object = paste0("not_in_standard/",basename(submissions[i])),
                                 bucket = config$forecasts_bucket,
                                 region = region_forecasts,
                                 base_url = AWS_S3_ENDPOINT_forecasts,
                                 key = Sys.getenv("OSN_KEY"),
                                 secret = Sys.getenv("OSN_SECRET"))){

          aws.s3::delete_object(object = submissions_bucket[i],
                                bucket = config$submissions_bucket,
                                region=AWS_DEFAULT_REGION_submissions,
                                base_url = AWS_S3_ENDPOINT_submissions,
                                key = Sys.getenv("AWS_ACCESS_KEY_SUBMISSIONS"),
                                secret = Sys.getenv("AWS_SECRET_ACCESS_KEY_SUBMISSIONS"))
        }
      }else{
        #Don't do anything because the date hasn't occur yet
      }
    }else{

      aws.s3::put_object(submissions_bucket[i],
                         object = paste0("not_in_standard/", theme,"/",basename(submissions[i])),
                         bucket = config$forecasts_bucket,
                         region = region_forecasts,
                         base_url = AWS_S3_ENDPOINT_forecasts,
                         key = Sys.getenv("OSN_KEY"),
                         secret = Sys.getenv("OSN_SECRET"))

    }
  }
  arrow::write_dataset(inventory_df, path = s3_inventory)
}



unlink(local_dir, recursive = TRUE)

message(paste0("Completed Processing Submissions ", Sys.time()))
