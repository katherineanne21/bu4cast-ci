library(tidyverse)
library(score4cast)
library(arrow)
library(googlesheets4)
library(glue)
library(ver4castHelpers)

config <- yaml::read_yaml("challenge_configuration.yaml")

AWS_DEFAULT_REGION_submissions <- stringr::str_split_fixed(config$submissions_endpoint,"\\.", 2)[,1]
region_submissions <- stringr::str_split_fixed(config$submissions_endpoint,"\\.", 2)[,1]
AWS_S3_ENDPOINT_submissions <- stringr::str_split_fixed(config$submissions_endpoint,"\\.", 2)[,2]

AWS_DEFAULT_REGION_forecasts <- stringr::str_split_fixed(config$endpoint,"\\.", 2)[,1]
region_forecasts <- stringr::str_split_fixed(config$endpoint,"\\.", 2)[,1]
AWS_S3_ENDPOINT_forecasts <- stringr::str_split_fixed(config$submissions,"\\.", 2)[,2]
endpoint_override_forecasts <- config$endpoint


message(paste0("Starting Processing Submissions ", Sys.time()))

local_dir <- file.path(here::here(), "submissions")
unlink(local_dir, recursive = TRUE)
fs::dir_create(local_dir)

# cannot  set region="" using environmental variables!!

Sys.setenv("AWS_DEFAULT_REGION" = AWS_DEFAULT_REGION_submissions,
           "AWS_S3_ENDPOINT" = AWS_S3_ENDPOINT_submissions)

Sys.unsetenv("AWS_SECRET_ACCESS_KEY")

message("Downloading forecasts ...")

## Note: s3sync stupidly also requires auth credentials even to download from public bucket

aws.s3::s3sync(local_dir, bucket = config$submissions_bucket,  direction= "download", verbose = FALSE, region = region_submissions)

submissions <- fs::dir_ls(local_dir, recurse = TRUE, type = "file")
submissions_bucket <- basename(submissions)

themes <- config$themes

if(length(submissions) > 0){

  Sys.unsetenv("AWS_DEFAULT_REGION")
  Sys.unsetenv("AWS_S3_ENDPOINT")
  Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")
  s3 <- arrow::s3_bucket(config$forecasts_bucket, endpoint_override = endpoint_override_forecasts)

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

            fc <- readr::read_csv(submissions[i])

            reference_datetime_format <- config$theme_datetime_format[which(themes == theme)]

            pubDate <- strftime(Sys.time(), format = "%Y-%m-%d %H:%M:%S", tz = "UTC")

            df <- mutate(fc, reference_datetime = strftime(lubridate::as_datetime(reference_datetime),
                                                           format = reference_datetime_format, tz = "UTC"))

            fc <- fc |> dplyr::mutate(date = lubridate::as_date(datetime),
                                      pubDate = pubDate)
            print(head(fc))
            path <- s3$path(paste0("parquet/", theme))
            fc |> write_dataset(path, format = 'parquet',
                                partitioning=c("model_id", "reference_datetime", "date"))



          aws.s3::put_object(submissions_bucket[i],
                              object = paste0("raw/", theme,"/",basename(submissions[i])),
                              bucket = config$forecasts_bucket,
                              region=region_forecasts)

          if(aws.s3::object_exists(object = paste0("raw/", theme,"/",basename(submissions[i])), bucket = config$forecasts_bucket, region = region_forecasts)){

            Sys.setenv("AWS_DEFAULT_REGION" = AWS_DEFAULT_REGION_submissions,
                       "AWS_S3_ENDPOINT" = AWS_S3_ENDPOINT_submissions)
            aws.s3::delete_object(object = submissions_bucket[i], bucket = config$submissions_bucket, region=region_forecasts)

          }
        } else {
          Sys.setenv("AWS_DEFAULT_REGION" = AWS_DEFAULT_REGION_forecasts,
                     "AWS_S3_ENDPOINT" = AWS_S3_ENDPOINT_forecasts)

          aws.s3::put_object(submissions_bucket[i],
                             object = paste0("not_in_standard/", theme,"/",basename(submissions[i])),
                             bucket = config$forecasts_bucket,
                             region=region_forecasts)
          if(aws.s3::object_exists(object = paste0("not_in_standard/",basename(submissions[i])), bucket = config$forecasts_bucket, region = region_forecasts)){

            Sys.setenv("AWS_DEFAULT_REGION" = AWS_DEFAULT_REGION_submissions,
                       "AWS_S3_ENDPOINT" = AWS_S3_ENDPOINT_submissions)

            aws.s3::delete_object(object = submissions_bucket[i], bucket = config$submissions_bucket, region=region_submissions)

          }
        }
      } else if(!(theme %in% themes)){
        Sys.setenv("AWS_DEFAULT_REGION" = AWS_DEFAULT_REGION_forecasts,
                   "AWS_S3_ENDPOINT" = AWS_S3_ENDPOINT_forecasts)

        aws.s3::put_object(submissions_bucket[i],
                           object = paste0("not_in_standard/", theme,"/",basename(submissions[i])),
                           bucket = config$forecasts_bucket,
                           region=region_forecasts)

        if(aws.s3::object_exists(object = paste0("not_in_standard/",basename(submissions[i])), bucket = config$forecasts_bucket, region = region_forecasts)){


          Sys.setenv("AWS_DEFAULT_REGION" = AWS_DEFAULT_REGION_submissions,
                     "AWS_S3_ENDPOINT" = AWS_S3_ENDPOINT_submissions)

          aws.s3::delete_object(object = submissions_bucket[i], bucket = config$submissions_bucket, region=region_submissions)

        }
      }else{
        #Don't do anything because the date hasn't occur yet
      }
    }else{
      Sys.setenv("AWS_DEFAULT_REGION" = AWS_DEFAULT_REGION_forecasts,
                 "AWS_S3_ENDPOINT" = AWS_S3_ENDPOINT_forecasts)

      aws.s3::put_object(submissions_bucket[i],
                         object = paste0("not_in_standard/", theme,"/",basename(submissions[i])),
                         bucket = config$forecasts_bucket,
                         region=region_forecasts)

    }
  }
}

unlink(local_dir, recursive = TRUE)

message(paste0("Completed Processing Submissions ", Sys.time()))
