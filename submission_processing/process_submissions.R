library(readr)
library(dplyr)
library(arrow)
library(glue)
library(vera4castHelpers)
library(here)
library(minioclient)
library(tools)
library(fs)
library(stringr)
library(lubridate)

install_mc()

config <- yaml::read_yaml("challenge_configuration.yaml")

minioclient::mc_alias_set("osn",
             config$endpoint,
             Sys.getenv("OSN_KEY"),
             Sys.getenv("OSN_SECRET"))

minioclient::mc_alias_set("submit",
             config$submissions_endpoint,
             Sys.getenv("AWS_ACCESS_KEY_SUBMISSIONS"),
             Sys.getenv("AWS_SECRET_ACCESS_KEY_SUBMISSIONS"))

message(paste0("Starting Processing Submissions ", Sys.time()))

local_dir <- file.path(here::here(), "submissions")
unlink(local_dir, recursive = TRUE)
fs::dir_create(local_dir)

message("Downloading forecasts ...")

minioclient::mc_mirror(from = paste0("submit/",config$submissions_bucket), to = local_dir)

submissions <- fs::dir_ls(local_dir, recurse = TRUE, type = "file")
submissions_filenames <- basename(submissions)

if(length(submissions) > 0){

  Sys.unsetenv("AWS_DEFAULT_REGION")
  Sys.unsetenv("AWS_S3_ENDPOINT")
  Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")

  s3 <- arrow::s3_bucket(config$forecasts_bucket,
                         endpoint_override = config$endpoint,
                         access_key = Sys.getenv("OSN_KEY"),
                         secret_key = Sys.getenv("OSN_SECRET"))

  s3_inventory <- arrow::s3_bucket("bio230121-bucket01/vera4cast",
                                   endpoint_override = config$endpoint,
                                   access_key = Sys.getenv("OSN_KEY"),
                                   secret_key = Sys.getenv("OSN_SECRET"))

  s3_inventory$CreateDir("inventory")

  s3_inventory <- arrow::s3_bucket(paste0(config$inventory_bucket,"/catalog"),
                                   endpoint_override = config$endpoint,
                                   access_key = Sys.getenv("OSN_KEY"),
                                   secret_key = Sys.getenv("OSN_SECRET"))

  inventory_df <- arrow::open_dataset(s3_inventory) |> dplyr::collect()

  time_stamp <- format(Sys.time(), format = "%Y%m%d%H%M%S")

  for(i in 1:length(submissions)){

    curr_submission <- basename(submissions[i])
    submission_dir <- dirname(submissions[i])
    print(curr_submission)

    if((tools::file_ext(curr_submission) %in% c("gz", "csv"))){

      valid <- forecast_output_validator(file.path(local_dir, curr_submission))

      if(valid){

        fc <- readr::read_csv(submissions[i], show_col_types = FALSE)

        pub_datetime <- strftime(Sys.time(), format = "%Y-%m-%d %H:%M:%S", tz = "UTC")

        if(!"duration" %in% names(fc)){
          if(stringr::str_detect(fc$datetime[1], ":")){
            fc <- fc |> dplyr::mutate(duration = "P1H")
          }else{
            fc <- fc |> dplyr::mutate(duration = "P1D")
          }
        }

        if(!("depth_m" %in% names(fc))){
          fc <- fc |>
            mutate(depth_m = NA,
                   depth_m = as.numeric(depth_m))
        }

        fc <- fc |>
          dplyr::mutate(pub_datetime = pub_datetime,
                 reference_datetime = lubridate::as_datetime(reference_datetime),
                 reference_date = lubridate::as_date(reference_datetime))

        print(head(fc))
        s3$CreateDir(paste0("parquet/"))
        path <- s3$path(paste0("parquet/"))
        fc |> arrow::write_dataset(path, format = 'parquet',
                            partitioning = c("duration","variable","model_id", "reference_date"))

        model_id <- fc$model_id[1]
        bucket <- config$forecasts_bucket
        reference_date <- fc$reference_date[1]
        duration <- fc$duration[1]
        endpoint <- config$endpoint
        curr_inventory <- fc |>
          dplyr::mutate(project_id = "vera4cast",
                 date = lubridate::as_date(datetime),
                 path = glue::glue("{bucket}/parquet/duration={duration}/variable={variable}"),
                 endpoint = config$endpoint) |>
          dplyr::distinct(project_id, duration, model_id, site_id, reference_date, variable, date, path, endpoint)

        inventory_df <- dplyr::bind_rows(inventory_df, curr_inventory)

        submission_timestamp <- paste0(submission_dir,"/T", time_stamp, "_", basename(submissions[i]))
        fs::file_copy(submissions[i], submission_timestamp)
        raw_bucket_object <- paste0("osn/",config$forecasts_bucket,"/raw/",basename(submission_timestamp))

        minioclient::mc_cp(submission_timestamp, dirname(raw_bucket_object))

        if(length(minioclient::mc_ls(raw_bucket_object)) > 0){
          minioclient::mc_rm(file.path("submit",config$submissions_bucket,curr_submission))
        }
      } else {

        submission_timestamp <- paste0(submission_dir,"/T", time_stamp, "_", basename(submissions[i]))
        fs::file_copy(submissions[i], submission_timestamp)
        raw_bucket_object <- paste0("osn/",config$forecasts_bucket,"/raw/",basename(submission_timestamp))

        minioclient::mc_cp(submission_timestamp, dirname(raw_bucket_object))

        if(length(minioclient::mc_ls(raw_bucket_object)) > 0){
          minioclient::mc_rm(file.path("submit",config$submissions_bucket,curr_submission))
        }

      }
    }
  }

  arrow::write_dataset(inventory_df, path = s3_inventory)

  s3_inventory <- arrow::s3_bucket(paste0(config$inventory_bucket),
                                   endpoint_override = config$endpoint,
                                   access_key = Sys.getenv("OSN_KEY"),
                                   secret_key = Sys.getenv("OSN_SECRET"))

  inventory_df |> dplyr::distinct(model_id, project_id) |>
    arrow::write_csv_arrow(s3_inventory$path("model_id/model_id-project_id-inventory.csv"))

}

unlink(local_dir, recursive = TRUE)

message(paste0("Completed Processing Submissions ", Sys.time()))
