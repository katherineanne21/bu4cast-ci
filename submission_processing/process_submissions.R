library(neon4cast) #project_specific


library(readr)
library(dplyr)
library(arrow)
library(glue)
library(here)
library(minioclient)
library(tools)
library(fs)
library(stringr)
library(lubridate)

install_mc()

config <- yaml::read_yaml("challenge_configuration.yaml")

minioclient::mc_alias_set("s3_store",
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

  s3_inventory <- arrow::s3_bucket(dirname(config$inventory_bucket),
                                   endpoint_override = config$endpoint,
                                   access_key = Sys.getenv("OSN_KEY"),
                                   secret_key = Sys.getenv("OSN_SECRET"))

  s3_inventory$CreateDir("inventory/catalog")

  s3_inventory <- arrow::s3_bucket(paste0(config$inventory_bucket,"/catalog"),
                                   endpoint_override = config$endpoint,
                                   access_key = Sys.getenv("OSN_KEY"),
                                   secret_key = Sys.getenv("OSN_SECRET"))

  inventory_df <- arrow::open_dataset(s3_inventory) |> dplyr::collect()

  time_stamp <- format(Sys.time(), format = "%Y%m%d%H%M%S")

  for(i in 1:length(submissions)){

    curr_submission <- basename(submissions[i])
    theme <-  stringr::str_split(curr_submission, "-")[[1]][1]
    model_id <-  stringr::str_split(tools::file_path_sans_ext(tools::file_path_sans_ext(curr_submission)), "-")[[1]][5]
    reference_datetime <- lubridate::as_datetime(paste0(stringr::str_split(curr_submission, "-")[[1]][2:4], collapse = "-"))
    submission_dir <- dirname(submissions[i])
    print(curr_submission)

    if((tools::file_ext(curr_submission) %in% c("gz", "csv", "nc"))){

      valid <- forecast_output_validator(file.path(local_dir, curr_submission))

      if(valid){

        fc <- read4cast::read_forecast(submissions[i])

        pub_datetime <- strftime(Sys.time(), format = "%Y-%m-%d %H:%M:%S", tz = "UTC")

        if(!"duration" %in% names(fc)){
          if(theme == "terrestrial_30min"){
            fc <- fc |> dplyr::mutate(duration = "P1H")
          }else if(theme %in% c("ticks","beetles")){
            fc <- fc |> dplyr::mutate(duration = "P1W")
          }else if(theme %in% c("aquatics","phenology","terrestrial_daily")){
            fc <- fc |> dplyr::mutate(duration = "P1D")
          }else{
            if(stringr::str_detect(fc$datetime[1], ":")){
              fc <- fc |> dplyr::mutate(duration = "P1H")
            }else{
              fc <- fc |> dplyr::mutate(duration = "P1D")
            }
          }
        }

        if(!("model_id" %in% colnames(fc))){
          fc <- fc |> mutate(model_id = model_id)
        }

        if(!("reference_datetime" %in% colnames(fc))){
          fc <- fc |> mutate(reference_datetime = reference_datetime)
        }

        fc <- fc |>
          dplyr::mutate(pub_datetime = lubridate::as_datetime(pub_datetime),
                        datetime = lubridate::as_datetime(datetime),
                        reference_datetime = lubridate::as_datetime(reference_datetime),
                        reference_date = lubridate::as_date(reference_datetime),
                        parameter = as.character(parameter),
                        project_id = "neon4cast")

        print(head(fc))
        s3$CreateDir(paste0("parquet/"))
        path <- s3$path(paste0("parquet/"))
        fc |> arrow::write_dataset(path, format = 'parquet',
                                   partitioning = c("project_id",
                                                    "duration",
                                                    "variable",
                                                    "model_id",
                                                    "reference_date"))

        bucket <- config$forecasts_bucket
        curr_inventory <- fc |>
          dplyr::mutate(date = lubridate::as_date(datetime),
                        path = glue::glue("{bucket}/parquet/project_id={project_id}/duration={duration}/variable={variable}"),
                        endpoint = config$endpoint) |>
          dplyr::distinct(project_id, duration, model_id, site_id, reference_date, variable, date, path, endpoint)

        inventory_df <- dplyr::bind_rows(inventory_df, curr_inventory)

        arrow::write_dataset(inventory_df, path = s3_inventory)

        submission_timestamp <- paste0(submission_dir,"/T", time_stamp, "_", basename(submissions[i]))
        fs::file_copy(submissions[i], submission_timestamp)
        raw_bucket_object <- paste0("s3_store/",config$forecasts_bucket,"/raw/",basename(submission_timestamp))

        minioclient::mc_cp(submission_timestamp, paste0(dirname(raw_bucket_object),"/", basename(submission_timestamp)))

        if(length(minioclient::mc_ls(raw_bucket_object)) > 0){
          minioclient::mc_rm(file.path("submit",config$submissions_bucket,curr_submission))
        }
      } else {

        submission_timestamp <- paste0(submission_dir,"/T", time_stamp, "_", basename(submissions[i]))
        fs::file_copy(submissions[i], submission_timestamp)
        raw_bucket_object <- paste0("s3_store/",config$forecasts_bucket,"/raw/",basename(submission_timestamp))

        minioclient::mc_cp(submission_timestamp, paste0(dirname(raw_bucket_object),"/", basename(submission_timestamp)))

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
