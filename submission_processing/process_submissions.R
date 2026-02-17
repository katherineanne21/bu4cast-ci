library(neon4cast) #  remotes::install_github("eco4cast/neon4cast")

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

# Read in configurations

config <- yaml::read_yaml("challenge_configuration.yaml")

sites <- readr::read_csv(config$catalog_config$site_metadata_url, show_col_types = FALSE) |>
  select(field_site_id, latitude, longitude) |>
  rename(site_id = field_site_id)

# Set up minio connections

OUR_LINK_read = 'bu4cast-ci-read'
OUR_LINK_write = 'bu4cast-ci-write'
OUR_ENDPOINT_OVERRIDE = 'https://minio-s3.apps.shift.nerc.mghpcc.org'

minioclient::mc_alias_set(OUR_LINK_read,
                          OUR_ENDPOINT_OVERRIDE,
                          Sys.getenv("OSN_KEY"),
                          Sys.getenv("OSN_SECRET"))

minioclient::mc_alias_set(OUR_LINK_write,
                          OUR_ENDPOINT_OVERRIDE,
                          Sys.getenv("OSN_KEY"),
                          Sys.getenv("OSN_SECRET"))


message(paste0("Starting Processing Submissions ", Sys.time()))

# Create a local directory

local_dir <- file.path(here::here(), "submissions")
unlink(local_dir, recursive = TRUE)
fs::dir_create(local_dir)

message("Downloading forecasts ...")

# Download write bucket to local directory 

minioclient::mc_mirror(from = paste0(OUR_LINK_write, config$submissions_bucket), to = local_dir)

submissions <- fs::dir_ls(local_dir, recurse = TRUE, type = "file") # lists all files in local_dir
submissions <- submissions[stringr::str_detect(submissions, "2023", negate = TRUE)] # filter out 2023 -> should we change this to 2024?
submissions <- submissions[stringr::str_detect(submissions, "usgsrc4cast", negate = TRUE)] # filter usgsrc4cast files out 

submissions_filenames <- basename(submissions) # grab just the filename not full path

if(length(submissions) > 0){

  # Prevent AWS connections
  
  Sys.unsetenv("AWS_DEFAULT_REGION")
  Sys.unsetenv("AWS_S3_ENDPOINT")
  Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")

  # Connect to DuckDB
  
  duckdbfs::duckdb_secrets(
                         endpoint = config$endpoint,
                         key = Sys.getenv("OSN_KEY"),
                         secret = Sys.getenv("OSN_SECRET"))

  s3 <- arrow::s3_bucket(config$forecasts_bucket,
                         endpoint_override = config$endpoint,
                         access_key = Sys.getenv("OSN_KEY"),
                         secret_key = Sys.getenv("OSN_SECRET"))

  time_stamp <- format(Sys.time(), format = "%Y%m%d%H%M%S")

  for(i in 1:length(submissions)){

    curr_submission <- basename(submissions[i])
    theme <-  stringr::str_split(curr_submission, "-")[[1]][1]
    file_name_model_id <-  stringr::str_split(tools::file_path_sans_ext(tools::file_path_sans_ext(curr_submission)), "-")[[1]][5]
    file_name_reference_datetime <- lubridate::as_datetime(paste0(stringr::str_split(curr_submission, "-")[[1]][2:4], collapse = "-"))
    submission_dir <- dirname(submissions[i])
    print(curr_submission)

    # not_tg <- stringr::str_detect(curr_submission, "tg", negate = TRUE)
    not_tg <- TRUE
    recent_date <- file_name_reference_datetime > lubridate::as_date("2023-12-31") #(Sys.Date() - lubridate::days(30))

    if((tools::file_ext(curr_submission) %in% c("gz", "csv", "nc")) & not_tg & recent_date & !is.na(file_name_reference_datetime)){

      valid <- forecast_output_validator(file.path(local_dir, curr_submission))

      if(valid){

        fc <- read4cast::read_forecast(submissions[i])

        pub_datetime <- strftime(Sys.time(), format = "%Y-%m-%d %H:%M:%S", tz = "UTC")

        if(!"duration" %in% names(fc)){
          if(theme == "terrestrial_30min"){
            fc <- fc |> dplyr::mutate(duration = "PT30M")
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

        fc <- fc |>
          mutate(duration = ifelse(duration == "PT30", "PT30M", duration))

        # FILTER HORIZONS LONGER THAN ALLOWED

        fc <- fc |>
          mutate(horizon = as.integer(as.POSIXct(datetime) - as.POSIXct(reference_datetime))/ (60*60*24),
                 max_horizon = ifelse(variable %in% c("amblyomma_americanum", "richness", "abundance"), 720, 35)) |>
          filter(horizon <= max_horizon) |>
          select(-horizon, -max_horizon)

        fc <- fc |>
          mutate(family = ifelse(family == "ensemble", "sample", family))

        if(!("model_id" %in% colnames(fc))){
          fc <- fc |> mutate(model_id = file_name_model_id)
        }else if(fc$model_id[1] == "null"){
          fc <- fc |> mutate(model_id = file_name_model_id)
        }

        if(!("reference_datetime" %in% colnames(fc))){
          fc <- fc |> mutate(reference_datetime = file_name_reference_datetime)
        }

        fc <- fc |>
          dplyr::mutate(pub_datetime = lubridate::as_datetime(pub_datetime),
                        datetime = lubridate::as_datetime(datetime),
                        reference_datetime = lubridate::as_datetime(reference_datetime),
                        reference_date = lubridate::as_date(reference_datetime),
                        parameter = as.character(parameter),
                        project_id = "neon4cast") |>
          dplyr::filter(datetime >= reference_datetime)

        print(head(fc))
        s3$CreateDir(paste0("parquet/"))

        ## arrow write has gone nuts... let's update
        fc |> duckdbfs::write_dataset(paste0("s3://", config$forecasts_bucket, "/parquet"),
                                      format = 'parquet',
                                      partitioning = c("project_id",
                                                    "duration",
                                                    "variable",
                                                    "model_id",
                                                    "reference_date"),
                                      options = list("PER_THREAD_OUTPUT false"))
        print("creating summaries")

        s3$CreateDir(paste0("summaries"))
        fc |>
          dplyr::summarise(prediction = mean(prediction), .by = dplyr::any_of(c("site_id", "datetime", "reference_datetime", "family", "duration", "model_id",
                                                                                "parameter", "pub_datetime", "reference_date", "variable", "project_id"))) |>
          score4cast::summarize_forecast(extra_groups = c("duration", "project_id")) |>
          dplyr::mutate(reference_date = lubridate::as_date(reference_datetime)) |>
          duckdbfs::write_dataset(paste0("s3://", config$forecasts_bucket, "/summaries"), format = 'parquet',
                               partitioning = c("project_id",
                                                "duration",
                                                "variable",
                                                "model_id",
                                                "reference_date"),
                                 options = list("PER_THREAD_OUTPUT false"))

        submission_timestamp <- paste0(submission_dir,"/T", time_stamp, "_", basename(submissions[i]))
        fs::file_copy(submissions[i], submission_timestamp)
        raw_bucket_object <- paste0("s3_store/",config$forecasts_bucket,"/raw/",basename(submission_timestamp))

        minioclient::mc_cp(submission_timestamp, paste0(dirname(raw_bucket_object),"/", basename(submission_timestamp)))

        if(length(minioclient::mc_ls(raw_bucket_object)) > 0){
          minioclient::mc_rm(file.path("submit",config$submissions_bucket,curr_submission))
        }

        print("finishing submission processing")

        rm(fc)
        gc()

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

}

unlink(local_dir, recursive = TRUE)

message(paste0("Completed Processing Submissions ", Sys.time()))
