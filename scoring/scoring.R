library(score4cast)
library(arrow)

past_days <- 365
n_cores <- 2

Sys.setenv(AWS_ACCESS_KEY_ID=Sys.getenv("OSN_KEY"),
           AWS_SECRET_ACCESS_KEY=Sys.getenv("OSN_SECRET"))

ignore_sigpipe()

config <- yaml::read_yaml("challenge_configuration.yaml")

endpoint <- config$endpoint

s3 <- arrow::s3_bucket("bio230121-bucket01/vera4cast",
                       endpoint_override = endpoint,
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))

s3$CreateDir("inventory")
s3$CreateDir("prov")
s3$CreateDir("scores")

Sys.setenv("AWS_EC2_METADATA_DISABLED"="TRUE")
Sys.unsetenv("AWS_DEFAULT_REGION")

s3_inv <- arrow::s3_bucket(paste0(config$inventory_bucket,"/catalog"), endpoint_override = endpoint)

variable_duration <- arrow::open_dataset(s3_inv) |>
  dplyr::distinct(variable, duration) |>
  dplyr::collect()

future::plan("future::multisession", workers = n_cores)

furrr::future_walk(1:nrow(variable_duration), function(k, variable_duration, config, endpoint){

  Sys.setenv(AWS_ACCESS_KEY_ID=Sys.getenv("OSN_KEY"),
             AWS_SECRET_ACCESS_KEY=Sys.getenv("OSN_SECRET"))

  variable <- variable_duration$variable[k]
  duration <- variable_duration$duration[k]

  s3_forecasts <- arrow::s3_bucket(config$forecasts_bucket, endpoint_override = endpoint)
  s3_targets <- arrow::s3_bucket(config$targets_bucket, endpoint_override = endpoint)
  s3_scores <- arrow::s3_bucket(config$scores_bucket, endpoint_override = endpoint)
  s3_prov <- arrow::s3_bucket(config$prov_bucket, endpoint_override = endpoint)
  s3_inv <- arrow::s3_bucket(paste0(config$inventory_bucket,"/catalog"), endpoint_override = endpoint)

  print(variable_duration[k,])

  local_prov <- paste0(duration,"-",variable, "-scoring_provenance.csv")

  if (!(local_prov %in% s3_prov$ls())) {
    arrow::write_csv_arrow(dplyr::tibble(new_id = "start") |> dplyr::mutate(new_id = as.character(new_id)), local_prov)
  }else{
    path <- s3_prov$path(paste0(local_prov))
    prov <- arrow::read_csv_arrow(path)
    arrow::write_csv_arrow(prov, local_prov)
  }

  prov_df <- readr::read_csv(local_prov, col_types = "c")

  s3_scores_path <- s3_scores$path(glue::glue("parquet/duration={duration}/variable={variable}"))

  urls <- c("https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/targets/duration=P1D/daily-insitu-targets.csv.gz",
           "https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/targets/duration=P1D/daily-inflow-targets.csv.gz",
           "https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/targets/duration=P1D/daily-met-targets.csv.gz",
           "https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/targets/duration=PT1H/hourly-met-targets.csv.gz")

  target_files <- tibble(url = urls) |>
    filter(stringr::str_detect(url, glue::glue("{duration}"), negate = TRUE)) |>
    pull(url)

  target <- readr::read_csv(target_files, show_col_types = FALSE) |>
    dplyr::filter(variable == variable_duration$variable[k] & duration == variable_duration$duration[k])

  curr_variable <- variable
  curr_duration <- duration

  groupings <- arrow::open_dataset(s3_inv) |>
    dplyr::filter(variable == curr_variable, duration == curr_duration) |>
    dplyr::select(-site_id) |>
    dplyr::collect() |>
    dplyr::distinct() |>
    dplyr::filter(date > Sys.Date() - lubridate::days(past_days)) |>
  dplyr::group_by(model_id, date, duration, path, endpoint) |>
    dplyr::summarise(reference_date =
                       paste(reference_date, collapse=","),
                     .groups = "drop")


  new_prov <- purrr::map_dfr(1:nrow(groupings), function(j, groupings, prov_df, s3_scores_path, curr_variable){

    group <- groupings[j,]
    ref <- group$date

    tg <- target |>
      dplyr::filter(lubridate::as_date(datetime) >= ref,
                    lubridate::as_date(datetime) < ref+lubridate::days(1))

    id <- rlang::hash(list(group[, c("model_id","reference_date","date","duration")],  tg))

    print(j)
    if (!(score4cast:::prov_has(id, prov_df, "new_id"))){
      print(j)

      reference_dates <- unlist(stringr::str_split(group$reference_date, ","))

      fc <-  arrow::open_dataset(paste0("s3://anonymous@",group$path,"/model_id=",group$model_id,"?endpoint_override=",group$endpoint)) |>
        dplyr::filter(reference_date %in% reference_dates) |>
        dplyr::collect() |>
        dplyr::filter(lubridate::as_date(datetime) >= ref,
                      lubridate::as_date(datetime) < ref+lubridate::days(1))

      fc |>
        dplyr::mutate(variable = curr_variable) |>
        score4cast::crps_logs_score(tg, extra_groups = c("depth_m")) |>
        dplyr::mutate(date = group$date,
                      model_id = group$model_id) |>
        dplyr::select(-variable) |>
        arrow::write_dataset(s3_scores_path,
                             partitioning = c("model_id", "date"))

      curr_prov <- dplyr::tibble(new_id = id)
    }else{
      curr_prov <- NULL
    }
  },
  groupings, prov_df, s3_scores_path,curr_variable
  )

  prov_df <- dplyr::bind_rows(prov_df, new_prov)
  arrow::write_csv_arrow(prov_df, s3_prov$path(local_prov))

  #message(paste(config$themes[i]," done in", time[["real"]]))
},
variable_duration,  config, endpoint
)
