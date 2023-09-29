library(score4cast)
library(arrow)
library(bench)

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
options(mc.cores=4L)

s3_forecasts <- arrow::s3_bucket(config$forecasts_bucket, endpoint_override = endpoint)
s3_targets <- arrow::s3_bucket(config$targets_bucket, endpoint_override = endpoint)
s3_scores <- arrow::s3_bucket(config$scores_bucket, endpoint_override = endpoint)
s3_prov <- arrow::s3_bucket(config$prov_bucket, endpoint_override = endpoint)
s3_inv <- arrow::s3_bucket(paste0(config$inventory_bucket,"/catalog"), endpoint_override = endpoint)

variable_duration <- arrow::open_dataset(s3_inv) |>
  dplyr::distinct(variable, duration) |>
  dplyr::collect()

for(k in 1:nrow(variable_duration)){

  variable <- variable_duration$variable[k]
  duration <- variable_duration$duration[k]

  print(variable_duration[k,])

  local_prov <- paste0(duration,"-",variable, "-scoring_provenance.csv")

  if (!(local_prov %in% s3_prov$ls())) {
    arrow::write_csv_arrow(dplyr::tibble(prov = NA, new_id = NA), local_prov)
  }else{
    path <- s3_prov$path(paste0(local_prov))
    prov <- arrow::read_csv_arrow(path)
    arrow::write_csv_arrow(prov, local_prov)
  }

  prov_df <- readr::read_csv(local_prov, col_types = "ic")

  s3_scores_path <- s3_scores$path(glue::glue("parquet/duration={duration}/variable={variable}"))
  bucket <- config$forecasts_bucket
  target <- readr::read_csv(glue::glue("https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/targets/duration={duration}/{duration}-targets.csv.gz"), show_col_types = FALSE) |>
    dplyr::filter(variable == variable_duration$variable[k] & duration == variable_duration$duration[k])

  inventory <- arrow::open_dataset(s3_inv) |>
    dplyr::filter(variable == variable_duration$variable[k] & duration == variable_duration$duration[k]) |>
    dplyr::collect() |>
    dplyr::distinct(model_id, date, path, endpoint)

  new_prov <- purrr::map_dfr(1:nrow(inventory), function(j, inventory, prov_df, s3_scores_path, variable ,duration){

    ref <- inventory$date[j]

    # NOTE: we cannot 'prefilter' grouping by prov, since once we have tg
    # we want to use it to score, not access it twice...
    tg <- target |>
      dplyr::filter(lubridate::as_date(datetime) >= ref, lubridate::as_date(datetime) < ref+lubridate::days(1))

    id <- rlang::hash(list(inventory[j, c("model_id", "date")],  tg))
    new_id <- rlang::hash(list(inventory[j, c("model_id", "date")],  tg))

    if (!(score4cast:::prov_has(id, prov_df, "new_id"))){

      fc <-  arrow::open_dataset(paste0("s3://anonymous@",inventory$path[j],"/model_id=",inventory$model_id[j],"?endpoint_override=",inventory$endpoint[j])) |>
        dplyr::mutate(date = as.Date(datetime)) |>
        dplyr::filter(date == inventory$date[j]) |>
        dplyr::collect()

      fc |>
        dplyr::mutate(variable = variable) |>
        score4cast::crps_logs_score(tg, extra_groups = c("depth_m")) |>
        dplyr::mutate(date = inventory$date[j],
                      model_id = inventory$model_id[j]) |>
        dplyr::select(-variable) |>
        arrow::write_dataset(s3_scores_path,
                             partitioning = c("model_id", "date"))
      new_prov <- dplyr::tibble(prov = NA_integer_, new_id = id)
    }else{
      new_prov <- NULL
    }
  },
  inventory, prov_df, s3_scores_path, variable, duration
  )

  prov_df <- dplyr::bind_rows(prov_df, new_prov)
  prov <- arrow::write_csv_arrow(prov_df, s3_prov$path(local_prov))

  #message(paste(config$themes[i]," done in", time[["real"]]))
}
