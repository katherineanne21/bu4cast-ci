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
s3_inv <- arrow::s3_bucket(config$inventory_bucket, endpoint_override = endpoint)

for(i in 1:length(config$themes)){

  theme <- config$themes[i]

  message(paste("starting theme: ", config$themes[i]))

  local_prov <- paste0(theme,"-scoring_provenance.csv")

  if (!(local_prov %in% s3_prov$ls())) {
    arrow::write_csv_arrow(dplyr::tibble(prov = NA), local_prov)
  }else{
  path <- s3_prov$path(paste0(local_prov))
  prov <- arrow::read_csv_arrow(path)
  arrow::write_csv_arrow(prov, local_prov)
  }

  prov_df <- readr::read_csv(local_prov, col_types = "ic")

  s3_scores_path <- s3_scores$path(glue::glue("parquet/{theme}",
                                              theme = theme))
  bucket <- config$forecasts_bucket
  target <- score4cast:::get_target(theme, s3_targets)

  inventory <- arrow::open_dataset(s3_inv) |> dplyr::filter(theme == {theme}) |> dplyr::collect() |> dplyr::distinct(model_id, date, path, endpoint)

  for(j in 1:nrow(inventory)){

    ref <- inventory$date[j]

   # NOTE: we cannot 'prefilter' grouping by prov, since once we have tg
   # we want to use it to score, not access it twice...
   tg <- target |>
     dplyr::filter(datetime >= ref, datetime < ref+lubridate::days(1))

   id <- rlang::hash(list(inventory[j, c("model_id", "date")],  tg))
   new_id <- rlang::hash(list(inventory[j, c("model_id", "date")],  tg))

   if (!(score4cast:::prov_has(id, prov_df, "new_id")))
   {
     fc <-  arrow::open_dataset(paste0("s3://anonymous@",inventory$path[j],"/model_id=",inventory$model_id[j],"?endpoint_override=",inventory$endpoint[j])) |>
       dplyr::mutate(date = as.Date(datetime)) |>
       dplyr::filter(date == inventory$date[j]) |>
       dplyr::collect()

     fc |>
       score4cast::crps_logs_score(tg) |>
       dplyr::mutate(date = inventory$date[j],
              model_id = inventory$model_id[j]) |>
       arrow::write_dataset(s3_scores_path,
                            partitioning = c("model_id", "date"))
     new_prov <- dplyr::tibble(prov = NA_integer_, new_id = id)
     prov_df <- dplyr::bind_rows(prov_df, new_prov)
   }
  }

  #prov <- arrow::open_dataset(local_prov, format = "csv",)
  #path <- s3_prov$path(local_prov)
  prov <- arrow::write_csv_arrow(prov_df, s3_prov$path(paste0(theme, "-", local_prov)))

  #message(paste(config$themes[i]," done in", time[["real"]]))
}
