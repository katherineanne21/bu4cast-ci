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

bucket <- arrow::s3_bucket(config$forecasts_bucket,
                           endpoint_override = endpoint,
                           anonymous = TRUE)

inventory <- arrow::s3_bucket(config$inventory_bucket,
                              endpoint_override = endpoint,
                              access_key = Sys.getenv("OSN_KEY"),
                              secret_key = Sys.getenv("OSN_SECRET"))


#score4cast:::update_s3_inventory(bucket, inventory)
paths <- bucket$ls(recursive = TRUE)
full_path <- stringi::stri_detect_fixed(paths, ".")


parts <- stringi::stri_split(paths[full_path], fixed = "/",
                             simplify = TRUE)
parts <- tibble::as_tibble(parts, .name_repair = "universal")
dest <- inventory$path(bucket$base_path)
arrow::write_dataset(parts, dest)

Sys.setenv("AWS_EC2_METADATA_DISABLED"="TRUE")
Sys.unsetenv("AWS_DEFAULT_REGION")
options(mc.cores=4L)

s3_forecasts <- arrow::s3_bucket(config$forecasts_bucket, endpoint_override = endpoint)
s3_targets <- arrow::s3_bucket(config$targets_bucket, endpoint_override = endpoint)
s3_scores <- arrow::s3_bucket(config$scores_bucket, endpoint_override = endpoint)
s3_prov <- arrow::s3_bucket(config$prov_bucket, endpoint_override = endpoint)

s3_inv <- arrow::s3_bucket(config$inventory_bucket, endpoint_override = endpoint)

for(i in 1:length(config$themes)){
  message(paste("starting theme: ", config$themes[i]))

  #score_theme(config$themes[i], s3_forecasts, s3_targets, s3_scores, s3_prov, s3_inv = s3_inv)

  #score4cast:::prov_download(s3_prov, local_prov)
  local_prov <- "scoring_provenance.csv"
  if (!(local_prov %in% s3_prov$ls())) {
    arrow::write_csv_arrow(dplyr::tibble(prov = NA), local_prov)
  }else{
    path <- s3_prov$path(local_prov)
    prov <- arrow::read_csv_arrow(path)
    arrow::write_csv_arrow(prov, local_prov)
  }



  prov_df <- readr::read_csv(local_prov, col_types = "cc")

  s3_scores_path <- s3_scores$path(glue::glue("parquet/{theme}",
                                              theme = theme))
  bucket <- config$forecasts_bucket
  target <- score4cast:::get_target(theme, s3_targets)

  # grouping <- score4cast:::get_grouping(s3_inv, theme, collapse = TRUE, endpoint = endpoint)

  groups <- arrow::open_dataset(s3_inv$path(config$forecasts_bucket)) |>
    dplyr::filter(...1 == "parquet", ...2 == {theme}) |>
    dplyr::select(model_id = ...3, reference_date = ...4) |>
    dplyr::mutate(model_id = gsub("model_id=", "", model_id), reference_date = gsub("reference_date=",
                                                                                   "", reference_date)) |>
    dplyr::collect()


   grouping <- groups

   group <- grouping[i,]
   ref <- lubridate::as_date(group$reference_date)

   # NOTE: we cannot 'prefilter' grouping by prov, since once we have tg
   # we want to use it to score, not access it twice...
   tg <- target |>
     filter(datetime >= ref, datetime < ref+lubridate::days(1))

   id <- rlang::hash(list(grouping[i, c("model_id", "date")],  tg))
   new_id <- rlang::hash(list(group,  tg))

   if ( !(prov_has(id, prov_df, "prov") ||
          prov_has(new_id, prov_df, "new_id")) )
   {
     fc <- get_fcst_arrow(endpoint, bucket, theme, group)
     fc |>
       filter(!is.na(family)) |> #hhhmmmm? what should we be doing about these forecasts?
       crps_logs_score(tg) |>
       mutate(date = group$date) |>
       arrow::write_dataset(s3_scores_path,
                            partitioning = c("model_id", "date"))
     prov_add(new_id, local_prov)
   }



  score4cast:::prov_upload(s3_prov, local_prov)



  message(paste(config$themes[i]," done in", time[["real"]]))
}
