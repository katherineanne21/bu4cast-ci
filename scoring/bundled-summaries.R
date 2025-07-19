remotes::install_github("cboettig/duckdbfs", upgrade=FALSE)


library(dplyr)
library(duckdbfs)
library(minioclient)
library(bench)
library(glue)
library(fs)

install_mc()
mc_alias_set("osn", "sdsc.osn.xsede.org", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))
duckdb_secrets(endpoint = "sdsc.osn.xsede.org", key = Sys.getenv("OSN_KEY"), secret = Sys.getenv("OSN_SECRET"), bucket = "bio230014-bucket01")


s3_summaries <- "s3://bio230014-bucket01/challenges/forecasts/summaries/project_id=neon4cast/"
existing_bundles <- "s3://bio230014-bucket01/challenges/forecasts/bundled-summaries/project_id=neon4cast/"


## how many currently bundled rows of data:
open_dataset(existing_bundles) |>   count()


grouping <- c("model_id", "reference_datetime", "site_id",
              "datetime", "family", "variable", "duration", "project_id")
bench::bench_time({
  bundled_summaries <- open_dataset(existing_bundles)
  new_summaries <- open_dataset(s3_summaries)
  union(bundled_summaries, new_summaries) |>
    filter(!is.na(model_id)) |>  ## model_id CANNOT BE NA!
    write_dataset("tmp.parquet")

# because we have written to tmp, we can stream back up to bundled-summaries
# Ensures partitions are written as a single shard
  open_dataset("tmp.parquet") |>
      group_by(across(any_of(grouping))) |>
      slice_max(pub_datetime) |>
      distinct() |>
      write_dataset("s3://bio230014-bucket01/challenges/forecasts/bundled-summaries/project_id=neon4cast",
                    partitioning = c("duration", 'variable', "model_id"),
                    options = list("PER_THREAD_OUTPUT false"))

})



archive_older <- function(remote_path = "osn/bio230014-bucket01/challenges/forecasts/summaries/project_id=neon4cast/",
                          keep_months = 0.25) {
  cutoff <- lubridate::dmonths(keep_months)
  contents <- mc_ls(remote_path, recursive = TRUE, details = TRUE)
  all_fc_files <- contents |> filter(!is_folder) |> pull(path)

  dates <- all_fc_files |>
    stringr::str_extract("reference_date=(\\d{4}-\\d{2}-\\d{2})", 1)  |>
    as.Date()
  drop <- dates < Sys.Date() - cutoff
  drop_paths <- all_fc_files[drop] |> na.omit()
  return(fs::path(drop_paths))
}

drop_f <- function(path,
                   from_pattern = "forecasts\\/summaries",
                   dest_pattern = "forecasts/archive-summaries") {
  if( grepl(".parquet$", path) ){
    mc_mv(path, gsub(from_pattern, dest_pattern, path))
    mc_rm(dirname(path), recursive = TRUE)
  } else {
    mc_rm(path)
  }
  invisible("success")
}

drop_paths <- archive_older("osn/bio230014-bucket01/challenges/forecasts/summaries/project_id=neon4cast/")
out <- parallel::mclapply(drop_paths, drop_f, from_pattern = "forecasts\\/summaries", dest_pattern =  "forecasts/archive-summaries", mc.cores = 12)

