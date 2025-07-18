
remotes::install_github("cboettig/duckdbfs", upgrade=FALSE)




library(tidyverse)
library(duckdbfs)
library(minioclient)
library(bench)
library(glue)
library(fs)

install_mc()
mc_alias_set("osn", "sdsc.osn.xsede.org", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))
# mc_alias_set("nrp", "s3-west.nrp-nautilus.io", Sys.getenv("EFI_NRP_KEY"), Sys.getenv("EFI_NRP_SECRET"))

duckdb_secrets(endpoint = "sdsc.osn.xsede.org", key = Sys.getenv("OSN_KEY"), secret = Sys.getenv("OSN_SECRET"), bucket = "bio230014-bucket01")


open_dataset("s3://bio230014-bucket01/challenges/forecasts/bundled-parquet",
             s3_endpoint = "sdsc.osn.xsede.org",
             anonymous = TRUE) |>
  count()

# make sure new-forecasts location exists and is empty.
fs::dir_create("new-forecasts"); fs::dir_delete("new-forecasts")
fs::dir_create("new-forecasts/bundled-parquet")


## Stream data, full mirror not needed!
#bench::bench_time({
#  mc_mirror("osn/bio230014-bucket01/challenges/forecasts/parquet/", "forecasts/parquet/", overwrite = TRUE, remove=TRUE)
#  mc_mirror("osn/bio230014-bucket01/challenges/forecasts/bundled-parquet/", "forecasts/bundled-parquet/", overwrite = TRUE, remove=TRUE)
#})

remote_path <- "osn/bio230014-bucket01/challenges/forecasts/parquet/project_id=neon4cast/"
remote_bundles <- "osn/bio230014-bucket01/challenges/forecasts/bundled-parquet/project_id=neon4cast/"



contents <- mc_ls(remote_path, recursive = TRUE, details = TRUE)
data_paths <- contents |> filter(!is_folder) |> pull(path)

# model paths are paths with at least one reference_datetime containing data files
model_paths <-
  data_paths |>
  str_replace_all("reference_date=\\d{4}-\\d{2}-\\d{2}/.*", "") |>
  str_replace("^osn\\/", "s3://") |>
  unique()




bundle_me <- function(path) {

  print(path)
  con = duckdbfs::cached_connection(tempfile())
  duckdb_secrets(endpoint = "sdsc.osn.xsede.org", key = Sys.getenv("OSN_KEY"), secret = Sys.getenv("OSN_SECRET"), bucket = "bio230014-bucket01")

  new <- open_dataset(path, conn = con) |>
  filter( !is.na(model_id),
          !is.na(parameter),
          !is.na(prediction)) |>
  select(-any_of(c("date", "reference_date", "...1")))  # (date is a short version of datetime from partitioning, drop it)

  new |> write_dataset("tmp_new.parquet")


  bundled_path <- path |>
    str_replace(fixed("forecasts/parquet"), "forecasts/bundled-parquet")


  old <-
     open_dataset(bundled_path, conn = con) |>
     filter( !is.na(model_id),
             !is.na(parameter),
             !is.na(prediction)) |>
     select(-any_of(c("date", "reference_date", "...1"))) |>
     write_dataset("tmp_old.parquet")

  by <- join_by(datetime, site_id, prediction, parameter, family,
            reference_datetime, pub_datetime, duration, model_id,
            project_id, variable)

  new <- open_dataset("tmp_new.parquet")
  old <- open_dataset("tmp_old.parquet") |>
        anti_join(new, by = by) # old and not duplicated in new


         # anti_join |> union_all() may be more efficient than union()

          union_all(old, new) |>
            write_dataset("new-forecasts/bundled-parquet/project_id=neon4cast",
                          partitioning = c("duration", 'variable', "model_id"),
                          options = list("PER_THREAD_OUTPUT false"))

          duckdbfs::close_connection(con); gc()
}


bench::bench_time({
lapply(data_paths, bundle_me)
})

# only if we have new stuff!
if(length(fs::dir_ls("new-forecasts/bundled-parquet/")) > 0) {

  ## checks that we have no corruption
  open_dataset("new-forecasts/bundled-parquet/") |> count()
  open_dataset(fs::path("new-forecasts/", "bundled-parquet/")) |>
    distinct(duration, variable, model_id)
  open_dataset(fs::path("new-forecasts/", "bundled-parquet/")) |>
    summarise(first_fc = min(reference_datetime), last_fc = max(reference_datetime),
              first_prediction = min(datetime), last_prediction = max(datetime))

  mc_mirror("new-forecasts/bundled-parquet/project_id=neon4cast/", remote_bundles, overwrite =TRUE)


} else {
  print("No new forecasts found")
}

#s3_bundles <- gsub("^osn\\/", "s3://", remote_bundles)
#open_dataset(s3_bundles) |>
#  summarise(first_fc = min(reference_datetime), last_fc = max(reference_datetime),
#            first_prediction = min(datetime), last_prediction = max(datetime))



## online tests
online <- open_dataset("s3://bio230014-bucket01/challenges/forecasts/bundled-parquet",
                       s3_endpoint = "sdsc.osn.xsede.org",
                       anonymous = TRUE)

## More checks
online |> count()
online |>  count(duration, variable, model_id)
date_range <- open_dataset("forecasts/bundled-parquet/") |>
  summarise(first_fc = min(reference_datetime), last_fc = max(reference_datetime),
            first_prediction = min(datetime), last_prediction = max(datetime))




## Drop old forecasts so we don't keep rebundling them.  (keep last n-months for safety?)

archive_older <- function(remote_path = "osn/bio230014-bucket01/challenges/forecasts/parquet/project_id=neon4cast",
                          keep_months = 6) {
  cutoff <- lubridate::dmonths(keep_months)
  all_fc_files <- paste0(remote_path, "/", mc_ls(remote_path, recursive = TRUE))
  dates <- all_fc_files |>
    stringr::str_extract("reference_date=(\\d{4}-\\d{2}-\\d{2})", 1)  |>
    as.Date()
  drop <- dates < Sys.Date() - cutoff
  drop_paths <- all_fc_files[drop] |> na.omit()
  return(drop_paths)
}


drop_f <- function(path,
                   from_pattern = "forecasts\\/parquet",
                   dest_pattern = "forecasts/archive-parquet") {
  if( grepl(".parquet$", path) ){
    mc_mv(path, gsub(from_pattern, dest_pattern, path))
    mc_rm(dirname(path), recursive = TRUE)
  } else {
    mc_rm(path)
  }
  invisible("success")
}

drop_paths <- archive_older(remote_path, 6)
parallel::mclapply(drop_paths, drop_f, mc.cores = 6)


# shouldn't be drop paths left:
print(length(archive_older(remote_path, 6)) == 0)


## We are done.




