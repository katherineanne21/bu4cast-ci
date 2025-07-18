
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


remote_path <- "osn/bio230014-bucket01/challenges/forecasts/parquet/project_id=neon4cast/"
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

  # these are both local, so we can stream back.
  new <- open_dataset("tmp_new.parquet")
  old <- open_dataset("tmp_old.parquet") |>
        anti_join(new, by = by)

  union_all(old, new) |>
            write_dataset(bundled_path,
                          options = list("PER_THREAD_OUTPUT false"))

  #We should now archive anything we have bundled:

  mc_path <- path |> str_replace(fixed("s3://"), "osn/")
  dest_path <- mc_path |>
    str_replace(fixed("forecasts/parquet"), "forecasts/archive-parquet")
  mc_mv(mc_path, dest_path, recursive = TRUE)

  # clears up empty folders (not necessary?)
  mc_rm(mc_path, recursive = TRUE)

  duckdbfs::close_connection(con); gc()
}


bench::bench_time({
lapply(model_paths, bundle_me)
})







