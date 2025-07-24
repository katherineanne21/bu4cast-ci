remotes::install_github("cboettig/duckdbfs", upgrade=FALSE)

library(tidyverse)
library(duckdbfs)
library(minioclient)
library(bench)
library(glue)
library(fs)
library(future.apply)
library(progressr)
handlers(global = TRUE)
handlers("cli")


install_mc()
mc_alias_set("osn", "sdsc.osn.xsede.org", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))
# mc_alias_set("nrp", "s3-west.nrp-nautilus.io", Sys.getenv("EFI_NRP_KEY"), Sys.getenv("EFI_NRP_SECRET"))

duckdb_secrets(endpoint = "sdsc.osn.xsede.org", key = Sys.getenv("OSN_KEY"), secret = Sys.getenv("OSN_SECRET"), bucket = "bio230014-bucket01")



remote_path <- "osn/bio230014-bucket01/challenges/forecasts/parquet/project_id=neon4cast/"
contents <- mc_ls(remote_path, recursive = TRUE, details = TRUE)
data_paths <- contents |> filter(!is_folder) |> pull(path)

# model paths are paths with at least one reference_datetime containing data files
model_paths <-
  data_paths |>
  str_replace_all("reference_date=\\d{4}-\\d{2}-\\d{2}/.*", "") |>
  str_replace("^osn\\/", "s3://") |>
  unique()

print(model_paths)

# bundled count at start
count <- open_dataset("s3://bio230014-bucket01/challenges/forecasts/bundled-parquet",
                      s3_endpoint = "sdsc.osn.xsede.org",
                      anonymous = TRUE) |>
  count()
print(count)

most_recent <- open_dataset("s3://bio230014-bucket01/challenges/forecasts/bundled-parquet",
             s3_endpoint = "sdsc.osn.xsede.org",
             anonymous = TRUE) |>
  group_by(model_id, variable) |>
  summarise(most_recent = max(reference_datetime))
print(most_recent)


bundle_me <- function(path) {

  print(path)
  con = duckdbfs::cached_connection(tempfile())
  duckdb_secrets(endpoint = "sdsc.osn.xsede.org", key = Sys.getenv("OSN_KEY"), secret = Sys.getenv("OSN_SECRET"), bucket = "bio230014-bucket01")
  bundled_path <- path |> str_replace(fixed("forecasts/parquet"), "forecasts/bundled-parquet")

  open_dataset(path, conn = con) |>
    filter( !is.na(model_id),
            !is.na(parameter),
            !is.na(prediction)) |>
    write_dataset("tmp_new.parquet")

  # special filters should not be needed on bundled copy
  open_dataset(bundled_path, conn = con) |>
     write_dataset("tmp_old.parquet")

  # these are both local, so we can stream back.
  new <- open_dataset("tmp_new.parquet")
  old <- open_dataset("tmp_old.parquet")

## We can just "append", we no longer face duplicates:
# by <- join_by(datetime, site_id, prediction, parameter, family, reference_datetime, pub_datetime, duration, model_id, project_id, variable)
#  filtered_n <- old |> anti_join(new, by = by) |> count() |> pull(n) # is this the bottleneck?
#  previous_n <- open_dataset("tmp_old.parquet") |> count() |> pull(n)
#  stopifnot(previous_n - filtered_n == 0)

  ## no partition levels left so we must write to an explicit .parquet
  bundled_dir <- bundled_path |> str_replace(fixed("s3://"), "osn/") |> mc_ls(details = TRUE)
  mc_bundled_path <- bundled_dir |> filter(!is_folder) |> pull(path)
  stopifnot(length(mc_bundled_path) == 1)
  bundled_path <- mc_bundled_path |> str_replace(fixed("osn/"), fixed("s3://"))

  ## once running consistently we can "append" with union_all instead of union
  # uses less RAM. since mc_rm / mc_mv removes anything we have already read
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

  invisible(path)
}




# We use future_apply framework to show progress while being robust to OOM kils.
# We are not actually running on multi-core, which would be RAM-inefficient
future::plan(future::sequential)

safe_bundles <- function(xs) {
  p <- progressor(along = xs)
  future_lapply(xs, function(x, ...) {
    bundle_me(x)
    p(sprintf("x=%s", x))
  },  future.seed = TRUE)
}


bench::bench_time({
  out <- safe_bundles(model_paths)
})

print(out)




# bundled count at end
count <- open_dataset("s3://bio230014-bucket01/challenges/forecasts/bundled-parquet",
                      s3_endpoint = "sdsc.osn.xsede.org",
                      anonymous = TRUE) |>
  count()
print(count)


open_dataset("s3://bio230014-bucket01/challenges/forecasts/bundled-parquet",
                      s3_endpoint = "sdsc.osn.xsede.org",
                      anonymous = TRUE) |>
filter()

# should we slice_max(pub_time) to ensure only most recent pub_time if duplicates submitted?
# grouping <- c("model_id", "reference_datetime", "site_id", "datetime", "family", "variable", "duration", "project_id")
