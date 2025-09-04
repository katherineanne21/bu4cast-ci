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


# bundled count at start
b <- open_dataset("s3://bio230014-bucket01/challenges/forecasts/bundled-summaries",
                      s3_endpoint = "sdsc.osn.xsede.org",
                      anonymous = TRUE)

b |> count() |> print()
b |> summarise(date = max(reference_datetime)) |> print()


install_mc()
mc_alias_set("osn", "sdsc.osn.xsede.org", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))

duckdb_secrets(endpoint = "sdsc.osn.xsede.org",
               key = Sys.getenv("OSN_KEY"),
               secret = Sys.getenv("OSN_SECRET"),
               bucket = "bio230014-bucket01")


remote_path <- "osn/bio230014-bucket01/challenges/forecasts/summaries/project_id=neon4cast/"
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
  duckdb_secrets(endpoint = "sdsc.osn.xsede.org",
                 key = Sys.getenv("OSN_KEY"),
                 secret = Sys.getenv("OSN_SECRET"),
                 bucket = "bio230014-bucket01")


  bundled_path <- path |>
    str_replace(fixed("forecasts/summaries"),
                "forecasts/bundled-summaries")

  open_dataset(path, conn = con) |> write_dataset("tmp_new.parquet")
  open_dataset(bundled_path, conn = con) |> write_dataset("tmp_old.parquet")

  # these are both local, so we can stream back.
  new <- open_dataset("tmp_new.parquet")
  old <- open_dataset("tmp_old.parquet")

  union_all(old, new) |>
    write_dataset(fs::path(bundled_path,"data_0.parquet"),
                  options = list("PER_THREAD_OUTPUT false"))

  #We should now archive anything we have bundled:

  mc_path <- path |> str_replace(fixed("s3://"), "osn/")
  dest_path <- mc_path |>
    str_replace(fixed("forecasts/summaries"), "forecasts/archive-summaries")
  mc_mv(mc_path, dest_path, recursive = TRUE)
  # clears up empty folders
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
  safe_bundles(model_paths)
})




# bundled count at end
count <- open_dataset("s3://bio230014-bucket01/challenges/forecasts/bundled-summaries",
                      s3_endpoint = "sdsc.osn.xsede.org",
                      anonymous = TRUE) |>
  count()

print(count)
