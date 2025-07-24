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
duckdb_secrets(endpoint = "sdsc.osn.xsede.org", key = Sys.getenv("OSN_KEY"), secret = Sys.getenv("OSN_SECRET"), bucket = "bio230014-bucket01")



remote_path <- "osn/bio230014-bucket01/challenges/forecasts/bundled-parquet/project_id=neon4cast/"
contents <- mc_ls(remote_path, recursive = TRUE, details = TRUE)
data_paths <- contents |> filter(!is_folder) |> pull(path)

# model paths are paths with at least one reference_datetime containing data files
model_paths <-
  data_paths |>
  str_replace("^osn\\/", "s3://") |>
  unique()



# bundled count at start
count <- open_dataset("s3://bio230014-bucket01/challenges/forecasts/bundled-parquet",
                      s3_endpoint = "sdsc.osn.xsede.org",
                      anonymous = TRUE) |>
  count()
print(count)

# De-duplicate model_id bundles 1 by 1.

process_me <- function(path) {

  con = duckdbfs::cached_connection(tempfile())
  duckdb_secrets(endpoint = "sdsc.osn.xsede.org", key = Sys.getenv("OSN_KEY"), secret = Sys.getenv("OSN_SECRET"), bucket = "bio230014-bucket01")

  n_start <- open_dataset(path, conn = con, recursive = FALSE)  |> count() |> pull(n)

  open_dataset(path, conn = con, recursive = FALSE) |>
    filter( !is.na(model_id),
            !is.na(parameter),
            !is.na(prediction)) |>
    distinct() |>
    write_dataset("tidy_bundle.parquet")

  n_now <- open_dataset("tidy_bundle.parquet", conn = con) |> count() |> pull(n)

  if (n_now != n_start) {
    print(paste("fixing", path))
  # special filters should not be needed on bundled copy
    open_dataset("tidy_bundle.parquet", conn = con) |>
    write_dataset(path,
                  options = list("PER_THREAD_OUTPUT false"))
  }

  duckdbfs::close_connection(con); gc()
  invisible(path)
}




# We use future_apply framework to show progress while being robust to OOM kils.
# We are not actually running on multi-core, which would be RAM-inefficient
future::plan(future::sequential)

safe_bundles <- function(xs) {
  p <- progressor(along = xs)
  future_lapply(xs, function(x, ...) {
    process_me(x)
    p(sprintf("x=%s", x))
  },  future.seed = TRUE)
}


bench::bench_time({
  safe_bundles(model_paths)
})


