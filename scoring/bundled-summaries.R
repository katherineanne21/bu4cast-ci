remotes::install_github("cboettig/duckdbfs", upgrade=FALSE)


library(dplyr)
library(duckdbfs)
library(minioclient)
library(bench)
library(glue)
library(fs)

install_mc()
mc_alias_set("osn", "sdsc.osn.xsede.org", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))


# make sure new-forecasts location exists and is empty.
fs::dir_create("new-forecasts"); fs::dir_delete("new-forecasts")
fs::dir_create("forecasts/bundled-summaries")
fs::dir_create("new-forecasts/bundled-summaries")

# Sync to local, fastest way to access all the bytes.
bench::bench_time({
  # mirror everything(!) crazy
  # Could focus on summaries here
  mc_mirror("osn/bio230014-bucket01/challenges/forecasts/summaries/", "forecasts/summaries/", overwrite = TRUE, remove = TRUE)
  mc_mirror("osn/bio230014-bucket01/challenges/forecasts/bundled-summaries/", "forecasts/bundled-summaries/", overwrite = TRUE, remove = TRUE)

})


grouping <- c("model_id", "reference_datetime", "site_id",
              "datetime", "family", "variable", "duration", "project_id")


## Tidy the current bundled summaries
#bundled_summmaries <- duckdbfs::open_dataset("forecasts/bundled-summaries/")
#bundled_summmaries |>
#  distinct() |>
#  write_dataset("new-forecasts/bundled-summaries/project_id=neon4cast",
#                partitioning = c("duration", 'variable', "model_id"),
#                options = list("PER_THREAD_OUTPUT false"))
#mc_mirror("new-forecasts/bundled-summaries/", "forecasts/bundled-summaries/", overwrite = TRUE, remove = TRUE)
#mc_mirror("forecasts/bundled-summaries/", "osn/bio230014-bucket01/challenges/forecasts/bundled-summaries/", overwrite = TRUE, remove = TRUE)



bench::bench_time({
  bundled_summaries <- open_dataset("./forecasts/bundled-summaries/project_id=neon4cast")
  new_summaries <- open_dataset("./forecasts/summaries/project_id=neon4cast/")
  union(bundled_summaries, new_summaries) |>
    filter(!is.na(model_id)) |>  ## model_id CANNOT BE NA!
    write_dataset("tmp.parquet")

# Ensures partitions are written as a single shard
  open_dataset("tmp.parquet") |>
      group_by(across(any_of(grouping))) |>
      slice_max(pub_datetime) |>
      distinct() |>
      write_dataset("new-forecasts/bundled-summaries/project_id=neon4cast",
                    partitioning = c("duration", 'variable', "model_id"),
                    options = list("PER_THREAD_OUTPUT false"))

})



# check that we have no corruption
n_bundled <- open_dataset(fs::path("new-forecasts", "bundled-summaries/")) |> count() |> collect()
n_groups <- open_dataset(fs::path("new-forecasts", "bundled-summaries/")) |>
  distinct(duration, variable, model_id) |> count() |> collect()


# PURGE all but last 2 months from un-bundled
all_fc_files <- fs::dir_ls("forecasts/summaries/project_id=neon4cast", type="file", recurse = TRUE)
dates <- all_fc_files |> stringr::str_extract("reference_date=(\\d{4}-\\d{2}-\\d{2})/", 1)  |> lubridate::as_date()
drop <- dates < (Sys.Date() - lubridate::dmonths(2))
all_fc_files[drop] |> fs::file_delete()


fs::dir_delete("forecasts/bundled-summaries/")
fs::dir_copy("new-forecasts/bundled-summaries/", "forecasts/bundled-summaries/", overwrite =TRUE)



## upload new bundles, overwriting old ones.
bench::bench_time({
  mc_mirror("forecasts/summaries",
            "osn/bio230014-bucket01/challenges/forecasts/summaries",
            remove=TRUE)
  mc_mirror("forecasts/bundled-summaries",
            "osn/bio230014-bucket01/challenges/forecasts/bundled-summaries",
            overwrite=TRUE, remove=TRUE)
})
