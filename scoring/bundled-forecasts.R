
remotes::install_github("cboettig/duckdbfs", upgrade=FALSE)




library(dplyr)
library(duckdbfs)
library(minioclient)
library(bench)
library(glue)
library(fs)

open_dataset("s3://bio230014-bucket01/challenges/forecasts/bundled-parquet",
             s3_endpoint = "sdsc.osn.xsede.org",
             anonymous = TRUE) |>
  count()

install_mc()
mc_alias_set("osn", "sdsc.osn.xsede.org", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))
#mc_alias_set("nrp", "s3-west.nrp-nautilus.io", Sys.getenv("EFI_NRP_KEY"), Sys.getenv("EFI_NRP_SECRET"))

# make sure new-forecasts location exists and is empty.
fs::dir_create("new-forecasts/bundled-parquet"); fs::dir_delete("new-forecasts/bundled-parquet")

fs::dir_create("forecasts/parquet")
fs::dir_create("new-forecasts/bundled-parquet")

# Sync local scores, fastest way(?)
# We could avoid this.  We could stream from both of these.
# Would still need to create new-forecasts locally and mc_mirror them back with overwrite but not remove.

bench::bench_time({
  # mirror everything(!) crazy
  mc_mirror("osn/bio230014-bucket01/challenges/forecasts/parquet/", "forecasts/parquet/", overwrite = TRUE, remove=TRUE)
  mc_mirror("osn/bio230014-bucket01/challenges/forecasts/bundled-parquet/", "forecasts/bundled-parquet/", overwrite = TRUE, remove=TRUE)

})


##OOOF, still fragile!
durations <- mc_ls("forecasts/parquet/project_id=neon4cast/")

by <- join_by(datetime, site_id, prediction, parameter, family,
              reference_datetime, pub_datetime, duration, model_id,
              project_id, variable)
bench::bench_time({ # 18m w/ union, ~ 50 GB used at times
  for (dur in durations) {
    variables <- mc_ls(glue("forecasts/parquet/project_id=neon4cast/{dur}"))
    for (var in variables) {
      models <- mc_ls(glue("forecasts/parquet/project_id=neon4cast/{dur}{var}"))
      for (model_id in models) {
        path = glue("./forecasts/parquet/project_id=neon4cast/{dur}{var}{model_id}")
        print(path)
        readr::write_lines(path, "bundled.log", append=TRUE)
        if(length(fs::dir_ls(path)) > 0) {

          con = duckdbfs::cached_connection(tempfile())

          new <- open_dataset(path, conn = con) |>
            filter( !is.na(model_id),
                    !is.na(parameter),
                    !is.na(prediction)) |>
            select(-any_of(c("date", "reference_date", "...1")))  # (date is a short version of datetime from partitioning, drop it)




          bundles <- glue("forecasts/bundled-parquet/project_id=neon4cast/{dur}{var}{model_id}")

          ## if this model (dur/var/model) has submitted before, we need
          ## to 'append' any new forecasts via union() to retain bundle


          if (fs::dir_exists(bundles)) {

             # force eval and offload to disk
             new |> write_dataset("tmp_new.parquet")

             # do some clean-up of old as well
             old <- open_dataset(bundles, conn = con) |>
               filter( !is.na(model_id),
                       !is.na(parameter),
                       !is.na(prediction)) |>
                    select(-any_of(c("date", "reference_date", "...1"))) |>
               write_dataset("tmp_old.parquet")

             new <- open_dataset("tmp_new.parquet")
             old <- open_dataset("tmp_old.parquet") |>
                    anti_join(new, by = by) # old and not duplicated in new
             new <- union_all(old, new)

             # anti_join |> union_all() may be more efficient than union()
          }
          new |>
            write_dataset("new-forecasts/bundled-parquet/project_id=neon4cast",
                          partitioning = c("duration", 'variable', "model_id"),
                          options = list("PER_THREAD_OUTPUT false"))

          duckdbfs::close_connection(con); gc()
        }
      }
    }
  }
})



## checks that we have no corruption
open_dataset("new-forecasts/bundled-parquet/") |> count()
open_dataset(fs::path("new-forecasts/", "bundled-parquet/")) |>
  distinct(duration, variable, model_id)
open_dataset(fs::path("new-forecasts/", "bundled-parquet/")) |>
  summarise(first_fc = min(reference_datetime), last_fc = max(reference_datetime),
            first_prediction = min(datetime), last_prediction = max(datetime))


## Now, new-bundled overwrites but does not remove bundled.  This appends to models with new forecasts, but doesn't remove models no longer receiving forecasts
fs::dir_copy("new-forecasts/bundled-parquet/", "forecasts/bundled-parquet/", overwrite =TRUE)

## More checks
date_range <- open_dataset("forecasts/bundled-parquet/") |>
  summarise(first_fc = min(reference_datetime), last_fc = max(reference_datetime),
            first_prediction = min(datetime), last_prediction = max(datetime))
#message(date_range)



## Drop old forecasts so we don't keep rebundling them.  (keep last month for safety?)
cutoff <- lubridate::dmonths(6)
all_fc_files <- fs::dir_ls("forecasts/parquet/project_id=neon4cast", type="file", recurse = TRUE)
dates <- all_fc_files |> stringr::str_extract("reference_date=(\\d{4}-\\d{2}-\\d{2})/", 1)  |> as.Date()
drop <- dates < Sys.Date() - cutoff
drop_paths <- all_fc_files[drop]

drop_paths |> fs::file_delete()

bench::bench_time({ # 12.1m
  mc_mirror("forecasts/bundled-parquet", "osn/bio230014-bucket01/challenges/forecasts/bundled-parquet",
            overwrite = TRUE, remove = TRUE)
})



## Instead of remove, we could move to archive.  Only once we have successfully updated the bundles!
## really really slow
s3_drop_paths <- paste0("osn/bio230014-bucket01/challenges/", gsub("^\\./", "", drop_paths))

drop_f <- function(path) {
  if(is.character(mc_ls(path)))
    mc_mv(path, gsub("forecasts\\/parquet", "forecasts/archive-parquet",  path))
  else
    invisible(NULL)
}
parallel::mclapply(s3_drop_paths, drop_f, mc.cores = parallel::detectCores)


## no need to mirror unchanged unbundled forecasts.


## We are done.


## online tests

online <- open_dataset("s3://bio230014-bucket01/challenges/forecasts/bundled-parquet",
                       s3_endpoint = "sdsc.osn.xsede.org",
                       anonymous = TRUE)
online |> count()
online |>  count(duration, variable, model_id)


