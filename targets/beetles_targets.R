#renv::restore()
## 02_process.R
##  Process the raw data into the target variable product

#renv::restore()
Sys.setenv("NEONSTORE_HOME" = "/home/rstudio/data/neonstore")
#Sys.setenv("NEONSTORE_DB" = "/home/rstudio/data/neonstore")
#Sys.setenv("NEONSTORE_DB")

library(neonstore)
library(tidyverse)
library(ISOweek)
source("targets/R/resolve_taxonomy.R")

print(neon_dir())

#message("Downloading: DP1.10022.001")
#neonstore::neon_download(product="DP1.10022.001",
#                         type = "expanded",
#                         start_date = NA,
#                         .token = Sys.getenv("NEON_TOKEN"))
#neon_store(product = "DP1.10022.001")

df <-  neonstore:::neon_data(product = "DP1.10022.001",
                             #start_date = "2023-06-01",
                             #end_date = "2023-08-01",
                             type="expanded")
sorting_urls <- df |>
  dplyr::filter(grepl("bet_sorting", name)) |>
  pull(url)

sorting <- duckdbfs::open_dataset(sorting_urls,
                             format="csv") |>
  select(collectDate, siteID, taxonID, individualCount, subsampleID,
         scientificName, morphospeciesID, taxonRank, sampleType,
         nativeStatusCode, sampleCondition) |>
  collect()

para_urls <- df |>
  dplyr::filter(grepl("bet_parataxonomistID", name)) |>
  pull(url)

para <- duckdbfs::open_dataset(para_urls,
                               format="csv") |>
  select(subsampleID, individualID, scientificName, taxonRank, taxonID, morphospeciesID) |>
  collect()

expert_urls <- df |>
  dplyr::filter(grepl("bet_expertTaxonomistIDProcessed", name)) |>
  pull(url)

expert <- duckdbfs::open_dataset(expert_urls,
                               format="csv") |>
  select(-uid, -namedLocation, -domainID, -siteID, -collectDate, -plotID, -setDate, -collectDate) |>
  collect()

field_urls <- df |>
  dplyr::filter(grepl("bet_fielddata", name)) |>
  pull(url)

field <- duckdbfs::open_dataset(expert_urls,
                                format="csv") |>
  select(collectDate, siteID, setDate) |>
  collect()

#### Generate derived richness table  ####################
beetles <- resolve_taxonomy(sorting, para, expert) %>%
  mutate(iso_week = ISOweek::ISOweek(collectDate),
         time = ISOweek::ISOweek2date(paste0(iso_week, "-1"))) %>%
  as_tibble()

richness <- beetles %>%
  select(taxonID, siteID, collectDate, time) %>%
  distinct() %>%
  count(siteID, time) %>%
  rename(richness = n)  %>%
  ungroup()

#### Generate derived abundance table ####################

## Using 'field' instead of 'beetles' Does not reflect taxonomic corrections!
## Allows for some counts even when richness is NA

effort <- field %>%
  mutate(iso_week = ISOweek::ISOweek(collectDate),
         time = ISOweek::ISOweek2date(paste0(iso_week, "-1"))) %>%
  group_by(siteID, time) %>%
  summarise(trapnights = as.integer(sum(collectDate - setDate)),
            .groups = "drop")

counts <- beetles %>%
  mutate(iso_week = ISOweek::ISOweek(collectDate),
         time = ISOweek::ISOweek2date(paste0(iso_week, "-1"))) %>%
  group_by(siteID, time) %>%
  summarise(count = sum(as.numeric(individualCount), na.rm = TRUE),
            .groups = "drop")

abund <- counts %>%
  left_join(effort, by = join_by(siteID, time)) %>%
  arrange(time) %>%
  mutate(abundance = count / trapnights) %>%
  select(siteID, time, abundance) %>%
  ungroup()

targets_na <- full_join(abund, richness, by = join_by(siteID, time))

## site-dates that have sampling effort but no counts should be
## treated as explicit observation 0s

## FIXME some may have effort but no sorting due only to latency, should not be treated as zeros

targets <- effort %>%
  select(siteID, time) %>%
  left_join(targets_na, by = join_by(siteID, time)) %>%
  tidyr::replace_na(list(richness = 0L, abundance = 0)) |>
  pivot_longer(-c("time","siteID"), names_to = "variable", values_to = "observation") |>
  rename(site_id = siteID) |>
  mutate(iso_week = ISOweek::ISOweek(time)) |>
  select(time, site_id, variable, observation, iso_week)

targets <- targets |>
  rename(datetime = time)

s3 <- arrow::s3_bucket("neon4cast-targets/beetles",
                       endpoint_override = "data.ecoforecast.org",
                       access_key = Sys.getenv("AWS_ACCESS_KEY_SUBMISSIONS"),
                       secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY_SUBMISSIONS"))

arrow::write_csv_arrow(targets, sink = s3$path("beetles-targets.csv.gz"))

targets2 <- targets |>
  mutate(datetime = lubridate::as_datetime(datetime),
         duration = "P1W",
         project_id = "neon4cast") |>
  select(project_id, site_id, datetime, duration, variable, observation)

s3 <- arrow::s3_bucket("bio230014-bucket01/challenges/targets/project_id=neon4cast/duration=P1W",
                       endpoint_override = "sdsc.osn.xsede.org",
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))

arrow::write_csv_arrow(targets2, sink = s3$path("beetles-targets.csv.gz"))
