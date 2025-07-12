library(neonstore)
library(tidyverse)
library(ISOweek)
library(minioclient)
source("targets/R/resolve_taxonomy.R")

install_mc()
mc_alias_set("osn", "sdsc.osn.xsede.org", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))


mc_mirror("osn/bio230014-bucket01/beetles-data/",  path.expand("~/beetles-data/"))

for(curr_year in 2015:year(Sys.Date())){
  print(curr_year)
  pass <- TRUE
  iter <- 0
  while(pass & iter < 10){
    iter <- iter + 1

    df <-  neonstore:::neon_data(product = "DP1.10022.001",
                                 start_date = paste0(curr_year, "-01-01"),
                                 end_date = paste0(curr_year, "-12-31"),
                                 type="expanded")

    if(file.exists(path.expand("~/beetles-data/DP1.10022.001.csv"))){
      full_df_old <- read_csv(path.expand("~/beetles-data/DP1.10022.001.csv"), show_col_types = FALSE)
    }else{
      full_df_old <- NULL
    }

    full_df <- bind_rows(full_df_old, df) %>%
      distinct()

    print(nrow(full_df))
    print(nrow(full_df_old))
    pass <- nrow(full_df) != nrow(full_df_old)

    write_csv(full_df, path.expand("~/beetles-data/DP1.10022.001.csv"))
  }
}

#full_df <- read_csv(path.expand("~/beetles-data/DP1.10022.001.csv"), show_col_types = FALSE)

sorting_urls <- full_df |>
  dplyr::filter(grepl("bet_sorting", name)) |>
  pull(url)

sorting <- duckdbfs::open_dataset(sorting_urls, format="csv") |>
  select(collectDate, siteID, taxonID, individualCount, subsampleID,
         scientificName, morphospeciesID, taxonRank, sampleType,
         nativeStatusCode, sampleCondition) |>
  collect()

para_urls <- full_df |>
  dplyr::filter(grepl("bet_parataxonomistID", name)) |>
  pull(url)

para <- duckdbfs::open_dataset(para_urls, format="csv") |>
  select(subsampleID, individualID, scientificName, taxonRank, taxonID, morphospeciesID) |>
  collect()

expert_urls <- full_df |>
  dplyr::filter(grepl("bet_expertTaxonomistIDProcessed", name)) |>
  pull(url)

expert <- duckdbfs::open_dataset(expert_urls, format="csv") |>
  select(-uid, -namedLocation, -domainID, -siteID, -collectDate, -plotID, -setDate, -collectDate) |>
  collect()

field_urls <- full_df |>
  dplyr::filter(grepl("bet_fielddata", name)) |>
  pull(url)

field <- duckdbfs::open_dataset(field_urls, format="csv") |>
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

#s3 <- arrow::s3_bucket("neon4cast-targets/beetles",
#                       endpoint_override = "data.ecoforecast.org",
#                       access_key = Sys.getenv("AWS_ACCESS_KEY_SUBMISSIONS"),
#                       secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY_SUBMISSIONS"))
#
#arrow::write_csv_arrow(targets, sink = s3$path("beetles-targets.csv.gz"))

targets2 <- targets |>
  mutate(datetime = lubridate::as_datetime(datetime),
         duration = "P1W",
         project_id = "neon4cast") |>
  select(project_id, site_id, datetime, duration, variable, observation)

s3 <- arrow::s3_bucket("bio230014-bucket01/challenges/targets/project_id=neon4cast/duration=P1W",
                       endpoint_override = "sdsc.osn.xsede.org",
                       access_key = Sys.getenv("OSN_KEY"),
                       secret_key = Sys.getenv("OSN_SECRET"))


write_csv(targets2, "beetles-targets.csv.gz")

mc_cp("beetles-targets.csv.gz", "osn/bio230014-bucket01/challenges/targets/project_id=neon4cast/duration=P1W/beetles-targets.csv.gz")

mc_mirror(path.expand("~/beetles-data"), "osn/bio230014-bucket01/beetles-data/")

RCurl::getURL("https://hc-ping.com/ed35da4e-01d3-4750-ae5a-ad2f5dfa6e99")
