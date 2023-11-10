uri <- "s3://anonymous@bio230014-bucket01/challenges/inventory/catalog/forecasts/project_id=neon4cast?endpoint_override=sdsc.osn.xsede.org"

library(duckdbfs)
library(dplyr)
library(sf)
load_spatial()

library(spData)
ca <- us_states |>
  filter(NAME == "California") |>
  pull(geometry) |>
  sf::st_as_text()


paths <- open_dataset(uri) |>
  mutate(geometry = ST_Point(longitude, latitude)) |>
  filter(st_within(geometry, ST_GeomFromText({ca}))) |>
  filter(date == as_date("2023-07-01"), variable == "gcc_90") |>
  to_sf() |>
  collect()


local_sites <- unique(paths$site_id)

open_dataset(paste0("s3://", paths$path_full), s3_endpoint = "sdsc.osn.xsede.org") |>
  filter(family == "ensemble", site_id %in% local_sites) |>
  collect() |>
  ggplot(aes(x = prediction)) +
  geom_histogram() +
  facet_wrap(~site_id, scale = "free")
