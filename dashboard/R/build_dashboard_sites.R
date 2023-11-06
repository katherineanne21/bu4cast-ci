library(tidyverse)
config <- yaml::read_yaml('challenge_configuration.yaml')
catalog_config <- config$catalog_config

project_sites <- read_csv(catalog_config$site_metadata_url, col_types = cols())
project_sites$site_lat_lon <- lapply(1:nrow(project_sites), function(i) c(project_sites$field_longitude[i], project_sites$field_latitude[i]))
project_sites$field_site_name_short <- gsub(' NEON', '',project_sites$field_site_name) # remove the NEON on back end of name

iterator_list <- 1:nrow(project_sites)

site_name_coords <- purrr::map(iterator_list, function(i)
  list(
   "type" = "Feature",
   "properties" = list(
     "site_id" = project_sites$field_site_name_short[i],
     "Partner" = "NEON",
     "n" =  5 ),
   "geometry" = list(
     "type" = "Point",
     "coordinates" = c(project_sites$field_longitude[i], project_sites$field_latitude[i])
  )))


site_info <- list(
  "type" = "FeatureCollection",
  "name" = "neon",
  "crs" = list(
    "type" = "name",
    "properties" = list(
      "name" = "urn:ogc:def:crs:OGC:1.3:CRS84")
    ),
  "features" = site_name_coords
)

dest <- 'dashboard/'
json <- file.path(dest, "sites.json")


jsonlite::write_json(site_info,
                     json,
                     pretty=TRUE,
                     auto_unbox=TRUE)

#stac4cast::stac_validate(json)



