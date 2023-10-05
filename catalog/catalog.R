source("catalog/R/catalog-common.R")
source('catalog/R/stac_functions.R')

config <- yaml::read_yaml('challenge_configuration.yaml')

build_catalog <- function(){
  catalog <- list(
    "type"= "Catalog",
    "id"= "vera-stac",
    "title"= "Virginia Ecoforecasting Reservoir Analysis STAC API",
    "description"= "Searchable spatiotemporal metadata describing forecasts and forecast scores for the VERA Forecasting Challenge",
    "stac_version"= "1.0.0",
    "conformsTo"= 'conformsTo',
    "links"= list(
      list(
        "rel"= "self",
        "type"= "application/json",
        "href" = 'catalog.json'
      ),
      list(
        "rel"= "root",
        "type"= "application/json",
        "href" = 'catalog.json'
      ),
      list(
        "rel"= "child",
        "type"= "application/json",
        "title"= "VERA Forecasts",
        "href" = 'forecasts/collection.json'),
      list(
        "rel"= "child",
        "type"= "application/json",
        "title"= "VERA Scores",
        "href" = 'scores/collection.json')
    )
  )

  dest <- "catalog/"
  jsonlite::write_json(catalog, file.path(dest, "catalog.json"),
                       pretty=TRUE, auto_unbox=TRUE)
  stac4cast::stac_validate(file.path(dest, "catalog.json"))

}

build_catalog()
