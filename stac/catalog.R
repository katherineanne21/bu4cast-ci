source("stac/R/catalog-common.R")
source('stac/R/stac_functions.R')

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
        "title"= "VERA Daily Forecast Challenge",
        "href" = 'daily/collection.json'),
      list(
        "rel"= "child",
        "type"= "application/json",
        "title"= "VERA Subdaily Forecasting Challenge",
        "href" = 'sites/collection.json')
    )
  )

  dest <- "stac/"
  jsonlite::write_json(catalog, file.path(dest, "catalog.json"),
                       pretty=TRUE, auto_unbox=TRUE)
  stac4cast::stac_validate(file.path(dest, "catalog.json"))

}

build_catalog()
