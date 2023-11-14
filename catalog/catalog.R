source("catalog/R/catalog-common.R")
source('catalog/R/stac_functions.R')

config <- yaml::read_yaml('challenge_configuration.yaml')

build_catalog <- function(){
  catalog <- list(
    "type"= "Catalog",
    "id"= paste0(config$project_id, "-stac"),
    "title"= paste0(config$challenge_long_name," Catalog"),
    "description"= paste0("A STAC (Spatiotemporal Asset Catalog) describing forecasts and forecast scores for the ",config$project_id," Forecasting Challenge"),
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
        "title"= "Forecasts",
        "href" = 'forecasts/collection.json'),
      list(
        "rel"= "child",
        "type"= "application/json",
        "title"= "Scores",
        "href" = 'scores/collection.json'),
      list(
        "rel"= "child",
        "type"= "application/json",
        "title"= "Inventory",
        "href" = 'inventory/collection.json')
    )
  )

  dest <- "catalog/"
  jsonlite::write_json(catalog, file.path(dest, "catalog.json"),
                       pretty=TRUE, auto_unbox=TRUE)
  stac4cast::stac_validate(file.path(dest, "catalog.json"))

}

build_catalog()
