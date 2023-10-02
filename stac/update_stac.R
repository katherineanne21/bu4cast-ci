library(jsonlite)
library(arrow)
library(dplyr)
library(lubridate)

# catalog
source('stac/catalog.R')

# forecasts
source('stac/forecasts/forecast_models.R')

rm(list = ls()) # remove all environmental vars between forecast and scores

# scores
source('stac/scores/scores_models.R')
