
## read in model documentation and only grab models for Aquatics theme
library(tidyverse)
library(arrow)
library(stac4cast)
library(reticulate)
library(rlang)
library(RCurl)

source('stac/R/stac_functions.R')

config <- yaml::read_yaml('challenge_configuration.yaml')

#get model ids

data_df <- duckdbfs::open_dataset(glue::glue("s3://{config$inventory_bucket}/catalog"),
                       s3_endpoint = "renc.osn.xsede.org", anonymous=TRUE) |>
  collect()

theme_models <- data_df |>
  distinct(model_id)


## just read in example forecast to extract schema information -- ask about better ways of doing this
# theme <- 'aquatics'
# reference_datetime <- '2023-05-01'
# site_id <- 'BARC'
# model_id <- 'flareGLM'
# variable_name <- 'temperature'

# s3_schema <- arrow::s3_bucket(
#   bucket = glue::glue("neon4cast-forecasts/parquet/{theme}/",
#                       "model_id={model_id}/",
#                       "reference_datetime={reference_datetime}/"),
#   endpoint_override = "data.ecoforecast.org",
#   anonymous = TRUE)
# theme_df <- arrow::open_dataset(s3_schema) %>%
#   filter(variable == variable_name, site_id == site_id)
#
# theme <- 'daily'
# reference_datetime <- '2023-05-01'
# site_id <- 'BARC'
# model_id <- 'flareGLM'
# variable_name <- 'temperature'

# s3_schema <- arrow::s3_bucket(
#   bucket = glue::glue("neon4cast-forecasts/parquet/{theme}/",
#                       "model_id={model_id}/",
#                       "reference_datetime={reference_datetime}/"),
#   endpoint_override = "data.ecoforecast.org",
#   anonymous = TRUE)
# theme_df <- arrow::open_dataset(s3_schema) %>%
#   filter(variable == variable_name, site_id == site_id)

# theme_df <- duckdbfs::open_dataset(glue::glue("s3://anonymous@neon4cast-forecasts/parquet/{theme}/
#                                                model_id={model_id}/reference_datetime={reference_datetime}?endpoint_override=sdsc.osn.xsede.org")) |>
#   filter(variable == variable_name, site_id == site_id)

theme_df <- duckdbfs::open_dataset(glue::glue("s3://{config$forecasts_bucket}/parquet"),
                                   s3_endpoint = "renc.osn.xsede.org", anonymous=TRUE) |>
  filter(model_id == 'climatology') #|>
  #collect()

## CREATE table for column descriptions
description_create <- data.frame(datetime = 'ISO 8601(ISO 2019)datetime the forecast starts from (a.k.a. issue time); Only needed if more than one reference_datetime is stored in a single file. Forecast lead time is thus datetime-reference_datetime. In a hindcast the reference_datetime will be earlier than the time the hindcast was actually produced (see pubDate in Section3). Date times are allowed to be earlier than the reference_datetime if a reanalysis/reforecast is run before the start of the forecast period. This variable was called start_time before v0.5 of the EFI standard.',
                                 site_id = 'For forecasts that are not on a spatial grid, use of a site dimension that maps to a more detailed geometry (points, polygons, etc.) is allowable. In general this would be documented in the external metadata (e.g., alook-up table that provides lon and lat); however in netCDF this could be handled by the CF Discrete Sampling Geometry data model.',
                                 family = 'For ensembles: “ensemble.” Default value if unspecified for probability distributions: Name of the statistical distribution associated with the reported statistics. The “sample” distribution is synonymous with “ensemble.”For summary statistics: “summary.”If this dimension does not vary, it is permissible to specify family as avariable attribute if the file format being used supports this (e.g.,netCDF).',
                                 parameter = 'ensemble member',
                                 variable = 'VERA forecast variable',
                                 prediction = 'predicted forecast value',
                                 pub_datetime = 'date of publication',
                                 #date = 'ISO 8601 (ISO 2019) datetime being predicted; follows CF convention http://cfconventions.org/cf-conventions/cf-conventions.html#time-coordinate. This variable was called time before v0.5 of the EFI convention. For time-integrated variables (e.g., cumulative net primary productivity), one should specify the start_datetime and end_datetime as two variables, instead of the single datetime. If this is not provided the datetime is assumed to be the MIDPOINT of the integration period.',
                                 reference_datetime = 'datetime that the forecast is run',
                                 model_id = 'unique model identifier',
                                 reference_date = 'date that the forecast is run')



## READ IN MODEL METADATA
googlesheets4::gs4_deauth()

registered_model_id <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/1-OsDaOoMZwPfQnz5U5aV-T9_vhTmyg92Ff5ARRunYhY/edit?usp=sharing")


forecast_sites <- c()

#test_models <- c(aquatic_models$model.id[1:2], 'tg_arima')
## loop over model ids and extract components if present in metadata table
for (m in theme_models$model_id[1]){
  print(m)
  model_date_range <- data_df |> filter(model_id == m) |> dplyr::summarise(min(date),max(date))
  model_min_date <- model_date_range$`min(date)`
  model_max_date <- model_date_range$`max(date)`

  #model_var_site_info <- generate_vars_sites(m_id = m, theme = 'vera4cast_daily')

  model_sites <- data_df |> filter(model_id == m) |> distinct(site_id)
  model_vars <- data_df |> filter(model_id == m) |> distinct(variable)


  # print(model_var_site_info[[1]])
  # print(model_var_site_info[[2]])

  forecast_sites <- append(forecast_sites,  get_site_coords(sites = model_sites$site_id))

  idx = which(registered_model_id$model_id == m)


  build_model(model_id = m,
              theme_id = 'daily',
              team_name = neon_docs$team.name[idx],
              model_description = registered_model_id[idx,"Describe your modeling approach in your own words"][[1]],
              start_date = model_min_date,
              end_date = model_max_date,
              var_values = model_vars$variable,
              site_values = model_sites$site_id,
              model_documentation = registered_model_id,
              destination_path = "stac/daily/forecasts/models/",
              description_path = "stac/daily/forecasts/models/asset-description.Rmd", # MIGHT REMOVE THIS
              aws_download_path = config$forecasts_bucket, # CHANGE THIS BUCKET NAME
              theme_title = "Forecasts",
              collection_name = 'forecasts',
              thumbnail_image_name = NULL,
              table_schema = theme_df,
              table_description = description_create)

  # if (m %in% neon_docs$model.id){
  #   print('has metadata')
  #
  #   idx = which(neon_docs$model.id == m)
  #
  #   build_model(model_id = neon_docs$model.id[idx],
  #               theme_id = 'vera4cast_daily',
  #               team_name = neon_docs$team.name[idx],
  #               model_description = neon_docs[idx,'model.description'][[1]],
  #               start_date = model_min_date,
  #               end_date = model_max_date,
  #               use_metadata = TRUE,
  #               var_values = model_var_site_info[[1]],
  #               var_keys = model_var_site_info[[3]][[1]],
  #               site_values = model_var_site_info[[2]],
  #               model_documentation = neon_docs,
  #               destination_path = "stac/vera4cast_daily/forecasts/models/",
  #               description_path = "stac/vera4cast_daily/forecasts/models/asset-description.Rmd", # MIGHT REMOVE THIS
  #               aws_download_path = 'neon4cast-forecasts/parquet/vera4cast_daily', # CHANGE THIS BUCKET NAME
  #               theme_title = "Forecasts",
  #               collection_name = 'forecasts',
  #               thumbnail_image_name = 'latest_forecast.png',
  #               table_schema = theme_df,
  #               table_description = description_create)
  # } else{
  #
  #   build_model(model_id = m,
  #               theme_id = 'vera4cast_daily',
  #               team_name = 'pending',
  #               model_description = 'pending',
  #               start_date = model_min_date,
  #               end_date = model_max_date,
  #               use_metadata = FALSE,
  #               var_values = model_var_site_info[[1]],
  #               var_keys = model_var_site_info[[3]][[1]],
  #               site_values = model_var_site_info[[2]],
  #               model_documentation = neon_docs,
  #               destination_path = "stac/vera4cast_daily/forecasts/models/",
  #               description_path = "stac/vera4cast_daily/forecasts/asset-description.Rmd",
  #               aws_download_path = 'neon4cast-forecasts/parquet/vera4cast_daily',
  #               theme_title = "Forecasts",
  #               collection_name = 'forecasts',
  #               thumbnail_image_name = 'latest_forecast.png',
  #               table_schema = theme_df,
  #               table_description = description_create)
  # }

  #rm(model_var_site_info)
}

 #forecast_sites <- unique(forecast_sites)
#forecast_sites_df <- data.frame(site_id = forecast_sites)

#write.csv(forecast_sites_df, 'stac/daily/forecasts/all_forecast_sites.csv', row.names = FALSE)
