
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


theme <- 'daily'
reference_datetime <- '2023-09-01'
site_id <- 'fcre'
model_id <- 'climatology'

# theme_df <- duckdbfs::open_dataset(glue::glue("s3://{config$forecasts_bucket}/parquet"),
#                                    s3_endpoint = "renc.osn.xsede.org", anonymous=TRUE) |>
#   filter(model_id == 'climatology')

theme_df <- arrow::open_dataset(arrow::s3_bucket(config$scores_bucket, endpoint_override = "renc.osn.xsede.org", anonymous = TRUE)) |>
  filter(model_id == model_id, site_id = site_id, reference_datetime = reference_datetime)

## CREATE table for column descriptions
description_create <- data.frame(reference_datetime ='ISO 8601(ISO 2019) datetime the forecast starts from (a.k.a. issue time); Only needed if more than one reference_datetime is stored in a single file. Forecast lead time is thus datetime-reference_datetime. In a hindcast the reference_date time will be earlier than the time the hindcast was actually produced (see pubDate in Section 3). Datetimes are allowed to be earlier than the reference_datetime if analysis/reforecast is run before the start of the forecast period. This variable was called start_time before v0.5 of the EFI standard.',
                                 site_id = 'For forecasts that are not on a spatial grid, use of a site dimension that maps to a more detailed geometry (points, polygons, etc.) is allowable. In general this would be documented in the external metadata (e.g., alook-up table that provides lon and lat); however in netCDF this could be handled by the CF Discrete Sampling Geometry data model.',
                                 datetime = 'ISO 8601(ISO 2019) datetime the forecast starts from (a.k.a. issue time); Only needed if more than one reference_datetime is stored in a single file. Forecast lead time is thus datetime-reference_datetime. In a hindcast the reference_date time will be earlier than the time the hindcast was actually produced (see pubDate in Section 3). Datetimes are allowed to be earlier than the reference_datetime if analysis/reforecast is run before the start of the forecast period. This variable was called start_time before v0.5 of the EFI standard.',
                                 family = 'For ensembles: “ensemble.” Default value if unspecified For probability distributions: Name of the statistical distribution associated with the reported statistics. The “sample” distribution is synonymous with “ensemble.” For summary statistics: “summary.”If this dimension does not vary, it is permissible to specify family as a variable attribute if the file format being used supports this (e.g.,netCDF).',
                                 variable = 'aquatic forecast variable',
                                 observation = 'observational data',
                                 crps = 'crps forecast score',
                                 logs = 'logs forecast score',
                                 mean = 'mean forecast prediction for all ensemble members',
                                 median = 'median forecast prediction for all ensemble members',
                                 sd = 'standard deviation of all enemble member forecasts',
                                 quantile97.5 = 'upper 97.5 percentile value of ensemble member forecasts',
                                 quantile02.5 = 'upper 2.5 percentile value of ensemble member forecasts',
                                 quantile90 = 'upper 90 percentile value of ensemble member forecasts',
                                 quantile10 = 'upper 10 percentile value of ensemble member forecasts',
                                 model_id = 'unique model identifier',
                                 date = 'ISO 8601 (ISO 2019) datetime being predicted; follows CF convention http://cfconventions.org/cf-conventions/cf-conventions.html#time-coordinate. This variable was called time before v0.5of the EFI convention. For time-integrated variables (e.g., cumulative net primary productivity), one should specify the start_datetime and end_datetime as two variables, instead of the single datetime. If this is not provided the datetime is assumed to be the MIDPOINT of the integration period.')
## READ IN MODEL METADATA
googlesheets4::gs4_deauth()

registered_model_id <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/1-OsDaOoMZwPfQnz5U5aV-T9_vhTmyg92Ff5ARRunYhY/edit?usp=sharing")


forecast_sites <- c()

#test_models <- c(aquatic_models$model.id[1:2], 'tg_arima')
## loop over model ids and extract components if present in metadata table
for (m in theme_models$model_id){
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
              destination_path = "stac/daily/scores/models",
              description_path = "stac/daily/scores/models/asset-description.Rmd", # MIGHT REMOVE THIS
              aws_download_path = config$forecasts_bucket, # CHANGE THIS BUCKET NAME
              theme_title = "Scores",
              collection_name = 'scores',
              thumbnail_image_name = NULL,
              table_schema = theme_df,
              table_description = description_create)
}
