library(arrow)
library(dplyr)

source('stac/R/stac_functions.R')
config <- yaml::read_yaml('challenge_configuration.yaml')

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

## just read in example forecast to extract schema information -- ask about better ways of doing this
theme <- 'daily'
reference_datetime <- '2023-09-01'
site_id <- 'fcre'
model_id <- 'climatology'

theme_df <- arrow::open_dataset(arrow::s3_bucket(config$scores_bucket, endpoint_override = "renc.osn.xsede.org", anonymous = TRUE)) |>
  filter(model_id == model_id, site_id = site_id, reference_datetime = reference_datetime)

## identify model ids from bucket -- used in generate model items function
data_df <- duckdbfs::open_dataset(glue::glue("s3://{config$inventory_bucket}/catalog"),
                                  s3_endpoint = "renc.osn.xsede.org", anonymous=TRUE) |>
  collect()

theme_models <- data_df |>
  distinct(model_id)

forecast_date_range <- data_df |> dplyr::summarise(min(date),max(date))
forecast_max_date <- forecast_date_range$`min(date)`
forecast_min_date <- forecast_date_range$`max(date)`

build_description <- "The catalog contains scores for the VERA Forecasting Challenge Daily theme. The forecasts are the raw forecasts that include all ensemble members (if a forecast represents uncertainty using an ensemble).  Due to the size of the raw forecasts, we recommend accessing the scores (summaries of the forecasts) to analyze forecasts (unless you need the individual ensemble members). You can access the forecasts at the top level of the dataset where all models, variables, and dates that forecasts were produced (reference_datetime) are available. The code to access the entire dataset is provided as an asset. Given the size of the forecast catalog, it can be time-consuming to access the data at the full dataset level. For quicker access to the forecasts for a particular model (model_id), we also provide the code to access the data at the model_id level as an asset for each model."

build_forecast_scores(table_schema = theme_df,
                      theme_id = theme,
                      table_description = description_create,
                      start_date = forecast_min_date,
                      end_date = forecast_max_date,
                      id_value = "daily-scores",
                      description_string = build_description,
                      about_string = 'https://projects.ecoforecast.org/neon4cast-docs/',
                      about_title = "VERA Forecasting Challenge Documentation",
                      theme_title = "Scores",
                      model_documentation ="https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv",
                      destination_path = "stac/daily/scores/",
                      aws_download_path = 'bio230121-bucket01/vera4cast/scores/parquet/daily')
