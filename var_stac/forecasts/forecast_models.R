library(arrow)
library(dplyr)

source('var_stac/R/stac_functions.R')
config <- yaml::read_yaml('challenge_configuration.yaml')

variable_groups <- c('Biological', 'Physical')
variable_list <- list(c('Chla_ugL'),
                      c('Temp_C'))

## CREATE table for column descriptions
forecast_description_create <- data.frame(datetime = 'ISO 8601(ISO 2019)datetime the forecast starts from (a.k.a. issue time); Only needed if more than one reference_datetime is stored in a single file. Forecast lead time is thus datetime-reference_datetime. In a hindcast the reference_datetime will be earlier than the time the hindcast was actually produced (see pubDate in Section3). Date times are allowed to be earlier than the reference_datetime if a reanalysis/reforecast is run before the start of the forecast period. This variable was called start_time before v0.5 of the EFI standard.',
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

## just read in example forecast to extract schema information -- ask about better ways of doing this
theme <- 'daily'
reference_datetime <- '2023-09-01'
site_id <- 'fcre'
model_id <- 'climatology'

forecast_theme_df <- arrow::open_dataset(arrow::s3_bucket(config$forecasts_bucket, endpoint_override = "renc.osn.xsede.org", anonymous = TRUE)) |>
  filter(model_id == model_id, site_id = site_id, reference_datetime = reference_datetime)

## identify model ids from bucket -- used in generate model items function
forecast_data_df <- duckdbfs::open_dataset(glue::glue("s3://{config$inventory_bucket}/catalog"),
                                  s3_endpoint = "renc.osn.xsede.org", anonymous=TRUE) |>
  collect()

theme_models <- forecast_data_df |>
  distinct(model_id)

forecast_date_range <- forecast_data_df |> dplyr::summarise(min(date),max(date))
forecast_max_date <- forecast_date_range$`min(date)`
forecast_min_date <- forecast_date_range$`max(date)`

build_description <- "The catalog contains forecasts for the VERA Forecasting Challenge Daily theme. The forecasts are the raw forecasts that include all ensemble members (if a forecast represents uncertainty using an ensemble).  Due to the size of the raw forecasts, we recommend accessing the scores (summaries of the forecasts) to analyze forecasts (unless you need the individual ensemble members). You can access the forecasts at the top level of the dataset where all models, variables, and dates that forecasts were produced (reference_datetime) are available. The code to access the entire dataset is provided as an asset. Given the size of the forecast catalog, it can be time-consuming to access the data at the full dataset level. For quicker access to the forecasts for a particular model (model_id), we also provide the code to access the data at the model_id level as an asset for each model."

#variable_group <- c('test_daily')


build_forecast_scores(table_schema = forecast_theme_df,
                      theme_id = 'Forecasts',
                      table_description = forecast_description_create,
                      start_date = forecast_min_date,
                      end_date = forecast_max_date,
                      id_value = "daily-forecasts",
                      description_string = build_description,
                      about_string = 'https://projects.ecoforecast.org/neon4cast-docs/',
                      about_title = "VERA Forecasting Challenge Documentation",
                      theme_title = "Forecasts",
                      model_documentation ="https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv",
                      destination_path = "var_stac/forecasts/",
                      aws_download_path = 'bio230121-bucket01/vera4cast/forecasts/parquet/daily',
                      link_items = generate_group_values(group_values = variable_groups),
                      thumbnail_link = "https://raw.githubusercontent.com/addelany/vera4cast/main/thumbnails/banner-2.jpg",
                      thumbnail_title = 'Falling Creek Reservoir')




## create separate JSON for model landing page

build_group_variables(table_schema = forecast_theme_df,
                      theme_id = 'models',
                      table_description = forecast_description_create,
                      start_date = forecast_min_date,
                      end_date = forecast_max_date,
                      id_value = "models",
                      description_string = build_description,
                      about_string = 'https://projects.ecoforecast.org/neon4cast-docs/',
                      about_title = "VERA Forecasting Challenge Documentation",
                      theme_title = "Models",
                      model_documentation ="https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv",
                      destination_path = "var_stac/forecasts/models",
                      aws_download_path = 'bio230121-bucket01/vera4cast/forecasts/parquet/daily',
                      group_var_items = generate_model_items(model_list = theme_models$model_id))

## create models

## READ IN MODEL METADATA
googlesheets4::gs4_deauth()

registered_model_id <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/1f177dpaxLzc4UuQ4_SJV9JWIbQPlilVnEztyvZE6aSU/edit?usp=sharing")


forecast_sites <- c()

## loop over model ids and extract components if present in metadata table
for (m in theme_models$model_id){
  print(m)
  model_date_range <- forecast_data_df |> filter(model_id == m) |> dplyr::summarise(min(date),max(date))
  model_min_date <- model_date_range$`min(date)`
  model_max_date <- model_date_range$`max(date)`

  model_sites <- forecast_data_df |> filter(model_id == m) |> distinct(site_id)
  model_vars <- forecast_data_df |> filter(model_id == m) |> distinct(variable)


  forecast_sites <- append(forecast_sites,  get_site_coords(sites = model_sites$site_id))

  idx = which(registered_model_id$model_id == m)

# STILL WORKING ON GETTING DATA ASSETS INTO CORRECT FORMAT
  build_model(model_id = m,
              theme_id = m,
              team_name = registered_model_id$`Long name of the model (can include spaces)`[idx],
              model_description = registered_model_id[idx,"Describe your modeling approach in your own words."][[1]],
              start_date = model_min_date,
              end_date = model_max_date,
              var_values = model_vars$variable,
              site_values = model_sites$site_id,
              model_documentation = registered_model_id,
              destination_path = "var_stac/forecasts/models/model_items",
              description_path = "stac/daily/forecasts/models/asset-description.Rmd", # MIGHT REMOVE THIS
              aws_download_path = config$forecasts_bucket, # CHANGE THIS BUCKET NAME
              theme_title = m,
              collection_name = 'forecasts',
              thumbnail_image_name = NULL,
              table_schema = forecast_theme_df,
              table_description = forecast_description_create)
}


## BUILD VARIABLE GROUPS

for (i in 1:length(variable_groups)){
  print(variable_groups[i])

  if (!dir.exists(paste0("var_stac/forecasts/",variable_groups[i]))){
    dir.create(paste0("var_stac/forecasts/",variable_groups[i]))
  }

  build_group_variables(table_schema = forecast_theme_df,
                        theme_id = variable_groups[i],
                        table_description = forecast_description_create,
                        start_date = forecast_min_date,
                        end_date = forecast_max_date,
                        id_value = variable_groups[i],
                        description_string = build_description,
                        about_string = 'https://projects.ecoforecast.org/neon4cast-docs/',
                        about_title = "VERA Forecasting Challenge Documentation",
                        theme_title = variable_groups[i],
                        model_documentation ="https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv",
                        destination_path = paste0("var_stac/forecasts/",variable_groups[i]),
                        aws_download_path = 'bio230121-bucket01/vera4cast/forecasts/parquet/daily',
                        group_var_items = generate_group_variable_items(variables = variable_list[[i]]))

  for (v in variable_list[[i]]){ # Make variable JSONS within each group
    print(v)

    if (!dir.exists(paste0("var_stac/forecasts/",variable_groups[i],'/',v))){
      dir.create(paste0("var_stac/forecasts/",variable_groups[i],'/',v))
    }

    var_data <- forecast_data_df |>
      filter(variable == v)

    var_date_range <- var_data |> dplyr::summarise(min(date),max(date))
    var_max_date <- var_date_range$`min(date)`
    var_min_date <- var_date_range$`max(date)`

    var_models <- var_data |> distinct(model_id)

    build_group_variables(table_schema = forecast_theme_df,
                          theme_id = v,
                          table_description = forecast_description_create,
                          start_date = var_min_date,
                          end_date = var_max_date,
                          id_value = v,
                          description_string = build_description,
                          about_string = 'https://projects.ecoforecast.org/neon4cast-docs/',
                          about_title = "VERA Forecasting Challenge Documentation",
                          theme_title = v,
                          model_documentation ="https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv",
                          destination_path = file.path("var_stac/forecasts",variable_groups[i],v),
                          aws_download_path = var_data$path[1],
                          group_var_items = generate_variable_model_items(model_list = var_models$model_id))

  }


}
