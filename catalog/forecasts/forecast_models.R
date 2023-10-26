library(arrow)
library(dplyr)
library(gsheet)
library(readr)

#source('catalog/R/stac_functions.R')
config <- yaml::read_yaml('challenge_configuration.yaml')
catalog_config <- config$catalog_config

# file.sources = list.files(c("../stac4cast/R"), full.names=TRUE,
#                           ignore.case=TRUE)
# sapply(file.sources,source,.GlobalEnv)

## CREATE table for column descriptions
forecast_description_create <- data.frame(datetime = 'datetime of the forecasted value (ISO 8601)',
                                          site_id = 'For forecasts that are not on a spatial grid, use of a site dimension that maps to a more detailed geometry (points, polygons, etc.) is allowable. In general this would be documented in the external metadata (e.g., alook-up table that provides lon and lat)',
                                          family = 'For ensembles: “ensemble.” Default value if unspecified for probability distributions: Name of the statistical distribution associated with the reported statistics. The “sample” distribution is synonymous with “ensemble.”For summary statistics: “summary.”',
                                          parameter = 'ensemble member or distribution parameter',
                                          variable = 'name of forecasted variable',
                                          prediction = 'predicted value for variable',
                                          pub_datetime = 'datetime that forecast was submitted',
                                          reference_datetime = 'datetime that the forecast was initiated (horizon = 0)',
                                          model_id = 'unique model identifier',
                                          reference_date = 'date that the forecast was initiated',
                                          project_id = 'unique identifier for the forecast project',
                                          depth_m = 'depth (meters) in water column of prediction',
                                          duration = 'temporal duration of forecast (hourly, daily, etc.); follows ISO 8601 duration convention')


## CHANGE THE WAY TO READ THE SCHEMA
## just read in example forecast to extract schema information -- ask about better ways of doing this
# theme <- 'daily'
# reference_datetime <- '2023-09-01'
# site_id <- 'fcre'
# model_id <- 'climatology'

forecast_theme_df <- arrow::open_dataset(arrow::s3_bucket(config$forecasts_bucket, endpoint_override = config$endpoint, anonymous = TRUE)) #|>
  #filter(model_id == model_id, site_id = site_id, reference_datetime = reference_datetime)
# NOTE IF NOT USING FILTER -- THE stac4cast::build_table_columns() NEEDS TO BE UPDATED
    #(USE strsplit(forecast_theme_df$ToString(), "\n") INSTEAD OF strsplit(forecast_theme_df[[1]]$ToString(), "\n"))

## identify model ids from bucket -- used in generate model items function
forecast_data_df <- duckdbfs::open_dataset(glue::glue("s3://{config$inventory_bucket}/catalog"),
                                  s3_endpoint = config$endpoint, anonymous=TRUE) |>
  collect()

theme_models <- forecast_data_df |>
  distinct(model_id)

forecast_date_range <- forecast_data_df |> dplyr::summarise(min(date),max(date))
forecast_min_date <- forecast_date_range$`min(date)`
forecast_max_date <- forecast_date_range$`max(date)`

build_description <- paste0("The catalog contains forecasts for the ", config$challenge_long_name,". The forecasts are the raw forecasts that include all ensemble members (if a forecast represents uncertainty using an ensemble).  Due to the size of the raw forecasts, we recommend accessing the scores (summaries of the forecasts) to analyze forecasts (unless you need the individual ensemble members). You can access the forecasts at the top level of the dataset where all models, variables, and dates that forecasts were produced (reference_datetime) are available. The code to access the entire dataset is provided as an asset. Given the size of the forecast catalog, it can be time-consuming to access the data at the full dataset level. For quicker access to the forecasts for a particular model (model_id), we also provide the code to access the data at the model_id level as an asset for each model.")

stac4cast::build_forecast_scores(table_schema = forecast_theme_df,
                      #theme_id = 'Forecasts',
                      table_description = forecast_description_create,
                      start_date = forecast_min_date,
                      end_date = forecast_max_date,
                      id_value = "daily-forecasts",
                      description_string = build_description,
                      about_string = catalog_config$about_string,
                      about_title = catalog_config$about_title,
                      theme_title = "Forecasts",
                      destination_path = catalog_config$forecast_path,
                      aws_download_path = catalog_config$aws_download_path,
                      link_items = stac4cast::generate_group_values(group_values = names(config$variable_groups)),
                      thumbnail_link = catalog_config$forecasts_thumbnail,
                      thumbnail_title = catalog_config$forecasts_thumbnail_title)

## create separate JSON for model landing page

stac4cast::build_group_variables(table_schema = forecast_theme_df,
                      table_description = forecast_description_create,
                      start_date = forecast_min_date,
                      end_date = forecast_max_date,
                      id_value = "models",
                      description_string = build_description,
                      about_string = catalog_config$about_string,
                      about_title = catalog_config$about_title,
                      theme_title = "Models",
                      #model_documentation = NULL,
                      destination_path = paste0(catalog_config$forecast_path,"models"),
                      aws_download_path = catalog_config$aws_download_path,
                      group_var_items = stac4cast::generate_model_items(model_list = theme_models$model_id))

## CREATE MODELS

## READ IN MODEL METADATA
# googlesheets4::gs4_deauth()
#
# registered_model_id <- googlesheets4::read_sheet(config$model_metadata_gsheet)

registered_model_id <- gsheet2tbl(config$model_metadata_gsheet)

forecast_sites <- c()

## LOOP OVER MODEL IDS AND CREATE JSONS
for (m in theme_models$model_id){
  print(m)
  model_date_range <- forecast_data_df |> filter(model_id == m) |> dplyr::summarise(min(date),max(date))
  model_min_date <- model_date_range$`min(date)`
  model_max_date <- model_date_range$`max(date)`

  model_sites <- forecast_data_df |> filter(model_id == m) |> distinct(site_id)
  model_vars <- forecast_data_df |> filter(model_id == m) |> distinct(variable)

  model_var_duration_df <- forecast_data_df |> filter(model_id == m) |> distinct(variable,duration) |>
    mutate(duration_name = ifelse(duration == 'P1D', 'daily', duration)) |>
    mutate(duration_name = ifelse(duration == 'PT1H', 'hourly', duration_name)) |>
    mutate(duration_name = ifelse(duration == 'PT30M', '30min', duration_name)) |>
    mutate(duration_name = ifelse(duration == 'P1W', 'weekly', duration_name))

  model_var_duration_df$full_variable_name <- paste0(model_var_duration_df$variable, "_", model_var_duration_df$duration_name)


  forecast_sites <- append(forecast_sites,  stac4cast::get_site_coords(site_metadata = catalog_config$site_metadata_url,
                                                            sites = model_sites$site_id))

  idx = which(registered_model_id$model_id == m)

  stac4cast::build_model(model_id = m,
              team_name = registered_model_id$`Long name of the model (can include spaces)`[idx],
              model_description = registered_model_id[idx,"Describe your modeling approach in your own words."][[1]],
              start_date = model_min_date,
              end_date = model_max_date,
              var_values = model_vars$variable,
              duration_names = model_var_duration_df$duration_name,
              site_values = model_sites$site_id,
              site_table = catalog_config$site_metadata_url,
              model_documentation = registered_model_id,
              destination_path = paste0(catalog_config$forecast_path,"models/model_items"),
              aws_download_path = config$forecasts_bucket, # CHANGE THIS BUCKET NAME
              collection_name = 'forecasts',
              thumbnail_image_name = NULL,
              table_schema = forecast_theme_df,
              table_description = forecast_description_create)
}


## BUILD VARIABLE GROUPS

for (i in 1:length(config$variable_groups)){ ## organize variable groups
  print(names(config$variable_groups)[i])

  if (!dir.exists(paste0(catalog_config$forecast_path,names(config$variable_groups[i])))){
    dir.create(paste0(catalog_config$forecast_path,names(config$variable_groups[i])))
  }

  for(j in 1:length(config$variable_groups[[i]]$variable)){ # FOR EACH VARIABLE WITHIN A MODEL GROUP

    ## restructure variable names
    var_values <- config$variable_groups[[i]]$variable
    var_name <- config$variable_groups[[i]]$variable[j]

    ## create new vector to store duration names
    duration_values <- config$variable_groups[[i]]$duration
    duration_values[which(duration_values == 'P1D')] <- 'daily'
    duration_values[which(duration_values == 'PT1H')] <- 'hourly'
    duration_values[which(duration_values == 'PT30M')] <- '30min'
    duration_values[which(duration_values == 'P1W')] <- 'weekly'

    var_name_combined_list <- paste0(var_values, '_',duration_values)

    ## CREATE VARIABLE GROUP JSONS
    group_description <- paste0('This page includes variables for the ',names(config$variable_groups[i]),' group.')

    stac4cast::build_group_variables(table_schema = forecast_theme_df,
                          #theme_id = names(config$variable_groups[i]),
                          table_description = forecast_description_create,
                          start_date = forecast_min_date,
                          end_date = forecast_max_date,
                          id_value = names(config$variable_groups[i]),
                          description_string = group_description,
                          about_string = catalog_config$about_string,
                          about_title = catalog_config$about_title,
                          theme_title = names(config$variable_groups[i]),
                          destination_path = paste0(catalog_config$forecast_path,names(config$variable_groups[i])),
                          aws_download_path = catalog_config$aws_download_path,
                          group_var_items = stac4cast::generate_group_variable_items(variables = var_name_combined_list))

    if (!dir.exists(paste0(catalog_config$forecast_path,names(config$variable_groups)[i],'/',var_name_combined_list[j]))){
      dir.create(paste0(catalog_config$forecast_path,names(config$variable_groups)[i],'/',var_name_combined_list[j]))
    }

    var_data <- forecast_data_df |>
      filter(variable == var_name)

    var_date_range <- var_data |> dplyr::summarise(min(date),max(date))
    var_min_date <- var_date_range$`min(date)`
    var_max_date <- var_date_range$`max(date)`

    var_models <- var_data |> distinct(model_id)

    var_description <- paste0('This page includes all models for the ',var_name_combined_list[j],' variable.')

    stac4cast::build_group_variables(table_schema = forecast_theme_df,
                          #theme_id = var_name_combined_list[j],
                          table_description = forecast_description_create,
                          start_date = var_min_date,
                          end_date = var_max_date,
                          id_value = var_name_combined_list[j],
                          description_string = var_description,
                          about_string = catalog_config$about_string,
                          about_title = catalog_config$about_title,
                          theme_title = var_name_combined_list[j],
                          destination_path = file.path(catalog_config$forecast_path,names(config$variable_groups)[i],var_name_combined_list[j]),
                          aws_download_path = var_data$path[1],
                          group_var_items = stac4cast::generate_variable_model_items(model_list = var_models$model_id))

  }
}
