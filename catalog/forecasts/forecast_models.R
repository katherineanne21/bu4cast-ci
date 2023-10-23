library(arrow)
library(dplyr)

source('catalog/R/stac_functions.R')
config <- yaml::read_yaml('challenge_configuration.yaml')
catalog_config <- config$catalog_config

# for(i in 1:length(config$variable_groups)){
#
#   variable_group <- names(config$variable_groups)[i]
#
#   for(j in 1:length(config$variable_groups[[i]]$variable)){
#   variable <- config$variable_groups[[i]]$variable[j]
#   duration <- config$variable_groups[[i]]$duration[j]
#   }
# }


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
theme <- 'daily'
reference_datetime <- '2023-09-01'
site_id <- 'fcre'
model_id <- 'climatology'

forecast_theme_df <- arrow::open_dataset(arrow::s3_bucket(config$forecasts_bucket, endpoint_override = config$endpoint, anonymous = TRUE)) |>
  filter(model_id == model_id, site_id = site_id, reference_datetime = reference_datetime)

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

#variable_group <- c('test_daily')


build_forecast_scores(table_schema = forecast_theme_df,
                      theme_id = 'Forecasts',
                      table_description = forecast_description_create,
                      start_date = forecast_min_date,
                      end_date = forecast_max_date,
                      id_value = "daily-forecasts",
                      description_string = build_description,
                      about_string = catalog_config$about_string,
                      about_title = catalog_config$about_title,
                      theme_title = "Forecasts",
                      #model_documentation = NULL,
                      destination_path = catalog_config$forecast_path,
                      aws_download_path = catalog_config$aws_download_path,
                      #link_items = generate_group_values(group_values = variable_groups),
                      link_items = generate_group_values(group_values = names(config$variable_groups)),
                      thumbnail_link = catalog_config$forecasts_thumbnail,
                      thumbnail_title = catalog_config$forecasts_thumbnail_title)

## create separate JSON for model landing page

build_group_variables(table_schema = forecast_theme_df,
                      theme_id = 'models',
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
                      group_var_items = generate_model_items(model_list = theme_models$model_id))

## create models

## READ IN MODEL METADATA
googlesheets4::gs4_deauth()

registered_model_id <- googlesheets4::read_sheet(config$model_metadata_gsheet)


forecast_sites <- c()

## loop over model ids and extract components if present in metadata table
for (m in theme_models$model_id){
  print(m)
  model_date_range <- forecast_data_df |> filter(model_id == m) |> dplyr::summarise(min(date),max(date))
  model_min_date <- model_date_range$`min(date)`
  model_max_date <- model_date_range$`max(date)`

  model_sites <- forecast_data_df |> filter(model_id == m) |> distinct(site_id)
  model_vars <- forecast_data_df |> filter(model_id == m) |> distinct(variable)

  model_var_duration_df <- forecast_data_df |> filter(model_id == m) |> distinct(variable,duration) |>
    mutate(duration_name = ifelse(duration == 'P1D', 'daily', duration)) |>
    mutate(duration_name = ifelse(duration == 'PT1H', 'hourly', duration)) |>
    mutate(duration_name = ifelse(duration == 'PT30M', '30min', duration)) |>
    mutate(duration_name = ifelse(duration == 'P1W', 'weekly', duration))

  model_var_duration_df$full_variable_name <- paste0(model_var_duration_df$variable, "_", model_var_duration_df$duration_name)


  forecast_sites <- append(forecast_sites,  get_site_coords(sites = model_sites$site_id))

  idx = which(registered_model_id$model_id == m)

  build_model(model_id = m,
              theme_id = m,
              team_name = registered_model_id$`Long name of the model (can include spaces)`[idx],
              model_description = registered_model_id[idx,"Describe your modeling approach in your own words."][[1]],
              start_date = model_min_date,
              end_date = model_max_date,
              var_values = model_vars$variable,
              duration_names = model_var_duration_df$duration_name,
              site_values = model_sites$site_id,
              model_documentation = registered_model_id,
              destination_path = paste0(catalog_config$forecast_path,"models/model_items"),
              aws_download_path = config$forecasts_bucket, # CHANGE THIS BUCKET NAME
              theme_title = m,
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

  group_description <- paste0('This page includes variables for the ',names(config$variable_groups[i]),' group.')

  build_group_variables(table_schema = forecast_theme_df,
                        theme_id = names(config$variable_groups[i]),
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
                        group_var_items = generate_group_variable_items(variables = names(config$variable_groups[i])))




  for(j in length(config$variable_groups[[i]]$variable)){

    var_name <- config$variable_groups[[i]]$variable[j]
    duration_name <- config$variable_groups[[i]]$duration[j]

    duration_name_title <- duration_name

    if (duration_name == 'P1D'){
      duration_name_title <- 'daily'
    } else if(duration_name == 'PT1H'){
      duraiton_name_title <- 'hourly'
    } else if(duration_name == 'PT30M'){
      duration_name_title == '30min'
    } else if(duration_name == 'P1W'){
      duration_name_title == 'weekly'
    }

    var_name_full <- paste0(var_name,'_',duration_name_title)

    if (!dir.exists(paste0(catalog_config$forecast_path,names(config$variable_groups)[i],'/',var_name_full))){
      dir.create(paste0(catalog_config$forecast_path,names(config$variable_groups)[i],'/',var_name_full))
    }

    var_data <- forecast_data_df |>
      filter(variable == var_name)

    var_date_range <- var_data |> dplyr::summarise(min(date),max(date))
    var_min_date <- var_date_range$`min(date)`
    var_max_date <- var_date_range$`max(date)`

    var_models <- var_data |> distinct(model_id)

    var_description <- paste0('This page includes all models for the ',var_name_full,' variable.')

    build_group_variables(table_schema = forecast_theme_df,
                          theme_id = var_name_full,
                          table_description = forecast_description_create,
                          start_date = var_min_date,
                          end_date = var_max_date,
                          id_value = var_name_full,
                          description_string = var_description,
                          about_string = catalog_config$about_string,
                          about_title = catalog_config$about_title,
                          theme_title = var_name_full,
                          destination_path = file.path(catalog_config$forecast_path,names(config$variable_groups)[i],var_name_full),
                          aws_download_path = var_data$path[1],
                          group_var_items = generate_variable_model_items(model_list = var_models$model_id))

  }
}

#   # for (d in unique(names(config$variable_groups[[i]]$duration[d]))){ # make a specific duration group for each variable (Bio_daily, Bio_houly, etc.)
#   #   #print(names(config$variable_groups[[i]]$duration[d]))
#   #   for(j in config$variable_groups[[i]]$variable[j]){
#   #
#   #   }
#   #
#   # }
#
#   for (v in variable_list[[i]]){ # Make variable JSONS within each group
#     print(v)
#
#     if (!dir.exists(paste0(catalog_config$forecast_path,variable_groups[i],'/',v))){
#       dir.create(paste0(catalog_config$forecast_path,variable_groups[i],'/',v))
#     }
#
#     var_data <- forecast_data_df |>
#       filter(variable == v)
#
#     var_date_range <- var_data |> dplyr::summarise(min(date),max(date))
#     var_min_date <- var_date_range$`min(date)`
#     var_max_date <- var_date_range$`max(date)`
#
#     var_models <- var_data |> distinct(model_id)
#
#     var_description <- paste0('This page includes all models for the ',v,' variable.')
#
#     build_group_variables(table_schema = forecast_theme_df,
#                           theme_id = v,
#                           table_description = forecast_description_create,
#                           start_date = var_min_date,
#                           end_date = var_max_date,
#                           id_value = v,
#                           description_string = var_description,
#                           about_string = catalog_config$about_string,
#                           about_title = catalog_config$about_title,
#                           theme_title = v,
#                           destination_path = file.path(catalog_config$forecast_path,variable_groups[i],v),
#                           aws_download_path = var_data$path[1],
#                           group_var_items = generate_variable_model_items(model_list = var_models$model_id))
#
#   }
#
#
# }
