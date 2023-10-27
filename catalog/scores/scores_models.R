library(arrow)
library(dplyr)
library(gsheet)
library(readr)

#source('catalog/R/stac_functions.R')
config <- yaml::read_yaml('challenge_configuration.yaml')
catalog_config <- config$catalog_config

# names(config$variable_groups)
# variable_groups <- names(config$variable_groups)
# variable_list <- config$variable_groups


## CREATE table for column descriptions
scores_description_create <- data.frame(reference_datetime ='datetime that the forecast was initiated (horizon = 0)',
                                 site_id = 'For forecasts that are not on a spatial grid, use of a site dimension that maps to a more detailed geometry (points, polygons, etc.) is allowable. In general this would be documented in the external metadata (e.g., alook-up table that provides lon and lat); however in netCDF this could be handled by the CF Discrete Sampling Geometry data model.',
                                 datetime = 'datetime of the forecasted value (ISO 8601)',
                                 family = 'For ensembles: “ensemble.” Default value if unspecified For probability distributions: Name of the statistical distribution associated with the reported statistics. The “sample” distribution is synonymous with “ensemble.” For summary statistics: “summary.”If this dimension does not vary, it is permissible to specify family as a variable attribute if the file format being used supports this (e.g.,netCDF).',
                                 variable = 'name of forecasted variable',
                                 observation = 'observed value for variable',
                                 crps = 'crps forecast score',
                                 logs = 'logs forecast score',
                                 mean = 'mean forecast prediction',
                                 median = 'median forecast prediction',
                                 sd = 'standard deviation forecasts',
                                 quantile97.5 = 'upper 97.5 percentile value of forecast',
                                 quantile02.5 = 'upper 2.5 percentile value of forecast',
                                 quantile90 = 'upper 90 percentile value of forecast',
                                 quantile10 = 'upper 10 percentile value of forecast',
                                 duration = 'temporal duration of forecast (hourly = PT1H, daily = P1D, etc.); follows ISO 8601 duration convention',
                                 depth_m = 'depth (meters) in water column of prediction',
                                 model_id = 'unique model identifier',
                                 date = 'ISO 8601 (ISO 2019) date of the predicted value; follows CF convention http://cfconventions.org/cf-conventions/cf-conventions.html#time-coordinate. This variable was called time before v0.5of the EFI convention. For time-integrated variables (e.g., cumulative net primary productivity), one should specify the start_datetime and end_datetime as two variables, instead of the single datetime. If this is not provided the datetime is assumed to be the MIDPOINT of the integration period.',
                                 pub_datetime = 'datetime that forecast was submitted',
                                 project_id = 'unique project identifier')


## just read in example forecast to extract schema information -- ask about better ways of doing this
# theme <- 'daily'
# reference_datetime <- '2023-09-01'
# site_id <- 'fcre'
# model_id <- 'climatology'

scores_theme_df <- arrow::open_dataset(arrow::s3_bucket(config$scores_bucket, endpoint_override = config$endpoint, anonymous = TRUE)) #|>
  #filter(model_id == model_id, site_id = site_id, reference_datetime = reference_datetime)

## identify model ids from bucket -- used in generate model items function
scores_data_df <- duckdbfs::open_dataset(glue::glue("s3://{config$inventory_bucket}/catalog"),
                                  s3_endpoint = config$endpoint, anonymous=TRUE) |>
  collect()

theme_models <- scores_data_df |>
  distinct(model_id)

scores_date_range <- scores_data_df |> dplyr::summarise(min(date),max(date))
scores_min_date <- scores_date_range$`min(date)`
scores_max_date <- scores_date_range$`max(date)`

build_description <- paste0("The catalog contains forecasts for the ", config$challenge_long_name,". The scores are summaries of the forecasts (i.e., mean, median, confidence intervals), matched observations (if available), and scores (metrics of how well the model distribution compares to observations). You can access the scores at the top level of the dataset where all models, variables, and dates that forecasts were produced (reference_datetime) are available. The code to access the entire dataset is provided as an asset. Given the size of the scores catalog, it can be time-consuming to access the data at the full dataset level. For quicker access to the scores for a particular model (model_id), we also provide the code to access the data at the model_id level as an asset for each model.")

#variable_group <- c('test_daily')


stac4cast::build_forecast_scores(table_schema = scores_theme_df,
                      #theme_id = 'Scores',
                      table_description = scores_description_create,
                      start_date = scores_min_date,
                      end_date = scores_max_date,
                      id_value = "daily-scores",
                      description_string = build_description,
                      about_string = catalog_config$about_string,
                      about_title = catalog_config$about_title,
                      theme_title = "Scores",
                      destination_path = catalog_config$scores_path,
                      aws_download_path = catalog_config$aws_download_path,
                      link_items = stac4cast::generate_group_values(group_values = names(config$variable_groups)),
                      thumbnail_link = catalog_config$scores_thumbnail,
                      thumbnail_title = catalog_config$scores_thumbnail_title)

## create separate JSON for model landing page

stac4cast::build_group_variables(table_schema = scores_theme_df,
                      #theme_id = 'models',
                      table_description = scores_description_create,
                      start_date = scores_min_date,
                      end_date = scores_max_date,
                      id_value = "models",
                      description_string = build_description,
                      about_string = catalog_config$about_string,
                      about_title = catalog_config$about_title,
                      theme_title = "Models",
                      destination_path = paste0(catalog_config$scores_path,"models"),
                      aws_download_path = catalog_config$aws_download_path,
                      group_var_items = stac4cast::generate_model_items(model_list = theme_models$model_id))

## CREATE MODELS

## READ IN MODEL METADATA
# googlesheets4::gs4_deauth()
#
# registered_model_id <- googlesheets4::read_sheet(config$model_metadata_gsheet)

registered_model_id <- gsheet2tbl(config$model_metadata_gsheet)

scores_sites <- c()

## loop over model ids and extract components if present in metadata table
for (m in theme_models$model_id){
  print(m)
  model_date_range <- scores_data_df |> filter(model_id == m) |> dplyr::summarise(min(date),max(date))
  model_min_date <- model_date_range$`min(date)`
  model_max_date <- model_date_range$`max(date)`

  model_sites <- scores_data_df |> filter(model_id == m) |> distinct(site_id)
  model_vars <- scores_data_df |> filter(model_id == m) |> distinct(variable)

  model_var_duration_df <- scores_data_df |> filter(model_id == m) |> distinct(variable,duration) |>
    mutate(duration_name = ifelse(duration == 'P1D', 'daily', duration)) |>
    mutate(duration_name = ifelse(duration == 'PT1H', 'hourly', duration_name)) |>
    mutate(duration_name = ifelse(duration == 'PT30M', '30min', duration_name)) |>
    mutate(duration_name = ifelse(duration == 'P1W', 'weekly', duration_name))

  model_var_duration_df$full_variable_name <- paste0(model_var_duration_df$variable, "_", model_var_duration_df$duration_name)

  scores_sites <- append(scores_sites,  stac4cast::get_site_coords(site_metadata = catalog_config$site_metadata_url,
                                                                   sites = model_sites$site_id))

  idx = which(registered_model_id$model_id == m)

  stac4cast::build_model(model_id = m,
              #theme_id = m,
              team_name = registered_model_id$`Long name of the model (can include spaces)`[idx],
              model_description = registered_model_id[idx,"Describe your modeling approach in your own words."][[1]],
              start_date = model_min_date,
              end_date = model_max_date,
              var_values = model_vars$variable,
              duration_names = model_var_duration_df$duration_name,
              site_values = model_sites$site_id,
              site_table = catalog_config$site_metadata_url,
              model_documentation = registered_model_id,
              destination_path = paste0(catalog_config$scores_path,"models/model_items"),
              aws_download_path = config$scores_bucket, # CHANGE THIS BUCKET NAME
              #theme_title = m,
              collection_name = 'scores',
              thumbnail_image_name = NULL,
              table_schema = scores_theme_df,
              table_description = scores_description_create)
}


## BUILD VARIABLE GROUPS

for (i in 1:length(config$variable_groups)){
  print(names(config$variable_groups)[i])

  if (!dir.exists(paste0(catalog_config$scores_path,names(config$variable_groups[i])))){
    dir.create(paste0(catalog_config$scores_path,names(config$variable_groups[i])))
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

    stac4cast::build_group_variables(table_schema = scores_theme_df,
                          #theme_id = names(config$variable_groups[i]),
                          table_description = scores_description_create,
                          start_date = scores_min_date,
                          end_date = scores_max_date,
                          id_value = names(config$variable_groups[i]),
                          description_string = group_description,
                          about_string = catalog_config$about_string,
                          about_title = catalog_config$about_title,
                          theme_title = names(config$variable_groups[i]),
                          destination_path = paste0(catalog_config$scores_path,names(config$variable_groups[i])),
                          aws_download_path = catalog_config$aws_download_path,
                          group_var_items = stac4cast::generate_group_variable_items(variables = var_name_combined_list))

    if (!dir.exists(paste0(catalog_config$scores_path,names(config$variable_groups)[i],'/',var_name_combined_list[j]))){
      dir.create(paste0(catalog_config$scores_path,names(config$variable_groups)[i],'/',var_name_combined_list[j]))
    }

    var_data <- scores_data_df |>
      filter(variable == var_name)

    var_date_range <- var_data |> dplyr::summarise(min(date),max(date))
    var_min_date <- var_date_range$`min(date)`
    var_max_date <- var_date_range$`max(date)`

    var_models <- var_data |> distinct(model_id)

    var_description <- paste0('This page includes all models for the ',var_name_combined_list[j],' variable.')

    stac4cast::build_group_variables(table_schema = scores_theme_df,
                          #theme_id = var_name_combined_list[j],
                          table_description = scores_description_create,
                          start_date = var_min_date,
                          end_date = var_max_date,
                          id_value = var_name_combined_list[j],
                          description_string = var_description,
                          about_string = catalog_config$about_string,
                          about_title = catalog_config$about_title,
                          theme_title = var_name_combined_list[j],
                          destination_path = file.path(catalog_config$scores_path,names(config$variable_groups)[i],var_name_combined_list[j]),
                          aws_download_path = var_data$path[1],
                          group_var_items = stac4cast::generate_variable_model_items(model_list = var_models$model_id))

  }


}
