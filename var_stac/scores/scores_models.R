library(arrow)
library(dplyr)

source('var_stac/R/stac_functions.R')
config <- yaml::read_yaml('challenge_configuration.yaml')

variable_groups <- c('Biological', 'Physical')
variable_list <- list(c('Chla_ugL_mean'),
                      c('Temp_C_mean'))

## CREATE table for column descriptions
scores_description_create <- data.frame(reference_datetime ='ISO 8601(ISO 2019) datetime the forecast starts from (a.k.a. issue time); Only needed if more than one reference_datetime is stored in a single file. Forecast lead time is thus datetime-reference_datetime. In a hindcast the reference_date time will be earlier than the time the hindcast was actually produced (see pubDate in Section 3). Datetimes are allowed to be earlier than the reference_datetime if analysis/reforecast is run before the start of the forecast period. This variable was called start_time before v0.5 of the EFI standard.',
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
                                 duration = 'temporal duration of forecast (hourly, daily, etc.)',
                                 depth_m = 'depths included in forecast',
                                 model_id = 'unique model identifier',
                                 date = 'ISO 8601 (ISO 2019) datetime being predicted; follows CF convention http://cfconventions.org/cf-conventions/cf-conventions.html#time-coordinate. This variable was called time before v0.5of the EFI convention. For time-integrated variables (e.g., cumulative net primary productivity), one should specify the start_datetime and end_datetime as two variables, instead of the single datetime. If this is not provided the datetime is assumed to be the MIDPOINT of the integration period.')


## just read in example forecast to extract schema information -- ask about better ways of doing this
theme <- 'daily'
reference_datetime <- '2023-09-01'
site_id <- 'fcre'
model_id <- 'climatology'

scores_theme_df <- arrow::open_dataset(arrow::s3_bucket(config$scores_bucket, endpoint_override = "renc.osn.xsede.org", anonymous = TRUE)) |>
  filter(model_id == model_id, site_id = site_id, reference_datetime = reference_datetime)

## identify model ids from bucket -- used in generate model items function
scores_data_df <- duckdbfs::open_dataset(glue::glue("s3://{config$inventory_bucket}/catalog"),
                                  s3_endpoint = "renc.osn.xsede.org", anonymous=TRUE) |>
  collect()

theme_models <- scores_data_df |>
  distinct(model_id)

scores_date_range <- scores_data_df |> dplyr::summarise(min(date),max(date))
scores_min_date <- scores_date_range$`min(date)`
scores_max_date <- scores_date_range$`max(date)`

build_description <- "The catalog contains scores for the VERA Forecasting Challenge theme.  The scores are summaries of the forecasts (i.e., mean, median, confidence intervals), matched observations (if available), and scores (metrics of how well the model distribution compares to observations). You can access the scores at the top level of the dataset where all models, variables, and dates that forecasts were produced (reference_datetime) are available. The code to access the entire dataset is provided as an asset. Given the size of the scores catalog, it can be time-consuming to access the data at the full dataset level. For quicker access to the scores for a particular model (model_id), we also provide the code to access the data at the model_id level as an asset for each model."

#variable_group <- c('test_daily')


build_forecast_scores(table_schema = scores_theme_df,
                      theme_id = 'Scores',
                      table_description = scores_description_create,
                      start_date = scores_min_date,
                      end_date = scores_max_date,
                      id_value = "daily-scores",
                      description_string = build_description,
                      about_string = 'https://projects.ecoforecast.org/neon4cast-docs/',
                      about_title = "VERA Forecasting Challenge Documentation",
                      theme_title = "Scores",
                      model_documentation ="https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv",
                      destination_path = "var_stac/scores/",
                      aws_download_path = 'bio230121-bucket01/vera4cast/scores/parquet/daily',
                      link_items = generate_group_values(group_values = variable_groups),
                      thumbnail_link = "https://raw.githubusercontent.com/addelany/vera4cast/main/thumbnails/banner-2.jpg",
                      thumbnail_title = 'Falling Creek Reservoir')




## create separate JSON for model landing page

build_group_variables(table_schema = scores_theme_df,
                      theme_id = 'models',
                      table_description = scores_description_create,
                      start_date = scores_min_date,
                      end_date = scores_max_date,
                      id_value = "models",
                      description_string = build_description,
                      about_string = 'https://projects.ecoforecast.org/neon4cast-docs/',
                      about_title = "VERA Forecasting Challenge Documentation",
                      theme_title = "Models",
                      model_documentation ="https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv",
                      destination_path = "var_stac/scores/models",
                      aws_download_path = 'bio230121-bucket01/vera4cast/scores/parquet/daily',
                      group_var_items = generate_model_items(model_list = theme_models$model_id))

## create models

## READ IN MODEL METADATA
googlesheets4::gs4_deauth()

registered_model_id <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/1f177dpaxLzc4UuQ4_SJV9JWIbQPlilVnEztyvZE6aSU/edit?usp=sharing")


scores_sites <- c()

## loop over model ids and extract components if present in metadata table
for (m in theme_models$model_id){
  print(m)
  model_date_range <- scores_data_df |> filter(model_id == m) |> dplyr::summarise(min(date),max(date))
  model_min_date <- model_date_range$`min(date)`
  model_max_date <- model_date_range$`max(date)`

  model_sites <- scores_data_df |> filter(model_id == m) |> distinct(site_id)
  model_vars <- scores_data_df |> filter(model_id == m) |> distinct(variable)


  scores_sites <- append(scores_sites,  get_site_coords(sites = model_sites$site_id))

  idx = which(registered_model_id$model_id == m)

  build_model(model_id = m,
              theme_id = m,
              team_name = registered_model_id$`Long name of the model (can include spaces)`[idx],
              model_description = registered_model_id[idx,"Describe your modeling approach in your own words."][[1]],
              start_date = model_min_date,
              end_date = model_max_date,
              var_values = model_vars$variable,
              site_values = model_sites$site_id,
              model_documentation = registered_model_id,
              destination_path = "var_stac/scores/models/model_items",
              description_path = "stac/daily/scores/models/asset-description.Rmd", # MIGHT REMOVE THIS
              aws_download_path = config$scores_bucket, # CHANGE THIS BUCKET NAME
              theme_title = m,
              collection_name = 'scores',
              thumbnail_image_name = NULL,
              table_schema = scores_theme_df,
              table_description = scores_description_create)
}


## BUILD VARIABLE GROUPS

for (i in 1:length(variable_groups)){
  print(variable_groups[i])

  if (!dir.exists(paste0("var_stac/scores/",variable_groups[i]))){
    dir.create(paste0("var_stac/scores/",variable_groups[i]))
  }

  group_description <- paste0('This page includes variables for the ',variable_groups[i],' group.')

  build_group_variables(table_schema = scores_theme_df,
                        theme_id = variable_groups[i],
                        table_description = scores_description_create,
                        start_date = scores_min_date,
                        end_date = scores_max_date,
                        id_value = variable_groups[i],
                        description_string = group_description,
                        about_string = 'https://projects.ecoforecast.org/neon4cast-docs/',
                        about_title = "VERA Forecasting Challenge Documentation",
                        theme_title = variable_groups[i],
                        model_documentation ="https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv",
                        destination_path = paste0("var_stac/scores/",variable_groups[i]),
                        aws_download_path = 'bio230121-bucket01/vera4cast/forecasts/parquet/daily',
                        group_var_items = generate_group_variable_items(variables = variable_list[[i]]))

  for (v in variable_list[[i]]){ # Make variable JSONS within each group
    print(v)

    if (!dir.exists(paste0("var_stac/scores/",variable_groups[i],'/',v))){
      dir.create(paste0("var_stac/scores/",variable_groups[i],'/',v))
    }

    var_data <- scores_data_df |>
      filter(variable == v)

    var_date_range <- var_data |> dplyr::summarise(min(date),max(date))
    var_min_date <- var_date_range$`min(date)`
    var_max_date <- var_date_range$`max(date)`

    var_models <- var_data |> distinct(model_id)

    var_description <- paste0('This page includes all models for the ',v,' variable.')

    build_group_variables(table_schema = scores_theme_df,
                          theme_id = v,
                          table_description = scores_description_create,
                          start_date = var_min_date,
                          end_date = var_max_date,
                          id_value = v,
                          description_string = var_description,
                          about_string = 'https://projects.ecoforecast.org/neon4cast-docs/',
                          about_title = "VERA Forecasting Challenge Documentation",
                          theme_title = v,
                          model_documentation ="https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv",
                          destination_path = file.path("var_stac/scores",variable_groups[i],v),
                          aws_download_path = var_data$path[1],
                          group_var_items = generate_variable_model_items(model_list = var_models$model_id))

  }


}
