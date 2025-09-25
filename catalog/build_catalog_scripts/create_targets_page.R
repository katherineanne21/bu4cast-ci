library(arrow)
library(dplyr)
library(gsheet)
library(readr)

config <- yaml::read_yaml('challenge_configuration.yaml')
catalog_config <- config$catalog_config

## CREATE table for column descriptions
targets_description_create <- data.frame(project_id = 'unique project identifier',
                                         site_id = 'unique site identifier',
                                         datetime = 'datetime of the observed value (ISO 8601)',
                                         duration = 'temporal duration of target (hourly = PT1H, daily = P1D, etc.); follows ISO 8601 duration convention',
                                         depth_m = 'depth (meters) in water column of observation',
                                         variable = 'observation variable',
                                         observation = 'observed value for variable')

num_target_groups <- length(config$target_groups)
target_files <- NULL
for(i in 1:num_target_groups){
  target_files <- c(target_files, config$target_groups[[i]]$targets_file)
}

targets <- read_csv(target_files, show_col_types = FALSE)

target_date_range <- targets |> dplyr::summarise(min(datetime),max(datetime))
target_min_date <- as.Date(target_date_range$`min(datetime)`)
target_max_date <- as.Date(target_date_range$`max(datetime)`)

build_description <- paste0("The targets are observations that can be used to evaluate and build forecasts.  We provide the code to access different targets as an asset.")

if (!dir.exists(paste0("../",catalog_config$targets_path))){
  dir.create(paste0("../",catalog_config$targets_path))
}

stac4cast::build_targets(table_schema = targets,
                         table_description = targets_description_create,
                         start_date = target_min_date,
                         end_date = target_max_date,
                         id_value = "targets",
                         description_string = build_description,
                         about_string = catalog_config$about_string,
                         about_title = catalog_config$about_title,
                         theme_title = "Targets",
                         destination_path = catalog_config$targets_path,
                         #link_items = stac4cast::generate_group_values(group_values = names(config$target_groups)),
                         link_items = NULL,
                         thumbnail_link = catalog_config$targets_thumbnail,
                         thumbnail_title = catalog_config$targets_thumbnail_title)
