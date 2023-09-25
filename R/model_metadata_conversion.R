library(tidyverse)
library(arrow)
library(stac4cast)
library(reticulate)
library(rlang)
library(RCurl)

googlesheets4::gs4_deauth()

registered_model_id <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/1-OsDaOoMZwPfQnz5U5aV-T9_vhTmyg92Ff5ARRunYhY/edit?usp=sharing")

binary_cols <- c(8, 26, 25, 9, 10, 11, 13, 15, 17) # (Yes, No, Not sure)

multi_choice_cols <- c(7, 18) # Just use value provided

# contact info
contact_df <- registered_model_id |>
  select(model_id, contact_name = `Contact name (or course instructor's name)`, contact_email = `Contact email (or course instructor's email)`, Institution)

# binary values
metadata_binary <- registered_model_id[,binary_cols]
metadata_binary[metadata_binary == 'Yes'] = '1'
metadata_binary[metadata_binary == 'No'] = '0'
metadata_binary[metadata_binary == 'Not sure'] = NA
binary_var_names <- c('dynamic_model', 'workshop_or_tutorial', 'instructor_contact', 'initial_conditions', 'time_varying_met_driver', 'drivers',
                      'process_error', 'multi_model_forecast_output', 'parameters')
names(metadata_binary) <- binary_var_names

# multiple choice values
metadata_multi_choice <- registered_model_id[,multi_choice_cols]
mc_var_names <- c('modeling_approach', 'data_assimilation_method')
names(metadata_multi_choice) <- mc_var_names


# extract specific model information from inventory bucket
config <- yaml::read_yaml('challenge_configuration.yaml')

inventory_df <- duckdbfs::open_dataset(glue::glue("s3://{config$inventory_bucket}/catalog"),
                                  s3_endpoint = "renc.osn.xsede.org", anonymous=TRUE) |>
  collect()

inventory_metadata <- inventory_df |>
  group_by(model_id) |>
  mutate(min_date = min(date)) |>
  mutate(max_date = max(date)) |>
  ungroup() |>
  distinct(model_id, .keep_all = TRUE) |>
  select(model_id, min_date, max_date)


# put it all together
metadata_convert <- cbind(contact_df,metadata_binary, metadata_multi_choice) |>
  right_join(inventory_metadata, by = c('model_id'))

write.csv(metadata_convert, 'model_metadata.csv', row.names = FALSE)
