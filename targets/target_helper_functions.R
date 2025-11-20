# Create urban metadata
# Site specific
# Columns:
  # Site_Code
  # Site_Name
  # Site_Lat
  # Site_Long
  # PM2.5_P1D_Active
  # PM2.5_P1D_StartDate
  # PM2.5_P1D_EndDate

urban_metadata_sites <- function(combined_data) {
  
  # Read in all site lat longs
  s3_site_metadata_url = 'https://minio-s3.apps.shift.nerc.mghpcc.org/bu4cast-ci-read/challenges/targets/project_id=bu4cast/urban_targets_sites.csv'
  old_metadata_df_sites = read.csv(s3_site_metadata_url)
  
  # Create an updated site metadata df
  new_metadata_df_sites <- combined_data %>%
    group_by(site_id) %>%
    summarise(
              # PM2.5 - Daily
              PM2.5_P1D_StartDate = as.Date(ifelse(any(variable == "PM2.5 - Daily"),
                                                   min(datetime[variable == "PM2.5 - Daily"], na.rm = TRUE), NA)),
              PM2.5_P1D_EndDate = as.Date(ifelse(any(variable == "PM2.5 - Daily"),
                                                 max(datetime[variable == "PM2.5 - Daily"], na.rm = TRUE), NA)),
              PM2.5_P1D_Active = ifelse(is.na(PM2.5_P1D_EndDate), FALSE,
                                        PM2.5_P1D_EndDate >= (Sys.Date() - 180)),
              
              # PM2.5 - Hourly
              PM2.5_P1H_StartDate = as.Date(ifelse(any(variable == "PM2.5 - Hourly"),
                                                   min(datetime[variable == "PM2.5 - Hourly"], na.rm = TRUE), NA)),
              PM2.5_P1H_EndDate = as.Date(ifelse(any(variable == "PM2.5 - Hourly"),
                                                 max(datetime[variable == "PM2.5 - Hourly"], na.rm = TRUE), NA)),
              PM2.5_P1H_Active = ifelse(is.na(PM2.5_P1H_EndDate), FALSE,
                                        PM2.5_P1H_EndDate >= (Sys.Date() - 180)),
              
              # PM10 - Daily
              PM10_P1D_StartDate = as.Date(ifelse(any(variable == "PM10 - Daily"),
                                                  min(datetime[variable == "PM10 - Daily"], na.rm = TRUE), NA)),
              PM10_P1D_EndDate = as.Date(ifelse(any(variable == "PM10 - Daily"),
                                                max(datetime[variable == "PM10 - Daily"], na.rm = TRUE), NA)),
              PM10_P1D_Active = ifelse(is.na(PM10_P1D_EndDate), FALSE,
                                       PM10_P1D_EndDate >= (Sys.Date() - 180)),
              
              # PM10 - Hourly
              PM10_P1H_StartDate = as.Date(ifelse(any(variable == "PM10 - Hourly"),
                                                  min(datetime[variable == "PM10 - Hourly"], na.rm = TRUE), NA)),
              PM10_P1H_EndDate = as.Date(ifelse(any(variable == "PM10 - Hourly"),
                                                max(datetime[variable == "PM10 - Hourly"], na.rm = TRUE), NA)),
              PM10_P1H_Active = ifelse(is.na(PM10_P1H_EndDate), FALSE,
                                       PM10_P1H_EndDate >= (Sys.Date() - 180)),
              
              # NO2 - Daily
              NO2_P1D_StartDate = as.Date(ifelse(any(variable == "NO2 - Daily"),
                                                 min(datetime[variable == "NO2 - Daily"], na.rm = TRUE), NA)),
              NO2_P1D_EndDate = as.Date(ifelse(any(variable == "NO2 - Daily"),
                                               max(datetime[variable == "NO2 - Daily"], na.rm = TRUE), NA)),
              NO2_P1D_Active = ifelse(is.na(NO2_P1D_EndDate), FALSE,
                                      NO2_P1D_EndDate >= (Sys.Date() - 180)),
              
              # NO2 - Hourly
              NO2_P1H_StartDate = as.Date(ifelse(any(variable == "NO2 - Hourly"),
                                                 min(datetime[variable == "NO2 - Hourly"], na.rm = TRUE), NA)),
              NO2_P1H_EndDate = as.Date(ifelse(any(variable == "NO2 - Hourly"),
                                               max(datetime[variable == "NO2 - Hourly"], na.rm = TRUE), NA)),
              NO2_P1H_Active = ifelse(is.na(NO2_P1H_EndDate), FALSE,
                                      NO2_P1H_EndDate >= (Sys.Date() - 180))
            )
  
  # Update the dates and active columns
  metadata_df <- full_join(
    old_metadata_df_sites,
    new_metadata_df_sites,
    by = "site_id",
    suffix = c("_old", "_new")
  )
  
  # Identify new columns that are not in the og dataset
  new_cols = names(new_metadata_df_sites)[names(new_metadata_df_sites) != "site_id"]
  
  # Overwrite data when it changes
  for (col in new_cols) {
    old_col <- paste0(col, "_old")
    new_col <- paste0(col, "_new")
    
    metadata_df[[col]] <- ifelse(
      is.na(metadata_df[[new_col]]),
      metadata_df[[old_col]],
      metadata_df[[new_col]]
    )
  }
  
  # Remove extra columns
  metadata_df <- metadata_df %>%
    select(site_id, all_of(new_cols), everything()) %>%
    select(-ends_with("_old"), -ends_with("_new"))
  
  # Return updated df
  return(metadata_df)
              
}

urban_metadata_pollutant <- function(new_data){
  
  # Read in from S3 bucket the old pollutant metadata
  s3_units_metadata_url = 'https://minio-s3.apps.shift.nerc.mghpcc.org/bu4cast-ci-read/challenges/targets/project_id=bu4cast/urban_targets_units.csv'
  old_metadata_df_units = read.csv(s3_units_metadata_url)
  
  # Create new metadata df
  new_metadata_df_units <- new_data %>%
    group_by(parameter) %>%
    summarise(units_of_measure = paste(unique(units_of_measure), collapse = ", "))
  
  # If different, update the units
  
  # Join old and new data columns
  merged <- full_join(
    old_metadata_df_units,
    new_metadata_df_units,
    by = "parameter",
    suffix = c("_old", "_new")
  )
  
  # Select parameter column
  metadata_df <- merged %>% 
    select(parameter) %>% 
    mutate(units_of_measure = NA_character_)
  
  # Cycle through each row and identify new comma seperated list of units
  for (i in seq_len(nrow(merged))) {
    
    old_units <- merged$units_of_measure_old[i]
    new_units <- merged$units_of_measure_new[i]
    
    # Leave as NA if both NA
    if (is.na(old_units) & is.na(new_units)) {
      metadata_df$units_of_measure[i] <- NA_character_
      next
    }
    
    # Create new comma seperated list
    old_vec <- if (!is.na(old_units)) strsplit(old_units, ",")[[1]] else character(0)
    new_vec <- if (!is.na(new_units)) strsplit(new_units, ",")[[1]] else character(0)
    
    old_vec <- trimws(old_vec)
    new_vec <- trimws(new_vec)

    combined <- unique(c(old_vec, new_vec))
    
    # Add back into metadata_df
    metadata_df$units_of_measure[i] <- paste(combined, collapse = ", ")
  }
  
  # Return updated df
  return(metadata_df)
}