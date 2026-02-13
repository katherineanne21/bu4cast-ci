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

library(readr)

max_date <- function(a, b) {
  as.Date(pmax(a, b, na.rm = TRUE), origin = "1970-01-01")
}

min_date <- function(a, b) {
  as.Date(pmin(a, b, na.rm = TRUE), origin = "1970-01-01")
}

urban_metadata_sites <- function(combined_data) {
  
  print(paste0("Running target_helper_functions.R - urban_metadata_sites at ", Sys.time()))
  
  # Read in all site lat longs
  s3_site_metadata_url = 'https://minio-s3.apps.shift.nerc.mghpcc.org/bu4cast-ci-read/challenges/targets/project_id=bu4cast/urban-targets-sites.csv'
  
  old_metadata_df_sites <- read_csv(
    s3_site_metadata_url,
    col_types = cols(
      # ID and Location: Character
      site_id = col_character(),
      site_lat = col_character(),
      site_long = col_character(),
      
      # Start/End Date: Date
      PM2.5_P1D_StartDate = col_date(),
      PM2.5_P1D_EndDate = col_date(),
      PM2.5_P1H_StartDate = col_date(),
      PM2.5_P1H_EndDate = col_date(),
      PM10_P1D_StartDate = col_date(),
      PM10_P1D_EndDate = col_date(),
      PM10_P1H_StartDate = col_date(),
      PM10_P1H_EndDate = col_date(),
      O3_StartDate = col_date(),
      O3_EndDate = col_date(),
      NO2_P1D_StartDate = col_date(),
      NO2_P1D_EndDate = col_date(),
      NO2_P1H_StartDate = col_date(),
      NO2_P1H_EndDate = col_date(),
      
      # Active: Logical
      PM2.5_P1D_Active = col_logical(),
      PM2.5_P1H_Active = col_logical(),
      PM10_P1D_Active = col_logical(),
      PM10_P1H_Active = col_logical(),
      O3_Active = col_logical(),
      NO2_P1D_Active = col_logical(),
      NO2_P1H_Active = col_logical(),
      
      # Defaults
      .default = col_character()
    )
  )
  
  
  # Create an updated site metadata df
  combined_data$date_local <- as.Date(combined_data$date_local)
  
  new_metadata_df_sites <- combined_data %>%
    group_by(site_id) %>%
    summarise(
      # Site Location
      site_lat = paste(unique(latitude), collapse = ", "),
      site_long = paste(unique(longitude), collapse = ", "),
      
      # PM2.5 - Daily
      PM2.5_P1D_StartDate = if (any(parameter == "PM2.5 - Daily")) {
        min(date_local[parameter == "PM2.5 - Daily"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM2.5_P1D_EndDate = if (any(parameter == "PM2.5 - Daily")) {
        max(date_local[parameter == "PM2.5 - Daily"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM2.5_P1D_Active = if (is.na(PM2.5_P1D_EndDate)) {
        FALSE
      } else {
        PM2.5_P1D_EndDate >= (Sys.Date() - 180)
      },
      
      # PM2.5 - Hourly
      PM2.5_P1H_StartDate = if (any(parameter == "PM2.5 - Hourly")) {
        min(date_local[parameter == "PM2.5 - Hourly"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM2.5_P1H_EndDate = if (any(parameter == "PM2.5 - Hourly")) {
        max(date_local[parameter == "PM2.5 - Hourly"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM2.5_P1H_Active = if (is.na(PM2.5_P1H_EndDate)) {
        FALSE
      } else {
        PM2.5_P1H_EndDate >= (Sys.Date() - 180)
      },
      
      # PM10 - Daily
      PM10_P1D_StartDate = if (any(parameter == "PM10 - Daily")) {
        min(date_local[parameter == "PM10 - Daily"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM10_P1D_EndDate = if (any(parameter == "PM10 - Daily")) {
        max(date_local[parameter == "PM10 - Daily"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM10_P1D_Active = if (is.na(PM10_P1D_EndDate)) {
        FALSE
      } else {
        PM10_P1D_EndDate >= (Sys.Date() - 180)
      },
      
      # PM10 - Hourly
      PM10_P1H_StartDate = if (any(parameter == "PM10 - Hourly")) {
        min(date_local[parameter == "PM10 - Hourly"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM10_P1H_EndDate = if (any(parameter == "PM10 - Hourly")) {
        max(date_local[parameter == "PM10 - Hourly"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM10_P1H_Active = if (is.na(PM10_P1H_EndDate)) {
        FALSE
      } else {
        PM10_P1H_EndDate >= (Sys.Date() - 180)
      },
      
      # O3
      O3_StartDate = if (any(parameter == "O3")) {
        min(date_local[parameter == "O3"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      O3_EndDate = if (any(parameter == "O3")) {
        max(date_local[parameter == "O3"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      O3_Active = if (is.na(O3_EndDate)) {
        FALSE
      } else {
        O3_EndDate >= (Sys.Date() - 180)
      },
      
      # NO2 - Daily
      NO2_P1D_StartDate = if (any(parameter == "NO2 - Daily")) {
        min(date_local[parameter == "NO2 - Daily"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      NO2_P1D_EndDate = if (any(parameter == "NO2 - Daily")) {
        max(date_local[parameter == "NO2 - Daily"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      NO2_P1D_Active = if (is.na(NO2_P1D_EndDate)) {
        FALSE
      } else {
        NO2_P1D_EndDate >= (Sys.Date() - 180)
      },
      
      # NO2 - Hourly
      NO2_P1H_StartDate = if (any(parameter == "NO2 - Hourly")) {
        min(date_local[parameter == "NO2 - Hourly"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      NO2_P1H_EndDate = if (any(parameter == "NO2 - Hourly")) {
        max(date_local[parameter == "NO2 - Hourly"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      NO2_P1H_Active = if (is.na(NO2_P1H_EndDate)) {
        FALSE
      } else {
        NO2_P1H_EndDate >= (Sys.Date() - 180)
      }
    )
  
  # Merge old and new sites
  metadata_df_joined <- full_join(
    old_metadata_df_sites,
    new_metadata_df_sites,
    by = "site_id",
    suffix = c("_old", "_new")
  )
  
  # Create new columns to store final data
  final_date_cols = c('PM2.5_P1D_StartDate', 'PM2.5_P1D_EndDate', 'PM2.5_P1D_Active',
                      'PM2.5_P1H_StartDate', 'PM2.5_P1H_EndDate', 'PM2.5_P1H_Active',
                      'PM10_P1D_StartDate', 'PM10_P1D_EndDate', 'PM10_P1D_Active',
                      'PM10_P1H_StartDate', 'PM10_P1H_EndDate', 'PM10_P1H_Active',
                      'O3_StartDate', 'O3_EndDate', 'O3_Active',
                      'NO2_P1D_StartDate', 'NO2_P1D_EndDate', 'NO2_P1D_Active',
                      'NO2_P1H_StartDate', 'NO2_P1H_EndDate', 'NO2_P1H_Active')
  
  # Identify all specific type columns
  start_date_cols = final_date_cols[grepl("StartDate$", final_date_cols)]
  end_date_cols = final_date_cols[grepl("EndDate$", final_date_cols)]
  active_cols = final_date_cols[grepl("Active$", final_date_cols)]

  # Create empty columns
  metadata_df_joined[start_date_cols] = as.Date(NA)
  metadata_df_joined[end_date_cols] = as.Date(NA)
  metadata_df_joined[active_cols] = NA
  
  # Merge based on if it's new, old, or both
  metadata_df_final <- metadata_df_joined %>%
    mutate(
      # Keep site_lat and site_long
      site_lat = coalesce(site_lat_new, site_lat_old),
      site_long = coalesce(site_long_new, site_long_old)
    ) %>%
    mutate(
      across(
        all_of(final_date_cols),
        ~ case_when(
          
          # New only: use _new
          is.na(get(paste0(cur_column(), "_old"))) &
            !is.na(get(paste0(cur_column(), "_new"))) ~
            get(paste0(cur_column(), "_new")),
          
          # Old only: use _old
          !is.na(get(paste0(cur_column(), "_old"))) &
            is.na(get(paste0(cur_column(), "_new"))) ~
            get(paste0(cur_column(), "_old")),
          
          # Both: use _new End Date and Active, and _old Start Date
          cur_column() %in% end_date_cols ~
            get(paste0(cur_column(), "_new")),
          
          cur_column() %in% active_cols ~
            get(paste0(cur_column(), "_new")),
          
          cur_column() %in% start_date_cols ~
            get(paste0(cur_column(), "_old")),
          
          # Default
          TRUE ~ get(paste0(cur_column(), "_new"))
        )
      )
    ) %>%
    select(-ends_with("_old"), -ends_with("_new")) # Remove _old and _new
  
  # Fix column order
  id_cols <- c("site_id", "site_lat", "site_long")
  final_cols <- c(id_cols, final_date_cols)
  metadata_df_final <- metadata_df_final[, final_cols]
  
  # Return updated df
  return(metadata_df_final)
              
}

urban_metadata_pollutant <- function(new_data){
  
  # Read in from S3 bucket the old pollutant metadata
  s3_units_metadata_url = 'https://minio-s3.apps.shift.nerc.mghpcc.org/bu4cast-ci-read/challenges/targets/project_id=bu4cast/urban-targets-units.csv'
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
    select(parameter, start_year) %>% 
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
