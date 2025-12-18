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

max_date <- function(a, b) {
  as.Date(pmax(a, b, na.rm = TRUE), origin = "1970-01-01")
}

min_date <- function(a, b) {
  as.Date(pmin(a, b, na.rm = TRUE), origin = "1970-01-01")
}

urban_metadata_sites <- function(combined_data) {
  
  print(colnames(combined_data))
  
  # Read in all site lat longs
  s3_site_metadata_url = 'https://minio-s3.apps.shift.nerc.mghpcc.org/bu4cast-ci-read/challenges/targets/project_id=bu4cast/urban-targets-sites.csv'
  old_metadata_df_sites = read.csv(s3_site_metadata_url)
  
  # Create an updated site metadata df
  new_metadata_df_sites <- combined_data %>%
    group_by(site_id) %>%
    summarise(
      # Site Location
      site_lat = paste(unique(latitude), collapse = ", "),
      site_long = paste(unique(longitude), collapse = ", "),
      
      # PM2.5 - Daily
      PM2.5_P1D_StartDate = if (any(variable == "PM2.5 - Daily")) {
        min(datetime[variable == "PM2.5 - Daily"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM2.5_P1D_EndDate = if (any(variable == "PM2.5 - Daily")) {
        max(datetime[variable == "PM2.5 - Daily"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM2.5_P1D_Active = if (is.na(PM2.5_P1D_EndDate)) {
        FALSE
      } else {
        PM2.5_P1D_EndDate >= Sys.Date() - 180
      },
      
      # PM2.5 - Hourly
      PM2.5_P1H_StartDate = if (any(variable == "PM2.5 - Hourly")) {
        min(datetime[variable == "PM2.5 - Hourly"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM2.5_P1H_EndDate = if (any(variable == "PM2.5 - Hourly")) {
        max(datetime[variable == "PM2.5 - Hourly"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM2.5_P1H_Active = if (is.na(PM2.5_P1H_EndDate)) {
        FALSE
      } else {
        PM2.5_P1H_EndDate >= Sys.Date() - 180
      },
      
      # PM10 - Daily
      PM10_P1D_StartDate = if (any(variable == "PM10 - Daily")) {
        min(datetime[variable == "PM10 - Daily"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM10_P1D_EndDate = if (any(variable == "PM10 - Daily")) {
        max(datetime[variable == "PM10 - Daily"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM10_P1D_Active = if (is.na(PM10_P1D_EndDate)) {
        FALSE
      } else {
        PM10_P1D_EndDate >= Sys.Date() - 180
      },
      
      # PM10 - Hourly
      PM10_P1H_StartDate = if (any(variable == "PM10 - Hourly")) {
        min(datetime[variable == "PM10 - Hourly"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM10_P1H_EndDate = if (any(variable == "PM10 - Hourly")) {
        max(datetime[variable == "PM10 - Hourly"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      PM10_P1H_Active = if (is.na(PM10_P1H_EndDate)) {
        FALSE
      } else {
        PM10_P1H_EndDate >= Sys.Date() - 180
      },
      
      # O3
      O3_StartDate = if (any(variable == "O3")) {
        min(datetime[variable == "O3"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      O3_EndDate = if (any(variable == "O3")) {
        max(datetime[variable == "O3"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      O3_Active = if (is.na(O3_EndDate)) {
        FALSE
      } else {
        O3_EndDate >= Sys.Date() - 180
      },
      
      # NO2 - Daily
      NO2_P1D_StartDate = if (any(variable == "NO2 - Daily")) {
        min(datetime[variable == "NO2 - Daily"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      NO2_P1D_EndDate = if (any(variable == "NO2 - Daily")) {
        max(datetime[variable == "NO2 - Daily"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      NO2_P1D_Active = if (is.na(NO2_P1D_EndDate)) {
        FALSE
      } else {
        NO2_P1D_EndDate >= Sys.Date() - 180
      },
      
      # NO2 - Hourly
      NO2_P1H_StartDate = if (any(variable == "NO2 - Hourly")) {
        min(datetime[variable == "NO2 - Hourly"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      NO2_P1H_EndDate = if (any(variable == "NO2 - Hourly")) {
        max(datetime[variable == "NO2 - Hourly"], na.rm = TRUE)
      } else {
        as.Date(NA)
      },
      NO2_P1H_Active = if (is.na(NO2_P1H_EndDate)) {
        FALSE
      } else {
        NO2_P1H_EndDate >= Sys.Date() - 180
      }
    )
  
  # Update the dates and active columns
  metadata_df_joined <- full_join(
    old_metadata_df_sites,
    new_metadata_df_sites,
    by = "site_id",
    suffix = c("_old", "_new")
  )
  
  # Keep only the non null cells for each site and keep end date where there are both old and new
  metadata_df <- metadata_df_joined %>%
    transmute(
      site_id,
      
      # Site Location
      site_lat  = coalesce(site_lat_new, site_lat_old),
      site_long = coalesce(site_long_new, site_long_old),
      
      # PM2.5 Daily
      PM2.5_P1D_StartDate = min_date(PM2.5_P1D_StartDate_old,
                                     PM2.5_P1D_StartDate_new),
      
      PM2.5_P1D_EndDate   = max_date(PM2.5_P1D_EndDate_old,
                                     PM2.5_P1D_EndDate_new),
      
      PM2.5_P1D_Active = PM2.5_P1D_EndDate >= Sys.Date() - 180,
      
      # PM2.5 Hourly
      PM2.5_P1H_StartDate = min_date(PM2.5_P1H_StartDate_old,
                                     PM2.5_P1H_StartDate_new),
      
      PM2.5_P1H_EndDate   = max_date(PM2.5_P1H_EndDate_old,
                                     PM2.5_P1H_EndDate_new),
      
      PM2.5_P1H_Active = PM2.5_P1H_EndDate >= Sys.Date() - 180,
      
      # PM10 Daily
      PM10_P1D_StartDate = min_date(PM10_P1D_StartDate_old,
                                    PM10_P1D_StartDate_new),
      
      PM10_P1D_EndDate   = max_date(PM10_P1D_EndDate_old,
                                    PM10_P1D_EndDate_new),
      
      PM10_P1D_Active = PM10_P1D_EndDate >= Sys.Date() - 180,
      
      # PM10 Hourly
      PM10_P1H_StartDate = min_date(PM10_P1H_StartDate_old,
                                    PM10_P1H_StartDate_new),
      
      PM10_P1H_EndDate   = max_date(PM10_P1H_EndDate_old,
                                    PM10_P1H_EndDate_new),
      
      PM10_P1H_Active = PM10_P1H_EndDate >= Sys.Date() - 180,
      
      # O3
      O3_StartDate = min_date(O3_StartDate_old,
                              O3_StartDate_new),
      
      O3_EndDate   = max_date(O3_EndDate_old,
                              O3_EndDate_new),
      
      O3_Active = O3_EndDate >= Sys.Date() - 180,
      
      # NO2 Daily
      NO2_P1D_StartDate = min_date(NO2_P1D_StartDate_old,
                                   NO2_P1D_StartDate_new),
      
      NO2_P1D_EndDate   = max_date(NO2_P1D_EndDate_old,
                                   NO2_P1D_EndDate_new),
      
      NO2_P1D_Active = NO2_P1D_EndDate >= Sys.Date() - 180,
      
      # NO2 Hourly 
      NO2_P1H_StartDate = min_date(NO2_P1H_StartDate_old,
                                   NO2_P1H_StartDate_new),
      
      NO2_P1H_EndDate   = max_date(NO2_P1H_EndDate_old,
                                   NO2_P1H_EndDate_new),
      
      NO2_P1H_Active = NO2_P1H_EndDate >= Sys.Date() - 180
    )
  
  # Return updated df
  return(metadata_df)
              
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
