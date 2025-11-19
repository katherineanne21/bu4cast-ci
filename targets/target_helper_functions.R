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

urban_metadata_sites <- function(data) {
  metadata_df <- data %>%
    group_by(state_county_site) %>%
    summarise(
              # Site Location
              site_lat = paste(unique(latitude), collapse = ", "),
              site_long = paste(unique(longitude), collapse = ", "),
              
              # PM2.5 - Daily
              PM2.5_P1D_StartDate = as.Date(ifelse(any(parameter == "PM2.5 - Daily"),
                                                   min(date_local[parameter == "PM2.5 - Daily"], na.rm = TRUE), NA)),
              PM2.5_P1D_EndDate = as.Date(ifelse(any(parameter == "PM2.5 - Daily"),
                                                 max(date_local[parameter == "PM2.5 - Daily"], na.rm = TRUE), NA)),
              PM2.5_P1D_Active = ifelse(is.na(PM2.5_P1D_EndDate), FALSE,
                                        PM2.5_P1D_EndDate >= (Sys.Date() - 180)),
              
              # PM2.5 - Hourly
              PM2.5_P1H_StartDate = as.Date(ifelse(any(parameter == "PM2.5 - Hourly"),
                                                   min(date_local[parameter == "PM2.5 - Hourly"], na.rm = TRUE), NA)),
              PM2.5_P1H_EndDate = as.Date(ifelse(any(parameter == "PM2.5 - Hourly"),
                                                 max(date_local[parameter == "PM2.5 - Hourly"], na.rm = TRUE), NA)),
              PM2.5_P1H_Active = ifelse(is.na(PM2.5_P1H_EndDate), FALSE,
                                        PM2.5_P1H_EndDate >= (Sys.Date() - 180)),
              
              # PM10 - Daily
              PM10_P1D_StartDate = as.Date(ifelse(any(parameter == "PM10 - Daily"),
                                                  min(date_local[parameter == "PM10 - Daily"], na.rm = TRUE), NA)),
              PM10_P1D_EndDate = as.Date(ifelse(any(parameter == "PM10 - Daily"),
                                                max(date_local[parameter == "PM10 - Daily"], na.rm = TRUE), NA)),
              PM10_P1D_Active = ifelse(is.na(PM10_P1D_EndDate), FALSE,
                                       PM10_P1D_EndDate >= (Sys.Date() - 180)),
              
              # PM10 - Hourly
              PM10_P1H_StartDate = as.Date(ifelse(any(parameter == "PM10 - Hourly"),
                                                  min(date_local[parameter == "PM10 - Hourly"], na.rm = TRUE), NA)),
              PM10_P1H_EndDate = as.Date(ifelse(any(parameter == "PM10 - Hourly"),
                                                max(date_local[parameter == "PM10 - Hourly"], na.rm = TRUE), NA)),
              PM10_P1H_Active = ifelse(is.na(PM10_P1H_EndDate), FALSE,
                                       PM10_P1H_EndDate >= (Sys.Date() - 180)),
              
              # NO2 - Daily
              NO2_P1D_StartDate = as.Date(ifelse(any(parameter == "NO2 - Daily"),
                                                 min(date_local[parameter == "NO2 - Daily"], na.rm = TRUE), NA)),
              NO2_P1D_EndDate = as.Date(ifelse(any(parameter == "NO2 - Daily"),
                                               max(date_local[parameter == "NO2 - Daily"], na.rm = TRUE), NA)),
              NO2_P1D_Active = ifelse(is.na(NO2_P1D_EndDate), FALSE,
                                      NO2_P1D_EndDate >= (Sys.Date() - 180)),
              
              # NO2 - Hourly
              NO2_P1H_StartDate = as.Date(ifelse(any(parameter == "NO2 - Hourly"),
                                                 min(date_local[parameter == "NO2 - Hourly"], na.rm = TRUE), NA)),
              NO2_P1H_EndDate = as.Date(ifelse(any(parameter == "NO2 - Hourly"),
                                               max(date_local[parameter == "NO2 - Hourly"], na.rm = TRUE), NA)),
              NO2_P1H_Active = ifelse(is.na(NO2_P1H_EndDate), FALSE,
                                      NO2_P1H_EndDate >= (Sys.Date() - 180))
            )
  
  return(metadata_df)
              
}

urban_metadata_pollutant <- function(data) {
  metadata_df <- data %>%
    group_by(parameter) %>%
    summarise(start_year = min(lubridate::year(date_local)),
              units_of_measure = paste(unique(units_of_measure), collapse = ", "))
  
  return(metadata_df)
}

create_urban_metadata <- function(data) {
  # Collect metadata
  pollutant_metadata = urban_metadata_pollutant(data)
  site_metadata_url = 'https://minio-s3.apps.shift.nerc.mghpcc.org/bu4cast-ci-read/challenges/targets/project_id=bu4cast/urban-targets-metadata.csv'
  
  # Format into text
  txt = c(
    "Metadata for Urban BU Forecasting Challenge",
    "-------------------------------------------",
    "",
    "The data currently being downloaded is for all sites in Suffolk (025), Essex (009),",
    "and Norfolk (021) county in Massachusetts.",
    "",
    "Here is the url to download a csv containing all current information about the",
    "sites that are currently being downloaded. This includes the site lat/long,",
    "which pollutants are active, and the start/end date of each pollutant.",
    site_metadata_url,
    "",
    "Below you will find the metadata for each pollutant. This contains the start",
    "year and units for each pollutant at each time scale.",
    "",
    paste(capture.output(write.table(pollutant_metadata, sep = "\t",
                                     row.names = FALSE, quote = FALSE)))
  )
  
  return(txt)
}
