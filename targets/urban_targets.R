## Urban Targets file for the BU Forecasting Challenge
## Created: 10/20/2025

library(dplyr)
library(arrow)
library(httr)
library(jsonlite)
library(lubridate)
library(RCurl)
source("target_helper_functions.R")

## Step 0: Reload data for appending

# Set Up Connection
s3_read <- arrow::s3_bucket('bu4cast-ci-read',
                            endpoint_override = 'https://minio-s3.apps.shift.nerc.mghpcc.org',
                            access_key = Sys.getenv("OSN_KEY"),
                            secret_key = Sys.getenv("OSN_SECRET"),
                            scheme = "https")

# Create file name/folder
challenge_name = 'urban'
filename = paste("challenges/targets/project_id=bu4cast/", challenge_name,
                 "-targets.csv.gz", sep = "")

# Read in old data

# Write out file and challenge name for debugging
print(paste("challenge_name:", challenge_name))
print(paste("filename:", filename))

old_data <- arrow::read_csv_arrow(s3_read$path(filename))

## Step 1: Download Data (last year and this year)

# Set variables for date, counties, and parameters

# Dates
last_year = as.numeric(format(Sys.Date(), '%Y')) - 1
today = format(Sys.Date(), '%Y%m%d')

# Parameter codes
pollutant_codes = c('88101', '81102', '44201', '42602')
pollutant_names = c('PM2.5', 'PM10', 'O3', 'NO2')

# County Codes
county_codes = c('025', '009', '021')
county_names = c('Suffolk', 'Essex', 'Norfolk')

# Write out parameter codes for debugging
print(paste0('Pollutant Codes: ', paste(pollutant_codes, collapse = ', ')))
print(paste0('Pollutant Names: ', paste(pollutant_names, collapse = ', ')))

# Create an empty df
updated_data = data.frame()

# Loop through counties
for (i in seq_along(county_codes)){
  county_code = county_codes[[i]]
  county_name = county_names[[i]]
  
  print(paste0('Downloading ', county_name, ' data...'))
  
  # Clear temp_df so we can check if data was downloaded
  temp_df1 = data.frame()
  temp_df2 = data.frame()
  
  # Download last year's data
  url = paste0(
    'https://aqs.epa.gov/data/api/sampleData/byCounty?',
    'email=', Sys.getenv("EPA_EMAIL"),
    '&key=', Sys.getenv("EPA_KEY"),
    '&param=', paste(pollutant_codes, collapse = ','),
    '&bdate=', last_year, '0101',
    '&edate=', last_year, '1231',
    '&state=25',
    '&county=', county_code
  )
  
  flag = tryCatch({
    # Download/Convert the data
    response = GET(url)
    data = fromJSON(content(response, 'text'))
    temp_df1 = data$Data
    
    # Append and overwrite data in old_data
    updated_data = rbind(updated_data, temp_df1)

    print(paste0('Processing ', last_year,' data'))
    
    # Set false to not flag
    FALSE
  }, error = function(e) {
    message(paste0('Error for ', last_year, ': ', e$message))
    
    # Set True for error flag
    TRUE
  })
  
  # Download this year's data
  url = paste0(
    'https://aqs.epa.gov/data/api/sampleData/byCounty?',
    'email=', Sys.getenv("EPA_EMAIL"),
    '&key=', Sys.getenv("EPA_KEY"),
    '&param=', paste(pollutant_codes, collapse = ','),
    '&bdate=', format(Sys.Date(), '%Y'), '0101',
    '&edate=', today,
    '&state=25',
    '&county=', county_code
  )
  
  flag = tryCatch({
    # Download/Convert the data
    response = GET(url)
    data = fromJSON(content(response, 'text'))
    temp_df2 = data$Data
    
    # Append and overwrite data in old_data
    updated_data = rbind(updated_data, temp_df2)

    print(paste0('Processing ', format(Sys.Date(), '%Y'),' data'))
    
    # Set false to not flag
    FALSE
  }, error = function(e) {
    message(paste0('Error for ', format(Sys.Date(), '%Y'), ': ', e$message))
    
    # Set True for error flag
    TRUE
  })
  
  if (flag) next

  # Let sleep after each county to prevent high influx of requests
  Sys.sleep(10)
}

# Stop if no new data downloaded
if (nrow(updated_data) == 0) {
  message("No new data downloaded. Stopping script.")
  stop("Exiting script because updated_data is empty.")
}

## Step 2: Organize

copy_updated_data = updated_data

# Set date as datetime
copy_updated_data$date_local = as.Date(copy_updated_data$date_local)

# Update duration colum to ISO 8601 format
copy_updated_data$sample_duration = gsub("1 HOUR", "PT1H", copy_updated_data$sample_duration)
copy_updated_data$sample_duration = gsub("24 HOUR", "P1D", copy_updated_data$sample_duration)

# Update variable name
copy_updated_data$parameter = gsub("PM2.5 - Local Conditions", 'PM2.5', copy_updated_data$parameter)
copy_updated_data$parameter = gsub("PM10 Total 0-10um STP", 'PM10', copy_updated_data$parameter)
copy_updated_data$parameter = gsub("Ozone", 'O3', copy_updated_data$parameter)
copy_updated_data$parameter = gsub("Nitrogen dioxide \\(NO2\\)", "NO2", copy_updated_data$parameter)
copy_updated_data <- copy_updated_data %>%
  mutate(parameter = case_when(
    parameter == "PM2.5" & sample_duration == "PT1H" ~ "PM2.5 - Hourly",
    parameter == "PM2.5" & sample_duration != "PT1H" ~ "PM2.5 - Daily",
    parameter == "PM10"  & sample_duration == "PT1H" ~ "PM10 - Hourly",
    parameter == "PM10"  & sample_duration != "PT1H" ~ "PM10 - Daily",
    parameter == "NO2"   & sample_duration == "PT1H" ~ "NO2 - Hourly",
    parameter == "NO2"   & sample_duration != "PT1H" ~ "NO2 - Daily",
    TRUE ~ parameter
  ))

copy_updated_data$state_county_site = paste(copy_updated_data$state_code,
                                            copy_updated_data$county_code,
                                            copy_updated_data$site_number,
                                      sep = '-')

# Create metadata
site_metadata_df = urban_metadata_sites(copy_updated_data)
metadata_text = create_urban_metadata(copy_updated_data)

# Select data
data = copy_updated_data[, c('state_county_site', 'date_local', 'sample_duration',
                       'parameter', 'sample_measurement')]

# Rename columns
colnames(data) = c('site_id', 'datetime', 'duration', 'variable', 'observation')

# Set project id
data$project_id = 'bu4cast'

# Reorder columns
data = data[, c('project_id', 'site_id', 'datetime', 'duration', 'variable',
                'observation')]

# Update data for the past two years
primary_keys <- c("project_id", "site_id", "date_time", "duration", "variable")

new_data <- old_data %>%
  anti_join(data, by = primary_keys) %>%
  bind_rows(data)

# Organize by date
new_data = new_data[order(data$datetime), ]

## Step 3: Write to S3 Bucket

# Set Up S3 Connection
s3_read <- arrow::s3_bucket('bu4cast-ci-read',
                            endpoint_override = 'https://minio-s3.apps.shift.nerc.mghpcc.org',
                            access_key = Sys.getenv("OSN_KEY"),
                            secret_key = Sys.getenv("OSN_SECRET"),
                            scheme = "https")

# Write to S3 bucket
# Change data if you changed the name of the cleaned data in the script
arrow::write_csv_arrow(new_data, sink = s3_read$path(filename))

# Write metadata to bucket
site_metadata_df_filename = paste("challenges/targets/project_id=bu4cast/", challenge_name,
                                        "-targets-metadata.csv", sep = "")
metadata_file_filename = paste("challenges/targets/project_id=bu4cast/", challenge_name,
                             "-targets-metadata.txt", sep = "")

arrow::write_csv_arrow(site_metadata_df, sink = s3_read$path(site_metadata_df_filename))

tmp_file <- tempfile(fileext = ".txt")
writeLines(metadata_text, con = tmp_file)
arrow::copy_files(tmp_file, s3_read$path(metadata_file_filename))

## Step 4: Clean Up and Health Check

# Remove file from working directory
csv_filename = paste(challenge_name, "-targets.csv.gz")
unlink(csv_filename)

# Health Check
# Created at www.healthchecks.io
# Currently set to bu4cast-ci-example
RCurl::getURL("https://hc-ping.com/9836911f-f0e5-485b-91a0-f1d98f11dd7a")
