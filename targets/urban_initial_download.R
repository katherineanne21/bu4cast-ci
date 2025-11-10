library(httr)
library(jsonlite)
library(dplyr)
library(ggplot2)

# Download credentials
email <- 'krein21@bu.edu'
key <- 'goldmallard76'

# Find codes for parameters
#url <- paste0('https://aqs.epa.gov/data/api/list/parametersByClass?email=', email ,'&key=', key, '&pc=criteria')
#response <- GET(url)
#params <- fromJSON(content(response, as = 'text'))
#parameters = params$Data

# Parameter codes
pollutant_codes = list('88101', '81102', '44201', '42602')
pollutant_names = list('PM2.5', 'PM10', 'O3', 'NO2')

# County Codes
county_codes = list('025', '009', '021')
county_names = list('Suffolk', 'Essex', 'Norfolk')

# Dates
current_year = as.numeric(format(Sys.Date(), '%Y'))
today = format(Sys.Date(), '%Y%m%d')
years_list = as.character(1970:current_year)
current_year = as.character(current_year)

# Prep dataframe
big_df = data.frame()

# Loop through all years and pollutants and counties
for (i in seq_along(county_codes)){
  # Massachusetts = 25
  county_code = county_codes[[i]]
  county_name = county_names[[i]]
  
  print(paste0('Downloading ', county_name, ' data...'))
  
  for (year in years_list){
    
    # Clear temp_df so we can check if data was downloaded
    temp_df = data.frame()
    
    # If we are in our current year, download from Jan 1 to today
    if (year == current_year){
      url = paste0(
        'https://aqs.epa.gov/data/api/sampleData/byCounty?',
        'email=', email,
        '&key=', key,
        '&param=', paste(pollutant_codes, collapse = ','),
        '&bdate=', current_year, '0101',
        '&edate=', today,
        '&state=25',
        '&county=', county_code
      )
    } else { # Otherwise, download from Jan 1 to Dec 31 for the year
      url = paste0(
        'https://aqs.epa.gov/data/api/sampleData/byCounty?',
        'email=', email,
        '&key=', key,
        '&param=', paste(pollutant_codes, collapse = ','),
        '&bdate=', year, '0101',
        '&edate=', year, '1231',
        '&state=25',
        '&county=', county_code
      )
    }
    
    flag = tryCatch({
      # Download/Convert the data
      response = GET(url)
      data = fromJSON(content(response, 'text'))
      temp_df = data$Data
      
      # Append data to big df
      big_df = rbind(big_df, temp_df)
      
      # Set false to not flag
      FALSE
    }, error = function(e) {
      message(paste0('Error for ', year, ' ', pol_name, ': ', e$message))
      
      # Set True for error flag
      TRUE
    })
    if (flag) next
    
    if (length(temp_df) > 0){
      print(paste0('Processing ', year, ' data...'))
      # Sleep for 7 seconds to prevent more than 10 requests per minute
      Sys.sleep(7)
    }
  }
  
  # Let sleep after each county to prevent high influx of requests
  Sys.sleep(30)
}

## Create metadata file
copy_big_df <- big_df

# Set date as datetime
copy_big_df$date_local = as.Date(copy_big_df$date_local)

# Update duration colum to ISO 8601 format
copy_big_df$sample_duration = gsub("1 HOUR", "PT1H", copy_big_df$sample_duration)
copy_big_df$sample_duration = gsub("24 HOUR", "P1D", copy_big_df$sample_duration)

# Update variable name
copy_big_df$parameter = gsub("PM2.5 - Local Conditions", 'PM2.5', copy_big_df$parameter)
copy_big_df$parameter = gsub("PM10 Total 0-10um STP", 'PM10', copy_big_df$parameter)
copy_big_df$parameter = gsub("Ozone", 'O3', copy_big_df$parameter)
copy_big_df$parameter = gsub("Nitrogen dioxide \\(NO2\\)", "NO2", copy_big_df$parameter)
copy_big_df <- copy_big_df %>%
  mutate(parameter = case_when(
    parameter == "PM2.5" & sample_duration == "PT1H" ~ "PM2.5 - Hourly",
    parameter == "PM2.5" & sample_duration != "PT1H" ~ "PM2.5 - Daily",
    parameter == "PM10"  & sample_duration == "PT1H" ~ "PM10 - Hourly",
    parameter == "PM10"  & sample_duration != "PT1H" ~ "PM10 - Daily",
    parameter == "NO2"   & sample_duration == "PT1H" ~ "NO2 - Hourly",
    parameter == "NO2"   & sample_duration != "PT1H" ~ "NO2 - Daily",
    TRUE ~ parameter
  ))

metadata_df <- copy_big_df %>%
  group_by(parameter) %>%
  summarise(start_year = min(lubridate::year(date_local)),
            units_of_measure = paste(unique(units_of_measure), collapse = ", ")) 

## Organize data 

# Select data
copy_big_df$state_county_site = paste(copy_big_df$state_code,
                                      copy_big_df$county_code,
                                      copy_big_df$site_number,
                                      sep = '-')

data = copy_big_df[, c('state_county_site', 'date_local', 'sample_duration',
                       'parameter', 'sample_measurement')]

# Rename columns
colnames(data) = c('site_id', 'datetime', 'duration', 'variable', 'observation')

# Set project id
data$project_id = 'bu4cast'

# Reorder columns
data = data[, c('project_id', 'site_id', 'datetime', 'duration', 'variable',
                'observation')]

# Write to file
filename = '/Users/katherineanne/Desktop/BU Work/Dietze/Dietze_Work/urban_targets.csv.gz'
write.csv(data, filename, row.names = FALSE)

## Showcase data timelines per pollutant

pollutant_list <- split(data, data$variable)

for (name in names(pollutant_list)) {
  pol_df <- pollutant_list[[name]]
  
  df_range <- pol_df %>%
    group_by(site_id) %>%
    summarise(start = min(datetime), end = max(datetime))
  
  ggplot(df_range, aes(y = site_id)) +
    geom_segment(aes(x = start, xend = end, yend = site_id), linewidth = 1) +
    scale_x_date(date_breaks = '2 years', date_labels = '%Y') +
    labs(x = 'Years', y = 'Site Number', 
         title = paste0('Data Availability by Site for ', name)) +
    theme_minimal() +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = 'none'  
    )
  
  plotname = paste0('/Users/katherineanne/Desktop/BU Work/Dietze/Dietze_Work/site_activity_', 
                    name, '.png')
  ggsave(plotname, width = 8, height = 5, dpi = 300)

}
