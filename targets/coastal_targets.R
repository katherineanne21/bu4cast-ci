## Coastal Targets Script for BU FRP Coastal
## Downloads buoy and MODIS data, formats to standard, writes to S3
## Author: Cami Webb, cwebb16@bu.edu
## Created: 02-01-2025

library(rerddap)
library(arrow)
library(dplyr)
library(lubridate)
library(foreach)      
library(doParallel)   
library(doSNOW)
library(ncdf4)
library(jsonlite)
library(RCurl)
library(tidyr)

# Source NASA download function (Author: Dongchen Zhang)
source("targets/R/NASA_DAAC_download.R")

## Configuration

challenge_name <- "coastal"
project_id <- "bu4cast"

# Create file name/folder
filename = paste("challenges/targets/project_id=bu4cast/", challenge_name,
                 "-targets.csv", sep = "")

# Write out file and challenge name for debugging
print(paste("challenge_name:", challenge_name))
print(paste("filename:", filename))

# Date range
start_date <- as.character("2006-01-01")  # Based on buoy record which starts 1/1/06
end_date <- as.character(Sys.Date() - 1)  # Yesterday

# Buoy location (mean lat/lon of buoy, need for MODIS box)
buoy_lat <- 43.022942490079 
buoy_lon <- -70.5475341233827  

# Read old data
s3_read <- arrow::s3_bucket(
  "bu4cast-ci-read",
  endpoint_override = "https://minio-s3.apps.shift.nerc.mghpcc.org",
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)

old_data <- tryCatch(
  arrow::read_csv_arrow(s3_read$path(filename)) %>% as.data.frame(),
  error = function(e) {
    message("No existing targets file found. Creating a new one.")
    NULL
  }
)

## Download buoy data

message("Downloading buoy data from ERDDAP...")

get_buoy_data <- function(start_date, end_date) {
  buoy <- tabledap(
    'UNH_WBD',
    url = 'https://data.neracoos.org/erddap',
    fields = c('time', 'latitude', 'longitude', 'chlorophyll', 'temperature', 'turbidity', 'wspd', 'wdir', 'airtemp', 'airpress', 'oxygen'),
    paste0('time>=', start_date, 'T00:00:00Z'),
    paste0('time<=', end_date, 'T23:59:59Z')
  )
  
  # Clean up
  buoy$datetime <- as.POSIXct(buoy$time, format = "%Y-%m-%dT%H:%M:%SZ", tz = "GMT")
  buoy$date <- as.Date(buoy$datetime)
  
  # Convert to numeric
  buoy$chlorophyll <- as.numeric(buoy$chlorophyll)
  buoy$temperature <- as.numeric(buoy$temperature)
  buoy$turbidity <- as.numeric(buoy$turbidity)
  buoy$oxygen <- as.numeric(buoy$oxygen)
  
  return(buoy)
}

# Download data
buoy_data <- get_buoy_data(start_date, end_date)

# Filter out bad coordinates (was getting longitudes of 10^20+)
buoy_data$latitude <- as.numeric(buoy_data$latitude)
buoy_data$longitude <- as.numeric(buoy_data$longitude)

buoy_data_clean <- buoy_data %>%
  filter(longitude > -180 & longitude < 180) %>%
  filter(latitude > -90 & latitude < 90) %>%
  filter(!is.na(latitude) & !is.na(longitude))

# Calculate buoy location for MODIS download
buoy_lat <- mean(buoy_data_clean$latitude, na.rm = TRUE)
buoy_lon <- mean(buoy_data_clean$longitude, na.rm = TRUE)

# Use cleaned data 
buoy_data <- buoy_data_clean
rm(buoy_data_clean)

buoy_daily <- buoy_data %>%
  mutate(date = as.Date(datetime)) %>%
  group_by(date) %>%
  summarise(
    latitude    = mean(latitude, na.rm = TRUE),
    longitude   = mean(longitude, na.rm = TRUE),
    chlorophyll = mean(chlorophyll, na.rm = TRUE),
    temperature = mean(temperature, na.rm = TRUE),
    turbidity   = mean(turbidity, na.rm = TRUE),
    wspd        = mean(wspd, na.rm = TRUE),
    wdir        = mean(wdir, na.rm = TRUE),
    airtemp     = mean(airtemp, na.rm = TRUE),
    airpress    = mean(airpress, na.rm = TRUE),
    oxygen      = mean(oxygen, na.rm = TRUE),
    n_obs       = n(),
    .groups = "drop"
  )

buoy_data <- buoy_daily
rm(buoy_daily)

## Download MODIS_Aqua data

message("Downloading MODIS-Aqua Level-2 Chlorophyll-a data...")

# Calculate bounding box for 5x5 pixels at 1km
# 1km * 5 pixels = 5km -> ~0.04 degrees per km
box_size_deg <- (1 * 5 * 0.04) / 2  

ul_lat <- buoy_lat + box_size_deg
lr_lat <- buoy_lat - box_size_deg
ul_lon <- buoy_lon - box_size_deg
lr_lon <- buoy_lon + box_size_deg

# DOI for Level-2 Ocean Color
modis_doi <- "10.5067/AQUA/MODIS/L2/OC/2022.0"

# Set up temp download directory
modis_dir <- file.path(tempdir(), "modis")
dir.create(modis_dir, showWarnings = FALSE, recursive = TRUE)

# Get all URLs to know total count (for progress bar later)
all_urls <- NASA_DAAC_download(
  ul_lat = ul_lat,
  ul_lon = ul_lon,
  lr_lat = lr_lat,
  lr_lon = lr_lon,
  from = start_date,
  to = end_date,
  doi = modis_doi,
  just_path = TRUE
)

total_files <- length(all_urls)
message(paste("Found", total_files, "files to download"))

# Define # of cores for parallel processing
ncore <- min(16, parallel::detectCores() - 1, total_files)

# Download with progress bar!
message("Starting MODIS chlor-a downloads...")
modis_files <- NASA_DAAC_download(
  ul_lat = ul_lat,
  ul_lon = ul_lon,
  lr_lat = lr_lat,
  lr_lon = lr_lon,
  from = start_date,
  to = end_date,
  doi = modis_doi,
  outdir = modis_dir,
  credential_path = "~/.netrc",
  ncore = ncore,
  just_path = FALSE # downloads
)

# Check if download returned NA (all files already exist)
if(length(modis_files) == 1 && is.na(modis_files)) {
  modis_files <- list.files(modis_dir, pattern = "\\.nc$", full.names = TRUE)
} else {
  message(paste("Successfully downloaded", length(modis_files), "files"))
}


## Process MODIS data

message("Processing MODIS 5x5 pixel boxes...")

process_modis <- function(ncfile, buoy_lon, buoy_lat) {
  
  nc <- nc_open(ncfile)
  on.exit(nc_close(nc), add = TRUE)
  
  chl <- ncvar_get(nc, "geophysical_data/chlor_a")
  kd  <- ncvar_get(nc, "geophysical_data/Kd_490")
  l2f <- ncvar_get(nc, "geophysical_data/l2_flags") 
  poc <- ncvar_get(nc, "geophysical_data/poc") 
  pic <- ncvar_get(nc, "geophysical_data/pic") 
  lon <- ncvar_get(nc, "navigation_data/longitude")
  lat <- ncvar_get(nc, "navigation_data/latitude")
  
  # get rid of negative/fill value
  chl[chl < 0] <- NA
  kd[kd < 0] <- NA
  poc[poc < 0] <- NA
  pic[pic < 0] <- NA
  
  # calculate center of 5x5 box
  d2 <- (lon - buoy_lon)^2 + (lat - buoy_lat)^2 # compute distance to buoy for each pixel
  idx <- which.min(d2) # index of closest pixel
  ij  <- arrayInd(idx, dim(chl))  # convert to row+column
  r0 <- ij[1]; c0 <- ij[2] # center of 5x5 box
  
  # 5x5 window bounds (centered on pixel closest to buoy)
  r1 <- max(1, r0 - 2); r2 <- min(nrow(chl), r0 + 2)
  c1 <- max(1, c0 - 2); c2 <- min(ncol(chl), c0 + 2)
  
  l2_flags <- l2f[r1:r2, c1:c2] # matrix of flags in box
  l2_flags_json <- jsonlite::toJSON(l2_flags, auto_unbox = TRUE)
  
  # 5x5 windows
  chl_vals <- as.vector(chl[r1:r2, c1:c2])
  kd_vals  <- as.vector(kd[r1:r2,  c1:c2])
  poc_vals  <- as.vector(poc[r1:r2,  c1:c2])
  pic_vals  <- as.vector(pic[r1:r2,  c1:c2])
  
  chl_vals <- chl_vals[is.finite(chl_vals)]
  kd_vals  <- kd_vals[is.finite(kd_vals)]
  poc_vals  <- poc_vals[is.finite(poc_vals)]
  pic_vals  <- pic_vals[is.finite(pic_vals)]
  
  # if all empty, skip
  if (length(chl_vals) == 0 && length(kd_vals) == 0 && length(poc_vals) == 0 && length(pic_vals) == 0) return(NULL)
  
  # get date from filename
  m <- regmatches(basename(ncfile), regexpr("\\.\\d{8}T\\d{6}", basename(ncfile)))
  if (length(m) == 0) return(NULL)
  file_date <- as.Date(substr(m, 2, 9), format = "%Y%m%d")
  
  data.frame(
    date = file_date,
    chlorophyll_mean = mean(chl_vals),
    chlorophyll_sd   = if (length(chl_vals) > 1) sd(chl_vals) else NA_real_,
    chlorophyll_n    = length(chl_vals),
    
    kd490_mean = if (length(kd_vals) > 0) mean(kd_vals) else NA_real_,
    kd490_sd   = if (length(kd_vals) > 1) sd(kd_vals) else NA_real_,
    kd490_n    = length(kd_vals),
    
    poc_mean = if (length(poc_vals) > 0) mean(poc_vals) else NA_real_,
    poc_sd   = if (length(poc_vals) > 1) sd(poc_vals) else NA_real_,
    poc_n    = length(poc_vals),
    
    pic_mean = if (length(pic_vals) > 0) mean(pic_vals) else NA_real_,
    pic_sd   = if (length(pic_vals) > 1) sd(pic_vals) else NA_real_,
    pic_n    = length(pic_vals),
    
    l2_flags = l2_flags_json,   # flags
    file = basename(ncfile),
    stringsAsFactors = FALSE
  )
}

# check which dates exist already
existing_dates <- as.Date(character(0))
if (exists("old_data") && !is.null(old_data)) {
  old_df <- as.data.frame(old_data)
  old_modis <- old_df[grepl("^MODIS_", old_df$site_id), , drop = FALSE]
  if (nrow(old_modis) > 0) {
    existing_dates <- unique(as.Date(substr(old_modis$datetime, 1, 10)))
    existing_dates <- existing_dates[!is.na(existing_dates)]
  }
}

# only process new files/dates
file_dates <- vapply(modis_files, function(fp) {
  m <- regmatches(basename(fp), regexpr("\\.\\d{8}T\\d{6}", basename(fp)))
  if (length(m) == 0) return(NA_character_)
  as.character(as.Date(substr(m, 2, 9), format = "%Y%m%d"))
}, character(1))
file_dates <- as.Date(file_dates)

to_process <- modis_files[is.na(file_dates) | !(file_dates %in% existing_dates)]
message("MODIS files: ", length(modis_files), " | to process: ", length(to_process))

# loop to process
out <- vector("list", length(to_process))
k <- 0L

for (i in seq_along(to_process)) {
  res <- tryCatch(
    process_modis(to_process[i], buoy_lon, buoy_lat),
    error = function(e) {
      message("MODIS failed: ", basename(to_process[i]), " :: ", e$message)
      NULL
    }
  )
  
  if (!is.null(res)) {
    k <- k + 1L
    out[[k]] <- res
  }
  
  if (i %% 50 == 0) gc()
}

# trim unused list slots
out <- out[seq_len(k)]

# make df for modis data
modis_data <- if (k == 0) data.frame() else do.call(rbind, out[seq_len(k)])

## Format

message("Formatting to standard format...")

format_to_standard <- function(data, site_id_prefix, data_source) {
  
  # Reshape to long format 
  if(data_source == "buoy") {
    long_data <- data %>%
      select(date, chlorophyll, temperature, turbidity, oxygen, wspd, wdir, airtemp, airpress) %>%
      tidyr::pivot_longer(
        cols = c(chlorophyll, temperature, turbidity, oxygen, wspd, wdir, airtemp, airpress),
        names_to = "variable",
        values_to = "observation"
      )
    duration <- "P1D"  # daily 
    
  } else if (data_source == "modis") {
    long_data <- data %>%
      select(date, chlorophyll_mean, kd490_mean, poc_mean, pic_mean) %>%
      rename(chlorophyll = chlorophyll_mean,
             kd_490 = kd490_mean,
             poc = poc_mean,
             pic = pic_mean) %>%
      tidyr::pivot_longer(
        cols = c(chlorophyll, kd_490, poc, pic),
        names_to = "variable",
        values_to = "observation"
      )
    duration <- "P1D"
  }
  
  # Add standard columns
  long_data <- long_data %>%
    mutate(
      project_id = project_id,
      site_id = paste0(site_id_prefix, "_", variable),
      datetime = as.character(date),
      duration = duration
    ) %>%
    select(project_id, site_id, datetime, duration, variable, observation) %>%
    filter(!is.na(observation))  # Remove NAs
  
  return(long_data)
}

# Format both datasets
buoy_formatted <- format_to_standard(
  buoy_data,
  site_id_prefix = "UNH_buoy",
  data_source = "buoy"
)

modis_formatted <- modis_data %>%
  select(date, chlorophyll_mean, kd490_mean, poc_mean, pic_mean, l2_flags) %>%
  rename(
    chlorophyll = chlorophyll_mean,
    kd_490 = kd490_mean,
    poc = poc_mean,
    pic = pic_mean
  ) %>%
  tidyr::pivot_longer(
    cols = c(chlorophyll, kd_490, poc, pic),
    names_to = "variable",
    values_to = "observation"
  ) %>%
  mutate(
    project_id = project_id,
    site_id = paste0("MODIS_", variable),
    datetime = as.character(date),
    duration = "P1D"
  ) %>%
  select(
    project_id, site_id, datetime, duration,
    variable, observation
  )

# Combine
all_targets <- dplyr::bind_rows(buoy_formatted, modis_formatted)

## Append to existing data

data <- all_targets %>%
  mutate(
    project_id  = as.character(project_id),
    site_id     = as.character(site_id),
    datetime    = as.character(datetime),
    duration    = as.character(duration),
    variable    = as.character(variable),
    observation = as.numeric(observation)
  )

primary_keys <- c("project_id", "site_id", "datetime", "duration", "variable")

# De-dupe today's run 
data <- data %>%
  distinct(across(all_of(primary_keys)), .keep_all = TRUE)

if (!is.null(old_data) && nrow(old_data) > 0) {
  
  old_data <- old_data %>%
    mutate(
      project_id  = as.character(project_id),
      site_id     = as.character(site_id),
      datetime    = as.character(datetime),
      duration    = as.character(duration),
      variable    = as.character(variable),
      observation = as.numeric(observation)
    ) %>%
    distinct(across(all_of(primary_keys)), .keep_all = TRUE)
  
  # keep old rows that are not being replaced, then add today's rows
  new_data <- old_data %>%
    anti_join(data, by = primary_keys) %>%
    bind_rows(data)
  
} else {
  new_data <- data
}

new_data <- new_data %>%
  arrange(site_id, datetime, variable)

message(paste0("Rows in old_data: ", ifelse(is.null(old_data), 0, nrow(old_data))))
message(paste0("Rows in data (this run): ", nrow(data)))
message(paste0("Rows in new_data (after merge): ", nrow(new_data)))

message("Writing updated targets back to S3...")
arrow::write_csv_arrow(new_data, sink = s3_read$path(filename))

message("Pinging health check...")
tryCatch(
  RCurl::getURL("https://hc-ping.com/af0bdaf6-3ec8-434b-bafd-d8fecc0508af"),
  error = function(e) message("Health check ping failed: ", e$message)
)

if (exists("modis_files")) {
  suppressWarnings(try(unlink(modis_files), silent = TRUE))
}

message("Coastal targets script complete.")