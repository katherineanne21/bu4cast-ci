#renv::restore()

library(tidyverse)


source("targets/R/downloadPhenoCam.R")
source("targets/R/calculatePhenoCamUncertainty.R")

sites <- readr::read_csv("neon4cast_field_site_metadata.csv") |>
  dplyr::filter(phenology == 1)

allData <- data.frame(matrix(nrow = 0, ncol = 5))

message(paste0("Downloading and generating phenology targets ", Sys.time()))

for(i in 1:nrow(sites)){
  siteName <- sites$phenocam_code[i]
  site_roi <- sites$phenocam_roi[i]
  message(siteName)
  ##URL for daily summary statistics
  URL_gcc90 <- paste('https://phenocam.nau.edu/data/archive/',siteName,"/ROI/",siteName,"_",site_roi,"_1day.csv",sep="")
  ##URL for individual image metrics
  URL_individual <- paste('https://phenocam.nau.edu/data/archive/',siteName,"/ROI/",siteName,"_",site_roi,"_roistats.csv",sep="")

  phenoData <- download.phenocam(URL = URL_gcc90)
  dates <- unique(phenoData$date)
  phenoData_individual <- download.phenocam(URL=URL_individual,skipNum = 17)
  ##Calculates standard deviations on daily gcc90 values
  gcc_sd <- calculate.phenocam.uncertainty(dat=phenoData_individual,dates=dates)
  rcc_sd <- calculate.phenocam.uncertainty(dat=phenoData_individual,dates=dates,target="rcc")

  subPhenoData <- phenoData %>%
    mutate(site_id = stringr::str_sub(siteName, 10, 13),
           time = date) %>%
    select(time, site_id, gcc_90, rcc_90) |>
    pivot_longer(-c("time", "site_id"), names_to = "variable", values_to = "observation") |>
    mutate(sd = ifelse(variable == "gcc_90", gcc_sd, rcc_sd))

  allData <- rbind(allData,subPhenoData)

}

full_time <- seq(min(allData$time),max(allData$time), by = "1 day")
combined <- NULL

for(i in 1:nrow(sites)){

  full_time_curr1 <- tibble(time = full_time,
                           site_id = rep(sites$field_site_id[i],length(full_time)),
                           variable = "gcc_90")

  full_time_curr2 <- tibble(time = full_time,
                            site_id = rep(sites$field_site_id[i],length(full_time)),
                            variable = "rcc_90")

  combined <- bind_rows(combined, full_time_curr1, full_time_curr2)
}



allData2 <- left_join(combined, allData, by = c("time", "site_id", "variable"))

allData2 <- allData2 |>
  select("time", "site_id", "variable", "observation") |>
  rename(datetime = time)

s3 <- arrow::s3_bucket("neon4cast-targets/phenology",
                              endpoint_override = "data.ecoforecast.org",
                              access_key = Sys.getenv("AWS_ACCESS_KEY_SUBMISSIONS"),
                              secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY_SUBMISSIONS"))

arrow::write_csv_arrow(allData2, sink = s3$path("phenology-targets.csv.gz"))

allData3 <- allData2 |>
  mutate(datetime = lubridate::as_datetime(datetime),
         duration = "P1D",
         project_id = "neon4cast") |>
  select(project_id, site_id, datetime, duration, variable, observation)

s3 <- arrow::s3_bucket("bio230014-bucket01/challenges/targets/project_id=neon4cast/duration=P1D",
                              endpoint_override = "sdsc.osn.xsede.org",
                              access_key = Sys.getenv("OSN_KEY"),
                              secret_key = Sys.getenv("OSN_SECRET"))

arrow::write_csv_arrow(allData3, sink = s3$path("phenology-targets.csv.gz"))

unlink("phenology-targets.csv.gz")

RCurl::getURL("https://hc-ping.com/f5d48d96-bb41-4c21-b028-930fa2b01c5a")
