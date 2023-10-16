#'
#' @author Abigail Lewis
#' @title target_generation_exo_daily
#' @description This function loads EXO-sonde data from FCR and BVR and generates daily targets for the vera forecasting challenge
#'
#' @param fcr_files vector of files with identical format that are are from FCR
#' @param bvr_current current BVR file
#' @param bvr_2020_2022 EDI BVR publication
#' @example target_generation_exo_daily(fcr_files, bvr_current, bvr_2020_2022)
#'
#' @return dataframe of cleaned and combined targets from the EXO-sonde
#'
library(tidyverse)

# fcr_files <- c("https://pasta.lternet.edu/package/data/eml/edi/271/7/71e6b946b751aa1b966ab5653b01077f",
#                "https://raw.githubusercontent.com/FLARE-forecast/FCRE-data/fcre-catwalk-data-qaqc/fcre-waterquality_L1.csv")
# bvr_files <- c("https://raw.githubusercontent.com/FLARE-forecast/BVRE-data/bvre-platform-data-qaqc/bvre-waterquality_L1.csv",
#                "https://pasta.lternet.edu/package/data/eml/edi/725/3/a9a7ff6fe8dc20f7a8f89447d4dc2038")

target_generation_exo_daily <- function (fcr_files,
                                         bvr_files) {

  # Load FCR data
  fcr_df <- readr::read_csv(fcr_files) |>
    dplyr::mutate(site_id = "fcre",
                  DateTime = lubridate::force_tz(DateTime, tzone = "EST"),
                  DateTime = lubridate::with_tz(DateTime, tzone = "UTC"),
                  Date = as.Date(DateTime))

  names(fcr_df |> select(starts_with('RDO_mgL' | 'EXODO') & ends_with('adjusted')))

  # Load BVR data
  bvr_df <- readr::read_csv(bvr_files) |>
    dplyr::mutate(site_id = "bvre",
                  DateTime = lubridate::force_tz(DateTime, tzone = "EST"),
                  DateTime = lubridate::with_tz(DateTime, tzone = "UTC"),
                  Date = as.Date(DateTime))


  # Format data to combine
  # FCR
  fcr_sum <- fcr_df |>
    dplyr::group_by(Date, site_id) |>
    dplyr::summarise(Cond_uScm_mean = mean(EXOCond_uScm_1, na.rm = T),
                     Temp_C_mean = mean(EXOTemp_C_1, na.rm = T),
                     SpCond_uScm_mean = mean(EXOSpCond_uScm_1, na.rm = T),
                     #DOsat_percent_mean = mean(EXODOsat_percent_1, na.rm = T),
                     #DO_mgL_mean = mean(EXODO_mgL_1, na.rm = T),
                     Chla_ugL_mean = mean(EXOChla_ugL_1, na.rm = T),
                     fDOM_QSU_mean = mean(EXOfDOM_QSU_1, na.rm = T),
                     Turbidity_FNU_mean = mean(EXOTurbidity_FNU_1, na.rm = T),
                     Bloom_binary_mean = as.numeric(mean(Chla_ugL_mean, na.rm = T)>20)#,
                     #EXODepth_m = mean(EXODepth_m, na.rm = T) #could use this line to have changing depths based on the exo depth sensor
                     )

  # BVR
  bvr_sum <- bvr_df |>
    dplyr::group_by(Date, site_id) |> #daily mean
    dplyr::summarise(Cond_uScm_mean = mean(EXOCond_uScm_1.5, na.rm = T),
                     Temp_C_mean = mean(EXOTemp_C_1.5, na.rm = T),
                     SpCond_uScm_mean = mean(EXOSpCond_uScm_1.5, na.rm = T),
                     #DOsat_percent_mean = mean(EXODOsat_percent_1.5, na.rm = T), # These are removed by Austin.
                     #DO_mgL_mean = mean(EXODO_mgL_1.5, na.rm = T),               # We buid full DO df below to include all depths
                     Chla_ugL_mean = mean(EXOChla_ugL_1.5, na.rm = T),
                     fDOM_QSU_mean = mean(EXOfDOM_QSU_1.5, na.rm = T),
                     Turbidity_FNU_mean = mean(EXOTurbidity_FNU_1.5, na.rm = T),
                     Bloom_binary_mean = as.numeric(mean(Chla_ugL_mean, na.rm = T)>20)#,
                     #EXODepth_m = mean(EXODepth_m, na.rm = T)
    )


  ## build DO for each site separately and then combine
  fcr_DO <- fcr_df |>
    select(DateTime, (starts_with('RDO') & ends_with('adjusted')) | (starts_with('EXODO'))) |>
    pivot_longer(-DateTime, names_to = 'variable', values_to = 'observation') |>
    mutate(Date = as.Date(DateTime)) |>
    mutate(depth_m = as.numeric(sapply(stringr::str_split(variable, "_"), function(x) x[3]))) |>
    mutate(variable = ifelse(grepl('RDO_mgL', variable), 'DO_mgL_mean', variable)) |>
    mutate(variable = ifelse(grepl('RDOsat', variable), 'DOsat_percent_mean', variable)) |>
    mutate(variable = ifelse(grepl('EXODO_mgL', variable), 'DO_mgL_mean', variable)) |>
    mutate(variable = ifelse(grepl('EXODOsat', variable), 'DOsat_percent_mean', variable)) |>
    summarise(obs_avg = mean(observation, na.rm = TRUE), .by = c('Date', 'variable', 'depth_m')) |>
    mutate(datetime=ymd_hms(paste0(Date,"","00:00:00"))) |>
    select(datetime, depth_m, observation = obs_avg, variable)

  fcr_DO$site_id <- 'fcr'


  bvr_DO <- bvr_df |>
    select(DateTime, (starts_with('RDO') & ends_with('adjusted')) | (starts_with('EXODO'))) |>
    pivot_longer(-DateTime, names_to = 'variable', values_to = 'observation') |>
    mutate(Date = as.Date(DateTime)) |>
    mutate(depth_m = as.numeric(sapply(stringr::str_split(variable, "_"), function(x) x[3]))) |>
    mutate(variable = ifelse(grepl('RDO_mgL', variable), 'DO_mgL_mean', variable)) |>
    mutate(variable = ifelse(grepl('RDOsat', variable), 'DOsat_percent_mean', variable)) |>
    mutate(variable = ifelse(grepl('EXODO_mgL', variable), 'DO_mgL_mean', variable)) |>
    mutate(variable = ifelse(grepl('EXODOsat', variable), 'DOsat_percent_mean', variable)) |>
    summarise(obs_avg = mean(observation, na.rm = TRUE), .by = c('Date', 'variable', 'depth_m')) |>
    mutate(datetime=ymd_hms(paste0(Date,"","00:00:00"))) |>
    select(datetime, depth_m, observation = obs_avg, variable, observation = obs_avg)

  bvr_DO$site_id <- 'bvr'


  combined_DO <- bind_rows(fcr_DO, bvr_DO) |>
    mutate(observation = ifelse(is.nan(observation), NA, observation))

  #depth is 1.5 at BVR and 1.6 and FCR

  #Combine all and format
  comb_sum <- fcr_sum |>
    dplyr::bind_rows(bvr_sum) |>
    dplyr::rename(datetime = Date) |>
    tidyr::pivot_longer(cols = Temp_C_mean:Bloom_binary_mean, names_to = "variable", values_to = "observation") |>
    dplyr::mutate(depth_m = NA,
                  depth_m = ifelse(site_id == "fcre", 1.6, depth_m),
                  depth_m = ifelse(site_id == "bvre", 1.5, depth_m)) |>
    #dplyr::rename(depth_m = EXODepth_m) |>
    dplyr::select(datetime, site_id, depth_m, observation, variable) |>
    dplyr::mutate(observation = ifelse(!is.finite(observation),NA,observation)) |>
    bind_rows(combined_DO) # append DO data

  return(comb_sum)
}
