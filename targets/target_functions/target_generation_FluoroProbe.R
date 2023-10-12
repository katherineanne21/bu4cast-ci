# Title: Generation of target files based on FluoroProbe data for VERA forecasts
# Author: Mary Lofton
# Date: 17Aug23

# Description: Generates the following targets using FluoroProbe data:

#' DeepChlorophyllMaximum_binary
#' TotalBiomass_ugL
#' GreenAlgae_ugL
#' Bluegreens_ugL
#' BrownAlgae_ugL
#' MixedAlgae_ugL
#' TotalBiomass_ugL_CM
#' GreenAlgae_ugL_CM
#' Bluegreens_ugL_CM
#' BrownAlgae_ugL_CM
#' MixedAlgae_ugL_CM
#' ChlorophyllMaximum_depth

# Load packages
library(tidyverse)
library(lubridate)
library(httr)

# historic_file <- "https://portal.edirepository.org/nis/dataviewer?packageid=edi.272.7&entityid=001cb516ad3e8cbabe1fdcf6826a0a45"
# current_file <-'https://raw.githubusercontent.com/CareyLabVT/Reservoirs/master/Data/DataNotYetUploadedToEDI/Raw_fluoroprobe/fluoroprobe_L1.csv'

target_generation_FluoroProbe <- function(current_file, historic_file){

  # read in current file
  message("Reading in unpublished data")
  new_data <- read_csv(current_file)

  # read in historical data file
  message("Reading in published data")
  edi <- readr::read_csv(historic_file)

  # additional code added by austin to qaqc edi depths
  message("Data wrangling...")
  edi_fcr_trim <- edi |> filter(Reservoir == 'FCR', Depth_m <= 9.5)
  edi_bvr_trim <- edi |> filter(Reservoir == 'BVR', Depth_m <= 10)
  edi_ccr_trim <- edi |> filter(Reservoir == 'CCR', Depth_m <= 21)

  edi_update <- dplyr::bind_rows(edi_fcr_trim, edi_bvr_trim, edi_ccr_trim)


  needed_cols <- c("Reservoir"       ,     "Site"            ,     "DateTime"  ,
                   "GreenAlgae_ugL_sample"   ,    "Bluegreens_ugL_sample"   ,    "BrownAlgae_ugL_sample"   ,
                   "MixedAlgae_ugL_sample"    ,   "TotalConc_ugL_sample"     ,
                   "Depth_m"            )

  ## bind the two files using bind_rows()
  # need to double-check that columns match
  fp <- bind_rows(edi_update, new_data) %>%
    rename(GreenAlgae_ugL_sample = GreenAlgae_ugL, Bluegreens_ugL_sample = Bluegreens_ugL, BrownAlgae_ugL_sample = BrownAlgae_ugL,
           MixedAlgae_ugL_sample = MixedAlgae_ugL, TotalConc_ugL_sample = TotalConc_ugL) |>
    filter(Reservoir %in% c("FCR","BVR") & Site == 50) %>%
    arrange(Reservoir, DateTime, Depth_m) |>
    select(any_of(needed_cols))

  biomass_exo <- fp %>%
    mutate(Date = date(DateTime)) %>%
    group_by(Date) %>%
    slice(which.min(abs(Depth_m - 1.6))) %>%
    pivot_longer(GreenAlgae_ugL_sample:TotalConc_ugL_sample, names_to = "variable", values_to = "observation") %>%
    rename(datetime = DateTime, depth_m = Depth_m) %>%
    mutate(site_id = ifelse(Reservoir == "FCR","fcre","bvre"),
           depth_m = 1.6) %>%
    ungroup() %>%
    select(datetime, site_id, depth_m, observation, variable)

  biomass_cmax <- fp %>%
    mutate(Date = date(DateTime)) %>%
    group_by(Date) %>%
    slice(which.max(TotalConc_ugL_sample)) %>%
    pivot_longer(GreenAlgae_ugL_sample:TotalConc_ugL_sample, names_to = "variable", values_to = "observation") %>%
    rename(datetime = DateTime, depth_m = Depth_m) %>%
    mutate(site_id = ifelse(Reservoir == "FCR","fcre","bvre"),
           variable = paste0(variable, "_CM")) %>%
    ungroup() %>%
    select(datetime, site_id, depth_m, observation, variable)

  cmax_depth <- fp %>%
    mutate(Date = date(DateTime)) %>%
    group_by(Date) %>%
    slice(which.max(TotalConc_ugL_sample)) %>%
    pivot_longer(Depth_m, names_to = "variable", values_to = "observation") %>%
    rename(datetime = DateTime) %>%
    mutate(site_id = ifelse(Reservoir == "FCR","fcre","bvre"),
           variable = "ChlorophyllMaximum_depth_sample",
           depth_m = NA) %>%
    ungroup() %>%
    select(datetime, site_id, depth_m, observation, variable)

  mean_biomass <- fp %>%
    mutate(Date = date(DateTime)) %>%
    group_by(Date) %>%
    summarize(mean_biomass = mean(TotalConc_ugL_sample, na.rm = TRUE))

  dcm <- fp %>%
    mutate(Date = date(DateTime)) %>%
    group_by(Date) %>%
    slice(which.max(TotalConc_ugL_sample)) %>%
    left_join(mean_biomass) %>%
    mutate(DeepChlorophyllMaximum_binary_sample = ifelse(TotalConc_ugL_sample > mean_biomass + 0.5 & TotalConc_ugL_sample > mean_biomass*1.5 & ((Reservoir == "FCR" & Depth_m > 0.93) | (Reservoir == "BVR" & Depth_m > 1)), 1, 0)) %>%
    pivot_longer(DeepChlorophyllMaximum_binary_sample, names_to = "variable", values_to = "observation") %>%
    rename(datetime = DateTime) %>%
    mutate(site_id = ifelse(Reservoir == "FCR","fcre","bvre"),
           depth_m = NA) %>%
    ungroup() %>%
    select(datetime, site_id, depth_m, observation, variable)

  final <- bind_rows(biomass_exo, biomass_cmax, cmax_depth, dcm)

  # final <- final |>
  #   rename(GreenAlgae_ugL_sample = GreenAlgae_ugL, Bluegreens_ugL_sample = Bluegreens_ugL, BrownAlgae_ugL_sample = BrownAlgae_ugL,
  #          MixedAlgae_ugL_sample = MixedAlgae_ugL, TotalConc_ugL_sample = TotalConc_ugL, GreenAlgae_ugL_CM_sample = GreenAlgae_ugL_CM,
  #          Bluegreens_ugL_CM_sample = Bluegreens_ugL_CM, BrownAlgae_ugL_CM_sample = BrownAlgae_ugL_CM, MixedAlgae_ugL_CM_sample = MixedAlgae_ugL_CM,
  #          TotalConc_ugL_CM_sample = TotalConc_ugL_CM, ChlorophyllMaximum_depth_sample = ChlorophyllMaximum_depth,  DeepChlorophyllMaximum_binary_sample = DeepChlorophyllMaximum_binary)


 return(final)

}
