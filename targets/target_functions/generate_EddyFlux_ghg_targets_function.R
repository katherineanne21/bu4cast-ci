# Function for generating the targets file for mean daily fluxes from EddyFlux
# Author: Adrienne Breef-Pilz
# 8 Sep 2023

pacman::p_load("tidyverse","lubridate")

generate_EddyFlux_ghg_targets_function <- function(current_data_file, edi_data_file){
  
  ## read in current data file which is found on GitHub
  
  dt1 <-read_csv(current_data_file)
  
  # read in historical data file 
  # EDI
  inUrl1 <- edi_data_file
  infile1 <- tempfile()
  try(download.file(inUrl1,infile1,method="curl"))
  if (is.na(file.size(infile1))) download.file(inUrl1,infile1,method="auto")
  
  # read in the data file downloaded from EDI
  dt2 <-read_csv(infile1) 
  
  # Filter to what you need
  targets_df<-bind_rows(dt2,dt1)%>% # bind observations together
    select(date, co2_flux_umolm2s, ch4_flux_umolm2s)%>% # select columns we want
    
    # Add a few QAQC checks 
    mutate(co2_flux_umolm2s=ifelse(co2_flux_umolm2s>100 | co2_flux_umolm2s< -100, NA, co2_flux_umolm2s))%>%
    mutate(ch4_flux_umolm2s=ifelse(ch4_flux_umolm2s>1 | ch4_flux_umolm2s< -1, NA, ch4_flux_umolm2s))%>%
    
    group_by(date)%>% # average if there are more than one sample taken during that day
    summarise_if(is.numeric, mean, na.rm = TRUE)%>%
    ungroup()%>%
    mutate(datetime=ymd_hms(paste0(date,"","00:00:00")))%>%
    mutate(Reservoir='fcre')%>% # change the name to the the reservoir code for FLARE
    mutate(Depth_m = NA)%>%      
    select(-date)%>%
    rename(site_id=Reservoir, # rename the columns for standard notation
           depth=Depth_m)%>%
    pivot_longer(cols=c(co2_flux_umolm2s, ch4_flux_umolm2s), # make the wide data frame into a long one so each observation has a depth
                 names_to='variable',
                 values_to='observation')%>%
    select(c('datetime', 'site_id', 'depth', "observation", 'variable')) # rearrange order of columns
  
  
  
  ## return dataframe formatted to match FLARE targets
  return(targets_df)
}

# Using the function with the EDI address for data
 # generate_EddyFlux_ghg_targets_function(
 #  current_data_file = "https://raw.githubusercontent.com/CareyLabVT/Reservoirs/master/Data/DataNotYetUploadedToEDI/EddyFlux_Processing/EddyPro_Cleaned_L1.csv", 
 #  edi_data_file = "https://pasta.lternet.edu/package/data/eml/edi/1061/2/f837d12dc12ab37a6772598578875e00"
 #  ) 

