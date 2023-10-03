# Function for generating the targets file for chemistry
# Author: Adrienne Breef-Pilz
# 17 Aug 2023

target_generation_chemistry_daily <- function(current_data_file, historic_data_file){
  
  ## read in current data file 
  # Right now there is no current Chemistry file to read in 
  dt1 <-current_data_file
  
  # read in historical data file 
  # EDI
  inUrl1 <- historic_data_file
  infile1 <- tempfile()
  try(download.file(inUrl1,infile1,method="curl"))
  if (is.na(file.size(infile1))) download.file(inUrl1,infile1,method="auto")
  
  
  dt2 <-read_csv(infile1) 
  
  ## manipulate the data files to match each other 
  # secchi: match current data file and select only secchi data in EDI
  # temperature: in correct format as EDI 
  # swimmibility: select the sfc water and air temp columns and just use those
  # Phtyos/MOMs: select columns that you need 
  #MOM: EDI 
  #Phytos: Taken from catwalk file
  
  ## bind the two files using row.bind()

    targets_df<-bind_rows(dt1,dt2)%>%
      filter(Reservoir=="FCR"|Reservoir=="BVR")%>%
      filter(Site==50)%>%
      select(-Site,-starts_with("Flag"),-DIC_mgL,-DC_mgL,-DN_mgL)%>% # get rid of the columns we don't want
      group_by(Reservoir,Depth_m,DateTime)%>%
      summarise_if(is.numeric, mean, na.rm = TRUE)%>% # average if there are reps taken at a depths
      ungroup()%>%
      mutate(Date=as.Date(DateTime))%>%
      group_by(Reservoir,Depth_m,Date)%>% # average if there are more than one sample taken during that day
      summarise_if(is.numeric, mean, na.rm = TRUE)%>%
      ungroup()%>%
      mutate(datetime=ymd_hms(paste0(Date,"","00:00:00")))%>%
      mutate(Reservoir=ifelse(Reservoir=="FCR",'fcre',Reservoir), # change the name to the the reservoir code for FLARE
             Reservoir=ifelse(Reservoir=="BVR",'bvre',Reservoir))%>%
      select(-Date, -Rep)%>%
      rename(site_id=Reservoir,
             depth=Depth_m)%>%
      pivot_longer(cols=c(TN_ugL:DOC_mgL),
                   names_to='variable',
                   values_to='observation')%>%
      select(c('datetime', 'site_id', 'depth', "observation", 'variable')) # rearrange order of columns
 
  
      
  ## return dataframe formatted to match FLARE targets
    return(targets_df)
}

# Using the function with the EDI address for data
# target_generation_chemistry_daily(
#  current_data_file=NULL,
#  historic_data_file="https://pasta.lternet.edu/package/data/eml/edi/199/11/509f39850b6f95628d10889d66885b76")

