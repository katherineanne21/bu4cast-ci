##### inflow targets generation function
## author: Austin Delany
## last edited : 2023/10/11

## files used in function

# current_inflow <- 'https://raw.githubusercontent.com/FLARE-forecast/FCRE-data/fcre-weir-data-qaqc/FCRWeir_L1.csv'
#
# historic_inflow <- "https://pasta.lternet.edu/package/data/eml/edi/202/10/c065ff822e73c747f378efe47f5af12b"
#
# historic_silica <- 'https://pasta.lternet.edu/package/data/eml/edi/542/1/791ec9ca0f1cb9361fa6a03fae8dfc95'
#
# historic_nutrients <- "https://pasta.lternet.edu/package/data/eml/edi/199/11/509f39850b6f95628d10889d66885b76"
#
# historic_ghg <- "https://pasta.lternet.edu/package/data/eml/edi/551/7/38d72673295864956cccd6bbba99a1a3"


inflow_generation <- function(historic_inflow, current_inflow, historic_nutrients, historic_silica, historic_ghg){

  # current flow
  df_current_in <- read_csv(current_inflow)

  # historic flow
  inUrl1  <- historic_inflow
  infile1 <- tempfile()
  try(download.file(inUrl1,infile1,method="curl"))
  if (is.na(file.size(infile1))) download.file(inUrl1,infile1,method="auto")

  df_hist_in <- read_csv(infile1)

  df_inflow <- bind_rows(df_current_in, df_hist_in) |>
    mutate(date = as.Date(DateTime)) |>
    mutate(inflow = ifelse(is.na(VT_Flow_cms), WVWA_Flow_cms, VT_Flow_cms)) |>
    mutate(inflow_temp = ifelse(is.na(VT_Temp_C), WVWA_Temp_C, VT_Temp_C)) |>
    group_by(date) |>
    mutate(flow_cms_mean = mean(inflow, na.rm = TRUE)) |>
    mutate(temp_c_mean = mean(inflow_temp, na.rm = TRUE)) |>
    ungroup() |>
    distinct(date, .keep_all = TRUE) |>
    select(date, flow_cms_mean, temp_c_mean) #|>
    #pivot_longer(!date , names_to = 'variable' , values_to = 'observation')

  df_inflow$flow_cms_mean <- ifelse(is.nan(df_inflow$flow_cms_mean), NA, df_inflow$flow_cms_mean)
  df_inflow$temp_c_mean <- ifelse(is.nan(df_inflow$temp_c_mean), NA, df_inflow$temp_c_mean)



  #historic nutrients

  inUrl2  <- historic_nutrients
  infile2 <- tempfile()
  try(download.file(inUrl2,infile2,method="curl"))
  if (is.na(file.size(infile2))) download.file(inUrl2,infile2,method="auto")

  df_hist_nutr <- read_csv(infile2) |>
    filter(Reservoir == 'FCR', Site == 100) |>
    mutate(date = as.Date(DateTime)) |>
    select(3:14, date, -Depth_m, -Rep, -DateTime) |>
    rename(TN_ugL_sample = TN_ugL, TP_ugL_sample = TP_ugL, NH4_ugL_sample = NH4_ugL,  NO3NO2_ugL_sample = NO3NO2_ugL, SRP_ugL_sample = SRP_ugL,
           DOC_mgL_sample = DOC_mgL, DIC_mgL_sample = DIC_mgL, DC_mgL_sample = DC_mgL, DN_mgL_sample = DN_mgL)

  # silica
  df_silica <- read_csv(historic_silica) |>
    filter(Reservoir == 'FCR', Site == 100) |>
    mutate(date = as.Date(DateTime)) |>
    select(date, DRSI_mgL_sample = DRSI_mgL)

  # GHG - ch4?
  df_ghg <- read_csv(historic_ghg) |>
    filter(Reservoir == 'FCR', Site == 100) |>
    mutate(date = as.Date(DateTime)) |>
    drop_na(CH4_umolL) |>
    group_by(date) |>
    mutate(CH4_umolL_sample = mean(CH4_umolL, na.rm = TRUE)) |> # na.rm might be redundant here
    ungroup() |>
    distinct(date, .keep_all = TRUE) |>
    select(date, CH4_umolL_sample)

  # build df of nutrients that match daily sensor data
  df_add_nutr_si_ghg <- df_inflow |>
    full_join(df_hist_nutr, by = c('date')) |>
    full_join(df_silica, by = c('date')) |>
    full_join(df_ghg, by = c('date'))

  df_long <- df_add_nutr_si_ghg |>
    pivot_longer(!date, names_to = 'variable', values_to = 'observation')

  # grab the missing inflow values that you want to keep (so user knows it's missing data and not just missing rows)
  df_missing_inflows_store <- df_long |>
    filter(is.nan(observation))

  df_inflow_targets_final <- df_long |>
    drop_na(observation) |>
    bind_rows(df_missing_inflows_store) |>
    rename(datetime = date) |>
    arrange(datetime)

  df_inflow_targets_final$observation <- ifelse(is.nan(df_inflow_targets_final$observation), NA, df_inflow_targets_final$observation)

  df_inflow_targets_final$site_id <- 'tubr'
  df_inflow_targets_final$depth_m <- NA
  df_inflow_targets_final$duration <- 'P1D'
  df_inflow_targets_final$project_id <- 'vera'


  return(df_inflow_targets_final)



}

# t <- inflow_generation(historic_inflow = historic_inflow,
#                        current_inflow = current_inflow,
#                        historic_nutrients = historic_nutrients,
#                        historic_silica = silica_df,
#                        historic_ghg = historic_ghg)
