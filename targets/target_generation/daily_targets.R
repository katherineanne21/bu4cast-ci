library(tidyverse)

source('targets/target_functions/target_generation_exo_daily.R')
source('targets/target_functions/target_generation_FluoroProbe.R')

fcr_files <- c("https://pasta.lternet.edu/package/data/eml/edi/271/7/71e6b946b751aa1b966ab5653b01077f",
               "https://raw.githubusercontent.com/FLARE-forecast/FCRE-data/fcre-catwalk-data-qaqc/fcre-waterquality_L1.csv")

bvr_files <- c("https://raw.githubusercontent.com/FLARE-forecast/BVRE-data/bvre-platform-data-qaqc/bvre-waterquality_L1.csv",
               "https://pasta.lternet.edu/package/data/eml/edi/725/3/a9a7ff6fe8dc20f7a8f89447d4dc2038")

exo_daily <- target_generation_exo_daily(fcr_files, bvr_files)




historic_data <- "https://portal.edirepository.org/nis/dataviewer?packageid=edi.272.7&entityid=001cb516ad3e8cbabe1fdcf6826a0a45"

current_data <- "https://raw.githubusercontent.com/CareyLabVT/Reservoirs/master/Data/DataNotYetUploadedToEDI/Raw_fluoroprobe/fluoroprobe_L1.csv"

fluoro_daily <- target_generation_FluoroProbe(current_file = current_data, historic_file = historic_data)


combined_targets <- bind_rows(exo_daily, fluoro_daily) |>
  dplyr::mutate(project_id = "vera4cast",
                 duration = "P1D")

s3 <- arrow::s3_bucket("bio230121-bucket01", endpoint_override = "renc.osn.xsede.org")
s3$CreateDir("vera4cast/targets/duration=P1D")

s3 <- arrow::s3_bucket("bio230121-bucket01/vera4cast/targets/duration=P1D", endpoint_override = "renc.osn.xsede.org")
arrow::write_csv_arrow(combined_targets, sink = s3$path("P1D-targets.csv.gz"))
