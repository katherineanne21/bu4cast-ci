library(tidyverse)

source('targets/target_functions/target_generation_FluoroProbe.R')

historic_data <- "https://portal.edirepository.org/nis/dataviewer?packageid=edi.272.7&entityid=001cb516ad3e8cbabe1fdcf6826a0a45"

current_data <- "https://raw.githubusercontent.com/CareyLabVT/Reservoirs/master/Data/DataNotYetUploadedToEDI/Raw_fluoroprobe/fluoroprobe_L1.csv"

fluoro_daily <- target_generation_FluoroProbe(current_file = current_data, historic_file = historic_data)

s3 <- arrow::s3_bucket("bio230121-bucket01", endpoint_override = "renc.osn.xsede.org")
s3$CreateDir("vera4cast/targets/daily")

s3 <- arrow::s3_bucket("bio230121-bucket01/vera4cast/targets/daily", endpoint_override = "renc.osn.xsede.org")
arrow::write_csv_arrow(fluoro_daily, sink = s3$path("daily-targets.csv.gz"))
