library(tidyverse)

source('R/target_generation_exo_daily.R')

fcr_files <- c("https://pasta.lternet.edu/package/data/eml/edi/271/7/71e6b946b751aa1b966ab5653b01077f",
               "https://raw.githubusercontent.com/FLARE-forecast/FCRE-data/fcre-catwalk-data-qaqc/fcre-waterquality_L1.csv")

bvr_files <- c("https://raw.githubusercontent.com/FLARE-forecast/BVRE-data/bvre-platform-data-qaqc/bvre-waterquality_L1.csv",
               "https://pasta.lternet.edu/package/data/eml/edi/725/3/a9a7ff6fe8dc20f7a8f89447d4dc2038")

exo_daily <- target_generation_exo_daily(fcr_files, bvr_files)

s3 <- arrow::s3_bucket("bio230121-bucket01", endpoint_override = "renc.osn.xsede.org")
s3$CreateDir("vera4cast/targets/daily")

s3 <- arrow::s3_bucket("bio230121-bucket01/vera4cast/targets/daily", endpoint_override = "renc.osn.xsede.org")
arrow::write_csv_arrow(exo_daily, sink = s3$path("daily-targets.csv.gz"))
