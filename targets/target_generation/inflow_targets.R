library(tidyverse)

source('targets/target_functions/inflow/target_generation_inflows.R')

current_inflow <- 'https://raw.githubusercontent.com/FLARE-forecast/FCRE-data/fcre-weir-data-qaqc/FCRWeir_L1.csv'

historic_inflow <- "https://pasta.lternet.edu/package/data/eml/edi/202/10/c065ff822e73c747f378efe47f5af12b"

historic_silica <- 'https://pasta.lternet.edu/package/data/eml/edi/542/1/791ec9ca0f1cb9361fa6a03fae8dfc95'

historic_nutrients <- "https://pasta.lternet.edu/package/data/eml/edi/199/11/509f39850b6f95628d10889d66885b76"

historic_ghg <- "https://pasta.lternet.edu/package/data/eml/edi/551/7/38d72673295864956cccd6bbba99a1a3"


inflow_daily <- target_generation_inflows(historic_inflow = historic_inflow,
                                          current_inflow = current_inflow,
                                          historic_nutrients = historic_nutrients,
                                          historic_silica = silica_df,
                                          historic_ghg = historic_ghg)

s3 <- arrow::s3_bucket("bio230121-bucket01", endpoint_override = "renc.osn.xsede.org")
s3$CreateDir("vera4cast/targets/daily")

s3 <- arrow::s3_bucket("bio230121-bucket01/vera4cast/targets/daily", endpoint_override = "renc.osn.xsede.org")
arrow::write_csv_arrow(fluoro_daily, sink = s3$path("daily-targets.csv.gz"))
