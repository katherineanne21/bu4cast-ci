library(tidyverse)
library(reticulate)


py_install("fastavro",pip=TRUE)
py_require(c("fastavro"))
py_require(c("pandas"))

read.avro <- function(path){
  d <- glue::glue(
    "from fastavro import reader\
import pandas as pd\
with open('{path}', 'rb') as f:\
  \tavro_reader = reader(f)\
  \trecords = list(avro_reader)\
df = pd.DataFrame(records)\
")

  py_run_string(d)

  avro_data <- tibble::as_tibble(py$df)

  return(avro_data)
}

df <- read.avro("targets/R/CRAM_L0_to_L1_Water_Quality_DP1.20288.001__2025-07-01.avro")

