library(tidyverse)
library(reticulate)


py_install("fastavro",pip=TRUE)
py_install("pandas",pip=TRUE)
py_require(c("fastavro"))
py_require(c("pandas"))


# Source the Python script
source_python("targets/R/read_avro.py")

# Call the Python function from R
avro_data <- read_avro_file("targets/R/CRAM_L0_to_L1_Water_Quality_DP1.20288.001__2025-07-01.avro")

# View the data
print(avro_data)



