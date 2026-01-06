library(tidyverse)
library(tsibble)
library(fable)
source('baseline_models/R/fablePersistenceModelFunction.R')
# 1.Read in the targets data
url <- "https://sdsc.osn.xsede.org/bio230014-bucket01/challenges/targets/project_id=neon4cast/duration=P1D/phenology-targets.csv.gz"

targets <- readr::read_csv(url, guess_max = 1e6) |>
  na.omit()

# 2. Make the targets into a tsibble with explicit gaps
targets_ts <- targets %>%
  mutate(datetime = as_date(datetime)) |>
  as_tsibble(key = c('variable', 'site_id'), index = 'datetime') %>%
  # add NA values up to today (index)
  fill_gaps(.end = Sys.Date())

# 2. Run through each via map
# Requires a dataframe that has each of the variable in the RW_forecast function
site_var_combinations <- expand.grid(site = unique(targets$site_id),
                                     var = unique(targets$variable)) %>%
  # assign the transformation depending on the variable
  mutate(transformation = 'none') %>%
  mutate(boot_number = 200,
         h = 35,
         bootstrap = T,
         verbose = T)

# runs the RW forecast for each combination of variable and site_id
RW_forecasts <- purrr::pmap_dfr(site_var_combinations, RW_daily_forecast)

# convert the output into EFI standard
RW_forecasts_EFI <- RW_forecasts %>%
  rename(parameter = .rep,
         prediction = .sim) %>%
  # For the EFI challenge we only want the forecast for future
  filter(datetime > Sys.Date()) %>%
  group_by(site_id, variable) %>%
  mutate(reference_datetime = min(datetime) - lubridate::days(1),
         family = "ensemble",
         model_id = "persistenceRW") %>%
  select(model_id, datetime, reference_datetime, site_id, family, parameter, variable, prediction) |>
  as_tibble() |>
  mutate(datetime = as_datetime(datetime),
         reference_datetime = as_datetime(reference_datetime),
         project_id = "neon4cast",
         duration = "P1D")


#RW_forecasts_EFI |>
#  filter(variable == "gcc_90") |>
#  filter(site_id %in% unique(RW_forecasts_EFI$site_id)[1:24]) |>
#  ggplot(aes(x = time, y = prediction, group = ensemble)) +
#  geom_line() +
#  facet_wrap(~site_id)

# 4. Write forecast file

file_date <- RW_forecasts_EFI$reference_datetime[1]

forecast_file <- paste("phenology", file_date, "persistenceRW.csv.gz", sep = "-")

write_csv(RW_forecasts_EFI, forecast_file)

neon4cast::submit(forecast_file = forecast_file,
                  metadata = NULL,
                  ask = FALSE)

unlink(forecast_file)
