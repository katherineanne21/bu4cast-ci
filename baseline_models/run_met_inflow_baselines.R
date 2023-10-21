print(paste0("Running Creating baselines at ", Sys.time()))

library(tidyverse)
library(lubridate)
library(aws.s3)
library(imputeTS)
library(tsibble)
library(fable)


#' set the random number for reproducible MCMC runs
set.seed(329)

forecast_horizon <- 35

config <- yaml::read_yaml("challenge_configuration.yaml")

#'Team name code
team_name <- "climatology"


targets <- readr::read_csv(paste0("https://", config$endpoint, "/", config$targets_bucket, "/project_id=vera4cast/duration=P1D/daily-met-targets.csv.gz"), guess_max = 10000, show_col_types = FALSE)

sites <- read_csv(config$site_table, show_col_types = FALSE)

site_names <- sites$site_id

target_clim <- targets %>%
  filter(variable %in% c("AirTemp_C_mean")) %>%
  mutate(doy = yday(datetime)) %>%
  group_by(doy, site_id, variable) %>%
  summarise(clim_mean = mean(observation, na.rm = TRUE),
            clim_sd = sd(observation, na.rm = TRUE),
            .groups = "drop") %>%
  mutate(clim_mean = ifelse(is.nan(clim_mean), NA, clim_mean))

#curr_month <- month(Sys.Date())
curr_month <- month(Sys.Date())
if(curr_month < 10){
  curr_month <- paste0("0", curr_month)
}

curr_year <- year(Sys.Date())
start_date <- Sys.Date() + days(1)

forecast_dates <- seq(start_date, as_date(start_date + days(forecast_horizon)), "1 day")
forecast_doy <- yday(forecast_dates)

forecast_dates_df <- tibble(datetime = forecast_dates,
                            doy = forecast_doy)

forecast <- target_clim %>%
  mutate(doy = as.integer(doy)) %>%
  filter(doy %in% forecast_doy) %>%
  full_join(forecast_dates_df, by = 'doy') %>%
  arrange(site_id, datetime)

subseted_site_names <- unique(forecast$site_id)
site_vector <- NULL
for(i in 1:length(subseted_site_names)){
  site_vector <- c(site_vector, rep(subseted_site_names[i], length(forecast_dates)))
}

forecast_tibble1 <- tibble(datetime = rep(forecast_dates, length(subseted_site_names)),
                           site_id = site_vector,
                           variable = "AirTemp_C_mean")

forecast_tibble2 <- NULL


forecast_tibble <- bind_rows(forecast_tibble1, forecast_tibble2)

foreast <- right_join(forecast, forecast_tibble, by = join_by("site_id", "variable", "datetime"))

site_count <- forecast %>%
  select(datetime, site_id, variable, clim_mean, clim_sd) %>%
  filter(!is.na(clim_mean)) |>
  group_by(site_id, variable) %>%
  summarize(count = n(), .groups = "drop") |>
  filter(count > 2) |>
  distinct() |>
  pull(site_id)

combined <- forecast %>%
  filter(site_id %in% site_count) |>
  select(datetime, site_id, variable, clim_mean, clim_sd) %>%
  rename(mean = clim_mean,
         sd = clim_sd) %>%
  group_by(site_id, variable) %>%
  mutate(mu = imputeTS::na_interpolation(x = mean),
         sigma = median(sd, na.rm = TRUE))

combined <- combined %>%
  pivot_longer(c("mu", "sigma"),names_to = "parameter", values_to = "prediction") |>
  mutate(family = "normal") |>
  mutate(reference_datetime = min(combined$datetime) - lubridate::days(1),
         model_id = "climatology") |>
  select(model_id, datetime, reference_datetime, site_id, variable, family, parameter, prediction)

#combined %>%
#  filter(variable == "AirTemp_C_mean") |>
#  pivot_wider(names_from = parameter, values_from = prediction) %>%
#  ggplot(aes(x = datetime)) +
#  geom_ribbon(aes(ymin=mu - sigma*1.96, ymax=mu + sigma*1.96), alpha = 0.1) +
#  geom_point(aes(y = mu)) +
#  facet_wrap(~site_id)

combined_met <- combined |>
  mutate(depth_m = NA,
         site_id = "fcre",
         project_id = "vera4cast",
         duration = "P1D")


####

targets <- readr::read_csv(paste0("https://", config$endpoint, "/", config$targets_bucket, "/duration=P1D/daily-inflow-targets.csv.gz"), guess_max = 10000, show_col_types = FALSE)

site_names <- "tubr"

target_clim <- targets %>%
  filter(variable %in% c("Flow_cms_mean","Temp_C_mean")) %>%
  mutate(doy = yday(datetime)) %>%
  group_by(doy, site_id, variable) %>%
  summarise(clim_mean = mean(observation, na.rm = TRUE),
            clim_sd = sd(observation, na.rm = TRUE),
            .groups = "drop") %>%
  mutate(clim_mean = ifelse(is.nan(clim_mean), NA, clim_mean))

#curr_month <- month(Sys.Date())
curr_month <- month(Sys.Date())
if(curr_month < 10){
  curr_month <- paste0("0", curr_month)
}

curr_year <- year(Sys.Date())
start_date <- Sys.Date() + days(1)

forecast_dates <- seq(start_date, as_date(start_date + days(forecast_horizon)), "1 day")
forecast_doy <- yday(forecast_dates)

forecast_dates_df <- tibble(datetime = forecast_dates,
                            doy = forecast_doy)

forecast <- target_clim %>%
  mutate(doy = as.integer(doy)) %>%
  filter(doy %in% forecast_doy) %>%
  full_join(forecast_dates_df, by = 'doy') %>%
  arrange(site_id, datetime)

subseted_site_names <- unique(forecast$site_id)
site_vector <- NULL
for(i in 1:length(subseted_site_names)){
  site_vector <- c(site_vector, rep(subseted_site_names[i], length(forecast_dates)))
}

forecast_tibble1 <- tibble(datetime = rep(forecast_dates, length(subseted_site_names)),
                           site_id = site_vector,
                           variable = "Flow_cms_mean")

forecast_tibble2 <- tibble(datetime = rep(forecast_dates, length(subseted_site_names)),
                           site_id = site_vector,
                           variable = "Temp_C_mean")


forecast_tibble <- bind_rows(forecast_tibble1, forecast_tibble2)

foreast <- right_join(forecast, forecast_tibble, by = join_by("site_id", "variable", "datetime"))

site_count <- forecast %>%
  select(datetime, site_id, variable, clim_mean, clim_sd) %>%
  filter(!is.na(clim_mean)) |>
  group_by(site_id, variable) %>%
  summarize(count = n(), .groups = "drop") |>
  filter(count > 2) |>
  distinct() |>
  pull(site_id)

combined <- forecast %>%
  filter(site_id %in% site_count) |>
  select(datetime, site_id, variable, clim_mean, clim_sd) %>%
  rename(mean = clim_mean,
         sd = clim_sd) %>%
  group_by(site_id, variable) %>%
  mutate(mu = imputeTS::na_interpolation(x = mean),
         sigma = median(sd, na.rm = TRUE))

combined <- combined %>%
  pivot_longer(c("mu", "sigma"),names_to = "parameter", values_to = "prediction") |>
  mutate(family = "normal") |>
  mutate(reference_datetime = min(combined$datetime) - lubridate::days(1),
         model_id = "climatology") |>
  select(model_id, datetime, reference_datetime, site_id, variable, family, parameter, prediction)

combined %>%
  pivot_wider(names_from = parameter, values_from = prediction) %>%
  ggplot(aes(x = datetime)) +
  geom_ribbon(aes(ymin=mu - sigma*1.96, ymax=mu + sigma*1.96), alpha = 0.1) +
  geom_point(aes(y = mu)) +
  facet_wrap(~variable, scale = "free")

combined_inflow <- combined |>
  mutate(depth_m = NA,
         site_id = "tubr",
         project_id = "vera4cast",
         duration = "P1D")


combined <- bind_rows(combined_met,combined_inflow)

combined %>%
  pivot_wider(names_from = parameter, values_from = prediction) %>%
  ggplot(aes(x = datetime)) +
  geom_ribbon(aes(ymin=mu - sigma*1.96, ymax=mu + sigma*1.96), alpha = 0.1) +
  geom_point(aes(y = mu)) +
  facet_wrap(~variable, scale = "free")

file_date <- combined$reference_datetime[1]

forecast_file <- paste("met_inflow", file_date, "climatology.csv.gz", sep = "-")

write_csv(combined, forecast_file)

vera4castHelpers::forecast_output_validator(forecast_file)

vera4castHelpers::submit(forecast_file = forecast_file,
                         ask = FALSE,
                         first_submission = FALSE)

unlink(forecast_file)

