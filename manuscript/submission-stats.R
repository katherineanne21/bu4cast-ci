library(tidyverse)
library(patchwork)
library(duckdbfs)

sites <- read_csv("neon4cast_field_site_metadata.csv") |>
  pull(field_site_id)

#Figure 1

all_results <- duckdbfs::open_dataset("s3://anonymous@bio230014-bucket01/challenges/scores/bundled-parquet/project_id=neon4cast/?endpoint_override=sdsc.osn.xsede.org")

df <- all_results |>
  filter(duration %in% c("P1D", "P1W")) |>
  distinct(reference_datetime, model_id, variable, duration, datetime, site_id) |>
  collect() |>
  filter(site_id %in% sites)

time_plot <- df |>
  distinct(reference_datetime, model_id, variable, duration, site_id) |>
  mutate(month = ym(paste0(year(reference_datetime),"-",month(reference_datetime)))) |>
  filter(reference_datetime >= as_date("2021-01-01 00:00:00") &
           reference_datetime < as_date("2025-02-01 00:00:00")) |>
  group_by(month) |>
  summarize(count = n()) |>
  ggplot(aes(x = month, y = count)) +
  geom_line() +
  labs(x = "Time", y = "# of submissions per month") +
  theme_bw()

ggsave("manuscript/submission_rate.png", time_plot, width = 3.5, height = 2.5)

# Figure 2

all_results <- duckdbfs::open_dataset("s3://anonymous@bio230014-bucket01/challenges/forecasts/bundled-parquet/project_id=neon4cast/duration=P1D/variable=temperature/model_id=flareGLM?endpoint_override=sdsc.osn.xsede.org")

df4 <- all_results |>
  filter(reference_datetime == as_datetime("2023-07-10 00:00:00")) |>
  collect()

aquatics_targets <- read_csv("https://sdsc.osn.xsede.org/bio230014-bucket01/challenges/targets/project_id=neon4cast/duration=P1D/aquatics-targets.csv.gz", show_col_types = FALSE)

df6 <- all_results |>
  filter(datetime == as_datetime("2023-07-17 00:00:00")) |>
  collect()


p1 <- df4 |>
  filter(site_id == "CRAM", parameter < 150) |>
  left_join(aquatics_targets) |>
  ggplot(aes(x = datetime, y = prediction, group = parameter)) +
  geom_line(color = "gray", alpha = 0.9) +
  geom_point(aes(y = observation)) +
  labs(y = "predicted water temperature (°C)", title = "Example 30-day ahead\nensemble forecast submission", tag = "(a)") +
  theme_bw() +
  theme(plot.title = element_text(size = 10))

p2 <- df6 |>
  filter(site_id == "CRAM" & parameter < 150) |>
  left_join(aquatics_targets) |>
  mutate(horizon = (datetime - reference_datetime)) |>
  ggplot(aes(x = horizon, y = prediction, group = parameter)) +
  geom_line(color = "gray", alpha = 0.9) +
  geom_point(aes(y  = observation)) +
  labs(y = "predicted water temperature (°C)", title = "Example of how the same\nobservation is forecasted\nwithdifferent lead times (horizon)", x = "Horizon (e.g., lead time), days", tag = "(b)") +
  theme_bw() +
  theme(plot.title = element_text(size = 10))

p3 <- p1 + p2 + plot_layout(axis_titles = "collect")

ggsave("manuscript/example.png", p3, width = 5.3, height = 3)

# Figure 3
#Submissions

d1 <- df |>
  distinct(model_id, reference_datetime, variable, duration, site_id) |>
  group_by(variable) |>
  filter(variable != "ixodes_scapularis") |>
  summarize(count = n())

df1_sum <- d1 |>
  summarize(sum = sum(count))

d1 <- d1 |>
  mutate(type = paste0("(a) Total forecast submssions: ", prettyNum(df1_sum$sum, big.mark = ",", scientific = FALSE)),
         tag = "(a)")

print(paste0("Mean submissions per month: ", df1_sum / (12 * 4)))

#Forecast observation pairs
d2 <- df |>
  distinct(model_id, reference_datetime, datetime, variable, duration, site_id) |>
  group_by(variable) |>
  filter(variable != "ixodes_scapularis") |>
  summarize(count = n())

df2_sum <- d2 |>
  summarize(sum = sum(count))


d2 <- d2 |>
  mutate(type = paste0("(b) Total forecast-observation pairs: ", prettyNum(df2_sum$sum, big.mark = ",", scientific = FALSE)),
         tag = "(b)")

#Teams
d3 <- df |>
  distinct(model_id, variable) |>
  group_by(variable) |>
  filter(variable != "ixodes_scapularis") |>
  summarize(count = n())

df3_sum <- df |>
  distinct(model_id) |>
  summarize(sum = n())

d3 <- d3 |>
  mutate(type = paste0("(c) Total unique models: ", prettyNum(df3_sum$sum, big.mark = ",", scientific = FALSE)),
         tag = "(c)")

# Combined together

combined<- bind_rows(d1, d2, d3) |>
  mutate(`Forecast\nsubmission\nfrequency` = "daily",
         `Forecast\nsubmission\nfrequency` = ifelse(variable %in% c("amblyomma_americanum", "richness", "abundance"), "weekly", `Forecast\nsubmission\nfrequency`),
         variable = ifelse(variable == "temperature", "water\ntemperature", variable),
         variable = ifelse(variable == "oxygen", "water\ndissolved\noxygen", variable),
         variable = ifelse(variable == "nee", "terrestrial\ncarbon\nexchange", variable),
         variable = ifelse(variable == "le", "terrestrial\nevapo-\ntranspiration", variable),
         variable = ifelse(variable == "gcc_90", "canopy\ngreenness", variable),
         variable = ifelse(variable == "rcc_90", "canopy\nredness", variable),
         variable = ifelse(variable == "amblyomma_americanum", "tick\ndensity", variable),
         variable = ifelse(variable == "abundance", "beetle\nabundance", variable),
         variable = ifelse(variable == "richness", "beetle\nrichness", variable),
         variable = ifelse(variable == "chla", "water\nchlorophyll-a", variable)) |>
  mutate(type = factor(type, levels = c(paste0("(a) Total forecast submssions: ", prettyNum(df1_sum$sum, big.mark = ",", scientific = FALSE)),
                                        paste0("(b) Total forecast-observation pairs: ", prettyNum(df2_sum$sum, big.mark = ",", scientific = FALSE)),
                                        paste0("(c) Total unique models: ", prettyNum(df3_sum$sum, big.mark = ",", scientific = FALSE))))) |>
  ggplot() +
  geom_col(aes(x = variable, y = count, fill = `Forecast\nsubmission\nfrequency`)) +
  theme_bw() +
  facet_wrap(~type, ncol = 1, scales = "free_y") +
  theme(legend.position = "bottom") +
  theme(axis.text.x = element_text(size = 9)) + theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))

ggsave("manuscript/counts.png",combined, width = 5.3, height = 6)

#Total Rows of data

urls <- c("https://sdsc.osn.xsede.org/bio230014-bucket01/challenges/targets/project_id=neon4cast/duration=P1D/terrestrial_daily-targets.csv.gz",
          "https://sdsc.osn.xsede.org/bio230014-bucket01/challenges/targets/project_id=neon4cast/duration=P1D/aquatics-targets.csv.gz",
          "https://sdsc.osn.xsede.org/bio230014-bucket01/challenges/targets/project_id=neon4cast/duration=P1D/phenology-targets.csv.gz",
          "https://sdsc.osn.xsede.org/bio230014-bucket01/challenges/targets/project_id=neon4cast/duration=P1W/beetles-targets.csv.gz",
          "https://sdsc.osn.xsede.org/bio230014-bucket01/challenges/targets/project_id=neon4cast/duration=P1W/ticks-targets.csv.gz")

df <- read_csv(urls)

df_target <- df |>
  filter(datetime < as_datetime("2025-01-01 00:00:00") & !is.na(observation)) |>
  filter(site_id %in% sites) |>
  summarize(count = n())

all_results <- duckdbfs::open_dataset("s3://anonymous@bio230014-bucket01/challenges/forecasts/bundled-parquet/project_id=neon4cast/?endpoint_override=sdsc.osn.xsede.org")

df_scores <- all_results |>
  filter(duration %in% c("P1D", "P1W"),
         reference_datetime >= as_datetime("2021-01-01 00:00:00"),
         reference_datetime < as_datetime("2025-02-01 00:00:00")) |>
  distinct(reference_datetime, model_id, variable, duration, datetime, site_id) |>
  filter(site_id %in% sites) |>
  summarize(count = n()) |>
  collect()


df_forecasts <- duckdbfs::open_dataset("s3://anonymous@bio230014-bucket01/challenges/forecasts/bundled-parquet/project_id=neon4cast/?endpoint_override=sdsc.osn.xsede.org")   |>
  filter(duration %in% c("P1D", "P1W"),
         reference_datetime >= as_datetime("2021-01-01 00:00:00"),
         reference_datetime < as_datetime("2025-02-01 00:00:00")) |>
  distinct(reference_datetime, model_id, variable, duration, datetime, site_id, parameter) |>
  filter(site_id %in% sites) |>
  summarize(count = n()) |>
  collect()


prettyNum(df_forecasts$count + df_scores$count + df_target$count, big.mark = ",", scientific = FALSE)








