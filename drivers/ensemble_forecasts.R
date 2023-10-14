source("drivers/download_ensemble_forecast.R")
source("drivers/submit_met_forecast.R")

model_id <- "gfs_seamless"
download_ensemble_forecast(model_id = model_id)
submit_met_forecast(model_id)

model_id <- "icon_seamless"
download_ensemble_forecast(model_id, forecast_horizon = 7, sites = "fcre")
submit_met_forecast(model_id)

model_id <- "gem_global"
download_ensemble_forecast(model_id, forecast_horizon = 32, sites = "fcre")
submit_met_forecast(model_id)

model_id <- "ecmwf_ifs04"
download_ensemble_forecast(model_id, forecast_horizon = 10, sites = "fcre")
submit_met_forecast(model_id)





