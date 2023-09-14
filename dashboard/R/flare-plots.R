# Script of functions for different plots

# Plot the temperature forecast
# depths - what depths to facet
plot_temp <- function(score_df, depths = 0.5) {

  # Generate labels for plots
  my_breaks <- lubridate::with_tz(seq(min(score_df$datetime), max(score_df$datetime), by = "1 day"),"America/New_York")
  my_label <- lubridate::with_tz(seq(lubridate::as_datetime(score_df$reference_datetime)[1], max(score_df$datetime), by = "5 days"),"America/New_York")
  my_labels <- as.character(my_breaks)
  my_labels[which(!(my_breaks %in% my_label))] <- " "

  my_labels <- as.Date(my_labels, format = "%Y-%m-%d")
  my_labels <- as.character(my_labels)
  my_labels[is.na(my_labels)] <- " "

  y_label <- expression(paste('Water temperature (',degree,'C)', sep = ""))

  # Generate the pot
  score_df |>
    # Filter the score_df and get in the right format
    dplyr::filter(depth %in% depths) |>
    dplyr::mutate(datetime = lubridate::with_tz(lubridate::as_datetime(datetime), "America/New_York"),
                  reference_datetime = lubridate::with_tz(lubridate::as_datetime(reference_datetime), "America/New_York"),
                  depth = paste0("Depth: ", depth)) |>
    dplyr::filter(datetime >= reference_datetime) |>

    ggplot(aes(x = datetime)) +
    geom_ribbon(aes(ymin = quantile10, ymax = quantile90), fill = "lightblue", color = "lightblue") +
    geom_line(aes(y = mean)) +
    scale_x_continuous(breaks = my_breaks, labels = my_labels) +
    facet_wrap(~depth) +
    labs(y = y_label) +
    ylim(c(-5,40)) +
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=0.2)) +
    theme(text = element_text(size = 20))
}


plot_temp_single_panel <- function(score_data,site_identifier, model_identifier, depth_values,site_name,y_axis_limits) {

  # find most recent forecast run date for the model and site
  most_recent <-  arrow::open_dataset(score_data) |>
    filter(site_id %in% site_identifier,
           model_id %in% model_identifier) |>
    summarize(max = max(reference_datetime)) |>
    collect() |>
    pull()

  # subset and collect data based off of site, model, depth, and reference datetime
  score_df <- arrow::open_dataset(score_data) |>
    filter(variable == "temperature",
           # depth %in% c(0.5),
           depth %in% depth_values,
           site_id %in% site_identifier,
           model_id %in% model_identifier,
           reference_datetime == most_recent) |>
    dplyr::collect()

  # Fix dates and rename columns to match plotting code
  plot_df <- score_df |>
    dplyr::mutate(datetime = lubridate::with_tz(lubridate::as_datetime(datetime), "America/New_York"),
                  reference_datetime = lubridate::with_tz(lubridate::as_datetime(reference_datetime), "America/New_York")) |>#,
    dplyr::filter(datetime >= reference_datetime) |>
    rename(date = datetime, forecast_mean = mean, forecast_sd = sd, forecast_upper_90 = quantile90, forecast_lower_90 = quantile10,
           observed = observation, forecast_start_day = reference_datetime)


  curr_tibble <- plot_df

  p <- ggplot2::ggplot(curr_tibble, ggplot2::aes(x = as.Date(date))) +
    ggplot2::ylim(y_axis_limits) +
    ggplot2::geom_line(ggplot2::aes(y = forecast_mean, color = as.factor(depth)), size = 0.5)+
    ggplot2::geom_ribbon(ggplot2::aes(ymin = forecast_lower_90, ymax = forecast_upper_90,
                                      fill = as.factor(depth)),
                         alpha = 0.2) +
    #ggplot2::geom_point(data = obs_hist, ggplot2::aes(y = value, color = as.factor(depth)), size = 2) +
    ggplot2::geom_vline(aes(xintercept = as.Date(forecast_start_day),
                            linetype = "solid"),
                        alpha = 1) +
    #alpha = forecast_start_day_alpha) +
    #ggplot2::annotate(x = as.Date(curr_tibble$forecast_start_day - 2*24*60*60), y = max(curr_tibble$forecast_upper_90), label = 'Past', geom = 'text') +
    #ggplot2::annotate(x = as.Date(curr_tibble$forecast_start_day + 3*24*60*60), y = max(curr_tibble$forecast_upper_90), label = 'Future', geom = 'text') +
    ggplot2::annotate(x = as.Date(curr_tibble$forecast_start_day - 24*60*60), y = max(curr_tibble$forecast_upper_90), label = 'Past', geom = 'text') +
    ggplot2::annotate(x = as.Date(curr_tibble$forecast_start_day + 24*60*60), y = max(curr_tibble$forecast_upper_90), label = 'Future', geom = 'text') +
    ggplot2::theme_light() +
    ggplot2::scale_fill_manual(name = "Depth (m)",
                               values = c("#D55E00", '#009E73', '#0072B2'),
                               labels = as.character(depth_values)) +
    #labels = c('0.1', '5.0', '10.0')) +
    ggplot2::scale_color_manual(name = "Depth (m)",
                                values = c("#D55E00", '#009E73', '#0072B2'),
                                labels = as.character(depth_values)) +
    #labels = c('0.1', '5.0', '10.0')) +
    ggplot2::scale_x_date(date_breaks = '4 days',
                          date_labels = '%b %d\n%a',
                          limits = c(as.Date(min(curr_tibble$date) - 1), as.Date(max(curr_tibble$date)))) +
    #limits = c(as.Date(min(obs_hist$date)), as.Date(max(curr_tibble$date)))) +
    #limits = c(as.Date(config$run_config$start_datetime) - 1, as.Date(config$run_config$forecast_start_datetime) + num_days_plot)) +
    ggplot2::scale_linetype_manual(name = "",
                                   values = c('solid'),
                                   labels = c('Forecast Date')) +
    ggplot2::scale_y_continuous(name = 'Temperature (°C)',
                                sec.axis = sec_axis(trans = (~.*(9/5) + 32), name = 'Temperature (°F)')) +
    ggplot2::labs(x = "Date",
                  y = "Temperature (°C)", #state_names[i],
                  fill = 'Depth (m)',
                  color = 'Depth',
                  title = paste0(site_name," water temperature forecast, ", lubridate::date(curr_tibble$forecast_start_day)),
                  caption = 'Points represent sensor observations of water temperature. Lines represents the mean prediction from the forecast ensembles, or the most likely outcome.\n The shaded areas represent the 90% confidence interval of the forecast, or the possible range of outcomes based on the forecast.') +
    ggplot2::theme(axis.text.x = ggplot2::element_text(size = 10),
                   plot.title = element_text(size = 16))

  print(p)

}



plot_depth <- function(score_df) {
  # Generate labels for plots
  my_breaks <- lubridate::with_tz(seq(min(score_df$datetime), max(score_df$datetime), by = "1 day"),"America/New_York")
  my_label <- lubridate::with_tz(seq(lubridate::as_datetime(score_df$reference_datetime)[1], max(score_df$datetime), by = "5 days"), "America/New_York")
  my_labels <- as.character(my_breaks)
  my_labels[which(!(my_breaks %in% my_label))] <- " "

  my_labels <- as.Date(my_labels, format = "%Y-%m-%d")
  my_labels <- as.character(my_labels)
  my_labels[is.na(my_labels)] <- " "


  # limits for axes
  depth_change <- ceiling((max(score_df$mean) - min(score_df$mean))*2)/2
  max_depth <- ceiling(max(score_df$mean)*2)/2

  # Generate plot
  score_df %>%
    # Filter the dataframe and get in right format
    dplyr::mutate(datetime = lubridate::with_tz(lubridate::as_datetime(datetime), "America/New_York"),
                  reference_datetime = lubridate::with_tz(lubridate::as_datetime(reference_datetime), "America/New_York")) %>%
    dplyr::filter(datetime >= reference_datetime) |>

    ggplot(aes(x=datetime))+
    geom_ribbon(aes(ymin = quantile10, ymax = quantile90), colour = 'lightgreen', fill = 'lightgreen') +
    geom_line(aes( y=mean)) +
    scale_x_continuous(breaks = my_breaks, labels = my_labels) +
    scale_y_continuous(limits = c(max_depth - depth_change, max_depth)) +
    labs(y = 'Lake depth (m)') +
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=0.2)) +
    theme(text = element_text(size = 20),
          panel.grid.minor = element_blank())
}

# Plot the % chance of being mixed - needs ensemble forecast
# eval_depths = depths used to determine mixing, either max min or specific depths
# use_density = use a density difference to determine mixing? T/F
# threshold = the density or temperature difference used to determine mixing

plot_mixing <- function(forecast_df, eval_depths = 'min/max', use_density = TRUE, threshold = 0.1) {

  # Labels for plot
  my_breaks <- lubridate::with_tz(seq(min(forecast_df$datetime), max(forecast_df$datetime), by = "1 day"),"America/New_York")
  my_label <- lubridate::with_tz(seq(lubridate::as_datetime(forecast_df$reference_datetime)[1], max(forecast_df$datetime), by = "5 days"), "America/New_York")
  my_labels <- as.character(my_breaks)
  my_labels[which(!(my_breaks %in% my_label))] <- " "

  my_labels <- as.Date(my_labels, format = "%Y-%m-%d")
  my_labels <- as.character(my_labels)
  my_labels[is.na(my_labels)] <- " "

  # which depths should be evaluated to determine mixing
  if (!is.numeric(eval_depths)) {
    # extracts the maximum and minimum in the forecast
    max_depth <- forecast_df |>
      filter(variable == "temperature") |>
      select(datetime, parameter, depth, variable, prediction) |>
      mutate(is_na = ifelse(is.na(prediction), 1, 0)) |>
      group_by(depth) |>
      summarize(num_na = sum(is_na), .groups = "drop") |>
      filter(num_na == 0) |>
      summarize(max = max(depth)) |>
      pull(max)

    min_depth <- min(forecast_df$depth, na.rm = T)
  } else {
    # or uses the user specified values
    max_depth <- max(eval_depths)
    min_depth <- min(eval_depths)
  }

  # if use_density is false uses a temperature difference
  if (use_density == FALSE) {
    message(paste0('using a ', threshold, ' C temperature difference to define mixing'))
    temp_forecast <- forecast_df |>
      filter(depth %in% c(max_depth, min_depth),
             datetime >= reference_datetime) |>
      pivot_wider(names_from = depth, names_prefix = 'wtr_', values_from = prediction)

    colnames(temp_forecast)[which(colnames(temp_forecast) == paste0('wtr_', min_depth))] <- 'min_depth'
    colnames(temp_forecast)[which(colnames(temp_forecast) == paste0('wtr_', max_depth))] <- 'max_depth'

    temp_forecast |>
      mutate(mixed = ifelse((min_depth - max_depth) < threshold,
                            1, 0)) |>
      group_by(datetime) |>
      summarise(percent_mix = 100*(sum(mixed)/n())) |>
      ggplot(aes(datetime, y=percent_mix)) +
      geom_line() +
      scale_x_continuous(breaks = my_breaks, labels = my_labels) +
      scale_y_continuous(limits = c(0,100)) +
      labs(y = '% chance of lake mixing') +
      theme_bw() +
      theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=0.2)) +
      theme(text = element_text(size = 20),
            plot.caption = element_text(size = 12),
            panel.grid.minor = element_blank())
  }


  if (use_density == TRUE) {
    message(paste0('using a ', threshold, ' kg/m3 density difference to define mixing'))

    temp_forecast <- forecast_df |>
      filter(depth %in% c(max_depth, min_depth),
             datetime >= reference_datetime) |>
      pivot_wider(names_from = depth, names_prefix = 'wtr_', values_from = prediction)# %>% na.omit()

    colnames(temp_forecast)[which(colnames(temp_forecast) == paste0('wtr_', min_depth))] <- 'min_depth'
    colnames(temp_forecast)[which(colnames(temp_forecast) == paste0('wtr_', max_depth))] <- 'max_depth'

    temp_forecast |>
      mutate(min_depth = rLakeAnalyzer::water.density(min_depth),
             max_depth = rLakeAnalyzer::water.density(max_depth),
             mixed = ifelse((max_depth - min_depth) < threshold,
                            1, 0)) |>
      group_by(datetime) |>
      summarise(percent_mix = 100*(sum(mixed)/n())) |>
      ggplot(aes(datetime, y=percent_mix)) +
      geom_line() +
      scale_x_continuous(breaks = my_breaks, labels = my_labels) +
      scale_y_continuous(limits = c(0,100)) +
      labs(y = '% chance of lake mixing') +
      theme_bw() +
      theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=0.2)) +
      theme(text = element_text(size = 20),
            plot.caption = element_text(size = 12),
            panel.grid.minor = element_blank())
  }



}


# Generate plot for ice chance %
plot_ice <- function(forecast_df) {


  # Labels for plot
  my_breaks <- lubridate::with_tz(seq(min(forecast_df$datetime), max(forecast_df$datetime), by = "1 day"),"America/New_York")
  my_label <- lubridate::with_tz(seq(lubridate::as_datetime(forecast_df$reference_datetime)[1], max(forecast_df$datetime), by = "5 days"),"America/New_York")
  my_labels <- as.character(my_breaks)
  my_labels[which(!(my_breaks %in% my_label))] <- " "

  my_labels <- as.Date(my_labels, format = "%Y-%m-%d")
  my_labels <- as.character(my_labels)
  my_labels[is.na(my_labels)] <- " "

  forecast_df %>%
    mutate(ice = ifelse(prediction > 0, 1, 0)) %>%
    dplyr::filter(datetime >= reference_datetime) |>
    group_by(datetime) %>%
    summarise(percent_ice = 100*(sum(ice)/n())) %>%
    ggplot(., aes(datetime, y=percent_ice)) +
    geom_line() +
    scale_x_continuous(breaks = my_breaks, labels = my_labels) +
    scale_y_continuous(limits = c(0,100)) +
    labs(y = '% chance of ice') +
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=0.2)) +
    theme(text = element_text(size = 20),
          plot.caption = element_text(size = 12),
          panel.grid.minor = element_blank())
}
