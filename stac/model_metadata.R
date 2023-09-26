googlesheets4::gs4_deauth()
registered_models <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/1-OsDaOoMZwPfQnz5U5aV-T9_vhTmyg92Ff5ARRunYhY/edit?usp=sharing")

i <- 1

metadata <- list()

metadata$creator$individual_name <- registered_models[i, 3]
metadata$creator$electronicMailAddress <- registered_models[i, 4]
metadata$creator$organizationName <- registered_models[i, 5]
metadata$model_id <- registered_models[i, 2]
metadata$model_description$model_version
metadata$model_description$name #LONG VERSION OF THE MODEL NAME
metadata$model_description$type <- registered_models[i, 16]
metadata$model_description$repository <- registered_models[i, 22]


if(registered_model_id[i,9] == "No"){
  metadata$uncertainty$uncertainty$initial_conditions$present <- FALSE
}else if(registered_model_id[i,9] == "Yes"){
  metadata$uncertainty$uncertainty$initial_conditions$present <- TRUE
}

metadata$uncertainty$uncertainty$initial_conditions$data_driven <- TRUE
metadata$uncertainty$uncertainty$initial_conditions$progation <- TRUE
if(metadata$uncertainty$uncertainty$initial_conditions$progation){
  metadata$uncertainty$uncertainty$initial_conditions$progation$assimilation <- "EnKF"
}

metadata$uncertainty$uncertainty$drivers$present <- TRUE
metadata$uncertainty$uncertainty$drivers$data_driven <- TRUE
metadata$uncertainty$uncertainty$drivers$progation <- TRUE

metadata$uncertainty$uncertainty$parameters$present <- TRUE
metadata$uncertainty$uncertainty$parameters$data_driven <- TRUE
metadata$uncertainty$uncertainty$parameters$progation <- TRUE

metadata$uncertainty$uncertainty$process$present <- TRUE
metadata$uncertainty$uncertainty$process$data_driven <- TRUE
metadata$uncertainty$uncertainty$process$progation <- TRUE

metadata$uncertainty$uncertainty$process$present <- TRUE
metadata$uncertainty$uncertainty$process$data_driven <- TRUE
metadata$uncertainty$uncertainty$process$progation <- TRUE

jsonlite::write_json(uncertainty, path = "~/Downloads/test.json")


