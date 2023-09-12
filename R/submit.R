## Technically this could become arrow-based

#' submit forecast to EFI
#'
#' @inheritParams forecast_output_validator
#' @param metadata path to metadata file
#' @param ask should we prompt for a go before submission?
#' @param s3_region subdomain (leave as is for EFI challenge)
#' @param s3_endpoint root domain (leave as is for EFI challenge)
#' @export
submit <- function(forecast_file,
                   metadata = NULL,
                   ask = interactive(),
                   s3_region = "submit",
                   s3_endpoint = "ltreb-reservoirs.org"
){
  if(file.exists("~/.aws")){
    warning(paste("Detected existing AWS credentials file in ~/.aws,",
                  "Consider renaming these so that automated upload will work"))
  }
  message("validating that file matches required standard")
  source("https://raw.githubusercontent.com/LTREB-reservoirs/vera4cast/main/R/forecast_output_validator.R")
  go <- forecast_output_validator(forecast_file)

  googlesheets4::gs4_deauth()
  message("Accessing registered model_ids")
  registered_model_id <- googlesheets4::read_sheet('https://docs.google.com/spreadsheets/d/17DNtk2uSxaIKkuj4Dhbs39kdT7NCQ01Tk9qzhMWev9k/edit?usp=sharing')
  df <- readr::read_csv(forecast_file, show_col_types = FALSE)
  model_id <- df$model_id[1]
  if(!(model_id %in% registered_model_id$`What is your model_id?`)){
    warning(paste0("model_id has not been registered yet\n",
                   "register at https://docs.google.com/forms/d/e/1FAIpQLSf4rinfAzkAREHwyYbwB0Co3DwV4YndPuTdRMtHJzBsqTfbvw/viewform?usp=sf_link"))
    return(NULL)
  }

  if(!go){

    warning(paste0("forecasts was not in a valid format and was not submitted\n",
                   "First, try read reinstalling neon4cast (remotes::install_github('eco4cast\\neon4cast'), restarting R, and trying again\n",
                   "Second, see https://projects.ecoforecast.org/neon4cast-docs/Submission-Instructions.html for more information on the file format"))
    return(NULL)
  }

  if(go & ask){
    go <- utils::askYesNo("Forecast file is valid, ready to submit?")
  }

  #GENERALIZATION:  Here are specific AWS INFO
  exists <- aws.s3::put_object(file = forecast_file,
                     object = basename(forecast_file),
                     bucket = "vera4cast-submissions",
                     region= s3_region,
                     base_url = s3_endpoint)

  if(exists){
    message("Successfully submitted forecast to server")
  }else{
    warning("Forecasts was not sucessfully submitted to server")
  }

  if(!is.null(metadata)){
    if(tools::file_ext(metadata) == "xml"){
      EFIstandards::forecast_validator(metadata)
      aws.s3::put_object(file = metadata,
                         object = basename(metadata),
                         bucket = "vera4cast-submissions",
                         region= s3_region,
                         base_url = s3_endpoint)
    }else{
      warning(paste("Metadata file is not an .xml file",
                    "Did you incorrectly submit the model description yml file instead of an xml file"))
    }
  }
}

#' Check that submission was successfully processed
#'
#' @param forecast_file Your forecast csv or nc file
#' @param s3_region subdomain (leave as is for EFI challenge)
#' @param s3_endpoint root domain (leave as is for EFI challenge)
#' @export
check_submission <- function(forecast_file,
                             s3_region = "submit",
                             s3_endpoint = "ltreb-reservoirs.org"){

  theme <- stringr::str_split_fixed(basename(forecast_file), "-", n = 2)

  base_name <- forecast_file

  exists <- suppressMessages(aws.s3::object_exists(object = file.path("raw", theme[,1], base_name),
                                                   bucket = "vera4cast-forecasts",
                                                   region= s3_region,
                                                   base_url = s3_endpoint))

  if(exists){
    message("Submission was successfully processed")
  }else{
    not_in_standard <- suppressMessages(aws.s3::object_exists(object = file.path("not_in_standard", basename(forecast_file)),
                                                              bucket = "vera4cast-forecasts",
                                                              region= s3_region,
                                                              base_url = s3_endpoint))
    if(not_in_standard){
      message("Submission is not in required format. Try running neon4cast::forecast_output_validator on your file to see what the issue may be")
    }else{
        message("Your forecast is still in queue to be processed by the server. Please check again in a few hours")
    }
  }
  invisible(exists)
}
