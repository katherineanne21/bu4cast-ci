## reads site_id, latitude, and longitude for each project from the s3 bucket and uploads a combined df of all sites across all projects
# needs to be run anytime we add new sites
library(arrow)
library(dplyr)
library(yaml)

config <- yaml::read_yaml("challenge_configuration.yaml")

s3 <- arrow::s3_bucket(
  config$s3_bucket_read,
  endpoint_override = config$endpoint,
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)

# Read each sites file from bucket
coastal_sites <- tryCatch(
  arrow::read_csv_arrow(s3$path(paste0(config$target_metadata_bucket, "/coastal-targets-sites.csv"))) %>%
    as.data.frame(),
  error = function(e) { message("Could not read coastal sites: ", e$message); NULL }
)

urban_sites <- tryCatch(
  arrow::read_csv_arrow(s3$path(paste0(config$target_metadata_bucket, "/urban-targets-sites.csv"))) %>%
    as.data.frame() %>%
    dplyr::select(field_site_id, latitude, longitude),
  error = function(e) { message("Could not read urban sites: ", e$message); NULL }
)

disease_sites <- tryCatch(
  arrow::read_csv_arrow(s3$path(paste0(config$target_metadata_bucket, "/disease-targets-sites.csv"))) %>%
    as.data.frame(),
  error = function(e) { message("Could not read disease sites: ", e$message); NULL }
)

# Standardize column names and combine
standardize_sites <- function(df) {
  df %>% transmute(
    field_site_id = as.character(field_site_id),
    latitude      = as.numeric(latitude),
    longitude     = as.numeric(longitude)
  )
}

all_sites <- bind_rows(
  if (!is.null(coastal_sites)) standardize_sites(coastal_sites),
  if (!is.null(urban_sites))   standardize_sites(urban_sites),
  if (!is.null(disease_sites)) standardize_sites(disease_sites)
)
message("Total sites: ", nrow(all_sites))

# Upload combined file back to bucket
arrow::write_csv_arrow(
  all_sites,
  sink = s3$path(paste0(config$target_metadata_bucket, "/field_sites.csv"))
)
message("Field sites upload complete.")
