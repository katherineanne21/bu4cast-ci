## reads site_id, latitude, and longitude for each project from the s3 bucket and uploads a combined df of all sites across all projects

library(arrow)
library(dplyr)

# S3 bucket connection 
s3_read <- arrow::s3_bucket(
  "bu4cast-ci-read",
  endpoint_override = "https://minio-s3.apps.shift.nerc.mghpcc.org",
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)

# Read each sites file from bucket
coastal_sites <- tryCatch(
  arrow::read_csv_arrow(s3_read$path("challenges/project_id=bu4cast/metadata/coastal-targets-sites.csv")) %>%
    as.data.frame(),
  error = function(e) { message("Could not read coastal sites: ", e$message); NULL }
)

urban_sites <- tryCatch(
  arrow::read_csv_arrow(s3_read$path("challenges/project_id=bu4cast/metadata/urban-targets-sites.csv")) %>%
    as.data.frame() %>%
    dplyr::select(field_site_id, latitude, longitude),
  error = function(e) { message("Could not read urban sites: ", e$message); NULL }
)

disease_sites <- tryCatch(
  arrow::read_csv_arrow(s3_read$path("challenges/project_id=bu4cast/metadata/disease-targets-sites.csv")) %>%
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
  sink = s3_read$path("challenges/project_id=bu4cast/metadata/field_sites.csv")
)

message("field_sites.csv upload complete.")
