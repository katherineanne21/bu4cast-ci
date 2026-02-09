## S3 Minio Buckets - Write
library(dplyr)
library(arrow)
library(microdatasus)
library(gitcreds)

#fetch and process disease data from the information system for notifiable diseases
GITHUB_PAT = Sys.getenv("MICRODATASUS_INSTALL") # create as a token (expires every 365 days)
install.packages("remotes")
remotes::install_github("rfsaldanha/microdatasus")
library(microdatasus)

raw_lv <- fetch_datasus(
  year_start = 2007,
  year_end   = 2025,
  uf = "all",
  information_system = "SINAN-LEISHMANIOSE-VISCERAL"
)

lv <- process_sinan_leishmaniose_visceral(raw_lv, municipality_data = TRUE)

#handling dates (some misclassified)
lv <- lv %>%
  mutate(
    DT_SIN_PRI = as.Date(DT_SIN_PRI),
    DT_NOTIFIC = as.Date(DT_NOTIFIC),
    ANO_NASC = as.numeric(ANO_NASC),
    
    # keep only plausible symptom dates
    DT_SIN_PRI_clean = if_else(
      !is.na(DT_SIN_PRI) &
        year(DT_SIN_PRI) >= 2007 & 
        year(DT_SIN_PRI) >= ANO_NASC,   # cannot be before birth
      DT_SIN_PRI,
      NA_Date_
    ),
    
    # final onset proxy
    onset_date = coalesce(DT_SIN_PRI_clean, DT_NOTIFIC),
    onset_month = floor_date(onset_date, "month")
  )

#selecting final variables and processing case count per municipality
lv_monthly_mun <- lv %>%
  mutate(
    date = floor_date(onset_month, "month")
  ) %>%
  filter(!is.na(date), !is.na(ID_MN_RESI)) %>%
  count(ID_MN_RESI, date, name = "vl_cases") %>%
  rename(property_id = ID_MN_RESI)%>%
  arrange(property_id, date)

#writing final csv (leave out if needed)
# write.csv(lv_monthly_mun, "vl_monthly.csv")

#set up connection
s3_read <- arrow::s3_bucket('bu4cast-ci-read',
                            endpoint_override = 'https://minio-s3.apps.shift.nerc.mghpcc.org',
                            access_key = Sys.getenv("OSN_KEY"),
                            secret_key = Sys.getenv("OSN_SECRET"),
                            scheme = "https")

#create file name/folder
challenge_name = 'tropical_disease'
filename = paste("challenges/targets/project_id=bu4cast/",challenge_name,
                 "-targets.csv",sep="")
arrow::write_csv_arrow(data,sink=s3_read$path(filename))


# Remove file from working directory
csv_filename = paste(challenge_name, "-targets.csv.gz")
unlink(csv_filename)

# Health Check
# Created at www.healthchecks.io
# Set to Tropical Disease Targets
RCurl::getURL("https://hc-ping.com/09656ca3-6b1a-45d4-a3dc-9e15a59d41b5")

