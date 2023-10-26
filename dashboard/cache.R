#install.packages("remotes")
#remotes::install_github("cboettig/minio")
library(minioclient)
config <- yaml::read_yaml("challenge_configuration.yaml")
install_mc()
mc_alias_set("mc_bucket",  endpoint = config$endpoint,
             access_key = "", secret_key = "")

mc(paste0("mirror --overwrite mc_bucket/",config$scores_bucket,"/parquet/project_id=", config$project_id," cache/"))
