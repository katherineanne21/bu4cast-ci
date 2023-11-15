FROM rocker/geospatial:latest

# Import GitHub Secret
ARG GITHUB_PAT
ENV GITHUB_PAT=$GITHUB_PAT

# Declares build arguments
# ARG NB_USER
# ARG NB_UID

# COPY --chown=${NB_USER} . ${HOME}

#USER root
RUN apt-get update && apt-get -y install cron
RUN apt-get update && apt-get -y install jags
RUN apt-get update && apt-get -y install libgd-dev
RUN apt-get update && apt-get -y install libnetcdf-dev

#USER ${NB_USER}

RUN install2.r devtools remotes

RUN R -e "remotes::install_github(c('cboettig/minioclient','eco4cast/EFIstandards','cboettig/aws.s3','rqthomas/cronR','eco4cast/score4cast','EcoForecast/ecoforecastR','eco4cast/neon4cast','cboettig/prov', 'eco4cast/read4cast','eco4cast/gefs4cast'))"

RUN install2.r arrow renv rjags neonstore ISOweek RNetCDF fable fabletools forecast imputeTS ncdf4 scoringRules tidybayes tidync udunits2 bench contentid flexdashboard shiny yaml RCurl here feasts future furrr

COPY cron.sh /etc/services.d/cron/run
