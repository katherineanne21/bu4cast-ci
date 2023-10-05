FROM rocker/geospatial:latest

# Import GitHub Secret
ARG GITHUB_PAT
ENV GITHUB_PAT=$GITHUB_PAT

# Declares build arguments
# ARG NB_USER
# ARG NB_UID

# COPY --chown=${NB_USER} . ${HOME}

# USER root
RUN apt-get update && apt-get -y install cron jags libgd-dev

# USER ${NB_USER}

    score4cast,
    minioclient,
    rmarkdown,
    glue,
    dplyr,
    ggplot2,
    arrow,
    bslib,
    bsicons,
    ggiraph,
    patchwork,
    pak,
    jsonlite,
    stac4cast,
    reticulate, 
    duckdbfs,
    furrr,
    future,
    googlesheets4

        github::eco4cast/score4cast,
    github::cboettig/minioclient,
    github::LTREB-reservoirs/ver4castHelpers,
    github::eco4cast/stac4cast

RUN install2.r devtools remotes arrow renv RNetCDF forecast imputeTS ncdf4 scoringRules tidybayes tidync udunits2 bench contentid yaml RCurl here feasts gsheet usethis
RUN R -e "devtools::install_github('cboettig/aws.s3')" 
RUN R -e "devtools::install_github('aemon-j/gotmtools', ref = 'yaml')" 
RUN R -e "devtools::install_github('eco4cast/read4cast')" 
RUN R -e "devtools::install_github('eco4cast/score4cast')" 
RUN R -e "devtools::install_github('eco4cast/neon4cast')" 
RUN R -e "devtools::install_github('eco4cast/EFIstandards')" 
RUN R -e "devtools::install_github('rqthomas/cronR')" 
RUN R -e "devtools::install_github('rqthomas/glmtools')" 
RUN R -e "devtools::install_github('FLARE-forecast/FLAREr')" 
RUN R -e "devtools::install_github('FLARE-forecast/GOTMr')" 
RUN R -e "devtools::install_github('FLARE-forecast/SimstratR')" 
RUN R -e "devtools::install_github('FLARE-forecast/LakeEnsemblR')" 
RUN R -e "devtools::install_github('FLARE-forecast/FLARErLER')"
RUN R -e "devtools::install_github('GLEON/rLakeAnalyzer', ref = 'e74974f74082111065bd9cd759527f16608b3c82')"
RUN R -e "devtools::install_github('FLARE-forecast/GLM3r', ref = 'glm_3.3.1a2_w_ice_fix')" 

#RUN ldd /usr/local/lib/R/site-library/GLM3r/exec/nixglm
