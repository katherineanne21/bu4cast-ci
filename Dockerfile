FROM rocker/geospatial:latest

# Import GitHub Secret
ARG GITHUB_PAT
ENV GITHUB_PAT=$GITHUB_PAT

# Declares build arguments
# ARG NB_USER
# ARG NB_UID

# COPY --chown=${NB_USER} . ${HOME}

RUN install2.r arrow bslib bsicons ggiraph patchwork pak jsonlite reticulate duckdbfs furrr future googlesheets4 here imputeTS tsibble fable RcppRoll
RUN R -e "devtools::install_github('eco4cast/score4cast')"
RUN R -e "devtools::install_github('cboettig/minioclient')"
RUN R -e "devtools::install_github('LTREB-reservoirs/ver4castHelpers')"
RUN R -e "devtools::install_github('eco4cast/stac4cast')"
RUN R -e "devtools::install_github('cboettig/duckdbfs')"
RUN R -e "devtools::install_github('cboettig/aws.s3')"
RUN R -e "devtools::install_github('FLARE-forecast/RopenMeteo')"
#RUN ldd /usr/local/lib/R/site-library/GLM3r/exec/nixglm
