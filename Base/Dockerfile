
 
# Base image https://hub.docker.com/u/rocker/
FROM rocker/tidyverse:latest

## copy files

COPY install_packages.R install_packages.R


## install R-packages
RUN Rscript install_packages.R

