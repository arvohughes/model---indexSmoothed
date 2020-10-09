
 
# Base image https://hub.docker.com/u/arvohughes/
FROM arvohughes/basersa4:latest

## copy files

COPY dockerised_sa4_index.R dockerised_sa4_index.R
##COPY install_packages.R install_packages.R


## install R-packages

##RUN Rscript install_packages.R
CMD Rscript dockerised_sa4_index.R
