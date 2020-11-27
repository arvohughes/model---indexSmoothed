############################
#### Generating SA4_Index Values using laspeyres
############################


### Functions & Libraries


print("Starting Workflow") 

print("Loading Packages") 

#suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(data.table))
suppressPackageStartupMessages(library(zoo))
suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(RPostgreSQL))
suppressPackageStartupMessages(library(RPostgres))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(micEconIndex))

##########################

print("Setting DB Connection") 


## DB Connection

db<-'superestate-property'
host_db<-'35.244.66.52'
db_port<-'5432'
db_user<-'postgres'
db_password<-'spacex'

con<-dbConnect(RPostgres::Postgres(),dbname=db,host=host_db,port=db_port,user=db_user,password=db_password)

dbListTables(con)

dbSendQuery(con, "TRUNCATE TABLE sa4_index_smoothed")


################

print("Running House Index for NSW between phase 0-720")

##############
### Variables
phase<-0:720
property_type<-'house'
sa4_indices<-data.frame(phase)
outlier_threshold<-1
#################



## Get SA4's for NSW
query<-dbSendQuery(con,glue::glue(" SELECT distinct sa4_16code as sa4
                                    FROM public.abs_geographies"))
sa4_NSW<-dbFetch(query)
dbClearResult(query)
#############################




#############################
### Truncate SA4_index table
dbSendQuery(con, "TRUNCATE TABLE sa4_index_smoothed")
#############################



###### Generate Index for each SA4 ( Creates individual dataframes for each)
###### Generate Index for each SA4 ( Creates individual dataframes for each)
for (i in sa4_NSW){
  print(paste0("Doing SA4 = ",i))
  
  query<-dbSendQuery(con,glue::glue("select ag.sa4_16code as sa4, se.price, pl.phase,se.address_id 
            from abs_geographies ag
            join sale_event se on se.address_id = ag.address_id
            JOIN phase_lookup pl
                ON pl.YEAR = date_part('year', se.contract_date )
                and pl.month = date_part('month', se.contract_date )
            JOIN address a ON a.id = ag.address_id
            where a.property_type ='{property_type}' and se.price > 200000 
            and ag.address_id in (
                select ag.address_id from abs_geographies ag
                join sale_event se on ag.address_id = se.address_id
                where ag.sa4_16code = {i}
                group by ag.address_id having count(se.id) between 2 and 10)
                                    "))
  
  sa4<-dbFetch(query)
  dbClearResult(query)
  
  
  dat = sa4 %>% 
    arrange(desc(phase)) %>%
    mutate(windows_3month=cut(phase, seq(min(phase), max(phase), 3))) %>%
    mutate(windows_6month=cut(phase, seq(min(phase), max(phase), 6))) %>%
    group_by(phase) %>% 
    mutate(z_score_group = log(price) - mean(log(price)) / sd(log(price))) %>%  
    ungroup %>% 
    group_by(windows_3month) %>%
 
    mutate(roll3_z_score=scale(log(price))) %>%
    ungroup %>% 
    group_by(windows_6month) %>%
    mutate(roll6_z_score=scale(price)) %>%
    arrange(desc(phase))
  
  
  dat$outlier_3window<-ifelse(abs(dat$roll3_z_score)>outlier_threshold, 1, 0)
  cleaned_3window<-subset(dat,outlier_3window == 0)
  
  
  
  cleaned_median3 = cleaned_3window %>%
    group_by(phase) %>%
    summarise(median=median(price),n=n())
  
  
  plot(cleaned_median3$phase,cleaned_median3$median)
  phase<-cleaned_median3$phase
  assign(paste0('index_outlier_',i),data.frame(indexVal=priceIndex( "median","n", 1, cleaned_median3,method="Laspeyres" ),phase,median=cleaned_median3$median))
  
} 






####### Apply a loess smoother of 10 ( own dataframe)


for (i in sa4_NSW){
  
  if(nrow(eval(as.name(paste0('index_outlier_',i)))) < 20 ){
    print("skipping because low counts")
    next
  }
  
  
  loessMod10 <- loess(indexVal ~ phase, data=eval(as.name(paste0('index_outlier_',i))), span=0.10) # 10% smoothing span
  assign(paste0('index_smoothed_outlier_',i),predict(loessMod10)) 
  
  
  plot((eval(as.name(paste0('index_outlier_',i)))$indexVal), x=eval(as.name(paste0('index_outlier_',i)))$phase, type="l", main=paste0('index_SA4_',i), xlab="Phase", ylab="Index")
  lines((eval(as.name(paste0('index_smoothed_outlier_',i)))), x=eval(as.name(paste0('index_outlier_',i)))$phase[1:length(eval(as.name(paste0('index_smoothed_outlier_',i))))], col="blue")
  
  
  assign(paste0('df_index_smoothed_',i),data.frame(sa4=eval(i),property_type_id=1,smoothed_indexVal=(eval(as.name(paste0('index_smoothed_outlier_',i)))),phase=eval(as.name(paste0('index_outlier_',i)))$phase[1:length(eval(as.name(paste0('index_smoothed_outlier_',i))))]))
  dbWriteTable(con, "sa4_index_smoothed", eval(as.name(paste0('df_index_smoothed_',i))), append = TRUE)
  
  
}

##########################################3


print("Running Apartment Index for NSW between phase 0-720")


# Apartment

##############
### Variables

property_type<-'apartment'
sa4_indices<-data.frame(phase)

#################



## Get SA4's for NSW
query<-dbSendQuery(con,glue::glue(" SELECT distinct sa4_16code as sa4
                                    FROM public.abs_geographies
                                     "))

sa4_NSW<-dbFetch(query)
dbClearResult(query)
#############################


###### Generate Index for each SA4 ( Creates individual dataframes for each)
for (i in sa4_NSW){
  print(paste0("Doing SA4 = ",i))
  
  
  query<-dbSendQuery(con,glue::glue("select ag.sa4_16code as sa4, se.price, pl.phase,se.address_id 
            from abs_geographies ag
            join sale_event se on se.address_id = ag.address_id
            JOIN phase_lookup pl
                ON pl.YEAR = date_part('year', se.contract_date )
                and pl.month = date_part('month', se.contract_date )
            JOIN address a ON a.id = ag.address_id
            where a.property_type ='{property_type}' and se.price > 200000 
            and ag.address_id in (
                select ag.address_id from abs_geographies ag
                join sale_event se on ag.address_id = se.address_id
                where ag.sa4_16code = {i}
                group by ag.address_id having count(se.id) between 2 and 10)
        "))
  
  sa4<-dbFetch(query)
  dbClearResult(query)
  
  
  dat = sa4 %>% 
    arrange(desc(phase)) %>%
    mutate(windows_3month=cut(phase, seq(min(phase), max(phase), 3))) %>%
    mutate(windows_6month=cut(phase, seq(min(phase), max(phase), 6))) %>%
    group_by(phase) %>% 
    mutate(z_score_group = log(price) - mean(log(price)) / sd(log(price))) %>%  
    ungroup %>% 
    group_by(windows_3month) %>%
    
    mutate(roll3_z_score=scale(log(price))) %>%
    ungroup %>% 
    group_by(windows_6month) %>%
    mutate(roll6_z_score=scale(price)) %>%
    arrange(desc(phase))
  
  
  dat$outlier_3window<-ifelse(abs(dat$roll3_z_score)>outlier_threshold, 1, 0)
  cleaned_3window<-subset(dat,outlier_3window == 0)
  
  
  
  cleaned_median3 = cleaned_3window %>%
    group_by(phase) %>%
    summarise(median=median(price),n=n())
  
  
  plot(cleaned_median3$phase,cleaned_median3$median)
  phase<-cleaned_median3$phase
  assign(paste0('index_outlier_',i),data.frame(indexVal=priceIndex( "median","n", 1, cleaned_median3,method="Laspeyres" ),phase,median=cleaned_median3$median))
  
} 




####### Apply a loess smoother of 10 ( own dataframe)

for (i in sa4_NSW$sa4){
  loessMod10 <- loess(indexVal ~ phase, data=eval(as.name(paste0('index_',i))), span=0.10) # 10% smoothing span
  assign(paste0('index_smoothed_',i),predict(loessMod10)) 
  

  
  assign(paste0('df_index_smoothed_',i),data.frame(sa4=eval(i),property_type_id=2,smoothed_indexVal=eval(as.name(paste0('index_smoothed_',i))),phase=eval(as.name(paste0('index_',i)))$phase[1:length(eval(as.name(paste0('index_smoothed_',i))))]))
  dbWriteTable(con, "sa4_index_smoothed", eval(as.name(paste0('df_index_smoothed_',i))), append = TRUE)
  
}
