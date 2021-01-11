############################
#### Generating SA4_Index Values using laspeyres
############################


### Functions & Libraries
usePackage <- function(p) 
{
  if (!is.element(p, installed.packages()[,1]))
    install.packages(p, dep = TRUE)
  require(p, character.only = TRUE)
}

usePackage('tidyverse')
usePackage('data.table')
usePackage('zoo')
usePackage('ggplot2')
usePackage('RPostgreSQL')
usePackage('RPostgres')
usePackage('DBI')
usePackage('micEconIndex')
usePackage('fANCOVA')
usePackage('roll')

##########################


## DB Connection

db<-'superestate-property'
host_db<-'35.244.66.52'
db_port<-'5432'
db_user<-'postgres'
db_password<-'spacex'

con<-dbConnect(RPostgres::Postgres(),dbname=db,host=host_db,port=db_port,user=db_user,password=db_password)

dbListTables(con)
################


##############
### Variables

state<-'VIC'


property_type<-'house'
property_type_id<-1
phase<-0:720
sa4_indices<-data.frame(phase)

outlier_threshold<-1


dbSendQuery(con, "TRUNCATE TABLE sa4_index_smoothed")

#################



## Get SA4's for NSW
query<-dbSendQuery(con,glue::glue(" SELECT distinct sa4_16code as sa4
                                    FROM public.abs_geographies
                                     "))
#where state = '{state}'


sa4_NSW<-dbFetch(query)
dbClearResult(query)



###########################
sa4_NSW1<-sa4_NSW[c(9,36),]

###### Generate Index for each SA4 ( Creates individual dataframes for each)
for (i in sa4_NSW1){
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
  
  
  #sa4$price<-log(sa4$price)
  
  dat = sa4 %>% 
    arrange(desc(phase)) %>%
    mutate(windows_3month=cut(phase, seq(min(phase), max(phase), 3))) %>%
    mutate(windows_6month=cut(phase, seq(min(phase), max(phase), 6))) %>%
    group_by(phase) %>% 
    #mutate(mean_score_group =  mean(price)) %>%  
    #mutate(sd_score_group =  sd(price)) %>%   
    mutate(z_score_group = log(price) - mean(log(price)) / sd(log(price))) %>%  
    ungroup %>% 
    group_by(windows_3month) %>%
    #mutate(mean_roll3_group =  mean(price)) %>%  
    #mutate(sd_roll3_group =  sd(price)) %>%  
    mutate(roll3_z_score=scale(log(price))) %>%
    ungroup %>% 
    group_by(windows_6month) %>%
    #mutate(mean_roll6_group =  mean(price)) %>%  
    #mutate(sd_roll6_group =  sd(price)) %>%  
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

for (i in sa4_NSW1){
  
  if(nrow(eval(as.name(paste0('index_outlier_',i)))) < 20 ){
    print("skipping because low counts")
    next
  }
   
  
  loessMod10 <- loess(indexVal ~ phase, data=eval(as.name(paste0('index_outlier_',i))), span=0.10) # 10% smoothing span
  assign(paste0('index_smoothed_outlier_',i),predict(loessMod10)) 
  
  
  plot((eval(as.name(paste0('index_outlier_',i)))$indexVal), x=eval(as.name(paste0('index_outlier_',i)))$phase, type="l", main=paste0('index_SA4_',i), xlab="Phase", ylab="Index")
  lines((eval(as.name(paste0('index_smoothed_outlier_',i)))), x=eval(as.name(paste0('index_outlier_',i)))$phase[1:length(eval(as.name(paste0('index_smoothed_outlier_',i))))], col="blue")
  
  #FTSE.lo3 <- loess.as(eval(as.name(paste0('index_outlier_',i)))$phase,eval(as.name(paste0('index_outlier_',i)))$indexVal , degree = 1, criterion = c("aicc", "gcv")[2], user.span = NULL, plot = T)
  #assign(paste0('FTSE.lo.predict3_',i),predict(FTSE.lo3,data.frame(eval(as.name(paste0('index_outlier_',i)))$phase)))
  
  #plot(eval(as.name(paste0('index_outlier_',i)))$phase,eval(as.name(paste0('FTSE.lo.predict3_',i))),main=paste0('index_SA4_',i), xlab="Phase", ylab="Index")
  
  
  assign(paste0('df_index_smoothed_',i),data.frame(sa4=eval(i),property_type_id=eval(property_type_id),smoothed_indexVal=(eval(as.name(paste0('index_smoothed_outlier_',i)))),phase=eval(as.name(paste0('index_outlier_',i)))$phase[1:length(eval(as.name(paste0('index_smoothed_outlier_',i))))]))
  dbWriteTable(con, "sa4_index_smoothed", eval(as.name(paste0('df_index_smoothed_',i))), append = TRUE)
  
  
}
  

  
  
###########################################################################################
 














 
  loessMod10 <- loess(indexVal ~ phase, data=eval(as.name(paste0('index_',i))), span=0.10) # 10% smoothing span
  assign(paste0('index_smoothed_',i),predict(loessMod10)) 
  
  
  plot(eval(as.name(paste0('index_',i)))$indexVal, x=eval(as.name(paste0('index_',i)))$phase, type="l", main=paste0('index_SA4_',i), xlab="Phase", ylab="Index")
  lines(eval(as.name(paste0('index_smoothed_',i))), x=eval(as.name(paste0('index_',i)))$phase[1:length(eval(as.name(paste0('index_smoothed_',i))))], col="blue")
  
  
  
  #FTSE.lo3 <- loess.as(eval(as.name(paste0('index_',i)))$phase,eval(as.name(paste0('index_',i)))$indexVal , degree = 1, criterion = c("aicc", "gcv")[2], user.span = NULL, plot = T)
  #assign(paste0('FTSE.lo.predict3_',i),predict(FTSE.lo3,data.frame(eval(as.name(paste0('index_',i)))$phase)))
  
  #plot(eval(as.name(paste0('index_',i)))$phase,eval(as.name(paste0('FTSE.lo.predict3_',i))),main=paste0('index_SA4_',i), xlab="Phase", ylab="Index")
  #predict(FTSE.lo3,data.frame(eval(as.name(paste0('index_',i)))$phase))
  
  #plot(paste0('index_',i)$phase,paste0('FTSE.lo.predict3_',i))
  
  
  #assign(paste0('df_index_smoothed_',i),data.frame(sa4=eval(i),property_type_id=2,smoothed_indexVal=eval(as.name(paste0('index_smoothed_',i))),phase=eval(as.name(paste0('index_',i)))$phase[1:length(eval(as.name(paste0('index_smoothed_',i))))]))
  #dbWriteTable(con, "sa4_index_smoothed", eval(as.name(paste0('df_index_smoothed_',i))), append = TRUE)
  
}

##########################################3
















  
  
  
  dat$log_price<-log(dat$price)
  


  dat = sa4 %>% 
    arrange(desc(phase)) %>%
    mutate(windows_3month=cut(phase, seq(min(phase), max(phase), 3))) %>%
    mutate(windows_6month=cut(phase, seq(min(phase), max(phase), 6))) %>%
    group_by(phase) %>% 
    #mutate(mean_score_group =  mean(price)) %>%  
    #mutate(sd_score_group =  sd(price)) %>%   
    mutate(z_score_group = (price - mean(price)) / sd(price)) %>%  
    ungroup %>% 
    group_by(windows_3month) %>%
    #mutate(mean_roll3_group =  mean(price)) %>%  
    #mutate(sd_roll3_group =  sd(price)) %>%  
    mutate(roll3_z_score=scale(price)) %>%
    ungroup %>% 
    group_by(windows_6month) %>%
    #mutate(mean_roll6_group =  mean(price)) %>%  
    #mutate(sd_roll6_group =  sd(price)) %>%  
    mutate(roll6_z_score=scale(price)) %>%
    arrange(desc(phase))
  
  
  
  


  dat$outlier<-ifelse(abs(dat$z_score_group)>outlier_threshold, 1, 0)
  dat$outlier_3window<-ifelse(abs(dat$roll3_z_score)>outlier_threshold, 1, 0)
  dat$outlier_6window<-ifelse(abs(dat$roll6_z_score)>outlier_threshold, 1, 0)
  
  cleaned<-subset(dat,outlier == 0)
  cleaned_3window<-subset(dat,outlier_3window == 0)
  cleaned_6window<-subset(dat,outlier_6window == 0)
  
  
  
  
  cleaned_median = cleaned %>%
    group_by(phase) %>%
    summarise(median=median(price),n=n())

  plot(cleaned_median$phase,cleaned_median$median)
  
  
  cleaned_median3 = cleaned_3window %>%
    group_by(phase) %>%
    summarise(median=median(price),n=n())
  
  plot(cleaned_median3$phase,cleaned_median3$median)
  
  
  
  
  cleaned_median6 = cleaned_6window %>%
    group_by(phase) %>%
    summarise(median=median(price),n=n())
  
  plot(cleaned_median6$phase,cleaned_median6$median)
  

  
  ######################################################  
  
  
  
  
  dat = sa4 %>% 
    arrange(desc(phase)) %>%
    mutate(windows_3month=cut(phase, seq(min(phase), max(phase), 3))) %>%
    mutate(windows_6month=cut(phase, seq(min(phase), max(phase), 6))) %>%
    group_by(phase) %>% 
    #mutate(mean_score_group =  mean(log_price)) %>%  
    #mutate(sd_score_group =  sd(log_price)) %>%   
    mutate(z_score_group = (log_price - mean(log_price)) / sd(log_price)) %>%  
    ungroup %>% 
    group_by(windows_3month) %>%
    #mutate(mean_roll3_group =  mean(log_price)) %>%  
    #mutate(sd_roll3_group =  sd(log_price)) %>%  
    mutate(roll3_z_score=scale(log_price)) %>%
    ungroup %>% 
    group_by(windows_6month) %>%
    #mutate(mean_roll6_group =  mean(log_price)) %>%  
    #mutate(sd_roll6_group =  sd(log_price)) %>%  
    mutate(roll6_z_score=scale(log_price)) %>%
    arrange(desc(phase))
  
  
  
  
  
  outlier_threshold<-1
  
  dat$outlier<-ifelse(abs(dat$z_score_group)>outlier_threshold, 1, 0)
  dat$outlier_3window<-ifelse(abs(dat$roll3_z_score)>outlier_threshold, 1, 0)
  dat$outlier_6window<-ifelse(abs(dat$roll6_z_score)>outlier_threshold, 1, 0)
  
  cleaned<-subset(dat,outlier == 0)
  cleaned_3window<-subset(dat,outlier_3window == 0)
  cleaned_6window<-subset(dat,outlier_6window == 0)
  
  
  
  
  cleaned_median = cleaned %>%
    group_by(phase) %>%
    summarise(median=median(log_price),n=n())
  
  plot(cleaned_median$phase,exp(cleaned_median$median))
  
  
  cleaned_median3 = cleaned_3window %>%
    group_by(phase) %>%
    summarise(median=median(log_price),n=n())
  
  plot(cleaned_median3$phase,exp(cleaned_median3$median))
  
  
  
  
  cleaned_median6 = cleaned_6window %>%
    group_by(phase) %>%
    summarise(median=median(log_price),n=n())
  
  plot(cleaned_median6$phase,exp(cleaned_median6$median))
  
  
  
  

  phase<-cleaned_median3$phase
  assign(paste0('index_outlier_',i),data.frame(indexVal=priceIndex( "median","n", 1, cleaned_median3,method="Laspeyres" ),phase,median=cleaned_median3$median))
  
  
  loessMod10 <- loess(indexVal ~ phase, data=eval(as.name(paste0('index_outlier_',i))), span=0.10) # 10% smoothing span
  assign(paste0('index_smoothed_outlier_',i),predict(loessMod10)) 
  
  
  plot(eval(as.name(paste0('index_outlier_',i)))$indexVal, x=eval(as.name(paste0('index_outlier_',i)))$phase, type="l", main=paste0('index_SA4_',i), xlab="Phase", ylab="Index")
  lines(eval(as.name(paste0('index_smoothed_outlier_',i))), x=eval(as.name(paste0('index_outlier_',i)))$phase[1:length(eval(as.name(paste0('index_smoothed_outlier_',i))))], col="blue")
  
  
  
  
  
  
  FTSE.lo3 <- loess.as(index_outlier_121$phase,index_121$indexVal , degree = 1, criterion = c("aicc", "gcv")[2], user.span = NULL, plot = T)
  FTSE.lo.predict3 <- predict(FTSE.lo3,data.frame(phase))
  
  plot(phase,FTSE.lo.predict3)
  
  