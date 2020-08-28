############################
#### Generating Postcode_Index Values using laspeyres
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

state<-'NSW'
property_type<-'house'
phase<-0:720
postcode_indices<-data.frame(phase)

#################



## Get postcode's for NSW
query<-dbSendQuery(con,glue::glue(" SELECT distinct postcode as postcode
                                    FROM public.abs_geographies
                                    where state = '{state}' "))

postcode_NSW<-dbFetch(query)
dbClearResult(query)
#############################


postcode_NSW$postcode<-sort(postcode_NSW$postcode)

###### Generate Index for each SA4 ( Creates individual dataframes for each)
for (i in postcode_NSW$postcode){
  print(paste0("Doing Postcode = ",i))
  query<-dbSendQuery(con,glue::glue("select  postcode, phase, percentile_disc(0.5) within group (order by price) as median, count(price) as n
        from (
            select ag.postcode as postcode, se.price, pl.phase
            from abs_geographies ag
            join sale_event se on se.address_id = ag.address_id
            JOIN phase_lookup pl
                ON pl.YEAR = date_part('year', se.contract_date )
                and pl.month = date_part('month', se.contract_date )
            JOIN address a ON a.id = ag.address_id
            where a.property_type ='{property_type}'
            and ag.address_id in (
                select ag.address_id from abs_geographies ag
                join sale_event se on ag.address_id = se.address_id
                where ag.postcode= '{i}'
                and se.price between 200000 and 6000000
                group by ag.address_id having count(se.id) between 2 and 10
            )
            and se.price between 200000 and 6000000
        ) index_data
        group by postcode, phase
        order by postcode , phase
        "))
  
  postcode<-dbFetch(query)
  dbClearResult(query)
  
  if (nrow(postcode) ==0 ) {
    print("Skipping")
    next
  }
  else {
    phase<-postcode$phase
    assign(paste0('index_',i),data.frame(indexVal=priceIndex( "median","n", 1, postcode,method="Laspeyres",na.rm=TRUE ),phase))
    #sa4_indices<-merge(sa4_indices,data.frame(assign(paste0('index_',i),priceIndex( "median","n", 1, sa4,method="Laspeyres" )),phase),by.x=phase,by.y=phase,all.x = TRUE)
  }
  
 
}






####### Apply a loess smoother of 10 ( own dataframe)

for (i in postcode_NSW$postcode){
if (exists((paste0('index_',i))) == FALSE) {
  print("Skipping")
  next 
}

   else if (nrow(eval(as.name(paste0('index_',i)))) <= 10){
    print("Skipping")
    next 
  }
  else {
  loessMod10 <- loess(indexVal ~ phase, data=eval(as.name(paste0('index_',i))), span=0.10) # 10% smoothing span
  assign(paste0('index_smoothed_',i),predict(loessMod10)) 
  
  plot(eval(as.name(paste0('index_',i)))$indexVal, x=eval(as.name(paste0('index_',i)))$phase, type="l", main=paste0('index_postcode_',i), xlab="Phase", ylab="Index")
  lines(eval(as.name(paste0('index_smoothed_',i))), x=eval(as.name(paste0('index_',i)))$phase[1:length(eval(as.name(paste0('index_smoothed_',i))))], col="blue")
  
  
  assign(paste0('df_index_smoothed_',i),data.frame(postcode=eval(i),property_type_id=1,smoothed_indexVal=eval(as.name(paste0('index_smoothed_',i))),phase=eval(as.name(paste0('index_',i)))$phase[1:length(eval(as.name(paste0('index_smoothed_',i))))]))
  dbWriteTable(con, "postcode_index_smoothed", eval(as.name(paste0('df_index_smoothed_',i))), append = TRUE)
  }
  }

##########################################3


