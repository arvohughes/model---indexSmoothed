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
sa4_indices<-data.frame(phase)

#################



## Get SA4's for NSW
query<-dbSendQuery(con,glue::glue(" SELECT distinct sa4_16code as sa4
                                    FROM public.abs_geographies
                                    where state = '{state}' "))

sa4_NSW<-dbFetch(query)
dbClearResult(query)
#############################


###### Generate Index for each SA4 ( Creates individual dataframes for each)
for (i in sa4_NSW$sa4){
  print(paste0("Doing SA4 = ",i))
query<-dbSendQuery(con,glue::glue("select  sa4, phase, percentile_disc(0.5) within group (order by price) as median, count(price) as n
        from (
            select ag.sa4_16code as sa4, se.price, pl.phase
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
                where ag.sa4_16code = {i}
                and se.price between 200000 and 6000000
                group by ag.address_id having count(se.id) between 2 and 10
            )
            and se.price between 200000 and 6000000
        ) index_data
        group by sa4, phase
        order by sa4 , phase
        "))

sa4<-dbFetch(query)
dbClearResult(query)


phase<-sa4$phase
assign(paste0('index_',i),data.frame(indexVal=priceIndex( "median","n", 1, sa4,method="Laspeyres" ),phase))
#sa4_indices<-merge(sa4_indices,data.frame(assign(paste0('index_',i),priceIndex( "median","n", 1, sa4,method="Laspeyres" )),phase),by.x=phase,by.y=phase,all.x = TRUE)
}






####### Apply a loess smoother of 10 ( own dataframe)

for (i in sa4_NSW$sa4){
loessMod10 <- loess(indexVal ~ phase, data=eval(as.name(paste0('index_',i))), span=0.10) # 10% smoothing span
assign(paste0('index_smoothed_',i),predict(loessMod10)) 

plot(eval(as.name(paste0('index_',i)))$indexVal, x=eval(as.name(paste0('index_',i)))$phase, type="l", main=paste0('index_SA4_',i), xlab="Phase", ylab="Index")
lines(eval(as.name(paste0('index_smoothed_',i))), x=eval(as.name(paste0('index_',i)))$phase[1:length(eval(as.name(paste0('index_smoothed_',i))))], col="blue")


assign(paste0('df_index_smoothed_',i),data.frame(sa4=eval(i),property_type_id=1,smoothed_indexVal=eval(as.name(paste0('index_smoothed_',i))),phase=eval(as.name(paste0('index_',i)))$phase[1:length(eval(as.name(paste0('index_smoothed_',i))))]))


dbWriteTable(con, "sa4_smoothed", eval(as.name(paste0('df_index_smoothed_',i))), append = TRUE)

}

##########################################3


