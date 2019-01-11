library(tidyverse)
library(DBI)
#library(odbc)
library(RODBC)
library(dplyr)
library(ggplot2)
#library(leaflet)
#library(leaflet.esri)
#library(leaflet.extras)
library(rjson)
#library(rgdal)
#library(rgeos)
library(RPostgreSQL)
library(data.table)
library(ggplot2)
library(forcats)
library(data.table)
library(reshape2)


#  Set up connection to PostgreSQL server
# Password
source("info.R")

# Load PostgreSQL driver
drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = base,
                 host = hst, port = 6666, user = un, 
                 password = pw)

dbExistsTable(con, "cartable")

# query waze data
qbig <- 
  "select route_Id, name, fromname, toname, length, time, historictime, updatetime, map_name
FROM internal_data.waze_routes as wz
where name like 'TD%'
order by updatetime desc
limit 10440;"
# This limit gives last 10 hours
  
# Load in big waze data that is based
waze.big <- dbGetQuery(con, qbig)
rm(pw, un, hst, base)
  
# Creat mph column
waze.big$mph <- (waze.big$length / waze.big$time) / 1609.34 * 3600
  
# Create new column for the principle street name 
waze.big$main_street <- NA

# Create vector of street names
streets <- c('Beacon St','Blue Hill Ave','Broadway',
               'Columbia Rd','Columbus Ave',
               'Commonwealth Ave','Huntington Ave',
               'Hyde Park Ave','Massachusetts Ave',
               'Tremont St','Washington St')
  
# Populate main_street column with streets names
for (street in streets){
  waze.big[waze.big$name %like% paste("TD - ",
                                             street, 
                                             sep = ""), 
                "main_street"] <- street
    
  print(paste(street, "complete"))
}
  
waze.split <- split(waze.big, waze.big$route_id) 






  
  
  
  
  
#  for (i in unique(waze.big$route_id)) {
#    if (waze.big$route_id ==i) {
      
#    }
#    paste(i) <- waze.big[waze.big$route_id == i]
#  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  