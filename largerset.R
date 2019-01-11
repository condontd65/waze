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

# Write function to create new dataframe
newdf.waze <- function(df) {
  dt <- data.table(df[1,1], df[1,2], df[1,3], df[1,4], df[1,5], mean(df[,6], df[1,10], df[1,11]))
  return(dt)
}









for (i in 1:87) {
  nam <- paste(i)
  #print(paste(i))
  #num <- as.character(i)
  assign(nam, newdf.waze(waze.split[i]))
  #print(i)
}

for (i in unique(waze.big$route_id)) {
  num <- as.character(i)
  print(waze.split$num)
}


t <- 10347
t.p <- paste(t)
newdf.waze(waze.split$t)



lapply(waze.split, newdf.waze)



dt <- data.table(waze.split$`10347`[1,1], 
                                        waze.split$`10347`[1,2], 
                                        waze.split$`10347`[1,3], 
                                        waze.split$`10347`[1,4], 
                                        waze.split$`10347`[1,5], 
                                        mean(waze.split$`10347`[,6]),
                                        waze.split$`10347`[1,10], 
                                        waze.split$`10347`[1,11]) 
  
  
  
  
#  for (i in unique(waze.big$route_id)) {
#    if (waze.big$route_id ==i) {
      
#    }
#    paste(i) <- waze.big[waze.big$route_id == i]
#  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  