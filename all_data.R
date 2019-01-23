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
library(xlsx)
library(googlesheets)


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
q.all <- 
  "select route_Id, name, fromname, toname, length, time, historictime, updatetime, map_name
FROM internal_data.waze_routes as wz
where name like 'TD%'
order by updatetime desc;"
# This limit gives last 10 hours

# Load in all waze data that is based
waze.all <- dbGetQuery(con, q.all)
rm(pw, un, hst, base)

# Creat mph column
waze.all$mph <- (waze.all$length / waze.all$time) / 1609.34 * 3600

# Create new column for the principle street name 
waze.all$main_street <- NA

# Create vector of street names
streets <- c('Beacon St','Blue Hill Ave','Broadway',
             'Columbia Rd','Columbus Ave',
             'Commonwealth Ave','Huntington Ave',
             'Hyde Park Ave','Massachusetts Ave',
             'Tremont St','Washington St')

# Populate main_street column with streets names
for (street in streets){
  waze.all[waze.all$name %like% paste("TD - ",
                                      street, 
                                      sep = ""), 
           "main_street"] <- street
  
  print(paste(street, "complete"))
}


# Order dataframe by street name and length so longest is first
waze.all <- waze.all[
  with(waze.all, order(main_street, length)),
  ]

row.names(waze.all) <- 1:nrow(waze.all)


# This gives a list of dataframes by each street (11)  
waze.street <- split(waze.all, waze.all$main_street) 

# Create data frames for the multiples
beacon.m <- subset(waze.street$`Beacon St`, 
                   waze.street$`Beacon St`$length != max(waze.street$`Beacon St`$length))
bluehill.m <- subset(waze.street$`Blue Hill Ave`, 
                   waze.street$`Blue Hill Ave`$length != max(waze.street$`Blue Hill Ave`$length))
broadway.m <- subset(waze.street$Broadway, 
                   waze.street$Broadway$length != max(waze.street$Broadway$length))
columbia.m <- subset(waze.street$`Columbia Rd`, 
                   waze.street$`Columbia Rd`$length != max(waze.street$`Columbia Rd`$length))
columbus.m <- subset(waze.street$`Columbus Ave`, 
                   waze.street$`Columbus Ave`$length != max(waze.street$`Columbus Ave`$length))
commonwealth.m <- subset(waze.street$`Commonwealth Ave`, 
                   waze.street$`Commonwealth Ave`$length != max(waze.street$`Commonwealth Ave`$length))
huntington.m <- subset(waze.street$`Huntington Ave`, 
                   waze.street$`Huntington Ave`$length != max(waze.street$`Huntington Ave`$length))
hydepark.m <- subset(waze.street$`Hyde Park Ave`, 
                   waze.street$`Hyde Park Ave`$length != max(waze.street$`Hyde Park Ave`$length))
massachusetts.m <- subset(waze.street$`Massachusetts Ave`, 
                   waze.street$`Massachusetts Ave`$length != max(waze.street$`Massachusetts Ave`$length))
tremont.m <- subset(waze.street$`Tremont St`, 
                   waze.street$`Tremont St`$length != max(waze.street$`Tremont St`$length))
washington.m <- subset(waze.street$`Washington St`, 
                   waze.street$`Washington St`$length != max(waze.street$`Washington St`$length))


# Create data frames for the full routes
beacon.f <- subset(waze.street$`Beacon St`, 
                   waze.street$`Beacon St`$length == max(waze.street$`Beacon St`$length))
bluehill.f <- subset(waze.street$`Blue Hill Ave`, 
                     waze.street$`Blue Hill Ave`$length == max(waze.street$`Blue Hill Ave`$length))
broadway.f <- subset(waze.street$Broadway, 
                     waze.street$Broadway$length == max(waze.street$Broadway$length))
columbia.f <- subset(waze.street$`Columbia Rd`, 
                     waze.street$`Columbia Rd`$length == max(waze.street$`Columbia Rd`$length))
columbus.f <- subset(waze.street$`Columbus Ave`, 
                     waze.street$`Columbus Ave`$length == max(waze.street$`Columbus Ave`$length))
commonwealth.f <- subset(waze.street$`Commonwealth Ave`, 
                         waze.street$`Commonwealth Ave`$length == max(waze.street$`Commonwealth Ave`$length))
huntington.f <- subset(waze.street$`Huntington Ave`, 
                       waze.street$`Huntington Ave`$length == max(waze.street$`Huntington Ave`$length))
hydepark.f <- subset(waze.street$`Hyde Park Ave`, 
                     waze.street$`Hyde Park Ave`$length == max(waze.street$`Hyde Park Ave`$length))
massachusetts.f <- subset(waze.street$`Massachusetts Ave`, 
                          waze.street$`Massachusetts Ave`$length == max(waze.street$`Massachusetts Ave`$length))
tremont.f <- subset(waze.street$`Tremont St`, 
                    waze.street$`Tremont St`$length == max(waze.street$`Tremont St`$length))
washington.f <- subset(waze.street$`Washington St`, 
                       waze.street$`Washington St`$length == max(waze.street$`Washington St`$length))

# Calculate full route time means and lengths and mph
tf.beacon <- round(mean(beacon.f$time), digits = 2)
tf.bluehill <- round(mean(bluehill.f$time), digits = 2)
tf.broadway <- round(mean(broadway.f$time), digits = 2)
tf.columbia <- round(mean(columbia.f$time), digits = 2)
tf.columbus <- round(mean(columbus.f$time), digits = 2)
tf.commonwealth <- round(mean(commonwealth.f$time), digits = 2)
tf.huntington <- round(mean(huntington.f$time), digits = 2)
tf.hydepark <- round(mean(hydepark.f$time), digits = 2)
tf.massachusetts <- round(mean(massachusetts.f$time), digits = 2)
tf.tremont <- round(mean(tremont.f$time), digits = 2)
tf.washington <- round(mean(washington.f$time), digits = 2)

lf.beacon <- max(beacon.f$length)
lf.bluehill <- max(bluehill.f$length)
lf.broadway <- max(broadway.f$length)
lf.columbia <- max(columbia.f$length)
lf.columbus <- max(columbus.f$length)
lf.commonwealth <- max(commonwealth.f$length)
lf.huntington <- max(huntington.f$length)
lf.hydepark <- max(hydepark.f$length)
lf.massachusetts <- max(massachusetts.f$length)
lf.tremont <- max(tremont.f$length)
lf.washington <- max(washington.f$length)

mphf.beacon <- round((lf.beacon / tf.beacon) / 1609.34 * 3600, digits = 2)
mphf.bluehill <- round((lf.bluehill / tf.bluehill) / 1609.34 * 3600, digits = 2)
mphf.broadway <- round((lf.broadway / tf.broadway) / 1609.34 * 3600, digits = 2)
mphf.columbia <- round((lf.columbia / tf.columbia) / 1609.34 * 3600, digits = 2)
mphf.columbus <- round((lf.columbus / tf.columbus) / 1609.34 * 3600, digits = 2)
mphf.commonwealth <- round((lf.commonwealth / tf.commonwealth) / 1609.34 * 3600, digits = 2)
mphf.huntington <- round((lf.huntington / tf.huntington) / 1609.34 * 3600, digits = 2)
mphf.hydepark <- round((lf.hydepark / tf.hydepark) / 1609.34 * 3600, digits = 2)
mphf.massachusetts <- round((lf.massachusetts / tf.massachusetts) / 1609.34 * 3600, digits = 2)
mphf.tremont <- round((lf.tremont / tf.tremont) / 1609.34 * 3600, digits = 2)
mphf.washington <- round((lf.washington / tf.washington) / 1609.34 * 3600, digits = 2)


## Now work to get these numbers for the multiples, but split by route_id
# Create dataframes for each street that are composed of the unique route_id
beacon.r <- data.frame(unique(beacon.m$route_id), unique(beacon.m$name))
bluehill.r <- data.frame(unique(bluehill.m$route_id), unique(bluehill.m$name))
broadway.r <- data.frame(unique(broadway.m$route_id), unique(broadway.m$name))
columbia.r <- data.frame(unique(columbia.m$route_id), unique(columbia.m$name))
columbus.r <- data.frame(unique(columbus.m$route_id), unique(columbus.m$name))
commonwealth.r <- data.frame(unique(commonwealth.m$route_id), unique(commonwealth.m$name))
huntington.r <- data.frame(unique(huntington.m$route_id), unique(huntington.m$name))
hydepark.r <- data.frame(unique(hydepark.m$route_id), unique(hydepark.m$name))
massachusetts.r <- data.frame(unique(massachusetts.m$route_id), unique(massachusetts.m$name))
tremont.r <- data.frame(unique(tremont.m$route_id), unique(tremont.m$name))
washington.r <- data.frame(unique(washington.m$route_id), unique(washington.m$name))

# Write function to separate and calculate means for segments
segment.time <- function(df,seg,col,col.length) {  #######################  Need to change 120
  round(median(df[(((seg-1)*(length(which(df[,col.length] == min(df[,col.length])))))+1):(seg*120),col]), digits = 2)
}

segment.length <- function(df,seg,col) {
  df[(((seg-1)*(length(which(df[,col] == min(df[,col])))))+1),col]
}

#try something
segment.length <- function(df,seg,col) {
  round(median(df[(((seg-1)*(length(which(df[,col] == min(df[,col])))))+1):(seg*120),col]), digits = 2)
}



segment.time <- function(df,seg,col,col.length) {
  obs <- (length(which(df[,col.length] == max(df[,col.length]))))
  print(obs)
  round(mean(df[(((seg-1)*obs)+1):(seg*obs),col]), digits = 2)
}

segment.length <- function(df,seg,col) {
  obs <- (length(which(df[,col] == max(df[,col]))))
  print(obs)
  df[(((seg-1)*obs)+100),col]
}


## Try a new way
beacon.u <- unique(beacon.m$route_id)
bluehill.u <- unique(bluehill.m$route_id)
broadway.u <- unique(broadway.m$route_id)
columbia.u <- unique(columbia.m$route_id)
columbus.u <- unique(columbus.m$route_id)
commonwealth.u <- unique(commonwealth.m$route_id)
huntington.u <- unique(huntington.m$route_id)
hydepark.u <- unique(hydepark.m$route_id)
massachusetts.u <- unique(massachusetts.m$route_id)
tremont.u <- unique(tremont.m$route_id)
washington.u <- unique(washington.m$route_id)

segment.time <- function(df, t, r, v, c) { #df is dataframe, t is time col#, r is routeID, v is unique vector of routeID, c is count within the vector
  round(mean(df[,t][df[,r] == v[c]]), digits = 2)
}

segment.length <- function(df, l, r, v, c) { #df is dataframe, l is length col#,, r is routeID, v is unique vector of routeID, c is count within the vector
  round(mean(df[,l][df[,r] == v[c]]), digits = 2)
}




#beacon
beacon.r$time <- c(segment.time(beacon.m,6,1,beacon.u,1), segment.time(beacon.m,6,1,beacon.u,2), segment.time(beacon.m,6,1,beacon.u,3), 
                   segment.time(beacon.m,6,1,beacon.u,4), segment.time(beacon.m,6,1,beacon.u,5), segment.time(beacon.m,6,1,beacon.u,6), 
                   segment.time(beacon.m,6,1,beacon.u,7))

beacon.r$length <- c(segment.length(beacon.m,5,1,beacon.u,1), segment.length(beacon.m,5,1,beacon.u,2), segment.length(beacon.m,5,1,beacon.u,3), 
                     segment.length(beacon.m,5,1,beacon.u,4), segment.length(beacon.m,5,1,beacon.u,5), segment.length(beacon.m,5,1,beacon.u,6), 
                     segment.length(beacon.m,5,1,beacon.u,7))

#bluehill
bluehill.r$time <- c(segment.time(bluehill.m,6,1,bluehill.u,1), segment.time(bluehill.m,6,1,bluehill.u,2), segment.time(bluehill.m,6,1,bluehill.u,3), 
                     segment.time(bluehill.m,6,1,bluehill.u,4), segment.time(bluehill.m,6,1,bluehill.u,5))

bluehill.r$length <- c(segment.length(bluehill.m,5,1,bluehill.u,1), segment.length(bluehill.m,5,1,bluehill.u,2), segment.length(bluehill.m,5,1,bluehill.u,3), 
                       segment.length(bluehill.m,5,1,bluehill.u,4), segment.length(bluehill.m,5,1,bluehill.u,5))

#broadway
broadway.r$time <- c(segment.time(broadway.m,6,1,broadway.u,1), segment.time(broadway.m,6,1,broadway.u,2), segment.time(broadway.m,6,1,broadway.u,3), 
                     segment.time(broadway.m,6,1,broadway.u,4), segment.time(broadway.m,6,1,broadway.u,5), segment.time(broadway.m,6,1,broadway.u,6), 
                     segment.time(broadway.m,6,1,broadway.u,7), segment.time(broadway.m,6,1,broadway.u,8))

broadway.r$length <- c(segment.length(broadway.m,5,1,broadway.u,1), segment.length(broadway.m,5,1,broadway.u,2), segment.length(broadway.m,5,1,broadway.u,3), 
                       segment.length(broadway.m,5,1,broadway.u,4), segment.length(broadway.m,5,1,broadway.u,5), segment.length(broadway.m,5,1,broadway.u,6), 
                       segment.length(broadway.m,5,1,broadway.u,7), segment.length(broadway.m,5,1,broadway.u,8))

#columbia
columbia.r$time <- c(segment.time(columbia.m,6,1,columbia.u,1), segment.time(columbia.m,6,1,columbia.u,2), segment.time(columbia.m,6,1,columbia.u,3), 
                     segment.time(columbia.m,6,1,columbia.u,4), segment.time(columbia.m,6,1,columbia.u,5))

columbia.r$length <- c(segment.length(columbia.m,5,1,columbia.u,1), segment.length(columbia.m,5,1,columbia.u,2), segment.length(columbia.m,5,1,columbia.u,3), 
                       segment.length(columbia.m,5,1,columbia.u,4), segment.length(columbia.m,5,1,columbia.u,5))

################# HERE 1/22/2019

#columbus
columbus.r$time <- c(segment.time(columbus.m,6,1,columbus.u,1), segment.time(columbus.m,6,1,columbus.u,2), segment.time(columbus.m,6,1,columbus.u,3), 
                     segment.time(columbus.m,6,1,columbus.u,4), segment.time(columbus.m,6,1,columbus.u,5), segment.time(columbus.m,6,1,columbus.u,6))

columbus.r$length <- c(segment.length(columbus.m,5,1,columbus.u,1), segment.length(columbus.m,5,1,columbus.u,2), segment.length(columbus.m,5,1,columbus.u,3), 
                       segment.length(columbus.m,5,1,columbus.u,4), segment.length(columbus.m,5,1,columbus.u,5), segment.length(columbus.m,5,1,columbus.u,6))

#commonwealth
commonwealth.r$time <- c(segment.time(commonwealth.m,6,1,commonwealth.u,1), segment.time(commonwealth.m,6,1,commonwealth.u,2), 
                         segment.time(commonwealth.m,6,1,commonwealth.u,3), segment.time(commonwealth.m,6,1,commonwealth.u,4), 
                         segment.time(commonwealth.m,6,1,commonwealth.u,5), segment.time(commonwealth.m,6,1,commonwealth.u,6), 
                         segment.time(commonwealth.m,6,1,commonwealth.u,7))

commonwealth.r$length <- c(segment.length(commonwealth.m,5,1,commonwealth.u,1), segment.length(commonwealth.m,5,1,commonwealth.u,2), 
                           segment.length(commonwealth.m,5,1,commonwealth.u,3), segment.length(commonwealth.m,5,1,commonwealth.u,4), 
                           segment.length(commonwealth.m,5,1,commonwealth.u,5), segment.length(commonwealth.m,5,1,commonwealth.u,6), 
                           segment.length(commonwealth.m,5,1,commonwealth.u,7))

#huntington
huntington.r$time <- c(segment.time(huntington.m,6,1,huntington.u,1), segment.time(huntington.m,6,1,huntington.u,2), 
                       segment.time(huntington.m,6,1,huntington.u,3), segment.time(huntington.m,6,1,huntington.u,4), 
                       segment.time(huntington.m,6,1,huntington.u,5))

huntington.r$length <- c(segment.length(huntington.m,5,1,huntington.u,1), segment.length(huntington.m,5,1,huntington.u,2), 
                         segment.length(huntington.m,5,1,huntington.u,3), segment.length(huntington.m,5,1,huntington.u,4), 
                         segment.length(huntington.m,5,1,huntington.u,5))

#hydepark
hydepark.r$time <- c(segment.time(hydepark.m,6,1,hydepark.u,1), segment.time(hydepark.m,6,1,hydepark.u,2), segment.time(hydepark.m,6,1,hydepark.u,3), 
                     segment.time(hydepark.m,6,1,hydepark.u,4), segment.time(hydepark.m,6,1,hydepark.u,5))

hydepark.r$length <- c(segment.length(hydepark.m,5,1,hydepark.u,1), segment.length(hydepark.m,5,1,hydepark.u,2), segment.length(hydepark.m,5,1,hydepark.u,3), 
                       segment.length(hydepark.m,5,1,hydepark.u,4), segment.length(hydepark.m,5,1,hydepark.u,5))

#massachusetts
massachusetts.r$time <- c(segment.time(massachusetts.m,6,1,massachusetts.u,1), segment.time(massachusetts.m,6,1,massachusetts.u,2), 
                          segment.time(massachusetts.m,6,1,massachusetts.u,3), segment.time(massachusetts.m,6,1,massachusetts.u,4), 
                          segment.time(massachusetts.m,6,1,massachusetts.u,5), segment.time(massachusetts.m,6,1,massachusetts.u,6), 
                          segment.time(massachusetts.m,6,1,massachusetts.u,7), segment.time(massachusetts.m,6,1,massachusetts.u,8), 
                          segment.time(massachusetts.m,6,1,massachusetts.u,9), segment.time(massachusetts.m,6,1,massachusetts.u,10), 
                          segment.time(massachusetts.m,6,1,massachusetts.u,11), segment.time(massachusetts.m,6,1,massachusetts.u,12),
                          segment.time(massachusetts.m,6,1,massachusetts.u,13))

massachusetts.r$length <- c(segment.length(massachusetts.m,5,1,massachusetts.u,1), segment.length(massachusetts.m,5,1,massachusetts.u,2), 
                            segment.length(massachusetts.m,5,1,massachusetts.u,3), segment.length(massachusetts.m,5,1,massachusetts.u,4), 
                            segment.length(massachusetts.m,5,1,massachusetts.u,5), segment.length(massachusetts.m,5,1,massachusetts.u,6), 
                            segment.length(massachusetts.m,5,1,massachusetts.u,7), segment.length(massachusetts.m,5,1,massachusetts.u,8), 
                            segment.length(massachusetts.m,5,1,massachusetts.u,9), segment.length(massachusetts.m,5,1,massachusetts.u,10), 
                            segment.length(massachusetts.m,5,1,massachusetts.u,11), segment.length(massachusetts.m,5,1,massachusetts.u,12),
                            segment.length(massachusetts.m,5,1,massachusetts.u,13))

#tremont
tremont.r$time <- c(segment.time(tremont.m,6,1,tremont.u,1), segment.time(tremont.m,6,1,tremont.u,2), segment.time(tremont.m,6,1,tremont.u,3), 
                    segment.time(tremont.m,6,1,tremont.u,4), segment.time(tremont.m,6,1,tremont.u,5))

tremont.r$length <- c(segment.length(tremont.m,5,1,tremont.u,1), segment.length(tremont.m,5,1,tremont.u,2), segment.length(tremont.m,5,1,tremont.u,3), 
                      segment.length(tremont.m,5,1,tremont.u,4), segment.length(tremont.m,5,1,tremont.u,5))

#washington
washington.r$time <- c(segment.time(washington.m,6,1,washington.u,1), segment.time(washington.m,6,1,washington.u,2), 
                       segment.time(washington.m,6,1,washington.u,3), segment.time(washington.m,6,1,washington.u,4), 
                       segment.time(washington.m,6,1,washington.u,5), segment.time(washington.m,6,1,washington.u,6), 
                       segment.time(washington.m,6,1,washington.u,7), segment.time(washington.m,6,1,washington.u,8), 
                       segment.time(washington.m,6,1,washington.u,9), segment.time(washington.m,6,1,washington.u,10))

washington.r$length <- c(segment.length(washington.m,5,1,washington.u,1), segment.length(washington.m,5,1,washington.u,2), 
                         segment.length(washington.m,5,1,washington.u,3), segment.length(washington.m,5,1,washington.u,4), 
                         segment.length(washington.m,5,1,washington.u,5), segment.length(washington.m,5,1,washington.u,6), 
                         segment.length(washington.m,5,1,washington.u,7), segment.length(washington.m,5,1,washington.u,8), 
                         segment.length(washington.m,5,1,washington.u,9), segment.length(washington.m,5,1,washington.u,10))


# Extract specific valued sums out of tables to graph
tm.beacon <- sum(beacon.r$time)
lm.beacon <- sum(beacon.r$length)

tm.bluehill <- sum(bluehill.r$time)
lm.bluehill <- sum(bluehill.r$length)

tm.broadway <- sum(broadway.r$time)
lm.broadway <- sum(broadway.r$length)

tm.columbia <- sum(columbia.r$time)
lm.columbia <- sum(columbia.r$length)

tm.columbus <- sum(columbus.r$time)
lm.columbus <- sum(columbus.r$length)

tm.commonwealth <- sum(commonwealth.r$time)
lm.commonwealth <- sum(commonwealth.r$length)

tm.huntington <- sum(huntington.r$time)
lm.huntington <- sum(huntington.r$length)

tm.hydepark <- sum(hydepark.r$time)
lm.hydepark <- sum(hydepark.r$length)

tm.massachusetts <- sum(massachusetts.r$time)
lm.massachusetts <- sum(massachusetts.r$length)

tm.tremont <- sum(tremont.r$time)
lm.tremont <- sum(tremont.r$length)

tm.washington <- sum(washington.r$time)
lm.washington <- sum(washington.r$length)

# Create columns from these values for two different tables for length / time (full_route, segmented_route, route)
l.full <- data.table(c(lf.beacon,lf.bluehill,lf.broadway,lf.columbia,lf.columbus,lf.commonwealth,lf.huntington,lf.hydepark,
                       lf.massachusetts,lf.tremont,lf.washington))
l.seg <- data.table(c(lm.beacon,lm.bluehill,lm.broadway,lm.columbia,lm.columbus,lm.commonwealth,lm.huntington,lm.hydepark,
                      lm.massachusetts,lm.tremont,lm.washington))
routes <- data.table(streets)

t.full <- data.table(c(tf.beacon,tf.bluehill,tf.broadway,tf.columbia,tf.columbus,tf.commonwealth,tf.huntington,tf.hydepark,
                       tf.massachusetts,tf.tremont,tf.washington))
t.seg <- data.table(c(tm.beacon,tm.bluehill,tm.broadway,tm.columbia,tm.columbus,tm.commonwealth,tm.huntington,tm.hydepark,
                      tm.massachusetts,tm.tremont,tm.washington))

# Bind the columns together into several data tables
length <- cbind(l.full, l.seg, routes)
colnames(length) <- c('full','segmented','route')

time <- cbind(t.full, t.seg, routes)
colnames(time) <- c('full','segmented','route')

all <- cbind(l.full, l.seg, t.full, t.seg, routes)
colnames(all) <- c('length_full','length_segmented','time_full','time_segmented','route')


# Additional calculations for tables
# Percent difference field
percent.dif <- function(new, old) {
  percentage <- ((new - old) / old) * 100
}

length$dif <- abs(length$full - length$segmented)
length$dif_percent <- percent.dif(length$segmented, length$full)

time$dif <- abs(time$full - time$segmented)
time$dif_percent <- percent.dif(time$segmented, time$full)

all$mph_full <- round(all$length_full / all$time_full / 1609.34 * 3600,
                      digits = 2)
all$mph_segmented <- round(all$length_segmented / all$time_segmented / 1609.34 * 3600,
                           digits = 2)

mph <- data.table(all$mph_full,all$mph_segmented,all$route)
colnames(mph) <- c('full_route','segmented_route','route')
mph$dif <- abs(mph$full_route - mph$segmented_route)
mph$dif_percent <- percent.dif(mph$segmented_route, mph$full_route)



### Plots
# Time plotting
time.nodif <- data.table(time$full, time$segmented, time$route)
colnames(time.nodif) <- c('Full Route','Segmented Routes','route')
time2 <- melt(time.nodif, id.vars = 'route')
head(time2)
colnames(time2) <- c('route','Legend','value')

l.t <-ggplot(time2, aes(x = route, y = value, fill = Legend, label = value)) +
  geom_bar(stat = 'identity', position = 'dodge')

png("images/time_comparison_all.png", width = 1200,
    height = 900)
l.t +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  ggtitle("Route Time Averaged Over All Observations") +
  theme(plot.title = element_text(hjust = 0.5)) +
  geom_text(size = 4, position = position_dodge(width = 1), vjust = -0.5) +
  ylab("Seconds") +
  xlab("Test Routes") +
  scale_y_continuous(expand = c(0,0), limits = c(0,1750)) +
  scale_color_hue(labels = c("Full Route", "Segmented Routes"))
dev.off()


# Length plotting
length.nodif <- data.table(length$full, length$segmented, length$route)
colnames(length.nodif) <- c('Full Route','Segmented Routes','route')
length2 <- melt(length.nodif, id.vars = 'route')
head(length2)
colnames(length2) <- c('route','Legend','value')

l.p <-ggplot(length2, aes(x = route, y = value, fill = Legend, label = value)) +
  geom_bar(stat = 'identity', position = 'dodge')

png("images/length_comparison_all.png", width = 1200,
    height = 900)
l.p +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  ggtitle("Route Length Comparison Over All Observations") +
  theme(plot.title = element_text(hjust = 0.5)) +
  geom_text(size = 3, position = position_dodge(width = 1), vjust = -0.5) +
  ylab("Meters") +
  xlab("Test Routes") +
  scale_y_continuous(expand = c(0,0), limits = c(0,6800)) +
  scale_color_hue(labels = c("Full Route", "Segmented Routes"))
dev.off()


# Speed plotting
mph.nodif <- data.table(mph$full, mph$segmented, mph$route)
colnames(mph.nodif) <- c('Full Route','Segmented Routes','route')
mph2 <- melt(mph.nodif, id.vars = 'route')
head(mph2)
colnames(mph2) <- c('route','Legend','value')

l.s <-ggplot(mph2, aes(x = route, y = value, fill = Legend, label = value)) +
  geom_bar(stat = 'identity', position = 'dodge')

png("images/speed_comparison_all.png", width = 1200,
    height = 900)
l.s +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  ggtitle("Route Speed Averaged Over All Observations") +
  theme(plot.title = element_text(hjust = 0.5)) +
  geom_text(size = 3, position = position_dodge(width = 1), vjust = -0.5) +
  ylab("Miles per Hour") +
  xlab("Test Routes") +
  scale_y_continuous(expand = c(0,0), limits = c(0,30)) +
  scale_color_hue(labels = c("Full Route", "Segmented Routes"))
dev.off()

# Create final table
all$length_dif <- length$dif
all$length_dif_percent <- length$dif_percent
all$time_dif <- time$dif
all$time_dif_percent <- time$dif_percent
all$mph_full <- mph$full_route
all$mph_segmented <- mph$segmented_route
all$mph_dif <- mph$dif
all$mph_dif_percent <- mph$dif_percent
waze.all <- subset(all, select = c(route,length_full,length_segmented,length_dif,length_dif_percent,
                                      time_full,time_segmented,time_dif,time_dif_percent,
                                      mph_full,mph_segmented,mph_dif,mph_dif_percent))


write.xlsx(waze.all, "waze_all.xlsx", row.names = FALSE)

gs_auth(new_user = TRUE)

gs_upload("waze_all.xlsx", sheet_title = "Waze Comparisons over All Observations")






