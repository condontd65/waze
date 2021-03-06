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


# Order dataframe by street name and length so longest is first
waze.big <- waze.big[
  with(waze.big, order(main_street, length)),
  ]

row.names(waze.big) <- 1:nrow(waze.big)


# This gives a list of dataframes by each street (11)  
waze.street <- split(waze.big, waze.big$main_street) 

# Write function to calculate amount based on field and dataframe
#amount <- function(field, df) {
#  am <- sum(field[1:(nrow(df)-120)])
#  return(am)
#}

# Create data frames for the multiples
beacon.m <- waze.street$`Beacon St`[1:(nrow(waze.street$`Beacon St`) - 120),]
bluehill.m <- waze.street$`Blue Hill Ave`[1:(nrow(waze.street$`Blue Hill Ave`)-120),]
broadway.m <- waze.street$`Broadway`[1:(nrow(waze.street$Broadway) - 120),]
columbia.m <- waze.street$`Columbia Rd`[1:(nrow(waze.street$`Columbia Rd`) - 120),]
columbus.m <- waze.street$`Columbus Ave`[1:(nrow(waze.street$`Columbus Ave`) - 120),]
commonwealth.m <- waze.street$`Commonwealth Ave`[1:(nrow(waze.street$`Commonwealth Ave`) - 120),]
huntington.m <- waze.street$`Huntington Ave`[1:(nrow(waze.street$`Huntington Ave`) - 120),]
hydepark.m <- waze.street$`Hyde Park Ave`[1:(nrow(waze.street$`Hyde Park Ave`) - 120),]
massachusetts.m <- waze.street$`Massachusetts Ave`[1:(nrow(waze.street$`Massachusetts Ave`) - 120),]
tremont.m <- waze.street$`Tremont St`[1:(nrow(waze.street$`Tremont St`) - 120),]
washington.m <- waze.street$`Washington St`[1:(nrow(waze.street$`Washington St`) - 120),]

# Create data frames for the full routes
beacon.f <- waze.street$`Beacon St`[(nrow(waze.street$`Beacon St`) - 119):(nrow(waze.street$`Beacon St`)),]
bluehill.f <- waze.street$`Blue Hill Ave`[(nrow(waze.street$`Blue Hill Ave`)-119):(nrow(waze.street$`Blue Hill Ave`)),]
broadway.f <- waze.street$Broadway[(nrow(waze.street$Broadway) - 119):(nrow(waze.street$Broadway)),]
columbia.f <- waze.street$`Columbia Rd`[(nrow(waze.street$`Columbia Rd`) - 119):(nrow(waze.street$`Columbia Rd`)),]
columbus.f <- waze.street$`Columbus Ave`[(nrow(waze.street$`Columbus Ave`) - 119):(nrow(waze.street$`Columbus Ave`)),]
commonwealth.f <- waze.street$`Commonwealth Ave`[(nrow(waze.street$`Commonwealth Ave`) - 119):(nrow(waze.street$`Commonwealth Ave`)),]
huntington.f <- waze.street$`Huntington Ave`[(nrow(waze.street$`Huntington Ave`) - 119):(nrow(waze.street$`Huntington Ave`)),]
hydepark.f <- waze.street$`Hyde Park Ave`[(nrow(waze.street$`Hyde Park Ave`) - 119):(nrow(waze.street$`Hyde Park Ave`)),]
massachusetts.f <- waze.street$`Massachusetts Ave`[(nrow(waze.street$`Massachusetts Ave`) - 119):(nrow(waze.street$`Massachusetts Ave`)),]
tremont.f <- waze.street$`Tremont St`[(nrow(waze.street$`Tremont St`) - 119):(nrow(waze.street$`Tremont St`)),]
washington.f <- waze.street$`Washington St`[(nrow(waze.street$`Washington St`) - 119):(nrow(waze.street$`Washington St`)),]

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
segment.time <- function(df,seg,col) {
  round(mean(df[(((seg-1)*120)+1):(seg*120),col]), digits = 2)
}

segment.length <- function(df,seg,col) {
  df[(((seg-1)*120)+1),col]
}
#beacon
beacon.r$time <- c(segment.time(beacon.m,1,6), segment.time(beacon.m,2,6), segment.time(beacon.m,3,6), segment.time(beacon.m,4,6),
                   segment.time(beacon.m,5,6), segment.time(beacon.m,6,6), segment.time(beacon.m,7,6))
beacon.r$length <- c(segment.length(beacon.m,1,5), segment.length(beacon.m,2,5), segment.length(beacon.m,3,5), 
                       segment.length(beacon.m,4,5), segment.length(beacon.m,5,5), segment.length(beacon.m,6,5), 
                       segment.length(beacon.m,7,5))
#bluehill
bluehill.r$time <- c(segment.time(bluehill.m,1,6), segment.time(bluehill.m,2,6), segment.time(bluehill.m,3,6), segment.time(bluehill.m,4,6),
                   segment.time(bluehill.m,5,6))
bluehill.r$length <- c(segment.length(bluehill.m,1,5), segment.length(bluehill.m,2,5), segment.length(bluehill.m,3,5), 
                     segment.length(bluehill.m,4,5), segment.length(bluehill.m,5,5))
#broadway
broadway.r$time <- c(segment.time(broadway.m,1,6), segment.time(broadway.m,2,6), segment.time(broadway.m,3,6), segment.time(broadway.m,4,6),
                   segment.time(broadway.m,5,6), segment.time(broadway.m,6,6), segment.time(broadway.m,7,6), segment.time(broadway.m,8,6))
broadway.r$length <- c(segment.length(broadway.m,1,5), segment.length(broadway.m,2,5), segment.length(broadway.m,3,5), 
                     segment.length(broadway.m,4,5), segment.length(broadway.m,5,5), segment.length(broadway.m,6,5), 
                     segment.length(broadway.m,7,5), segment.length(broadway.m,8,5))

#columbia
columbia.r$time <- c(segment.time(columbia.m,1,6), segment.time(columbia.m,2,6), segment.time(columbia.m,3,6), segment.time(columbia.m,4,6),
                   segment.time(columbia.m,5,6))
columbia.r$length <- c(segment.length(columbia.m,1,5), segment.length(columbia.m,2,5), segment.length(columbia.m,3,5), 
                     segment.length(columbia.m,4,5), segment.length(columbia.m,5,5))

#columbus
columbus.r$time <- c(segment.time(columbus.m,1,6), segment.time(columbus.m,2,6), segment.time(columbus.m,3,6), segment.time(columbus.m,4,6),
                   segment.time(columbus.m,5,6), segment.time(columbus.m,6,6))
columbus.r$length <- c(segment.length(columbus.m,1,5), segment.length(columbus.m,2,5), segment.length(columbus.m,3,5), 
                     segment.length(columbus.m,4,5), segment.length(columbus.m,5,5), segment.length(columbus.m,6,5))

#commonwealth
commonwealth.r$time <- c(segment.time(commonwealth.m,1,6), segment.time(commonwealth.m,2,6), segment.time(commonwealth.m,3,6), segment.time(commonwealth.m,4,6),
                   segment.time(commonwealth.m,5,6), segment.time(commonwealth.m,6,6), segment.time(commonwealth.m,7,6))
commonwealth.r$length <- c(segment.length(commonwealth.m,1,5), segment.length(commonwealth.m,2,5), segment.length(commonwealth.m,3,5), 
                     segment.length(commonwealth.m,4,5), segment.length(commonwealth.m,5,5), segment.length(commonwealth.m,6,5), 
                     segment.length(commonwealth.m,7,5))

#huntington
huntington.r$time <- c(segment.time(huntington.m,1,6), segment.time(huntington.m,2,6), segment.time(huntington.m,3,6), segment.time(huntington.m,4,6),
                   segment.time(huntington.m,5,6))
huntington.r$length <- c(segment.length(huntington.m,1,5), segment.length(huntington.m,2,5), segment.length(huntington.m,3,5), 
                     segment.length(huntington.m,4,5), segment.length(huntington.m,5,5))

#hydepark
hydepark.r$time <- c(segment.time(hydepark.m,1,6), segment.time(hydepark.m,2,6), segment.time(hydepark.m,3,6), segment.time(hydepark.m,4,6),
                   segment.time(hydepark.m,5,6))
hydepark.r$length <- c(segment.length(hydepark.m,1,5), segment.length(hydepark.m,2,5), segment.length(hydepark.m,3,5), 
                     segment.length(hydepark.m,4,5), segment.length(hydepark.m,5,5))

#massachusetts
massachusetts.r$time <- c(segment.time(massachusetts.m,1,6), segment.time(massachusetts.m,2,6), segment.time(massachusetts.m,3,6), 
                          segment.time(massachusetts.m,4,6), segment.time(massachusetts.m,5,6), segment.time(massachusetts.m,6,6), 
                          segment.time(massachusetts.m,7,6), segment.time(massachusetts.m,8,6), segment.time(massachusetts.m,9,6),
                          segment.time(massachusetts.m,10,6), segment.time(massachusetts.m,11,6), segment.time(massachusetts.m,12,6),
                          segment.time(massachusetts.m,13,6))
massachusetts.r$length <- c(segment.length(massachusetts.m,1,5), segment.length(massachusetts.m,2,5), segment.length(massachusetts.m,3,5), 
                     segment.length(massachusetts.m,4,5), segment.length(massachusetts.m,5,5), segment.length(massachusetts.m,6,5), 
                     segment.length(massachusetts.m,7,5), segment.length(massachusetts.m,8,5), segment.length(massachusetts.m,9,5),
                     segment.length(massachusetts.m,10,5), segment.length(massachusetts.m,11,5), segment.length(massachusetts.m,12,5),
                     segment.length(massachusetts.m,13,5))

#tremont
tremont.r$time <- c(segment.time(tremont.m,1,6), segment.time(tremont.m,2,6), segment.time(tremont.m,3,6), segment.time(tremont.m,4,6),
                   segment.time(tremont.m,5,6))
tremont.r$length <- c(segment.length(tremont.m,1,5), segment.length(tremont.m,2,5), segment.length(tremont.m,3,5), 
                     segment.length(tremont.m,4,5), segment.length(tremont.m,5,5))

#washington
washington.r$time <- c(segment.time(washington.m,1,6), segment.time(washington.m,2,6), segment.time(washington.m,3,6), 
                       segment.time(washington.m,4,6), segment.time(washington.m,5,6), segment.time(washington.m,6,6), 
                       segment.time(washington.m,7,6), segment.time(washington.m,8,6), segment.time(washington.m,9,6),
                       segment.time(washington.m,10,6))
washington.r$length <- c(segment.length(washington.m,1,5), segment.length(washington.m,2,5), segment.length(washington.m,3,5), 
                     segment.length(washington.m,4,5), segment.length(washington.m,5,5), segment.length(washington.m,6,5), 
                     segment.length(washington.m,7,5), segment.length(washington.m,8,5), segment.length(washington.m,9,5),
                     segment.length(washington.m,10,5))

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

png("images/timelast10hrs.png", width = 1200,
    height = 900)
l.t +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  ggtitle("Route Time Averaged Over Last 10 Hours") +
  theme(plot.title = element_text(hjust = 0.5)) +
  geom_text(size = 3, position = position_dodge(width = 1), vjust = -0.5) +
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

png("images/lengthcomparison.png", width = 1200,
    height = 900)
l.p +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  ggtitle("Route Length Comparison") +
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

png("images/speedlast10hrs.png", width = 1200,
    height = 900)
l.s +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  ggtitle("Route Speed Averaged Over Last 10 Hours") +
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
all$time_dif_percent <- time$dif
all$mph_full <- mph$full_route
all$mph_segmented <- mph$segmented_route
all$mph_dif <- mph$dif
all$mph_dif_percent <- mph$dif_percent
waze.latest <- subset(all, select = c(route,length_full,length_segmented,length_dif,length_dif_percent,
                                       time_full,time_segmented,time_dif,time_dif_percent,
                                       mph_full,mph_segmented,mph_dif,mph_dif_percent))

write.xlsx(waze.latest, "waze_latest.xlsx", row.names = FALSE)

gs_auth(new_user = TRUE)

gs_upload("waze_latest.xlsx", sheet_title = "Waze over Last 10 Hours")





  