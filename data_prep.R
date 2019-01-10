 
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
q1 <- 
"select t1.route_Id, t1.name, t1.fromname, t1.toname, t1.length, t1.time, t1.historictime, t1.updatetime, t1.map_name
from internal_data.waze_routes t1 inner join
(
	select max(updatetime) updatetime, name
	from internal_data.waze_routes
	group by name
) as t2
	on t1.name = t2.name
	and t1.updatetime = t2.updatetime
where t1.name like 'TD%'
order by updatetime desc;"

# Load in latest waze data that is based
waze.latest <- dbGetQuery(con, q1)
rm(pw, un, hst)

# Creat mph column
waze.latest$mph <- (waze.latest$length / waze.latest$time) / 1609.34 * 3600

# Create new column for the principle street name 
waze.latest$main_street <- NA

# Create vector of street names
streets <- c('Beacon St','Blue Hill Ave','Broadway',
             'Columbia Rd','Columbus Ave',
             'Commonwealth Ave','Huntington Ave',
             'Hyde Park Ave','Massachusetts Ave',
             'Tremont St','Washington St')

# Populate main_street column with streets names
for (street in streets){
  waze.latest[waze.latest$name %like% paste("TD - ",
                                            street, 
                                            sep = ""), 
              "main_street"] <- street
  
  print(paste(street, "complete"))
}

# Order dataframe by street name and length so longest is first
waze.latest <- waze.latest[
  with(waze.latest, order(main_street, length)),
]

row.names(waze.latest) <- 1:nrow(waze.latest)

df.sts <- split(waze.latest, waze.latest$main_street)


#df.sts <- split(waze.latest, forcats::fct_inorder(factor(waze.latest$main_street)))
#dt <- data.table(waze.latest)
#dt[, grp := .GRP, by = main_street]
#setkey(dt, grp)
#dt.sts <- dt[, list(list(.SD)), by = grp]$



#df.sts$`Beacon St`$mph


### Make plot of time to travel along routes
# Start by creating travel time and length sums for each
streets.n <- c('Beacon','BlueHill','Broadway',
             'Columbia','Columbus',
             'Commonwealth','Huntington',
             'HydePark','Massachusetts',
             'Tremont','Washington')

# Order all by length
#df.sts$`Beacon St` <- order(df.sts$`Beacon St`$length)


# Write function to calculate amount based on field and dataframe
amount <- function(field, df) {
  am <- sum(field[1:(nrow(df)-1)])
  return(am)
}

# Execute function to determine lengths from multiple (lm) and times from multiple (tm)

# Length multiple
lm.beacon <- amount(df.sts$`Beacon St`$length, df.sts$`Beacon St`)
lm.bluehill <- amount(df.sts$`Blue Hill Ave`$length, df.sts$`Blue Hill Ave`)
lm.broadway <- amount(df.sts$Broadway$length, df.sts$Broadway)
lm.columbia <- amount(df.sts$`Columbia Rd`$length, df.sts$`Columbia Rd`)
lm.columbus <- amount(df.sts$`Columbus Ave`$length, df.sts$`Columbus Ave`)
lm.commonwealth <- amount(df.sts$`Commonwealth Ave`$length, df.sts$`Commonwealth Ave`)
lm.huntington <- amount(df.sts$`Huntington Ave`$length, df.sts$`Huntington Ave`)
lm.hydepark <- amount(df.sts$`Hyde Park Ave`$length, df.sts$`Hyde Park Ave`)
lm.mass <- amount(df.sts$`Massachusetts Ave`$length, df.sts$`Massachusetts Ave`)
lm.tremont <- amount(df.sts$`Tremont St`$length, df.sts$`Tremont St`)
lm.washington <- amount(df.sts$`Washington St`$length, df.sts$`Washington St`)


lm <- data.table(c(lm.beacon, lm.bluehill, lm.broadway, lm.columbia, lm.columbus, lm.commonwealth,
        lm.huntington, lm.hydepark, lm.mass, lm.tremont, lm.washington))
rm(lm.beacon, lm.bluehill, lm.broadway, lm.columbia, lm.columbus, lm.commonwealth,
   lm.huntington, lm.hydepark, lm.mass, lm.tremont, lm.washington)

# Length
l.beacon <- df.sts$`Beacon St`$length[nrow(df.sts$`Beacon St`)]
l.bluehill <- df.sts$`Blue Hill Ave`$length[nrow(df.sts$`Blue Hill Ave`)]
l.broadway <- df.sts$Broadway$length[nrow(df.sts$Broadway)]
l.columbia <- df.sts$`Columbia Rd`$length[nrow(df.sts$`Columbia Rd`)]
l.columbus <- df.sts$`Columbus Ave`$length[nrow(df.sts$`Columbus Ave`)]
l.commonwealth <- df.sts$`Commonwealth Ave`$length[nrow(df.sts$`Commonwealth Ave`)]
l.huntington <- df.sts$`Huntington Ave`$length[nrow(df.sts$`Huntington Ave`)]
l.hydepark <- df.sts$`Hyde Park Ave`$length[nrow(df.sts$`Hyde Park Ave`)]
l.mass <- df.sts$`Massachusetts Ave`$length[nrow(df.sts$`Massachusetts Ave`)]
l.tremont <- df.sts$`Tremont St`$length[nrow(df.sts$`Tremont St`)]
l.washington <- df.sts$`Washington St`$length[nrow(df.sts$`Washington St`)]

l <- data.table(c(l.beacon, l.bluehill, l.broadway, l.columbia, l.columbus, l.commonwealth,
        l.huntington, l.hydepark, l.mass, l.tremont, l.washington))
rm(l.beacon, l.bluehill, l.broadway, l.columbia, l.columbus, l.commonwealth,
   l.huntington, l.hydepark, l.mass, l.tremont, l.washington)

# Time multiple
tm.beacon <- amount(df.sts$`Beacon St`$time, df.sts$`Beacon St`)
tm.bluehill <- amount(df.sts$`Blue Hill Ave`$time, df.sts$`Blue Hill Ave`)
tm.broadway <- amount(df.sts$Broadway$time, df.sts$Broadway)
tm.columbia <- amount(df.sts$`Columbia Rd`$time, df.sts$`Columbia Rd`)
tm.columbus <- amount(df.sts$`Columbus Ave`$time, df.sts$`Columbus Ave`)
tm.commonwealth <- amount(df.sts$`Commonwealth Ave`$time, df.sts$`Commonwealth Ave`)
tm.huntington <- amount(df.sts$`Huntington Ave`$time, df.sts$`Huntington Ave`)
tm.hydepark <- amount(df.sts$`Hyde Park Ave`$time, df.sts$`Hyde Park Ave`)
tm.mass <- amount(df.sts$`Massachusetts Ave`$time, df.sts$`Massachusetts Ave`)
tm.tremont <- amount(df.sts$`Tremont St`$time, df.sts$`Tremont St`)
tm.washington <- amount(df.sts$`Washington St`$time, df.sts$`Washington St`)

tm <- data.table(c(tm.beacon, tm.bluehill, tm.broadway, tm.columbia, tm.columbus, tm.commonwealth,
        tm.huntington, tm.hydepark, tm.mass, tm.tremont, tm.washington))
rm(tm.beacon, tm.bluehill, tm.broadway, tm.columbia, tm.columbus, tm.commonwealth,
   tm.huntington, tm.hydepark, tm.mass, tm.tremont, tm.washington)

# Time
t.beacon <- df.sts$`Beacon St`$time[nrow(df.sts$`Beacon St`)]
t.bluehill <- df.sts$`Blue Hill Ave`$time[nrow(df.sts$`Blue Hill Ave`)]
t.broadway <- df.sts$Broadway$time[nrow(df.sts$Broadway)]
t.columbia <- df.sts$`Columbia Rd`$time[nrow(df.sts$`Columbia Rd`)]
t.columbus <- df.sts$`Columbus Ave`$time[nrow(df.sts$`Columbus Ave`)]
t.commonwealth <- df.sts$`Commonwealth Ave`$time[nrow(df.sts$`Commonwealth Ave`)]
t.huntington <- df.sts$`Huntington Ave`$time[nrow(df.sts$`Huntington Ave`)]
t.hydepark <- df.sts$`Hyde Park Ave`$time[nrow(df.sts$`Hyde Park Ave`)]
t.mass <- df.sts$`Massachusetts Ave`$time[nrow(df.sts$`Massachusetts Ave`)]
t.tremont <- df.sts$`Tremont St`$time[nrow(df.sts$`Tremont St`)]
t.washington <- df.sts$`Washington St`$time[nrow(df.sts$`Washington St`)]

t <- data.table(c(t.beacon, t.bluehill, t.broadway, t.columbia, t.columbus, t.commonwealth,
        t.huntington, t.hydepark, t.mass, t.tremont, t.washington))
rm(t.beacon, t.bluehill, t.broadway, t.columbia, t.columbus, t.commonwealth,
   t.huntington, t.hydepark, t.mass, t.tremont, t.washington)

# Combine it all into a datatable and give colnames and a new column with types
difs <- cbind(l, lm, t, tm)
difs$route <-c('Beacon St','Blue Hill Ave','Broadway',
               'Columbia Rd','Columbus Ave',
               'Commonwealth Ave','Huntington Ave',
               'Hyde Park Ave','Massachusetts Ave',
               'Tremont St','Washington St')

# Cbind to create the dataframe
length <- cbind(l,lm)
length$route <-c('Beacon St','Blue Hill Ave','Broadway',
                     'Columbia Rd','Columbus Ave',
                     'Commonwealth Ave','Huntington Ave',
                     'Hyde Park Ave','Massachusetts Ave',
                     'Tremont St','Washington St')
colnames(length) <- c('full_route','segmented_route','route') 

# Add in the difference field
length$dif <- abs(length$full_route - length$segmented_route)


# cbind to create the time dataframe
time <- cbind(t,tm)
time$route <-c('Beacon St','Blue Hill Ave','Broadway',
                   'Columbia Rd','Columbus Ave',
                   'Commonwealth Ave','Huntington Ave',
                   'Hyde Park Ave','Massachusetts Ave',
                   'Tremont St','Washington St')
colnames(time) <- c('full_route','segmented_route','route')

# Add in the difference field
time$diff <- abs(time$full_route - time$segmented_route)

colnames(difs) <- c('full_length','segmented_length','full_time','segmented_time','route')

# Percent difference field
percent.dif <- function(new, old) {
  percentage <- ((new - old) / old) * 100
}

length$perdif <- percent.dif(length$segmented_route, length$full_route)
time$perdif <- percent.dif(time$segmented_route, time$full_route)














## Plot it out
# Length plotting
length.nodif <- data.table(length$full_route, length$segmented_route, length$route)
colnames(length.nodif) <- c('Full Route','Segmented Routes','route')
length2 <- melt(length.nodif, id.vars = 'route')
head(length2)
colnames(length2) <- c('route','Legend','value')

l.p <-ggplot(length2, aes(x = route, y = value, fill = Legend, label = value)) +
  geom_bar(stat = 'identity', position = 'dodge')

l.p +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  ggtitle("Route Length Comparisons") +
  theme(plot.title = element_text(hjust = 0.5)) +
  geom_text(size = 3, position = position_dodge(width = 1), vjust = -0.5) +
  ylab("Meters") +
  xlab("Test Routes") +
  scale_y_continuous(expand = c(0,0), limits = c(0,6800)) +
  scale_color_hue(labels = c("Full Route", "Segmented Routes"))



# Time plotting
time.nodif <- data.table(time$full_route, time$segmented_route, time$route)
colnames(time.nodif) <- c('Full Route','Segmented Routes','route')
time2 <- melt(time.nodif, id.vars = 'route')
head(time2)
colnames(time2) <- c('route','Legend','value')

l.t <-ggplot(time2, aes(x = route, y = value, fill = Legend, label = value)) +
  geom_bar(stat = 'identity', position = 'dodge')

l.t +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  ggtitle("Route Time Comparisons") +
  theme(plot.title = element_text(hjust = 0.5)) +
  geom_text(size = 3, position = position_dodge(width = 1), vjust = -0.5) +
  ylab("Seconds") +
  xlab("Test Routes") +
  scale_y_continuous(expand = c(0,0), limits = c(0,1750)) +
  scale_color_hue(labels = c("Full Route", "Segmented Routes"))













# Separate out into individual data frames, it's done this way to avoid 
# the spaces that would be in the data frame names if done in an automated fashion
# Explore doing this automated if needed in the future (obvy)
#beacon <- subset(waze.latest, waze.latest$main_street == 'Beacon St')
#bluehill <- subset(waze.latest, waze.latest$main_street == 'Blue Hill Ave')
#broadway <- subset(waze.latest, waze.latest$main_street == 'Broadway')
#columbia <- subset(waze.latest, waze.latest$main_street == 'Columbia Rd')
#columbus <- subset(waze.latest, waze.latest$main_street == 'Columbus Ave')
#commonwealth <- subset(waze.latest, waze.latest$main_street == 'Commonwealth Ave')
#huntington <- subset(waze.latest, waze.latest$main_street == 'Huntington Ave')
#hydepark <- subset(waze.latest, waze.latest$main_street == 'Hyde Park Ave')
#mass <- subset(waze.latest, waze.latest$main_street == 'Massachusetts Ave')
#tremont <- subset(waze.latest, waze.latest$main_street == 'Tremont St')
#washington <- subset(waze.latest, waze.latest$main_street == 'Washington St')








