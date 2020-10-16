## STEP 1 in Annual Signals Temperature Analysis 
## Danielle Hare
## 2019

## This script only uses the USGS station names provided in the input file

## Function Inputs
library(rnoaa)
library(dplyr)
library(rgdal) # convert from UTM to lat long if necessary
library(sf)# convert geomtry datums
library(tidyverse) # for extract geomtry to two columns
library(dataRetrieval) #for USGS files


#Set input parameters
f_out <- "./output\\Station_ID_NOAA_Match_.txt"
f_in <- "./input\\station_input_example.txt"
delim <-  "\t" # "," 
#create folders for air temperature
try(dir.create("./input/AirT/"))
try(dir.create("./input/AirT/NOAA_pullR"))
air_out <- "./input/AirT/NOAA_pullR"
#create folder for surface water temperature
try(dir.create("./input/SWT/"))
sw_out <- "./input/SWT/"
#set working directory
setwd('..') #moves back one subfolder to get out of 'scripts' folder and into main folder
getwd()
# Read full SW location dataset
# Location data has to use column names Latitude and Longitude, or Northing and Easting if UTM

lat_lon_SW_df <- read.csv(f_in,
                          sep = delim, 
                          header=T,
                          stringsAsFactors=F,
                          colClasses=c("site_no"="character")
                          )# Makes sure ids that start with '0' are mantained,
 
## Needs to have SW station id column name as 'id', amy require editting if using different dataframe
try(colnames(lat_lon_SW_df)[colnames(lat_lon_SW_df)=="site_no"] <- "id")
    
if(!("id" %in% colnames(lat_lon_SW_df)))
{
  stop("Dataframe needs 'id' as column name")
}


#Pull data from NWIS, using the start date provided in the Annual Signals Output
#Prompts user for starting and end times of interest - need as a function because cant seem to get prompt to work during script run (not line by line)
print("For User Prompted dates (not 2010-01-01 to 2020-01-01) remove commentedout lines 50-53 in R file")
datefun <- function(){
  Start_Date <- readline(prompt="Enter Start Date (YYYY-MM-DD): ")
  End_Date <- readline(prompt="Enter End Date (YYYY-MM-DD): ")
  
  Start_Date <-as.Date(Start_Date)
  End_Date <- as.Date(End_Date)
  
  return(list(Start_Date, End_Date))
}

#Default Dates
Start_Date <- "2010-01-01"
End_Date <- "2020-01-01"

Start_Date <-as.Date(Start_Date)
End_Date <- as.Date(End_Date)

#Allow user input
date_se <- datefun()


# retrives temperature data for the timeframe requested 
#will only be nwis locations

for (n in 1:nrow(lat_lon_SW_df)){
  ID <- lat_lon_SW_df$id[n]
  if ((nchar(as.character(ID)) >= 8) & is.na(as.numeric(ID))== FALSE){ #only use usgs names (speeds up process!)

    # If the NOAA station is not already downloaded, download it. This is so i dont have to manipulate input files for speed
      station_df <- data.frame() # ensure clean dataframe is available
      try(
      station_df <- renameNWISColumns(readNWISdv(ID, "00010", #Temperature (C)
                                              startDate = date_se[1],
                                              endDate = date_se[2])))
    
# Create a new column in the SW location data table to indicate if there is data available for the time frame
# requested, if the whole list of SW is not run the remaining will be '0's as well. 
  if (nrow(station_df)> 0){
    lat_lon_SW_df$dataavailable[n] <-  1
    fn <- sprintf('%s.csv', ID)
    fp <- file.path(sw_out,fn)
    write.csv(station_df, file = fp)
  }
  else { #will this catch < 8 ID length (nonUSGS)
    lat_lon_SW_df$dataavailable[n] <-  0
  }
  }
}



#Get a list of all the NOAA stations currently available
#G_st <- ghcnd_stations() #Use first time, or if need of an update - takes a long time. 
load(".\\input\\NOAA_Stations_All_20190509.RData")

# Filter SW station input data to only have the SW stations that meet the time requirements
lat_lon_SW_df <- filter(lat_lon_SW_df, dataavailable == 1)

# Determine the nearest NOAA station to each of the SW station in the list filter for only SW stations that meet the timeframe requirements
# Here we look for the NOAA station within 25 mi of station, no data returned for SW stations
# without any within that range. Limit = 1 only provide 1 per station (the closet one)
nearby_stations <- meteo_nearby_stations(lat_lon_df = lat_lon_SW_df, 
                              lat_colname = "dec_lat_va",#latitude
                              lon_colname = "dec_long_va", #longitude
                              station_data = G_st, #either loaded or used ghcnd_stations()
                              year_min = 2011,# year plus 1 to capture full years
                              year_max = 2020,
                              var = c("TMAX", "TMIN", "TAVG"),
                              radius = 25, #km
                              limit = 2) # 1 or 2 depending on your objectives

# Convert USGS lat/long nomenclature to basic
try(
  colnames(lat_lon_SW_df)[colnames(lat_lon_SW_df) %in% c("dec_lat_va", "dec_long_va")] <- c("latitude", "longitude")
)
# Merge the nearby station data to one dataframe
match_data = do.call(rbind, nearby_stations) # make output of meteo nearby stations to a usable df

# Make a list of the unique NOAA station ids (remove duplicates)
noaa_pull <- unique(match_data$id)
# remove NA values (if some stations do not have lat long this will occur)
noaa_pull <- noaa_pull[!is.na(noaa_pull)]

#put in question to pull air data
# Pull the data for each of the NOAA stations identified as closest stations to the input SW stations
for (i in noaa_pull) {
  fn <- sprintf('%s.csv', i)
  fp <- file.path(air_out,fn)
  # If the NOAA station is not already downloaded, download it. This is so i dont have to manipulate input files for speed
  if(!file.exists(fp)){
  # If there is an error the script will continue to run
    tryCatch({
      # Pull the station temperature data from after 2010
        df <- meteo_pull_monitors(monitors = i, 
                                  date_min = date_se[1],
                                  date_max = date_se[2],
                                  var = c("TAVG", "TMIN", "TMAX"))
        rec <- colnames(df[3:length(df)]) #columns with temperture
        
        for (n in rec){
          df_temp <- df[n]/10 #convert to degrees C
          df[n] <- df_temp
        }
        
        write.csv(df, file = fp)
    }, 
    error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
  }
}

#change index/rownames to be a active column - locname is consistent for next step (2)
match_data$locname <- row.names(match_data)
#change original input siteno/id to locname for match (needed to be id for last package to work)
colnames(lat_lon_SW_df)[colnames(lat_lon_SW_df)=="id"] <- "locname"

#Change Column names to match input for step 2, as well as provide appropriate colnames for join with SW location data
colnames(match_data)[names(match_data) == "id"] <- "NOAA_ID"
colnames(match_data)[names(match_data) == "name"] <- "NOAA_NAME"
colnames(match_data)[names(match_data) == "latitude"] <- "NEAR_Y"
colnames(match_data)[names(match_data) == "longitude"] <- "NEAR_X"
colnames(match_data)[names(match_data) == "distance"] <- "NEAR_DIST"

#remove geometry columns if present
lat_lon_SW_df <- select(lat_lon_SW_df,locname, latitude, longitude, agency_cd)

#As we have two air station ids, we want to do a "right join" and have a column that removes the (.1/.2) from the id
match_data <- match_data %>% 
  separate(locname, c("locname", "NOAA_NUM"), 
           sep = '[.]',
           remove = FALSE) #seperates by '.'

# Merge the SW and Air Station data together (in the same format as the ArcGIS python script)
station_loc <- right_join(lat_lon_SW_df,match_data)

# drop SW stations with no NOAA stations data
station_loc <- station_loc[complete.cases(station_loc[ , 4]),]

#Fill NOAA_NUM with 1 if NA
station_loc$NOAA_NUM[is.na(station_loc$NOAA_NUM)] <- 1

#Export this merge data set for next step (step 2 in python)
write.csv(station_loc, file = "./output/Station_ID_NOAA_Match_.txt")
