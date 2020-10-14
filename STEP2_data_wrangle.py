## Danielle Hare
## NRE 5585 Final Project


# Import Modules
import numpy as np # for calcualtions and working in numpy arrays
import datetime as dt # converting datetime conventions
import pandas as pd # for dataframe manipulation, import and export
from dbfread import DBF # May be to be downloaded, this allows for reading of DBF files without arcpy
# arcpy is used within the last export step; however, anlaysis can be completed without this summary shp file export
# this is why import arcpy is only specified at the end of the script.

import os
import sys
import os.path
import glob
import DKH_SignalProcessModules_REMOVEFFT #DKH module
import matplotlib.pyplot as plt # for plotting results
import matplotlib.cm as cm
import pickle # to output variable to file
import time # to use sleep function
from func_timeout import func_timeout, FunctionTimedOut

##working directory 
wdir = os.path.dirname(os.getcwd())
os.chdir(wdir)
print os.getcwd()

# Set up File name and paths
# Location of input air files
wksp_in_air = os.path.join(wdir, r'input\AirT\NOAA_pullR')
wksp_in_air = r'C:\Users\hared\Dropbox\UConn\Projects\300_Network_GW_Temp\900_FinalProjectFiles\Stream-Temperature-Annual-Signals\input\Air_T_data'

# Location of input SW files
wksp_in = os.path.join(wdir, r'input\SWT')
wksp_in = r'C:\Users\hared\Dropbox\UConn\Projects\300_Network_GW_Temp\900_FinalProjectFiles\Stream-Temperature-Annual-Signals\input\SW_T_data'

# Location of output files
try: 
    os.mkdir(r'final_run_output\STEP2_Output')
except: 
    print('folder already exists')
wksp_out = r'final_run_output\STEP2_Output'

# Filepath of the SW location data with NOAA match
loc_fn = r'final_run_output\Station_ID_NOAA_Match_.txt'

# Basename extensions
csv_bn = "*.csv" #wildcard
dbf_bn = ".dbf"
shp_bn = ".shp"

# List all of the available download Air
csv_fn = os.path.join(wksp_in_air, csv_bn)
air_file_list = glob.glob(csv_fn)

#CSV use:
loc_df = pd.read_csv(loc_fn, dtype =  {'locname': str})
loc_df["fn"] = "" # add a column for file cirectory to go

#create two loc files, one for the closest NOAA station and one for the second closest
# 2nd closest (2)
loc_df_2 = loc_df[loc_df.NOAA_NUM ==2]
# closest (1)
loc_df = loc_df[loc_df.NOAA_NUM ==1]
loc_df = loc_df.reset_index()

#Output locations for entire analysis
#final output data dictionary
    output = dict()
    Ar_Station_Run_List = [] # Made with the hopes of being able to start mid-analysis if script stall
    SW_station_list = []
    SW_station_list_avail = []
    Ar_Station_NoGo_List = []
    Ar_Station_NoGo_Reason = []
    SW_Station_NoGo_List = []
    SW_Station_NoGo_Reason = []
    
    # Review Avaiable Air Temperature Records (must be csv in AirT folder)
    for i in air_file_list:
        time.sleep(0.2)
        bn = os.path.basename(i)
        bn = bn[:-4] #remove .csv
        air_station_id = bn
        Ar_Station_Run_List.append(air_station_id)
    
    #air_station_id = 'USW00014704'#,'WBAN54767'
    #WBAN_id = "WBAN" + air_station_id[-5:] #USW to WBAN id value
    
    
        for r in range(len(loc_df)):# goes through entire loop to make sure duplicates are processed
    
    ## For CSV location file use the following code:
            if loc_df['NOAA_ID'][r] == air_station_id:
                # --- Get Location ID of SW Station to run
    
                #####CHANGE TO MEET YOUR REQUIREMENTS
                SW_station_id = str(loc_df['locname'][r])
    
                bsn = SW_station_id + '.csv' # add .00 when running Quash, need to fix export
                fn = os.path.join(wksp_in, bsn)
    
                #Allows search in larger directort with sub, will only use first found file
                for dirpath, dirnames, filenames in os.walk(wksp_in):
                    for filename in [f for f in filenames if f.startswith(SW_station_id)]:
    
                        fn = os.path.join(dirpath, filename)
                        #print fn
                        break #should only print one fn per station- even though there are multiples within the file structure
                # If there is a file with filename perform the analysis, kept this so "file not available" is still colected
                if os.path.isfile(fn):
                    #print "{} station id file is available".format(SW_station_id)
                    SW_station_list_avail.append(SW_station_id)
                    loc_df["fn"][r] = fn
                else:
                    SW_Station_NoGo_List.append(SW_station_id)
                    SW_Station_NoGo_Reason.append("file is not available")
                    continue #return to top
    
    ## For new effciient trial ran the first part seperately
    #loc_df = loc_df.dropna(subset=['fn']) # relocnamemove all stations without a filename (fn is na)
    loc_df = loc_df[loc_df.fn != ""] # drop all stations without available file (fn string is empty)
    loc_df =loc_df.reset_index() # so the columns can be called sequencely
    
    print("Now starting evaluate/clean SW data")

for r in range(len(loc_df)): #for debugging range(10)
    air_station_id = loc_df['NOAA_ID'][r]
    SW_station_id = loc_df['locname'][r]
    fn = loc_df['fn'][r]
    
    print ">>>>>" + SW_station_id #user can see what SW we are on. 

##Single Debug reun
#    air_station_id = 'USC00120922'
#    SW_station_id = 'WashingtonCoast_5050'
#    fn = r'C:\Users\hared\Dropbox\UConn\Projects\300_Network_GW_Temp\900_FinalProjectFiles\Stream-Temperature-Annual-Signals\input\SW_T_data\WashingtonCoast_5050.csv'
##

## ---- SURFACE WATER DATA
    try:
        time.sleep(0.2) # attempts to maintain a low consumption of CPU resources
        # --- Read csv data file
        try:
            #df0 = pd.read_csv(fn, na_values=(-9999),parse_dates=['datetime'])#-9999 maintains number category # SW raw data
            df0 = pd.read_csv(fn, na_values=(-9999),parse_dates=['Date'])
            df0 = df0.rename(index=str, columns={"Date": "datetime"})


        except FunctionTimedOut:
            print "SW_{} Timed out".format(SW_station_id)
            SW_Station_NoGo_List.append(SW_station_id)
            SW_Station_NoGo_Reason.append("Timed Out")
            continue

        except:
            try:
                df0 = pd.read_csv(fn, na_values=(-9999),parse_dates=['DATE'])
                df0 = df0.rename(index=str, columns={"DATE": "datetime"})
            except:
                df0 = pd.read_csv(fn, na_values=(-9999),parse_dates=['datetime'])
                df0 = df0.rename(index=str, columns={"datetime": "datetime"})



        df1 = df0 #so we preserve original SW input file as df0, can be ajusted later once functional

        # --- Reformat columns
        # rename loocation column
        try:
            df1['location_name'] = df1['location_name'].astype(str)
        except:
            try:
                df1.rename(columns={'location_id':'location_name'}, inplace=True)
                df1['location_name'] = df1['location_name'].astype(str)
            except:
                df1.rename(columns={'site_no':'location_name'}, inplace=True)
                df1['location_name'] = df1['location_name'].astype(str)

        # make sure there is a column named TAVG, only have correction for 'temperature (deg C)
        try:
            test = df1['TAVG'].dtype #test that df1['TAVG'] exists
        except:
            try :
                df1.rename(columns={'Wtemp':'TAVG'}, inplace=True) # rename column name to be easier to manage
            except:
                try :
                    df1.rename(columns={'temperature (degC)':'TAVG'}, inplace=True)
                except:
                #print "Temperature Column Name is not recongized, data from this station is not processed"
                    SW_Station_NoGo_Reason.append("Temperature Column Name is not recongized")
                    SW_Station_NoGo_List.append(SW_station_id)
                    continue

        df1['DATE'] = df1['datetime'].dt.normalize() # normalize keeps datetime64 (with seconds) #.dt.date #creates new column with only YMD

    ##--- Calculate TAVG if not found ---- #
        # Sum all the na in TAVG (after normalized), if greater than 80% try to use the average of TMAX and TMIN, if not then dont bother
        NA_SUM_SW= df1['TAVG'].isna().sum()
        
        if NA_SUM_SW/float(len(df1['TAVG'])) > 0.8:
            #If TMAX exists as a column name in df1 (the following is intensive so should only be done if helpful)
            if 'TMAX' in df1.columns:
            # If TAVG Nan fill in with average of TMAX and TMIN
                for i in range(len(df1)):
                    if np.isnan(df1['TAVG'].iloc[i]):
                        try:
                            df_avg = (df1['TMAX'].iloc[i]+ df1['TMIN'].iloc[i])/2
                            df1['TAVG'].iloc[i] = df_avg
                        except:
                            df1['TAVG'].iloc[i] = np.nan  # could I use continue instead?

        #print "NAN fill"
        # if TAVG has too many NA break current loop, used to stop error in stime, min not found
        NA_SUM_SW= df1['TAVG'].isna().sum()
        
        if NA_SUM_SW/float(len(df1['TAVG'])) > 0.8:
            #print "Too many missing values in Surface Water Temperature"
            SW_Station_NoGo_List.append(SW_station_id)
            SW_Station_NoGo_Reason.append("Too many missing values in Surface Water Temperature")
            continue

        ##--Calculate Daily Temp if subdaily
        df_daily = df1.dropna(axis=0, subset=['TAVG']).groupby('DATE').mean() # avg temp by day (will work for any daily or subdaily interval)
        df_daily['DATE'] = df_daily.index #keep date was index, but also have a column with date values to work with
        
        ##-- Remove Erroneous Data points 
        df_daily['TAVG'][df_daily['TAVG'] > 60] = np.nan # greater than 60 C with NaN, something wrong with data
        
        ## Calculate running average
        # tried to us cumsum numpy way, but the lengths end up being different - this function below is part of panadas        
#        temp_rAvg = df_daily['TAVG'].rolling(7).mean()
#        
#        # needs to have date index to work in FFT function - do I need this for non running mean?
#        df_daily['TAVG'] = temp_rAvg

        # --- Determine full data set start date and end date
        # -- Inputs for FFT Module
        stime = str(min(df_daily.index)) # should be min, but messy early data, need to determine how to pick "cleanest data"
        etime = str(max(df_daily.index))
        temp_SW = df_daily['TAVG']


    # --- Perform FFT through created Module
        try:
            output['SW_' + SW_station_id]= func_timeout(60, DKH_SignalProcessModules_REMOVEFFT.swT_fft, args = (temp_SW,stime,etime))
        except FunctionTimedOut:
            print "SW_{} Timed out".format(SW_station_id)
            SW_Station_NoGo_List.append(SW_station_id)
            SW_Station_NoGo_Reason.append("Timed Out")
            continue
        except:
            print "SW_{} does not have sufficient consectutive years to run analysis".format(SW_station_id)
            SW_Station_NoGo_List.append(SW_station_id)
            SW_Station_NoGo_Reason.append("Not sufficient consectutive years")
            continue
        #print "continued script"

        # create list from dictonary output for ease of working with
        temp_SW = output['SW_' + SW_station_id]['temp_raw'].set_index('DATE')
        temp_SW_raw = temp_SW['TAVG']
        #temp_SW_filt = temp_SW['filt']

    except:
        print "Error with Surface Water Data"
        SW_Station_NoGo_List.append(SW_station_id)
        SW_Station_NoGo_Reason.append("Error with SW data")
        continue

## ---- AIR TEMPERATURE DATA
    air_try = 1 # how many noaa stations tried
    y = "none"
    
    while True: #this loop allows for multiple air stations to be run (in this case just 2)
        x = (air_station_id + "." +SW_station_id)
        print (air_station_id + "." +SW_station_id)

        if x == y:# This should stop run away values
            break
        
        try:
        # --- Read Air Data from csv [NOAA data]
        # start and end time based on the filted SW data
            y = (air_station_id + "." +SW_station_id)
            stime = min(temp_SW.index)
            etime = max(temp_SW.index)
            try:
                # because there is multiple sw per air temperature stations, need a uniqur name for each
                AR_output_nm = "AR_" + air_station_id + "." +SW_station_id
                
                output[AR_output_nm] = func_timeout(60, DKH_SignalProcessModules_REMOVEFFT.airT_fft, args = (wksp_in_air, air_station_id, stime, etime))
                
                temp_AR = output[AR_output_nm]['temp_raw']
                #temp_AR_filt = output[AR_output_nm]['temp_filt_fft']
                
            except FunctionTimedOut:
                try:
                    row_df2 = loc_df_2["NOAA_ID"][loc_df_2['locname' ] == SW_station_id]
    
                    air_station_id = row_df2.iloc[0] #new 2nd closest air station id
                    air_try += 1
                    continue # go back to top of while loop
                except:
                    Ar_Station_NoGo_List.append(air_station_id)
                    Ar_Station_NoGo_Reason.append("TimedOut")
                    air_try += 1
                    break #break whlie
            
            except:
                try:
                    row_df2 = loc_df_2["NOAA_ID"][loc_df_2[ 'locname' ] == SW_station_id]
                    air_station_id = row_df2.iloc[0] #new 2nd closest air station id
                    air_try += 1
                    continue # go back to top of while loop
                except:
                    Ar_Station_NoGo_List.append(air_station_id)
                    Ar_Station_NoGo_Reason.append("FFT Error")
                    air_try += 1
                    break #break while
                    
        ## Next Step in Air Temp
        except:
            try:
                row_df2 = loc_df_2["NOAA_ID"][loc_df_2[ 'locname' ] == SW_station_id]
    
                air_station_id = row_df2.iloc[0] #new 2nd closest air station id
                air_try += 1
                continue # go back to top of while loop
            except:
                Ar_Station_NoGo_Reason.append("Error with Air Temperature Data")
                Ar_Station_NoGo_List.append(air_station_id)
                air_try += 1
                break #break while loop
    
#---- Amplitude Ratio Calculation
        try:
            # make date a dataframe
            date = pd.DataFrame({'date':output["SW_" + SW_station_id]['date'].values})
            # have a column of year values
            date['year']=date['date'].dt.year
            date['year_int'] = map(int, date['year'])
        
            # make dataframe of normalized temp values filtered # the outputs are different, which could be a problem in the future
            temp_raw_AR =  pd.DataFrame({'date':output[AR_output_nm]['temp_raw'].index, 'temp_raw':output[AR_output_nm]['temp_raw'].values})
           # merge the temp_norm and date with column as year into a single
            # Date set was SW range so SW first df
        
            # Merge air raw
            df2 = pd.merge(temp_SW, #leftframe
                           temp_raw_AR,
                           left_on = [temp_SW.index], # date
                           right_on = 'date',
                           how = 'left')
        
            df2.rename(columns={'temp_raw':'temp_raw_AR'},
                       inplace=True)
            df2.rename(columns={'TAVG':'temp_raw_SW' },
                   inplace=True)
        
           ##-------- Make sure only AR temperature that matches 75% of the SW data.
           ##-------- If not try the 2nd closest NOAA station
        
           #Make df with only rows that have both air and SW
            test = df2[np.isfinite(df2['temp_raw_AR'])] #after mergeing remove rows without Air match
    
            if (test.shape[0] < (0.75*df2.shape[0])) & (air_try <2): #(0.75*df2.shape[0]) : # There is less than 50% of the merged data with both Air and SW
                print "Not enough matching Air Temperature data - try 2nd"
            # in loc_df_2 find SW location ID and use Air Temperature ID
                try:
                    row_df2 = loc_df_2["NOAA_ID"][loc_df_2[ 'locname' ] == SW_station_id]
                    air_station_id = row_df2.iloc[0] #new 2nd closest air station id
                    air_try += 1
                    continue # go back to top of while loop
                except:
                    SW_Station_NoGo_List.append(SW_station_id)
                    SW_Station_NoGo_Reason.append("Air and SW temp do not overlap substanially")
                    break #break while loop
            if (air_try > 2):
                SW_Station_NoGo_List.append(SW_station_id)
                SW_Station_NoGo_Reason.append("Air and SW temp do not overlap substanially")
                break #break while loop
            print "year set"
            
#            ## ---- Remove times of year with avg weekly air temperature in the negatives ------ ## 
#            # tried to us cumsum numpy way, but the lengths end up being different - this function below is part of panadas        
#            df2['AirRAvg'] = df2['temp_raw_AR'].rolling(7).mean()
#            # drop times of year with weekly avg below 0C
#            df2 = df2[df2['AirRAvg']> 0]
            
            # determine years in the dataset
            year_set = map(int, set(df2['year']))
            year_summary = []
            for year in year_set:
                is_year = df2['year'] == year
                df_ = df2[is_year]
                df = pd.DataFrame([year])
                year_summary.append(df)
                del df
                
            #print "year set completed"
            #merge the multiple dataframes together for amp and phase lag
            #summary of each year of data available
            output['SW_' + SW_station_id]['YearlySummary'] = pd.concat(year_summary, axis=1)#.set_index('variable')
            output['SW_' + SW_station_id]['YearlySummary'].index = ["Year"]
            #Add SW station name to list - should only include SW stations that produced an output, rest should have been excluded in try/except statement
            SW_station_list.append(SW_station_id)
            break #end while loop
            
        except:
            SW_Station_NoGo_List.append(SW_station_id)
            SW_Station_NoGo_Reason.append("Error with Phase and Amp Calc")
            print "Error with Phase and Magnitude Calculation"
            break 

print "final step - summarizing data"
# ---- Final results table and Scatter Plot
#Format for plotting
Summary_Results = []

for ss in SW_station_list:
    summ = output['SW_' + ss]['YearlySummary'].transpose() #flip rows to columns index is header
    summ['SW_ID']= ss
    summ = summ.set_index('SW_ID')
    Summary_Results.append(summ)

Summary_Results = pd.concat(Summary_Results)

#---- Export Output Variable
variable_out = wksp_out + r'\STEP2_output.dmp'
P_output = open(variable_out, 'wb')
pickle.dump(output, P_output)
P_output.close()

# --- Export Summary Data Table
filename = "STEP2_Summary_table_test.csv"
output_table_fn = os.path.join(wksp_out, filename)
Summary_Results.to_csv(output_table_fn)

# ---- Export Results to Figure in arcmap

# Add Modules
#import arcpy

Summary_Results_Mean = Summary_Results.groupby(Summary_Results.index).mean()

