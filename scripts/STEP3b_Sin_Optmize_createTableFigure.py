# -*- coding: utf-8 -*-
"""
Created on Sat Dec 29 10:34:42 2018

@author: hared
"""
import os.path
import glob
from dbfread import DBF 
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt # for plotting results
import matplotlib.cm as cm
import datetime

# File setup

#set working directory
##working directory 
wdir = os.path.dirname(os.getcwd())
os.chdir(wdir)
print os.getcwd()


# Set up File name and paths

wksp_in = r'output\Step3a_Output'

# Location of output files
try: 
    os.mkdir(r'output\STEP3b_Output')
except: 
    print('folder already exists')
    
wksp_out = r'output\Step3b_Output'

loc_fn = r'final_run_output\Station_ID_NOAA_Match_.txt'


csv_bn = "*Sin Data.csv"
dbf_bn = ".dbf"
shp_bn = ".shp"
loc_fn_dbf = loc_fn + dbf_bn
loc_fn_shp = loc_fn + shp_bn


air_station_list = []
units_day = 365
time_units = units_day  # **CHOOSE CORRECT UNITS
# DBF only
#columns=list(['SW_ID', 'AR_ID', 'AmpRatio', 'PhaseLag', 'RMSE_SW', 'RMSE_AR','SDate', 'EDate','Maheu_TCat'])
# CSV only
columns=list(['SW_ID', 'AR_ID', 'AmpRatio', 'PhaseLag', 'RMSE_SW', 'RMSE_AR','SDate', 'EDate'])
columns_equ= list(['SW_ID', 'i', 'AR_A', 'AR_B', 'AR_C', 
                   'SW_A', 'SW_B', 'SW_C', 
                                       
                                       'AR_arctanBA','SW_arctanBA',
                                       'AR_phaseday','SW_phaseday',
                                       'AR - SW', 
                                       'AR_max_date', 'AR_max_radi',"AR_max_Jday",
                                       'SW_max_date', 'SW_max_radi',"SW_max_Jday",
                                       'Jday_Max_SW-AR'
                                       ])
#Create output dataframes
output_df = pd.DataFrame(columns= ['SW_ID', 'AR_ID', 'AmpRatio', 'PhaseLag', 'RMSE_SW', 'RMSE_AR','SDate', 'EDate'])
output_df_equ = pd.DataFrame(columns= ['SW_ID', 'i', 'AR_A', 'AR_B', 'AR_C', 
                   'SW_A', 'SW_B', 'SW_C', 
                                       'AR_arctanBA','SW_arctanBA',
                                       'AR_phaseday','SW_phaseday',
                                       'AR - SW', 
                                       'AR_max_date', 'AR_max_radi',"AR_max_Jday",
                                       'SW_max_date', 'SW_max_radi',"SW_max_Jday",
                                       'Jday_Max_SW-AR'
                                       ])


# List all of the available download Air 
csv_fn = os.path.join(wksp_in, csv_bn)  
air_file_list = glob.glob(csv_fn)

#if dbf
#loc_df = DBF(loc_fn_dbf, load=True)
# if csv
loc_df = pd.read_csv(loc_fn, dtype =  {'locname': str})

## Create "error" variables
SW_Station_NoGo_List = []
SW_Station_NoGo_Reason = []

# Extract all availble air csv station ids. 
for i in air_file_list:
    bn = os.path.basename(i)
    bn = bn[:-13] #remove .csv
    if bn[:2] == "AR":
        #print bn
        air_station_id = bn[3:] # remove AR_ 
        air_station_list.append(air_station_id)
    else:
        continue # next in for loop because the file is not Air
        
# Extract Air Station ID from original join
for i in air_station_list:
    #remove sw id from AR - named j
    sep = '.'
    AR_id = i.split(sep, 1)[0]
    SW_AR_id = i.split(sep, 1)[1]
    for r in range(len(loc_df)):
        #DBF
        #if loc_df.records[r]['NOAA_ID'] == i: # records for reading DBF, will get any multiple records
        if loc_df['NOAA_ID'][r] == AR_id:
            #print "** SW Station", loc_df.records[r]['locname']
            
            # --- Get Location of SW Station to Run 
            try: #for dbf
                SW_station_id = str(loc_df.records[r]['locname'])
            except: # if csv loc input file
                SW_station_id = str(loc_df['locname'][r])
                
            try: #only for maheu (assumes it is dbf)
                SW_TCat = str(loc_df.records[r]['T_Cat']) 
                SW_TCat = str(loc_df.records[r]['T_Cat'])
            except: # if not maheu
                pass
            
            # Only use the Ar temp file that matches SW
            if SW_AR_id != SW_station_id:
                continue #return to next iteration
                
            #print SW_station_id
            bsn = "SW_{} Sin Data".format(SW_station_id) + '.csv'
            fn = os.path.join(wksp_in, bsn)
            
# If there is a file with filename perform the analysis
            if os.path.isfile(fn):
                #print "{} station id file is available".format(SW_station_id)
                AR_bsn = "AR_{} Sin Data.csv".format(i)
                AR_p_bsn = "AR_{} Sin Parameters.csv".format(i)
                SW_p_bsn = "SW_{} Sin Parameters.csv".format(SW_station_id)
                
                ## ---- Read in DATA 
                try:
                    # --- Read csv data file
                    SW_df = pd.read_csv(fn, parse_dates=['DATE']) # SW raw data
                    AR_df = pd.read_csv(os.path.join(wksp_in, AR_bsn), parse_dates=['DATE'])
                    
                    ## --- Read csv parameter file
                    SW_sinT_para = pd.read_csv(os.path.join(wksp_in, SW_p_bsn),header=None) 
                    AR_sinT_para = pd.read_csv(os.path.join(wksp_in, AR_p_bsn),header=None)
                    
                    ## --- Merge AR and SW together for QC step
                    merge_df = pd.merge(SW_df, AR_df, how = 'inner', on = 'DATE')
                    # was originally 'outer' to allow for differences in values; however, with dropping Air values that are negative, 
                    #I want to see those same date drop in SW data, I realize this will cause more drops than I would like
                    merge_para = pd.merge(SW_sinT_para, AR_sinT_para, how = 'outer' )
                    
                    test = merge_df[np.isfinite(merge_df['TAVG_y'])] #after mergeing remove rows without Air match
                    
                    if test.shape[0] > (0.5*364*2): #50% of two years of data with both Air and SW, this is different than the data requirement, as now winter data is dropped
 

                        ## ---- Output into files
                        # Data
                        full_name_id = i + '.csv'
                        output_merge_fn = os.path.join(wksp_out, full_name_id)
                        merge_df.to_csv(output_merge_fn)
                        # Sin Parameters
                        full_name_id = 'sinPara'+ i + '.csv'
                        output_merge_fn = os.path.join(wksp_out, full_name_id)
                        merge_para.to_csv(output_merge_fn)
                        
                    else:
                        SW_Station_NoGo_List.append(SW_station_id)
                        SW_Station_NoGo_Reason.append("Not 75% of two years of SW and Air data, not run")
                        continue #return to for loop
                
                except:
                    print "input files are incorrect"
                
                ## Pull out Sin Temp Data
                SW_sinT = SW_df['T_sin']
                AR_sinT = AR_df['T_sin']
                
                ## Pull out Date Range
                sDate = min(SW_df['DATE'])
                eDate = max(SW_df['DATE'])
                
                ## Calculate RMSD
                SW_RSME_sinT = np.sqrt(np.mean(SW_df['residual']**2))
                AR_RSME_sinT = np.sqrt(np.mean(AR_df['residual']**2))
                
                #Convert Julian Day to radian for phase evaluation - not needed
                SW_df['degrees'] = (SW_df['J_day']/366)*360
                # Convert degrees to radians
                SW_df['radians'] = np.deg2rad(SW_df['degrees'])
                
                ##Calculate Date of Max Temperature SW
                max_ind = np.argmax(SW_df['T_sin']) #row value with max
                SW_max_date = SW_df['DATE'][max_ind]
                SW_max_radi = SW_df['radians'][max_ind]
                SW_max_jday = SW_df['J_day'][max_ind]
                

                
                #Convert Julian Day to radian for phase evaluation - not needed
                AR_df['degrees'] = (SW_df['J_day']/366)*360
                # Convert degrees to radians
                AR_df['radians'] = np.deg2rad(SW_df['degrees'])
                
                ##Calculate Date of Max Temperature AR
                max_ind = np.argmax(AR_df['T_sin']) #row value with max
                AR_max_date = AR_df['DATE'][max_ind]
                AR_max_radi = AR_df['radians'][max_ind]
                AR_max_jday = AR_df['J_day'][max_ind]
                

                
                
                ## Calculate Amp and Phase
                AR_Phase = (units_day/(2*np.pi))*np.arctan(AR_sinT_para[0][1] /AR_sinT_para[0][0]) #arctan(b/a)
                SW_Phase = (units_day/(2*np.pi))*np.arctan(SW_sinT_para[0][1] /SW_sinT_para[0][0]) #arctan(b/a)
                
                AR_Amp = np.sqrt(AR_sinT_para[0][0]**2 + AR_sinT_para[0][1]**2)
                SW_Amp = np.sqrt(SW_sinT_para[0][0]**2 + SW_sinT_para[0][1]**2)
                
                ## Calculate Phase Lag and Amplitude Ratio
                Amp_Ratio = SW_Amp / AR_Amp
                Ph_Lag = AR_Phase - SW_Phase #updated for #arctan(b/a)
                

                ## Output into Summary Table for Amp/Phase Ratio data
                try:
                    data = [SW_station_id, i, Amp_Ratio, Ph_Lag, SW_RSME_sinT, AR_RSME_sinT, sDate, eDate, SW_TCat]
                    equ_data = [SW_station_id, i, 
                                AR_sinT_para[0][0], AR_sinT_para[0][1], AR_sinT_para[0][2], 
                                SW_sinT_para[0][0], SW_sinT_para[0][1], SW_sinT_para[0][2],
                                np.arctan(AR_sinT_para[0][1] /AR_sinT_para[0][0]),
                                np.arctan(SW_sinT_para[0][1] /SW_sinT_para[0][0]),
                                (units_day/(2*np.pi))*np.arctan(AR_sinT_para[0][1] /AR_sinT_para[0][0]),
                                (units_day/(2*np.pi))*np.arctan(SW_sinT_para[0][1] /SW_sinT_para[0][0]),
                                AR_Phase - SW_Phase,
                                AR_max_date, AR_max_radi, AR_max_jday,
                                SW_max_date, SW_max_radi, SW_max_jday,
                                SW_max_jday - AR_max_jday]
                    
                except:
                    data = [SW_station_id, i, Amp_Ratio, Ph_Lag, SW_RSME_sinT, AR_RSME_sinT, sDate, eDate]
                    equ_data = [SW_station_id, i, 
                                AR_sinT_para[0][0], AR_sinT_para[0][1], AR_sinT_para[0][2], 
                                SW_sinT_para[0][0], SW_sinT_para[0][1], SW_sinT_para[0][2],
                                np.arctan(AR_sinT_para[0][1] /AR_sinT_para[0][0]),
                                np.arctan(SW_sinT_para[0][1] /SW_sinT_para[0][0]),
                                (units_day/(2*np.pi))*np.arctan(AR_sinT_para[0][1] /AR_sinT_para[0][0]),
                                (units_day/(2*np.pi))*np.arctan(SW_sinT_para[0][1] /SW_sinT_para[0][0]),
                                AR_Phase - SW_Phase, #due to the phase shift being negative
                                AR_max_date, AR_max_radi, AR_max_jday,
                                SW_max_date, SW_max_radi, SW_max_jday,
                                SW_max_jday - AR_max_jday
                                ]
                
                if eDate < datetime.datetime.strptime('2010-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'):
                    SW_Station_NoGo_List.append(SW_station_id)
                    SW_Station_NoGo_Reason.append("SW data end date is before 2010-01-01 00:00:00")
                    continue # return to next in for loop
                else: #keep going 
                    # create data output row
                    row = pd.DataFrame([data],columns= columns) # need list of columns or else will add to end (more columns)
                    output_df = output_df.append(row, ignore_index=True)
                    #create parameter output row
                    row = pd.DataFrame([equ_data],columns= columns_equ)
                    output_df_equ = output_df_equ.append(row, ignore_index=True)
 

### EXPORT ITEMS ###
# --- Export Summary Data Table
# Simplifed Table
output_df['SW_ID'] = output_df['SW_ID'].astype(str)
filename = "_Sin_Output_Summary_table.csv"
output_table_fn = os.path.join(wksp_out, filename)
output_df.to_csv(output_table_fn)

# --- Export Summary Equation Data Table
#Table with Calculated Parameters
output_df_equ['SW_ID'] = output_df_equ['SW_ID'].astype(str)
filename = "_EquationData_Output_Summary_table.csv"
output_table_fn = os.path.join(wksp_out, filename)
output_df_equ.to_csv(output_table_fn)

# --- Merge with original station locations table
#make a column with air and temp merge to join on (because of the 2 noaa stations)
loc_df['Ar_SW'] = loc_df['NOAA_ID'] + "." + loc_df['locname']

#Table with all original input values
filename = "_Full_Output_Summary_table.csv"
output_table_fn = os.path.join(wksp_out, filename)
output_m = output_df
output_m['SW_ID'] = output_m['SW_ID']# remove SW for merge with original station locations data

output_f = pd.merge(output_m, loc_df, how='right', left_on='AR_ID', right_on='Ar_SW')
output_f = output_f.dropna(subset=['SW_ID'])
output_f.to_csv(output_table_fn)

## Data with Errors, want to rerun for new NOAA station
error_df = pd.DataFrame(SW_Station_NoGo_List, SW_Station_NoGo_Reason)
filename = "Error_Files_3b.csv"
output_table_fn = os.path.join(wksp_out, filename)
error_df.to_csv(output_table_fn)

# --- Generate Plots
try:           
    SW_plot = pd.DataFrame(dict(x=output_df['PhaseLag'], 
                                y=output_df['AmpRatio'], 
                                label=output_df['Maheu_TCat']))
    
    SW_plot = SW_plot.reset_index()
    groups = SW_plot.groupby('label')
    
    color_rain = cm.rainbow(np.linspace(0, 1, len(groups)))
    filename = "PhaseLvs.AmpR_MaheuTCat.jpeg"
    
    fig, ax = plt.subplots(1,1, figsize=(6, 3))
    ax.margins(0.05) # Optional, just adds 5% padding to the autoscaling
    r = 0 
    for name, group in groups:
        ax.plot(group.x, group.y, 
                marker='o', linestyle='', ms=5, label=name,
                color=color_rain[r])
        r+= 1
    # Put legend outside of plot box
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    # Save figure to output folder with specified filename
    plt.savefig(os.path.join(wksp_out, filename), bbox_inches='tight')


except:# Plot by SW ID
    SW_plot = pd.DataFrame(dict(x=output_df['PhaseLag'], 
                                y=output_df['AmpRatio'], 
                                label=output_df['SW_ID']))
    
    SW_plot = SW_plot.reset_index()
    groups = SW_plot.groupby('label')
    
    color_rain = cm.rainbow(np.linspace(0, 1, len(groups)))
    filename = "PhaseLvs.AmpR_SWStation.jpeg"
    
    fig, ax = plt.subplots(1,1, figsize=(6, 3))
    ax.margins(0.05) # Optional, just adds 5% padding to the autoscaling
    r = 0 
    for name, group in groups:
        ax.plot(group.x, group.y, 
                marker='o', linestyle='', ms=5, label=name,
                color=color_rain[r])
        r+= 1
    # Put legend outside of plot box
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    # Save figure to output folder with specified filename
    plt.savefig(os.path.join(wksp_out, filename), bbox_inches='tight')
    

