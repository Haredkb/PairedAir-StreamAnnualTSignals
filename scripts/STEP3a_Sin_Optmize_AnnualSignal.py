# -*- coding: utf-8 -*-
"""
Created on Fri Dec 21 14:48:02 2018

@author: hared
"""
## Run after the STEP2 - the "output" variable is required
##Note: This set will drop data <2010 due to do air temperature records only having >2010 records

import datetime as dt # converting datetime conventions
import pandas as pd
import numpy as np
from scipy import optimize
import matplotlib.pyplot as plt
import os.path
import csv
import pickle


# File setup
##working directory 
wdir = os.path.dirname(os.getcwd())
os.chdir(wdir)
print os.getcwd()

# General Variable
units_day = 365
time_units = units_day  # **CHOOSE CORRECT UNITS
sin_output = dict()

# Location of output files
try: 
    os.mkdir(r'output\STEP3a_Output')
except: 
    print('folder already exists')
    
wksp_out = os.path.join(os.getcwd(),  r'output\STEP3a_Output')

#Set inital Variables
Count_SW= 0
Count_AR = 0
filename = "_sin_table.csv"

output_table_fn = os.path.join(wksp_out, filename)
### --------------------
#Sin function

def test_func(x, A, B, C):
    return (A * np.sin(x)) + (B * np.cos(x)) + C

### ----------------------- 

# Code 
num=0
start = dt.datetime.now()           
for i in output.keys():
    # Only Output files that have not already been processed (for speed)
#    filename = '{} Sin Data.csv'.format(i)
#    if os.path.isfile(os.path.join(wksp_completed, filename)) == False:
#        num+=1
#    else:
#        continue # back to top
        
    # Perform the analysis
    try:
        if i[:2] == "AR" :
            temp = output[i]['temp_raw'] 
            temp = pd.DataFrame(temp)
            temp.reset_index(inplace=True)
            temp['year'] = temp['DATE'].dt.year # pull out year and make column
            temp['J_day'] = map(int, temp['DATE'].dt.strftime('%j'))
            year1 = temp['year'][0]
            temp['input_date'] = ((temp['year'] - year1)*365) + temp['J_day']
            temp['TAVG'][temp['TAVG']>120] = np.nan # greater than 120 C with NaN, something wrong with data
            ## --- Drop negative air temp, which is be dropped dates in both SW and Air in Step3b, but that wont work sin is already created... adding drop function to sw < 1 C
            # tried to us cumsum numpy way, but the lengths end up being different - this function below is part of panadas        
            # drop times of year with weekly avg below 0C
            #temp = temp[temp['TAVG']> 0]
            
            # drop nan, was throwing errors
            temp = temp[np.isfinite(temp['TAVG'])]
            
            temp['degrees'] = (temp['J_day']/366)*360

            # Convert degrees to radians
            temp['radians'] = np.deg2rad(temp['degrees'])

            timex = 2*np.pi*(temp['input_date']/time_units)
            temp['timex']= timex

            
            print (dt.datetime.now() - start) 
            Count_AR += 1
            print Count_AR
            
        else: # should just be "SW"
            temp = output[i]['temp_raw']
            temp = pd.DataFrame(temp)
            temp.reset_index(inplace=True) # sometimes error with inplace = True, some have it from previous output variable
            temp['year'] = temp['DATE'].dt.year # pull out year and make column
            temp['J_day'] = map(int, temp['DATE'].dt.strftime('%j'))
            year1 = temp['year'][0]
            temp['input_date'] = ((temp['year'] - year1)*365) + temp['J_day'] 
            temp['TAVG'][temp['TAVG']>60] = np.nan# greater than 60 C with NaN, something wrong with data *duplicated in Step2?
            temp['TAVG'][temp['TAVG']<1] = np.nan# cuts out temperatures that are too low *should only be here as to prevent locations being dropped due to 30+ days missing due to this deletion
            
            # drop times of year with daily avg below 0.5C - do not want the sin fitting to the flat signal - also dampens the max response, so need to do this. 
            #temp = temp[temp['TAVG']> 0.5]
            
            # Drops all rows with nan TAVG values - was throwing errors.
            temp = temp[np.isfinite(temp['TAVG'])]
            

            
            timex = 2*np.pi*(temp['input_date']/time_units)
            temp['timex']= timex
             
            
            
            print dt.datetime.now() - start 
            Count_SW += 1
            print Count_SW
           
        data = temp['TAVG']
        res_max, cov_max = optimize.curve_fit(test_func, 
                                           timex, 
                                           data)             
        temp['T_sin'] = test_func(timex, *res_max)
        temp['residual'] = test_func(timex, *res_max) - data
        
        # bbuild dict small to large for < errors
        d = dict()
        d['df'] = temp
        d['sin_param']= res_max
        filename = '{} Sin Data.csv'.format(i)
        output_table_fn = os.path.join(wksp_out, filename)
        temp.to_csv(output_table_fn)
        filename = '{} Sin Parameters.csv'.format(i)
        output_table_fn = os.path.join(wksp_out, filename)
        np.savetxt(output_table_fn, res_max, delimiter=",")
        
        sin_output[i + "_"]= d
        
        filename = 'Sin Data for {}.png'.format(i)
        plt.figure()
        plt.plot(timex, data, 'ro')
        plt.plot(timex, test_func(timex, *res_max), 'b-')
        plt.savefig(os.path.join(wksp_out, filename))
        plt.close('all')
        
        del temp
        
    except Exception as e:
        print "Error in {}".format(i)
        print(e)

variable_out = wksp_out + r'\sin_step3a_output.pkl'
P_output = open(variable_out, 'wb')
pickle.dump(sin_output, P_output)
P_output.close()
   
#filename = "Summary_table_SinOptimize.csv"
#output_table_fn = os.path.join(wksp_out, filename)
#sin_output.to_csv(output_table_fn)
### --------------------






