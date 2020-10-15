# -*- coding: utf-8 -*-
"""
Created on Tue Nov  6 16:27:01 2018

@author: dkh17003work
"""
def airT_fft(wksp_in, station_id, stime, etime):
    import datetime as dt
    import numpy as np
    import scipy as sp
    import scipy.fftpack
    import pandas as pd
    import matplotlib.pyplot as plt
    import os.path
    import math
    import sys
    
    # single runs
    #station_id = "WQBCHMET" #air_station_id
    #wksp_in = wksp_in_air

    bn = station_id + '.csv'
    fn = os.path.join(wksp_in, bn) 
    
    try: 
        df0 = pd.read_csv(fn, na_values=(-9999),parse_dates=['DATE'])
    
    except: #Some NOAA exports are lower case
        df0 = pd.read_csv(fn, na_values=(-9999),parse_dates=['date'])
        df0.rename(columns={'date':"DATE",
                            'tmin':'TMIN',
                            'tmax':'TMAX'},inplace=True)
        try: # some of the stations do not have tavg column which would throw error if included above
            df0.rename(columns={'tavg':'TAVG'}, inplace=True)
        except:
            pass            
    # Cut Air Temperature data to SW temperature date range             
    df = df0[df0['DATE'] >= stime]
    df = df[df['DATE'] <= etime]  
    df['DATE_YMD'] = df['DATE'].dt.normalize() # normalize keeps datetime64 (with seconds) #.dt.date #creates new column with only YMD
    df['DATE'] = df['DATE_YMD'] #Going straight to date was not working, see if this approach works
    
    # Group data by dates, determine if AVG is availble
    try:
        df_avg = df.dropna(axis=0, subset=['TAVG']).groupby('DATE').mean()
    
    except:
        try: 
            df_avg = df.dropna(axis=0, subset=['DAILYAverageDryBulbTemp']).groupby('DATE').mean()
        except:
            try:
                df_avg = (df['TMAX']+ df['TMIN'])/2
                df['TAVG'] = df_avg
                #df['TAVG'] = pd.to_numeric(df["TAVG"])
                df_avg = df.dropna(axis=0, subset=['TAVG']).groupby('DATE').mean()
            except:
                print "Air Temperature does not have daily average value"
    
    # Make sure there is more than X% of SW time series covered by Air Temperature or exit analysis for this SW
    NA_SUM_Air = df['TAVG'].isna().sum() # sum NAs in TAVG
    if NA_SUM_Air/float(len(df['TAVG'])) > 0.5:
        print "Too many missing values in Air Temperature"
        exit # exit module
        
    # make a list of dates     
    date = df_avg.index
    
    # Convert F to C if needed - no Celsius data set should have an avergae > 32 C
    if df_avg['TAVG'].mean()>32: 
        temp = ((df_avg['TAVG'])-32) * 0.5556
    else:
        temp = df_avg['TAVG']
        
    # Number of temp records was for FFT, but may have been dropped.     
    N = len(temp)
    
    ## ----- compute the Fourier transform and the spectral density of the signal.
    temp_fft = sp.fftpack.fft(temp)
    temp_psd = np.abs(temp_fft) ** 2
    fftfreq = sp.fftpack.fftfreq(len(temp_psd), 1.0 / 365)
    # only real numbers (remove neg frequencies)
    i = fftfreq > 0
    
    temp_fft_bis = temp_fft.copy()
    temp_fft_bis[np.abs(fftfreq) > 1.5] = 0
    
    #Extract Mag (Amp) and Angle (Phase)
    #trial - if amp is >1.1 or phase is <-30 for any year drop that year, the -30 is decided based on all data, we see an issue with dropping years due to the FFT fitting issues on the first and last years (edge effects)
    magnitude = np.abs(temp_fft_bis)
    phase = np.angle(temp_fft_bis)

    #Convert back to time series
    temp_filt = np.real(sp.fftpack.ifft(temp_fft_bis))

    #adds date index to the filtered data
    df_avg['filt'] = temp_filt
    temp_filt = df_avg['filt']
    
    temp_mean = np.mean(temp_filt)
    temp_filt_norm = temp_filt - temp_mean

    # Create Export dictonary
    d = dict()
    d['temp_raw'] = temp
    d['temp_filt_fft'] = temp_filt
    #d['temp_filt_sin'] = temp_sin_fit
    d['temp_norm'] = temp_filt_norm
    d['date'] = date
    d['amp'] = magnitude
    d['phase']= phase

    return d

###############################################################################
# Surface water temperature extraction by FFT Module
## --- Extract FFT for SW signal
def swT_fft(temp, StartTime, EndTime):
#    for single runs/ debugging
#    temp = temp_SW
#    StartTime = stime
#    EndTime = etime
    import datetime
    import numpy as np
    import scipy as sp
    import scipy.fftpack
    import pandas as pd
    import matplotlib.pyplot as plt
    import os.path
    import math
    import sys

    stime = datetime.datetime.strptime(StartTime, '%Y-%m-%d %H:%M:%S')
    etime = datetime.datetime.strptime(EndTime, '%Y-%m-%d %H:%M:%S')
    idx = pd.date_range(stime, etime)
    
    temp = temp.loc[stime : etime] #replace temperature variable with only defined range
    date_ori = temp.index
    
    
    # fill in missing daily dates and fill with day prior
    temp = temp.reindex(idx) # fill values will be NaN #fill_value=-9999)
    date = temp.index
    
    # create a column of year values for indivdual review
    temp = pd.DataFrame(temp)
    temp.reset_index(inplace=True) # make date a column again
    temp.columns = ['DATE', 'TAVG']
    temp['DATE'] = pd.to_datetime(temp['DATE'], errors='coerce')
    temp['year'] = temp['DATE'].dt.year
    
    #convert F to C if necessary
    if temp['TAVG'].mean()>32: #This does assume that the average F will always be greater than 32F and the average C will always be less than 32C
        temp['TAVG'] = ((temp['TAVG'])-32) * 0.5556
    else:
        pass
    #temp.set_index('DATE', drop=True, inplace=True)
        
    # List the unique years        
    year_set = map(int, set(temp['year']))

# Clean Data Set to only include contigous years    
    clean_temp= pd.DataFrame()# temp.drop(temp.index[:len(temp)])
    
    for year in year_set:
        isyear = temp['year'] == year # Creates true false lst
        df_ = temp[isyear] # only pulls out rows with true statemnet from isyear
        
        # Find Consective Missing data points
        longest = 0 
        current = 0 
        for row in range(len(df_)):
            if np.isnan(float(df_['TAVG'].iloc[row])):
                current += 1
            else:
                longest = max(longest, current)
                current = 0
        # If there is a data chuck missing that is greater than 21 days then dont use that year
        if longest > 30:
            print "{} is missing greater than 30 day segment".format(year)# try next year in sequence
        else:        
            # Find if total number of data points are missing  from total year      
            missingval = df_.isnull().sum()
            # calculate number of dp in the year
            dpPercent = (len(df_) - missingval) / 365
            # If total number of data points missing is greater than xpercent days remove from data analysis
            if dpPercent['TAVG'] < 0.75:
                print "{} is missing more than 25% of the days of the year".format(year)
            else:
                clean_temp = clean_temp.append(df_)

    temp = clean_temp.sort_index()
    # Sort by date, can't sort a set...
    year_set = map(int, set(temp['year']))
    year_set.sort(reverse=True)
    print year_set
    
    # Find longest data set and use for FFT
    consective_year = []
    for r in range(len(year_set)):
        try: 
            if year_set[r+1] == (year_set[r] - 1):
                try: 
                    if year_set[r+2] == (year_set [r] - 2):
                        try:
                    # make list of data years with three or four years
                            if year_set[r+3] == (year_set [r] - 3):
                                yearlist = [year_set[r], year_set[r+1], year_set[r+2], year_set[r+3]]
                                consective_year.append(yearlist)
                            else:
                                yearlist = [year_set[r], year_set[r+1], year_set[r+2]]
                                consective_year.append(yearlist)
                        except:
                            continue
                    else:
                        yearlist = [year_set[r], year_set[r+1]]
                        consective_year.append(yearlist)
                except:
                    yearlist = [year_set[r], year_set[r+1]]
                    consective_year.append(yearlist)
                    continue
        except: 
             continue
         
    # Use most recent & longest consectative year
    consective_year = sorted(consective_year, key=len, reverse = True)
#    print consective_year[0]
    cons_temp= pd.DataFrame()
    
    for year in consective_year[0]:
        isyear = temp['year'] == year # Creates true false lst
        df_ = temp[isyear]
        cons_temp = cons_temp.append(df_)
    temp = cons_temp.sort_index()
    
# --- Fill in Data Gaps
    temp = temp.fillna(method='ffill') # fill data gaps with previous value
    tavg = temp['TAVG']
    # compute the Fourier transform and the spectral density of the signal
    temp_fft = sp.fftpack.fft(tavg)
    temp_psd = np.abs(temp_fft) ** 2
    fftfreq = sp.fftpack.fftfreq(len(temp_psd), 1.0 / 365)
    # only real numbers (remove neg frequencies)
    i = fftfreq > 0
    
    temp_fft_bis = temp_fft.copy()
    temp_fft_bis[np.abs(fftfreq) > 1.1] = 0
    
    #Extract Mag (Amp) and Angle (Phase)
    magnitude = np.abs(temp_fft_bis)
    phase = np.angle(temp_fft_bis)
    #Convert back to time series
    temp_filt = np.real(sp.fftpack.ifft(temp_fft_bis))
    temp['filt']= temp_filt
    
    # Normalize to mean and plot
    temp_mean = np.mean(temp_filt)
    temp_filt_norm = temp_filt - temp_mean
    #plt.plot(date, temp_filt_norm)
    
### -------  sin fit
    
#    from scipy import optimize
#    import numpy as np
#    
#    #units_min = 525600 
#    #units_hr = 8760
#    units_day = 365
#    time_units = units_day  # **CHOOSE CORRECT UNITS
#    timex_air = 2*np.pi*temp['DATE']/time_units 
#
#    def test_func(x, A, B, C, D):
#         return (A * np.sin(B*x) + C) + D
#
#    res_avg, cov_avg = optimize.curve_fit(test_func, 
#                                               timex_air,
#                                               temp) 
#                                               
#    temp_sin_fit = test_func(timex_air, *res_avg)
    
    d = dict()
    d['temp_raw']=temp
    d['temp_filt_fft']=temp['filt']
    #d['temp_filt_sin'] = temp_sin_fit
    d['temp_norm'] = temp_filt_norm
    d['date'] = date
    d['amp'] = magnitude
    d['phase']= phase

    return d
