# ==================================================================================
#  Copyright (c) 2020 HCL Technologies Limited.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# ==================================================================================

import os
from statsmodels.tsa.api import VAR
from statsmodels.tsa.stattools import adfuller
from collections import Counter
import pandas as pd
import numpy as np
import joblib
import fnmatch

count = 0
cols = ['PDCPBytesUL', 'PDCPBytesDL']

def adfuller_test(series, signif=0.05, name='', verbose=False):
    """Perform ADFuller to test for Stationarity of given series and return True or False"""
    r = adfuller(series, autolag='AIC')
    output = {'test_statistic':round(r[0], 4), 'pvalue':round(r[1], 4), 'n_lags':round(r[2], 4), 'n_obs':r[3]}
    p_value = output['pvalue']
    if p_value <= signif:
        return True
    else:
        return False

def invert_transformation(df_train, df_forecast): 
    """If data is stationary, return the train data. Otherwise, revert the differencing
        to get the forecast to original scale.""" 
    df_fc = df_forecast.copy() 
    columns = df_train.columns 
    if count > 0 :  # For 1st differencing
        df_fc[str(col)+'_f'] = df_train[col].iloc[0] + df_fc[str(col)+'_f'].cumsum()
    return df_fc    


def make_stationary(df_stat):
    """ call adfuller_test() to check for stationary
        If the column is stationary, perform 1st differencing and return data"""
    res_adf=[] # boolean value to store the result of ADF
    for name, column in df_stat.iteritems():
        res_adf.append(adfuller_test(column, name=column.name)) # Perform ADF test
    if False in res_adf: # If not stationary, make the first differencing of the Non-Stationary column
        df_stat = make_stationary(df_stat.diff().dropna(inplace=True))
        count = count + 1
    return df_stat

def process(data, cid):
    """ Call make_stationary() to check for Stationarity and make the Time Series Stationary
        Make a VAR model, call the fit method with the desired lag order.
        Forecast VAR model and return the forcasted data.
    """    
        
    nobs = 1
    df = data.copy()    
    df_differenced = make_stationary(df) # check for Stationarity and make the Time Series Stationary
    
    model = VAR(df_differenced)    # Make a VAR model
    model_fit = model.fit(10)   # call fit method with lag order
    model_fit.summary()         # summary result of the model fitted
    lag_order = model_fit.k_ar  # Get the lag order
    forecast_input = df_differenced.values[-lag_order:] # Input data for forecasting

    # Forecast and Invert the transformation to get the real forecast values
    fc = model_fit.forecast(y=forecast_input, steps=nobs)

    inp_file = os.getcwd() + cid
    joblib.dump(model_fit, inp_file)


def train():
    """Read all the csv mentioned in the below path
     call process() to forecast the qp data
    """
    files = os.listdir(os.getcwd())
    for filename in files:
        if fnmatch.fnmatch(filename, '555*.csv'):
           cid = filename.split('.')[0]
           df = pd.read_csv(filename)
           time = df['MeasTimestampRF']
           df = df[cols]
           process(df, cid)


