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
import pandas as pd
import os
import joblib

class parse:
    def __init__(self):
        self.data = pd.DataFrame()

    def fetch(self, cellData):
        #cellData is list of dictionories
        df = pd.DataFrame(cellData)
        self.data = self.data.append(df[['CellID','MeasTimestampPDCPBytes','PDCPBytesDL','PDCPBytesUL']])

def forecast(data, model):
    """
     forecast the time series using the saved model.
    """
    time = data.MeasTimestampRF.values[-1]
    data = data[['PDCPBytesDL','PDCPBytesUL']]
    if os.path.isfile(os.getcwd() + '/' + model):
        model = joblib.load(model)
        p = model.forecast(y=data.values, steps=1)
        p = [int(pred) for pred in p[0]]
        return p 

