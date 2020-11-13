# ==================================================================================
#       Copyright (c) 2020 AT&T Intellectual Property.
#       Copyright (c) 2020 HCL Technologies Limited.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# ==================================================================================
"""
mock qp module

RMR Messages:
 #define TS_QOE_PRED_REQ 30001
 #define TS_QOE_PREDICTION 30002
30001 is the message type QP receives from the driver;
sends out type 30002 which should be routed to TS.

"""

import random
import os
from mdclogpy import Logger
from ricxappframe.xapp_frame import RMRXapp, rmr
import json
from qp.prediction import parse, forecast 
from qp.qptrain import train
import pandas as pd 

# pylint: disable=invalid-name
qp_xapp = None
logger = Logger(name=__name__)
buf = pd.DataFrame()

def post_init(self):
    """
    Function that runs when xapp initialization is complete
    """
    self.predict_requests = 0
    logger.debug("QP xApp started")


def qp_default_handler(self, summary, sbuf):
    """
    Function that processes messages for which no handler is defined
    """
    logger.debug("default handler received message type {}".format(summary[rmr.RMR_MS_MSG_TYPE]))
    # we don't use rts here; free this
    self.rmr_free(sbuf)


def qp_predict_handler(self, summary, sbuf):
    """
    Function that processes messages for type 30001
    """
    logger.debug("predict handler received message type {}".format(summary[rmr.RMR_MS_MSG_TYPE]))
    logger.debug("predict handler received message type {}".format(summary[rmr.RMR_MS_PAYLOAD]))
    pred_msg = predict(summary[rmr.RMR_MS_PAYLOAD])
    self.predict_requests += 1
    # we don't use rts here; free this
    self.rmr_free(sbuf)
    # send a mock message
    #mock_msg = '{ "12345" : { "310-680-200-555001" : [ 2000000 , 1200000 ], '\
    #           '              "310-680-200-555002" : [  800000 , 400000  ], '\
    #           '              "310-680-200-555003" : [  800000 , 400000  ] } }'
    #ueid = pd.read_csv('ue_id.csv')
    #ueid = df.sample()
    #pred_msg = predict(ue)
    success = self.rmr_send(pred_msg.encode(), 30002)
    if success:
        logger.debug("predict handler: sent message successfully")
    else:
        logger.warning("predict handler: failed to send message")


def predict(payload):
    """
     Function that forecast the time series
    """
    if not os.path.isfile('555011'):
        train()

    print("************Entering into predict() ******** ")
    data = json.loads(payload)
    cell_data = data['CellMeasurements']
    test = pd.read_csv('/tmp/qp/cell_test.csv')
    tp = {}

    #cid_list = [data['UEMeasurements']['ServingCellID']]
    cid_list = []
    
    for cell in cell_data:        
        
        cid_list.append(cell['CellID'])
    for cid in cid_list:
        id = random.choice(test.cellid.unique())        
        inp = test[test['cellid'] == int(id)]
        if len(inp) != 0:
            tp[cid] = forecast(inp, str(id))
    prediction  = {data["PredictionUE"] : tp}
    print("prediction :", prediction)
    return prediction

def start(thread=False):
    """
    This is a convenience function that allows this xapp to run in Docker
    for "real" (no thread, real SDL), but also easily modified for unit testing
    (e.g., use_fake_sdl). The defaults for this function are for the Dockerized xapp.
    """
    logger.debug("QP xApp starting")
    global qp_xapp
    
    fake_sdl = os.environ.get("USE_FAKE_SDL", None)
    qp_xapp = RMRXapp(qp_default_handler, rmr_port=4560, post_init=post_init, use_fake_sdl=bool(fake_sdl))
    qp_xapp.register_callback(qp_predict_handler, 30001)
    qp_xapp.run(thread)

def stop():
    """
    can only be called if thread=True when started
    TODO: could we register a signal handler for Docker SIGTERM that calls this?
    """
    global qp_xapp
    qp_xapp.stop()


def get_stats():
    """
    hacky for now, will evolve
    """
    global qp_xapp
    return {"PredictRequests": qp_xapp.predict_requests}

#predict()
start()
