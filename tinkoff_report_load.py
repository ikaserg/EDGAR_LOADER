import tinkoff
import edgar_load
import pandas as pd
import os
import datetime
import time

def load_all_tinkoff_reports(report_path, skip_path, token):
    tin = tinkoff.load_ticker_dataframe(token)
    sk = pd.read_csv(skip_path, header=None)
    for i, t in tin[(tin['type'] == 'Stock')&(tin['currency'] == 'USD')].iterrows():
        if (t['ticker'] not in sk[0].values):
            try:
                print(t['ticker'], end = ': ')
                last_time = datetime.datetime.now()
                download_ticker_reports(t['ticker'], report_path)        
                load_time = datetime.datetime.now() - last_time
                delay_step = 1
                print(load_time)
            except:            
                print(' sleep: ' + str(load_time.total_seconds() * delay_step))
                time.sleep(load_time.total_seconds() * delay_step)
                delay_step = delay_step + 2                