import pandas as pd
from datetime import datetime,timedelta
import numpy as np
# from tqdm import tqdm
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=False)
# df = pd.read_csv("delivery_orders_march.csv",nrows=1000)
df = pd.read_csv("delivery_orders_march.csv")
def utc_to_dt(utc):
    dt = datetime.utcfromtimestamp(utc)+timedelta(hours=8)
    # dt = datetime.utcfromtimestamp(utc)
    return datetime(year=dt.year,month=dt.month,day=dt.day)
def find(x):
    sla_name = ['metro manila','luzon','visayas','mindanao']
    sla_matrix = {'metro manila':{'metro manila':3,'luzon':5,'visayas':7,'mindanao':7},
                 'luzon':{'metro manila':5,'luzon':5,'visayas':7,'mindanao':7},
                 'visayas':{'metro manila':7,'luzon':7,'visayas':7,'mindanao':7},
                 'mindanao':{'metro manila':7,'luzon':7,'visayas':7,'mindanao':7},
                 }
    public_holiday=[datetime(2020,3,8),datetime(2020,3,15),datetime(2020,3,22),datetime(2020,3,25),datetime(2020,3,29),datetime(2020,3,30),datetime(2020,3,31),datetime(2020,4,5)]
    seller_add=""
    buyer_add=""
    selleraddress = x.selleraddress.lower()
    buyeraddress = x.buyeraddress.lower()
    for i in range(4):
        if selleraddress.find(sla_name[i])!=-1:
            seller_add = sla_name[i]
    for i in range(4):
        if buyeraddress.find(sla_name[i])!=-1:
            buyer_add = sla_name[i]
#     print(x.selleraddress,x.buyeraddress)
    sla_duration = sla_matrix[seller_add][buyer_add]
    pickup = utc_to_dt(x.pick)
    first = pickup + timedelta(days=sla_duration)
    # extra = 0
    for i in public_holiday:
        if pickup<=i<=first:
            first+=timedelta(days=1)
            # extra+=1
    # first = first + timedelta(days=extra)
    actual_first = utc_to_dt(x['1st_deliver_attempt'])
    if actual_first>first:
        return 1
    if pd.isnull(x['2nd_deliver_attempt']):
        # print(x)
        return 0
    second = actual_first + timedelta(days=3)
    # extra = 0
    for i in public_holiday:
        if actual_first<=i<=second:
            second+=timedelta(days=1)
            # extra+=1
    # second = second + timedelta(days=extra)
    actual_second = utc_to_dt(x['2nd_deliver_attempt'])
    if actual_second>second:
        return 1
    return 0
# tqdm.pandas()
df['is_late'] = df.parallel_apply(find,axis=1)
df.drop(["pick","1st_deliver_attempt","2nd_deliver_attempt","buyeraddress","selleraddress"],axis=1,inplace=True)
df.to_csv("result.csv",index=False)