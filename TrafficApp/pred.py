import pickle
import pandas as pd
import numpy as np
import requests
import json
import geopy
from datetime import datetime
import uuid
from pytz import timezone

df = pd.read_csv("./data/traffic_pop_data.csv")
df_den = df[['ZipCode', 'Density(/sqmi)']]
df_pop = df[['ZipCode', 'Population Estimate (as of July 1) - 2018 - Both Sexes; Median age (years)']]

reg = pickle.load(open("models/best_regressor.pickle","rb"))
classi = pickle.load(open("models/clf.pickle","rb"))


tomtomapikey = 'tomtomapikey'
zoomLevel = 10



columns = ['LocationLat^1', 'Severity^1 x LocationLat^1',
       'Severity^1 x LocationLng^1', 'Severity^1 x Density(/sqmi)^1',
       'Severity^1 x isWeekday^1', 'LocationLat^1 x LocationLng^1',
       'LocationLat^1 x ZipCode^1', 'LocationLat^1 x Density(/sqmi)^1',
       'LocationLat^1 x isWeekday^1', 'LocationLat^1 x SecondHalfHour^1',
       'LocationLng^1 x ZipCode^1', 'LocationLng^1 x Density(/sqmi)^1',
       'LocationLng^1 x Mon^1', 'LocationLng^1 x Noon(12-18)^1',
       'ZipCode^1 x Mon^1', 'Density(/sqmi)^1 x Mon^1',
       'Density(/sqmi)^1 x Noon(12-18)^1', 'Mon^1 x Autum^1',
       'Mon^1 x Morning(6-12)^1', 'Spring^1 x Morning(6-12)^1']

classi_col = ['Severity', 'LocationLat', 'LocationLng', 'ZipCode', 'Duration',
       'Population Estimate (as of July 1) - 2018 - Both Sexes; Median age (years)',
       'Density(/sqmi)', 'isWeekday', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat',
       'Sun', 'Spring', 'Summer', 'Autum', 'Winter', 'Morning(6-12)',
       'Noon(12-18)', 'Night(18-24)', 'midNight(24-6)', 'firstHalfHour',
       'SecondHalfHour']

reg = pickle.load(open("models/best_regressor.pickle","rb"))

def get_dur_pred(lati_, longi_, time_now):
    LocationLat = lati_
    LocationLng = longi_
    Severity = get_severity(lati_, longi_)
    ZipCode = get_zipcode(geolocator, lati_, longi_)
    try:
        Density = df_den[df_den['ZipCode'] == ZipCode]['Density(/sqmi)'].unique()[0]
    except:
        Density = df_den['Density(/sqmi)'].mean()
    
    if Severity == 0:
        msg = "No congestion detected."
        status = 0
    else:
        #########
        stime = pd.DataFrame([time_now], columns = ['StartTime(UTC)'])
        time_df = get_time_data(stime)
        #########
        try:
            population = df_pop[df_pop['ZipCode'] == ZipCode]['Population Estimate (as of July 1) - 2018 - Both Sexes; Median age (years)'].unique()[0]
        except:
            population = df_pop['Population Estimate (as of July 1) - 2018 - Both Sexes; Median age (years)'].mean()
        isWeekday = time_df['isWeekday'][0]

        Mon = time_df['Mon'][0]
        Tue = time_df['Tue'][0]
        Wed = time_df['Wed'][0]
        Thu = time_df['Thu'][0]
        Fri = time_df['Fri'][0]
        Sat = time_df['Sat'][0]
        Sun = time_df['Sun'][0]

        Spring = time_df['Spring'][0]
        Summer = time_df['Summer'][0]
        Autum = time_df['Autum'][0]
        Winter = time_df['Winter'][0]

        Morning = time_df['Morning(6-12)'][0]
        Noon = time_df['Noon(12-18)'][0]
        Night = time_df['Night(18-24)'][0]
        midNight = time_df['midNight(24-6)'][0]

        firstHalfHour = time_df['firstHalfHour'][0]
        SecondHalfHour = time_df['SecondHalfHour'][0]

        to_pred=[[
            LocationLat,
            (Severity * LocationLat),
            (Severity * LocationLng),
            (Severity * Density),
            (Severity * isWeekday),
            (LocationLat * LocationLng),
            (LocationLat * ZipCode),
            (LocationLat * Density),
            (LocationLat * isWeekday),
            (LocationLat * SecondHalfHour),
            (LocationLng * ZipCode),
            (LocationLng * Density),
            (LocationLng * Mon),
            (LocationLng * Noon),
            (ZipCode * Mon),
            (Density * Mon),
            (Density * Noon),
            (Mon * Autum),
            (Mon * Morning),
            (Spring * Morning)
        ]]

        to_pred_df = pd.DataFrame(to_pred, columns = columns )
        dur_pred = reg.predict(to_pred_df)[0]

        ### Anomaly detection setup

        classi_pred = [[
            Severity,
            LocationLat,
            LocationLng,
            ZipCode,
            dur_pred,
            population,
            Density,
            isWeekday,
            Mon,
            Tue,
            Wed,
            Thu,
            Fri,
            Sat,
            Sun,
            Spring,
            Summer,
            Autum,
            Winter,
            Morning,
            Noon,
            Night,
            midNight,
            firstHalfHour,
            SecondHalfHour
        ]]

        clas_pred_df = pd.DataFrame(classi_pred, columns = classi_col )
        ano_pred = classi.predict(clas_pred_df)[0]

        if ano_pred == -1:
            flag = '!!!This is an anomaly!!! Please contact local traffic authorities.'
            status = 2
        else:
            flag = 'This is normal, please be patient.'
            status = 1

        msg = f'Congestion detected, estimated duration: {dur_pred:.2f} minutes. {flag}'
    return status, msg
    
    
    
def get_severity(lati_, longi_):
    lat = lati_
    lon = longi_
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/{zoomLevel}/json?point={lat}%2C{lon}&unit=MPH&key={tomtomapikey}"
    response = requests.request("get", url)
    r = response.json()
    spd_diff = (r['flowSegmentData']['freeFlowSpeed'] - r['flowSegmentData']['currentSpeed'])
    if  spd_diff>0 & spd_diff<=10:
        severity = 1
    elif spd_diff>10 & spd_diff<=20:
        severity = 2
    elif spd_diff>20:
        severity = 3
    else:
        severity = 0
    return severity

geolocator = geopy.Nominatim(user_agent='my-application')
def get_zipcode(geolocator, lat_field, lon_field):
    location = geolocator.reverse((lat_field, lon_field))
    return int(location.raw['address']['postcode'])

def get_time_now():
    date_format='%Y-%m-%d %H:%M:%S'
    date = datetime.now(tz=pytz.utc)
    print ('Current date & time is:', date.strftime(date_format))

#     date = date.astimezone(timezone('US/Pacific'))

#     print ('Local date & time is  :', date.strftime(date_format))
    return str(date.strftime(date_format))


def get_time_data(df_enc):
    def Mon(x):
        return x.split(" ")[0].split('-')[1]
    def Day(x):
        return x.split(" ")[0].split('-')[2]
    def Hour(x):
        return x.split(" ")[1].split(':')[0]
    def Minute(x):
        return x.split(" ")[1].split(':')[1]

    df_enc['StartMonth'] = df_enc['StartTime(UTC)'].apply(lambda x: Mon(x)).astype(int)
    df_enc['StartDay'] = df_enc['StartTime(UTC)'].apply(lambda x: Day(x)).astype(int)
    df_enc['StartHour'] = df_enc['StartTime(UTC)'].apply(lambda x: Hour(x)).astype(int)
    df_enc['StartMinute'] = df_enc['StartTime(UTC)'].apply(lambda x: Minute(x)).astype(int)

    df_enc['StartTime(UTC)'] = pd.to_datetime(df_enc['StartTime(UTC)'])

    def get_dayofweek(x):
        return x.dayofweek
    df_enc['DayOfWeek'] = df_enc['StartTime(UTC)'].apply(lambda x: get_dayofweek(x)).astype(int)
    
    df_enc['isWeekday'] = np.where(df_enc['DayOfWeek'] <=4, 1, 0)
    
    df_enc['Mon'] = np.where(df_enc['DayOfWeek'] ==0, 1, 0)
    df_enc['Tue'] = np.where(df_enc['DayOfWeek'] ==1, 1, 0)
    df_enc['Wed'] = np.where(df_enc['DayOfWeek'] ==2, 1, 0)
    df_enc['Thu'] = np.where(df_enc['DayOfWeek'] ==3, 1, 0)
    df_enc['Fri'] = np.where(df_enc['DayOfWeek'] ==4, 1, 0)
    df_enc['Sat'] = np.where(df_enc['DayOfWeek'] ==5, 1, 0)
    df_enc['Sun'] = np.where(df_enc['DayOfWeek'] ==6, 1, 0)
    
    df_enc['Spring'] = np.where((df_enc['StartMonth'] ==1) | (df_enc['StartMonth'] ==2) | (df_enc['StartMonth'] ==3), 1, 0)
    df_enc['Summer'] = np.where((df_enc['StartMonth'] ==4) | (df_enc['StartMonth'] ==5) | (df_enc['StartMonth'] ==6), 1, 0)
    df_enc['Autum'] = np.where((df_enc['StartMonth'] ==7) | (df_enc['StartMonth'] ==8) | (df_enc['StartMonth'] ==9), 1, 0)
    df_enc['Winter'] = np.where((df_enc['StartMonth'] ==10) | (df_enc['StartMonth'] ==11) | (df_enc['StartMonth'] ==12), 1, 0)
    
    df_enc['Morning(6-12)'] = np.where((df_enc['StartHour'] >= 6) & (df_enc['StartHour'] < 12), 1, 0)
    df_enc['Noon(12-18)'] = np.where((df_enc['StartHour'] >= 12) & (df_enc['StartHour'] < 18), 1, 0)
    df_enc['Night(18-24)'] = np.where((df_enc['StartHour'] >= 18) & (df_enc['StartHour'] < 24), 1, 0)
    df_enc['midNight(24-6)'] = np.where((df_enc['StartHour'] >= 0) & (df_enc['StartHour'] < 6), 1, 0)
    
    df_enc['firstHalfHour'] = np.where((df_enc['StartMinute'] <=30), 1, 0)
    df_enc['SecondHalfHour'] = np.where((df_enc['StartMinute'] > 30), 1, 0)
    
    return df_enc