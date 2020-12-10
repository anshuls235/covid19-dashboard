import pandas as pd
import requests

url = 'https://api.rootnet.in/covid19-in/stats/history'
r = requests.get(url)
data = r.json()

lastRefreshed = data['lastRefreshed']
lastOriginUpdate = data['lastOriginUpdate']

df = pd.DataFrame(columns=['date','location','confirmed','recoveries','deaths'])
ind = 0
if data['success']:
    for day in range(len(data['data'])):
        current_data = data['data'][day]
        current_date = current_data['day']
        for loc_data in current_data['regional']:
            df.loc[ind,'date'] = current_date
            df.loc[ind,'location'] = loc_data['loc']
            df.loc[ind,'confirmed'] = loc_data['totalConfirmed']
            df.loc[ind,'recoveries'] = loc_data['discharged']
            df.loc[ind,'deaths'] = loc_data['deaths']
            ind+=1

df.loc[df.location=='Nagaland#','location'] = 'Nagaland'
df.loc[df.location=='Jharkhand#','location'] = 'Jharkhand'
df.loc[df.location=='Madhya Pradesh#','location'] = 'Madhya Pradesh'

df['daily_confirmed'] = df.groupby('location')['confirmed']\
    .transform(lambda x: x - x.shift(1))
df['daily_recoveries'] = df.groupby('location')['recoveries']\
    .transform(lambda x: x - x.shift(1))
df['daily_deaths'] = df.groupby('location')['deaths']\
    .transform(lambda x: x - x.shift(1))

df.to_csv('India-cases.csv',index=False)
