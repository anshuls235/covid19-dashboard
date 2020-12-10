import pandas as pd
import numpy as np
import datetime as dt
import os
import pickle
import requests
from collections import defaultdict


class Dataset:
  def __init__(self):
      """Initialise the class."""

      self.curr_date = dt.date.today()
      self.INDIA_PATH = 'data/cases-india.csv'
      self.STATES_PATH = 'data/cases-states.csv'
      self.LATEST_PATH = 'data/latest-data.csv'
      self.OBJECTS_PATH = 'data/objs.pkl'
      self.URL_INDIA = 'https://api.covid19india.org/data.json'
      self.URL_STATES = 'https://api.covid19india.org/states_daily.json'
      self.URL_META = 'https://api.covid19india.org/misc.json'

  def get_metadata(self):
      """Fetch metadata with respect to states."""

      r = requests.get(self.URL_META)
      data = r.json()
      states_meta = data['state_meta_data']
      d_states = defaultdict()
      for state_meta in states_meta:
          d_states[state_meta['abbreviation'].lower()] = state_meta['stateut']
      return d_states

  def add_attributes(self,df,cols=[],update=False):
      """Add extra attributes."""

      for col in cols:
          if update == False or col == 'active':
              df[f"{col}_prev"] = df.groupby('state')[col].shift(1).fillna(0)\
                                .astype(np.int32)
          df[f"{col}_diff"] = df[col] - df[f"{col}_prev"]
          df[f"{col}_perc"] = df.apply(lambda x:\
                np.round((x[f"{col}_diff"]/x[f"{col}_prev"])*100,2)\
                if x[f"{col}_prev"] > 0 else 0, axis=1)
          if update == False:
              df[f"{col}_cum"] = df.groupby('state')[col].transform(np.cumsum)
          df[f"{col}_cum_prev"] = df.groupby('state')[f"{col}_cum"].shift(1)\
                                    .fillna(0).astype(np.int32)
          df[f"{col}_cum_diff"] = df[f"{col}_cum"] - df[f"{col}_cum_prev"]
          df[f"{col}_cum_perc"] = df.apply(lambda x:\
              np.round((x[f"{col}_cum_diff"]/x[f"{col}_cum_prev"])*100,2) \
              if x[f"{col}_cum_prev"] > 0 else 0, axis=1)
      return df

  def process_data(self,data,update=False):
      """Process the dataframe and convert into long format."""

      df = pd.DataFrame(data)
      df.drop('dd',axis=1,inplace=True)
      id_vars = ['date','dateymd','status']
      states = [col for col in df.columns if col not in id_vars]
      df = pd.melt(df, id_vars=id_vars,value_vars=states,
                   var_name='state')
      df.value = df.value.astype(np.int32)
      df = pd.pivot_table(df,values='value',
                        index=['date','dateymd','state'],columns='status',
                        aggfunc='sum').reset_index()
      df.state = df.state.map(self.get_metadata())
      df.dateymd = pd.to_datetime(df.dateymd,format='%Y-%m-%d')
      df.sort_values(['dateymd','state'],inplace=True)
      df.reset_index(drop=True,inplace=True)
      df.drop('date',axis=1,inplace=True)
      df.rename(columns = {col:col.lower() for col in df.columns},inplace=True)
      df.columns.name = ''
      if update == False:
          df = self.add_attributes(df,['confirmed','deceased','recovered'])
          df['active'] = df['confirmed'] - df['recovered'] - df['deceased']
          df['active_cum'] = df['confirmed_cum'] - df['recovered_cum'] -\
                            df['deceased_cum']
          df = self.add_attributes(df,['active'])
      return df

  def download(self):
      """Download the data through API and store processed data into files."""

      r = requests.get(self.URL_STATES)
      data = r.json()
      df = self.process_data(data['states_daily'])

      if os.path.exists('data') == False:
          os.mkdir('data')

      latest_date = df.dateymd.max()
      # Saving the objects:
      with open(self.OBJECTS_PATH, 'wb') as f:
          pickle.dump(latest_date, f)
      latest_df = df[df['dateymd']==latest_date]
      latest_df.to_csv(self.LATEST_PATH, index=False)

      df_states = df[df['state'] != 'India']
      df_india = df[df['state'] == 'India']
      df_states.to_csv(self.STATES_PATH, index=False)
      df_india.to_csv(self.INDIA_PATH, index=False)

  def get_last_updated_date(self):
      """Returns the last updated date as a date object and string."""

      with open(self.OBJECTS_PATH,'rb') as f:
          date = pickle.load(f)
      date_str = date.strftime('%Y-%m-%d')
      return date, date_str

  def update_objects(self, df, date):
      """Update the stored objects."""

      date_str = date.strftime('%Y-%m-%d')
      with open(self.OBJECTS_PATH,'wb') as f:
          pickle.dump(date, f)
      latest_df = df.loc[df['dateymd']==date_str,:]
      latest_df.to_csv(self.LATEST_PATH, index=False)

  def update(self):
      """Update the stored data with new data."""

      date_old, date_old_str = self.get_last_updated_date()
      r = requests.get(self.URL_STATES)
      data = r.json()
      date_new = pd.to_datetime(data['states_daily'][-1]['dateymd'],
                                format='%Y-%m-%d')
      if date_old == date_new:
          print(f'Dataset already updated on {dt.datetime.now()}')
          return
      new_days = (date_new - date_old).days + 1
      new_days*=3
      df = self.process_data(data['states_daily'][-new_days:], update=True)
      last_df = pd.read_csv('data/latest-data.csv')
      cols = ['confirmed','recovered','deceased']
      for col in cols:
          df[f"{col}_prev"] = df.groupby('state')[col].shift(1).fillna(0)\
                                .astype(np.int32)
          df[f"{col}_cum"] = df[col]
      df.dateymd = df.dateymd.apply(lambda x: x.strftime('%Y-%m-%d'))
      df = pd.concat([last_df,df[df['dateymd'] != date_old_str]])
      for col in cols:
          df[f"{col}_cum"] = df.groupby('state')[f"{col}_cum"].transform(np.cumsum)
      df = self.add_attributes(df,cols,update=True)
      df['active'] = df['confirmed'] - df['recovered'] - df['deceased']
      df['active_cum'] = df['confirmed_cum'] - df['recovered_cum'] -\
                         df['deceased_cum']
      df = self.add_attributes(df,['active'],update=True)
      self.update_objects(df, date_new)
      df = df[df['dateymd']!=date_old_str]
      df_states = df[(df['state'] != 'India')]
      df_india = df[(df['state'] == 'India')]
      df_states.to_csv('data/cases-states.csv', mode='a', header=False,
                        index=False)
      df_india.to_csv('data/cases-india.csv', mode='a', header=False,
                        index=False)
      print(f'Dataset was updated on {dt.datetime.now()}')
