import os
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from dataclasses import dataclass
import json
import pandas as pd
from urllib.error import HTTPError
from time import sleep

@dataclass
class ARTrans:

  def create_service(self):
    print('Creating Service...')
    retries = 0
    service = None
    succeed = 0
    while retries < 3:
      try:
        keypath = '/opt/airflow/dags/includes/gcp/gcp-service-account.json'
        print(os.getcwd())
        creds = Credentials.from_service_account_file(keypath)
        service = build(serviceName='sheets', version='v4', credentials=creds)
        succeed = 1
        break
      except HTTPError as e:
        print(f'retry due to {e}')
        sleep(3)
    if succeed:
      print('Service Created!')
      return service
    else:
      print('Failed')

  def get_spreadsheet_info(self):
    print('Getting spreadsheet info...')
    service = self.create_service()
    spreadsheetId = '1JbXR9vEZM3TcbQbxysqfYuYjkYbUPaToOlCpS4c8mZ8'
    retries = 0
    succeed = 0
    while retries < 3:
      try:
        result = service.spreadsheets().get(spreadsheetId=spreadsheetId).execute()
        succeed = 1
        break
      except HTTPError as e:
        print(f'retry due to {e}')
        sleep(3)
    if succeed:
      print('Completed!')
      return json.dumps(result, indent=2)
    else:
      print('Failed!')

  def sheet_to_csv(self):
    print('Downloading spreadsheet...')
    retries = 0
    succeed = 0
    service = self.create_service()
    spreadsheetId = '1JbXR9vEZM3TcbQbxysqfYuYjkYbUPaToOlCpS4c8mZ8'
    range = 'Realisasi_History.csv'
    filepath = '/opt/airflow/dags/includes/data/Realisasi_History.csv'
    while retries < 3:
      try:
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=range).execute()
        succeed = 1
        break
      except HTTPError as e:
        print(f'retry due to {e}')
        sleep(3)
    if succeed:
      df = pd.DataFrame(result['values'])
      df.columns = df.iloc[0]
      df.iloc[1:].to_csv(filepath, index=False, mode='w')
      print('Download Completed!')
    else:
      print('Download Failed!')


if __name__ == '__main__':
  artrans = ARTrans()
  artrans.sheet_to_csv()
  # service = artrans.create_service()
  # sheet_info = artrans.get_spreadsheet_info(service=service, spreadsheetId='1JbXR9vEZM3TcbQbxysqfYuYjkYbUPaToOlCpS4c8mZ8')
  # print(sheet_info)
  # artrans.sheet_to_csv(service=service, spreadsheetId='1JbXR9vEZM3TcbQbxysqfYuYjkYbUPaToOlCpS4c8mZ8', range='Realisasi_History.csv', filepath='./data/Realisasi_History.csv')