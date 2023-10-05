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
        keypath = 'realisasi-bus-760296b211e8.json'
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

  def get_spreadsheet_info(self, service, spreadsheetId):
    print('Getting spreadsheet info...')
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

  def sheet_to_csv(self, service, spreadsheetId, range, filepath):
    print('Downloading spreadsheet...')
    retries = 0
    succeed = 0
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
      df.iloc[1:].to_csv(filepath, index=False)
      print('Download Completed!')
    else:
      print('Download Failed!')


if __name__ == '__main__':
  artrans = ARTrans()
  service = artrans.create_service()
  sheet_info = artrans.get_spreadsheet_info(service=service, spreadsheetId='1vzY7XY8fMOJBa1Mx7bH-GclTnBy-CtAwPn0StvFQXFw')
  print(sheet_info)
  artrans.sheet_to_csv(service=service, spreadsheetId='1vzY7XY8fMOJBa1Mx7bH-GclTnBy-CtAwPn0StvFQXFw', range='Realisasi History', filepath='Realisasi_History2.csv')