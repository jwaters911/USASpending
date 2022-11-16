## This script downloads files from USA Spending API award download URL endpoint
## The Contracts Prime Award Summaries table is joined to
## The Assistance and Contracts Prime Award Summaries are converted to GDB tables

from datetime import datetime, timedelta
import time
import requests
import numpy as np
import json
import urllib
from pandas.io.json import json_normalize
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import arcpy
import os
from os import path
import pandas as pd


## Set local varables
arcpy.env.overwriteOutput = True
localPath = r'C:\Users\josephwaters\Desktop\USASpendingDownloads\3'
outputGDB = r"C:\Users\josephwaters\Documents\Esri_Exports\Esri_Exports\USA_Spending.gdb"

contractSubstring = 'Contracts_PrimeAwardSummaries'
assistanceSubstring = 'Assistance_PrimeAwardSummaries'
contractCSV = ""
url = "https://api.usaspending.gov/api/v2/download/awards/"

## Delete all files from download location
print('Remove CSV from download directory...')
for f in os.listdir(localPath):
    os.remove(os.path.join(localPath, f))

## Create JSON payload for TAS Code
print('Create USA Spending API payload...')
payload = json.dumps({
"filters":{

    "agencies":[
        {"type":"funding",
                 "tier":"subtier",
                 "name":"Forest Service",
                 "toptier_name":"Department of Agriculture"},
                {"type":"awarding",
                 "tier":"toptier",
                 "name":"Department of Agriculture"}],
    "tas_codes":{
        "require":[["012-5716"]]},

}
})
headers = {
  'Content-Type': 'application/json',
  'Cookie': 'cookiesession1=678A3E0DCDEFGHIJKLNOPQRSTUV08936'
}
## Download response
response = requests.request("POST", url, headers=headers, data=payload)
response.raise_for_status()
type(response)
data = response.json()
fileURL = data['file_url']
print('Zip URL: ',  fileURL)

## Extract files
print('Extract files...')
with urlopen(fileURL) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall(localPath)

pathFiles = os.listdir(localPath)

## Identify Assistance and Contract tables
for i in pathFiles:
    if i.startswith(contractSubstring) and i.endswith('.csv'):
        contractCSV = i

fullPath = os.path.join(localPath + '\\' + contractCSV)
print(fullPath)

## Read Contract CSV and get contract IDs
## Use ContractIDs to query the accounts endpoint
print('Query USA Spending API to join Contracts to Accounts...')
df = pd.read_csv(fullPath)
contractIDs = df["contract_award_unique_key"].tolist()
print('contractIDs: ' + str(contractIDs))

url = "https://api.usaspending.gov/api/v2/awards/accounts/"

headers = {
  'Content-Type': 'application/json',
  'Cookie': 'cookiesession1=678A3E0DCDEFGHIJKLNOPQRSTUV08936'
}
cols = {"transaction_obligated_amount": [], "federal_account": [], "account_title": [], "funding_agency_name": [],
        "funding_agency_id": [],"funding_toptier_agency_id": [],"funding_agency_slug": [],"awarding_agency_name": [],
        "awarding_agency_id": [],"awarding_toptier_agency_id": [],"awarding_agency_slug": [],"object_class": [],
        "object_class_name": [],"program_activity_code": [],"program_activity_name": [], "reporting_fiscal_year":[],
        "reporting_fiscal_quarter": [],"reporting_fiscal_month": [],"is_quarterly_submission": []
        }

df2 = pd.DataFrame(cols)

## Pass contract award IDs to the award_id filter
for i in contractIDs:
    print("Create payload for: " + i)
    payload = json.dumps({
    "limit": 100,
    "sort": "total_transaction_obligated_amount",
    "order": "desc",
    "award_id": i,
    "page": 1
    })

    response = requests.request("POST", url, headers=headers, data=payload)
    response.raise_for_status()  # raises exception when not a 2xx response
    if response.status_code != 204:
        data = response.json()
        df = pd.DataFrame(data["results"])
        df['award_id'] = i
        frames = [df, df2]
        df2 = pd.concat(frames, sort=False)
    else:
        print('error')
        pass
## Generate table from joined output
print('Create CSV from results...')
df2 = df2[(df2.federal_account == "012-5716")]
df = pd.read_csv(fullPath)
joinedOutput = df.set_index('contract_award_unique_key').join(df2.set_index('award_id'), lsuffix='_AWARD', rsuffix='_ACCOUNT')
joinedOutput.to_csv(os.path.join(localPath + '\\' + 'Contract_PrimeAwardSummaries.csv'))

## Convert CSVs to GDB Tables
print("Generate GDB Tables from CSVs...")

for i in os.listdir(localPath):
    if i == "Contracts_PrimeAwardSummaries.csv":
        filePath = os.path.join(localPath + '\\' + i)
        print(i.rsplit('.csv')[0])
        arcpy.TableToTable_conversion(filePath, outputGDB, i.rsplit('.csv')[0])
    elif i.startswith("Assistance_PrimeAwardSummaries"):
        filePath = os.path.join(localPath + '\\' + i)
        print(i.rsplit('_', 3)[0])
        arcpy.TableToTable_conversion(filePath, outputGDB, i.rsplit('_', 3)[0])

print('Process complete.')
