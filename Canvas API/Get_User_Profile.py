import requests
import pandas as pd
import json

domain = 'ucdenver.instructure.com'
token = ''
user_id = '3' # user id

response = requests.get('https://'+domain+'/api/v1/users/'+user_id + '/profile', headers={'Authorization': 'Bearer '+token})
# print(response.status_code)
json_details = (response.json())

df = pd.io.json.json_normalize(json_details)
print(df)

user_profile = df[[
    'id',
    'short_name',
    'bio'
]]

print(user_profile)

user_profile.to_csv('CanvasGetCrystalData.csv')
