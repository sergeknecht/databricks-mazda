import requests

DBX__HOST = ""
DBX__PAT = ""

# get the content of the config directory
url = f"https://{DBX__HOST}/api/2.0/workspace/list?path=/Workspace/Repos/sknecht@mazdaeur.com/databricks-mazda/config"
headers = {
    "Authorization": f"Bearer {DBX__PAT}"
}

response = requests.get(url, headers=headers)
if response.status_code == 200:
    print("Request successful!")
    print(response.json())
else:
    print("Request failed!")
    print(response.text)

# loop though response and get all entries ending with GENERATED_PY



# for each entry, get the content of the file
# remove GENERATED_PY from filename
# encode64 the content of the file
# post the encoded content to the databricks api

url = f"https://{DBX__HOST}/api/2.0/workspace/list?path=/Workspace/Repos/sknecht@mazdaeur.com/databricks-mazda/config"
data = {
    "key": "value"
}
response = requests.post(url, headers=headers, data=data)

if response.status_code == 200:
    print("Request successful!")
    print(response.json())
else:
    print("Request failed!")
    print(response.text)
