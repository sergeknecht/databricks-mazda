# Databricks notebook source
import os
import sys
import urllib
# import urllib2
# import cookielib
# import urlparse
import hashlib 
import hmac
import base64
import json
import random
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import pprint as pp

import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import Window

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

# black list: audiences that contain 'UC1' or 'UC2' in the name, but should be excluded (list with audience IDs)
black_list = (620872,723633,723634,727041,730349,731333,731885,731886,731896,731901,732177,732179,732181,753251,753257,621150,752775,621152,731897,732182,732183,749499,621149,751682,752774,621049,621047,752776,621046)

# COMMAND ----------

# MAGIC %md
# MAGIC #### CXU

# COMMAND ----------

def get_auth_token_cxu(username: str, password: str, idcs_url = 'idcs-b7562c48970d4eed98cde4c77d4b4c34.identity.oraclecloud.com', client_id = 'CXUNITYSI-88f55a2a391c4a9a8a941858255d004c_APPID', client_secret = '1cf8910a-b6da-468b-93b6-3299baeb202a', idcs_primary_audience_url = 'urn:opc:entitlementid=638686749'):
  """
  Goal: Use this function to get an OAuth 2.0 authentication token using the Oracle Identity Cloud Service (IDCS)
  
  Docs: https://docs.oracle.com/en/cloud/saas/cx-unity/cx-unity-develop/docs/authentication/authenticating-oauth.htm
  
  Arguments (contact for the last 4 arguments: Koen De Vroede):
  - username: IDCS user account (the user account accessing the API must have the Instance admin role in Oracle Unity)
  - password: password for IDCS user account
  - idcs_url: path to your IDCS instance
  - client_id: IDCS Client ID (you can find your Client ID in the IDCS Admin Console by selecting your application and looking under Configuration > General Information)
  - client_secret: IDCS Client Secret (you can find your Client Secret in the IDCS Admin Console by selecting your application and looking under Configuration > General Information)
  - idcs_primary_audience_url: Primary Audience URL of your IDCS application (you can find this URL in your IDCS instance under Configuration > Resources > Primary Audience)
  
  Response: base64 encrypted authentication token
  """
  # construct IDCS authentication endpoint
  url = f'https://{idcs_url}/oauth2/v1/token'
  
  # construct token for authentication using IDCS Client ID and Client Secret: '<base64Encoded client_id:client_secret>'
  clientid_clientsecret = f'{client_id}:{client_secret}'
  token = base64.b64encode(clientid_clientsecret.encode()).decode("utf-8")
  
  # request for authentication should include a x-www-form-urlencoded body with the following parameters:
  body = {
    "grant_type":"password",
    "scope":f"offline_access {idcs_primary_audience_url}cxunity", #offline_access is used as prefix to include a refresh token in the response
    "username":f"{username}",
    "password":f"{password}"
    }
  
  # perform request (POST) to IDCS authentication endpoint to get authentication token
  # use the following Authorization header (HTTP basic): 'Authorization: Basic <base64Encoded client_id:client_secret>'
  # use the following Content-Type header: 'application/x-www-form-urlencoded'
  r = requests.post(url, headers = {"Authorization": f'Basic {token}', 'Content-Type': 'application/x-www-form-urlencoded'}, data = body)
  j = r.json()
  authentication_token = j['access_token']
  
  return authentication_token

# COMMAND ----------

def call_api_cxu(method: str, endpoint: str, account_url = 'mmecdp.cxunity.ocs.oraclecloud.com', tenant_id = '100016', access_key = 'b28b1ed037214ba9ab4814b0d72b2982', limit = None):
  """
  Goal: Use this function to send HTTP requests (GET or POST) to the Oracle Unity API.
  
  Docs: https://docs.oracle.com/en/cloud/saas/cx-unity/cx-unity-develop/docs/concepts/sending-requests.htm
  
  Arguments:
  - method: HTTP request method (GET, POST)
  - endpoint: path of the Oracle Unity API endpoint (see Docs for all endpoints: https://oracleunityapis.docs.apiary.io/#reference)
  - acccount_url: URL to the Oracle Unity account (contact your tenant administrator if you do not know your Account URL) (contact: Koen De Vroede)
  - tenant_id: some endpoints also require a Unity tenant id (see https://docs.oracle.com/en/cloud/saas/cx-unity/cx-unity-develop/docs/get-started/determining-tenant-access-keys.htm)
  - access_key: some endpoints also require a Unity tenant access key (see https://docs.oracle.com/en/cloud/saas/cx-unity/cx-unity-develop/docs/get-started/determining-tenant-access-keys.htm)
  - limit: number of rows to extract from the API to limit the response
  
  Response: JSON response of the API call
  """
  # first get authentication token
  authentication_token = get_auth_token_cxu(
    username='databricks', #'jasper.penneman@element61.be', 
    password='t!FM#$$eps0fm4yk' #'lkg8Os*ge#GeMyWm'
  )
  
  # construct request url
  if limit == None:
    temp_url = f'https://{account_url}{endpoint}'
  else:
    temp_url = f'https://{account_url}{endpoint}?limit={limit}'
    
  url = temp_url.replace('accesskey', access_key) # fill in access_key (if needed)
  
  # perform request (GET or POST) to Unity API
  # use the following Authorization header (Bearer): 'Authorization: Bearer <authentication_token>'
  # use the following Content-Type header: 'application/json'
  # use the following Accept header: 'application/json'
  # fill in the tenant id in the x-mcps-tenantId header
  # fill in the access_key in the x-mcps-tenantkey header
  if method == 'GET':
    r = requests.get(url, headers = {'Authorization': f'Bearer {authentication_token}', 'Content-Type': 'application/json', 'Accept': 'application/json', 'x-mcps-tenantId': tenant_id, 'x-mcps-tenantkey': access_key})
  elif method == 'POST':
    r = requests.post(url, headers = {'Authorization': f'Bearer {authentication_token}', 'Content-Type': 'application/json', 'Accept': 'application/json', 'x-mcps-tenantId': tenant_id, 'x-mcps-tenantkey': access_key})
  j = r.json()
  
  return j

# COMMAND ----------

# function to get a table with all CXU segments
def cxu_get_segments():
  # get table with all CXU segments by doing a GET request
  # endpoint: '/cxunity/segmentation/v1/segments'
  # note: use the 'limit' argument to get all the data (without this argument, the default of 100 rows is returned)
  response = call_api_cxu(
    method = 'GET',
    endpoint = '/cxunity/segmentation/v1/segments',
    limit = 500
  )
  
  return response

# COMMAND ----------

# function to refresh the count of a CXU segment
def cxu_refresh_segment_count(unique_id):
  print(unique_id)
  # refresh count for a segment by doing a POST request and using the segmentId (this POST updates the 'lastCount' column)
  # general endpoint: '/cxunity/segmentation/v1/segments/{segmentId}/count'
  # note: the response of this POST request can be -1 (meaning: count is 'in progress')
  response = call_api_cxu(
  method = 'POST',
  endpoint = f'/cxunity/segmentation/v1/segments/{unique_id}/count',
  )
  
  # extract count from response
  count = response['data'][0][0]
  print('count: ', count)
  
  return count

# COMMAND ----------

# function to create a new row (with a refreshed count) for the table uc_reporting.cxu_segment_counts
def create_new_count_row(unique_id, segments_df, count):
  # get the name of the segment
  segment_name = segments_df[segments_df['uniqueId'] == unique_id]['name'].values[0]
  
  # based on the name of the segment: extract the NSC and use case
  segment_name_split = segment_name.split('_')
  if len(segment_name_split) > 1 and segment_name_split[1].startswith('UC'):
    nsc = segment_name_split[0]
    uc = segment_name_split[1]
  else:
    nsc = None
    uc = None

  # construct the new row    
  new_count = [segment_name, unique_id, nsc, uc, count]
  
  return new_count

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bluekai (DMP)

# COMMAND ----------

def generate_url_signature_bluekai(method: str, url: str, data = None, bkuid = '1baf3150d8a57589e4bbbb51d7989fe420c30656b2f7758e59c5e32ccb174a49', bksecretkey = 'af8f4a9979b5f083d34fbfbf64f83f7abc01ee51d318c6659b03bbcb3e6cbee6'):
  """
  Goal: Use this function to generate a new url that includes the authentication signature
  
  Docs: 
  - https://docs.oracle.com/en/cloud/saas/data-cloud/data-cloud-help-center/Developers/getting_started/api_authentication.html
  - https://docs.oracle.com/en/cloud/saas/data-cloud/data-cloud-help-center/Developers/getting_started/api_examples.html
  
  Arguments:
  - method: HTTP request method (GET, POST, PUT)
  - url: url of the BlueKai API endpoint (including query string). For GET (List) requests, add the desired sort and filter options in the query string. For GET (Read), PUT or DELETE requests, append the item ID to the Url path. * NOTE: For the Campaign, Order, and Pixel URL APIs, insert the item ID in the query string instead of the Url path.
  - data: string including the JSON body needed for POST and PUT requests
  - bkuid: Web Service User Key, which is your unique ID for accessing the Oracle Data Cloud web services (Log in to partner.bluekai.com, click Tools, and click the Web Service Key Tool link. If the Web Service Key Tool link is not displayed, contact My Oracle Support (MOS) and request access to Oracle Data Cloud web services.)
  - bksecretkey: Web Service Private Key, which is your authentication key (Log in to partner.bluekai.com, click Tools, click the Web Service Key Tool link and Click Show Private Key.)
  
  Response: New url that includes the authentication signature - this new url can be used in HTTP requests.
  """
  # first create a string_to_sign containing the following elements: HTTP_METHOD + URI_PATH + QUERY_ARG_VALUES + POST_DATA
  
  # 1st part of the string to sign: HTTP method ('GET', 'POST', 'PUT')
  string_to_sign = method

  # 2nd part of the string to sign: URI path
  parsed_url = urllib.parse.urlparse(url)
  string_to_sign += parsed_url.path

  # 3rd part of the string to sign: values for the query parameters (if any present)
  # splitting the query into array of parameters separated by the '&' character
  query_array = parsed_url.query.split('&')
  # add each parameter value to the string to sign
  if len(query_array) > 0:
      for param_value in query_array:
          param_value_split = param_value.split('=', 1)
          if len(param_value_split) > 1:
              string_to_sign += param_value_split[1]

  # 4th part of the string to sign: data in a POST or PUT request (if needed)
  if data != None :
      string_to_sign += data 

  # generate the authentication signature using the string_to_sign and the private key (bksecretkey) - HmacSHA256 encryption is used for this
  h = hmac.new(bksecretkey.encode(), string_to_sign.encode(), hashlib.sha256)
  s = base64.standard_b64encode(h.digest())
  u = urllib.parse.quote_plus(s)

  # create new url including the authentication signature
  new_url = url 
  if url.find('?') == -1 :
      new_url += '?'
  else:
      new_url += '&'

  new_url += 'bkuid=' + bkuid + '&bksig=' + u 

  return new_url

# COMMAND ----------

def call_api_bluekai(method: str, url: str, data = None):
  """
  Goal: Use this function to send HTTP requests (GET, POST, PUT) to the BlueKai API.
  
  Arguments:
  - method: HTTP request method (GET, POST)
  - url: url of the BlueKai API endpoint (including query string). For GET (List) requests, add the desired sort and filter options in the query string. For GET (Read), PUT or DELETE requests, append the item ID to the Url path. * NOTE: For the Campaign, Order, and Pixel URL APIs, insert the item ID in the query string instead of the Url path.
  - data: string including the JSON body needed for POST and PUT requests
  - bkuid: Web Service User Key, which is your unique ID for accessing the Oracle Data Cloud web services (Log in to partner.bluekai.com, click Tools, and click the Web Service Key Tool link. If the Web Service Key Tool link is not displayed, contact My Oracle Support (MOS) and request access to Oracle Data Cloud web services.)
  - bksecretkey: Web Service Private Key, which is your authentication key (Log in to partner.bluekai.com, click Tools, click the Web Service Key Tool link and Click Show Private Key.)
  
  Response: JSON response of the API call
  """
  # first generate a new url that includes the authentication signature
  new_url = generate_url_signature_bluekai(method = method, url = url, data = data)
  
  # perform request (GET or POST) to BlueKai API
  # use the headers specified in the Docs
  headers = {'Accept':'application/json','Content-type':'application/json','User_Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; en-US; rv:1.9.1) Gecko/20090624 Firefox/3.5'}
  if method == 'GET':
    r = requests.get(url = new_url, headers = headers)
  elif method == 'POST':
    r = requests.post(url = new_url, headers = headers, data = data)
  j = r.json()
  
  return j

# COMMAND ----------

# function to get a table with all Bluekai audiences
def bluekai_get_audiences():
  # get table with all Bluekai audiences by doing a GET request
  # endpoint: https://services.bluekai.com/Services/WS/audiences
  response = call_api_bluekai(
  method = 'GET',
  url = 'https://services.bluekai.com/Services/WS/audiences'
  )
  
  return response

# COMMAND ----------

# function to get a count of a Bluekai audience
def bluekai_get_audience_count(unique_id):
  print(unique_id)
  # get count for an audience by doing a GET request and using the audienceId
  # general endpoint: https://services.bluekai.com/Services/WS/audiences/{audienceId}
  response = call_api_bluekai(
    method = 'GET',
    url = f'https://services.bluekai.com/Services/WS/audiences/{unique_id}'
    )
  
  # extract count from response
  count = response['reach']
  print('count: ', count)
  
  return count

# COMMAND ----------

# MAGIC %md
# MAGIC #### Infinity

# COMMAND ----------

# function to create a token to authenticate to the API of infinity
def create_token_infinity(username, password):
  username_password = f'{username}:{password}'
  token = base64.b64encode(username_password.encode()).decode("utf-8")
  return token

# COMMAND ----------

def infinity_extract_data_uc3_4(url, date, use_case, audience):
  # get token to authenticate to the API of Infinity
  token = create_token_infinity(
    username='jasper.penneman@element61.be', 
    password='zhMJkJUJ^Z2VW@bV'
  )
  
  for i in range(0,3):
    # get data from the Infinity report using the API - do this 3 times just to 'refresh'
    response = requests.get(url=url, headers = {'Authorization': f'Basic {token}'})
    data = response.json()
    time.sleep(10)

  # parse JSON response to usable dataframe
  counter = 0
  nr_parts = len(data['dimensions'])
  raw_cols = ['WebsiteVariant', 'Domain']
  values_all = []

  for part in range(0,nr_parts):
    website_variant = data['dimensions'][part]['value']
    # extract different data parts from the JSON response
    data_part = data['dimensions'][part]['dimensions']
    for row in data_part:
      values_row = []
      # add website variant to each row
      values_row.append(website_variant)
      # extract the domain for each row
      domain = row['value']
      values_row.append(domain)
      if counter == 0:
        for i in row['measures']:
          # get and collect the columns once
          raw_cols.append(i['guid'])
      for i in row['measures']:
        # get and collect the values for each row in the JSON response
        values_row.append(i['value'])

      # append the row to the overall result 
      values_all.append(values_row)

      counter = counter + 1
      
  clean_cols = [col.replace(' ', '_') for col in raw_cols]
  dataframe_pd = pd.DataFrame(values_all, columns = clean_cols)
  
  # config table with link between internet extension and NSC code
  data = [
    ('ch','Switzerland','MS'),
    ('be','Belgium','MMB'),
    ('at','Austria','MAG'),
    ('uk','United Kingdom','MMUK'),
    ('cz','Czech Republic','MMCZ'),
    ('de','Germany','MMD'),
    ('es','Spain','MAE'),
    ('fr','France','MAF'),
    ('hr','Croatia','MMCR'),
    ('hu','Hungary','MMH'),
    ('ie','Ireland','MMIRL'),
    ('it','Italy','MMI'),
    ('lu','Luxembourg','MMB-LUX'),
    ('pl','Poland','MMPOL '),
    ('pt','Portugal','MMP'),
    ('ro','Romania','MMRO'),
    ('se','Sweden','MMS'),
    ('si','Slovenia','MMSI'),
    ('sk','Slovakia','MMSK'),
    ('nl','Netherlands','MMNL'),
    ('no','Norway','MMN'),
    ('dk',' Denmark','MMDK')
  ]
  cols = ['extension','country','nsc']
  extension_nsc_df = spark.createDataFrame(data,cols)
  
  dataframe_spark = (spark.createDataFrame(dataframe_pd)
                     # add date
                     .withColumn('date_str', F.lit(date))
                     .withColumn('date', F.to_date('date_str', 'yyyy/MM/dd'))
                     .withColumn('year', F.year('date'))
                     .withColumn('quarter', F.quarter('date'))
                     .withColumn('month', F.month('date'))
                     .withColumn('week', F.weekofyear('date'))
                     .withColumn('week', F.when( (F.col('week') > 50) & ((F.col('month') == 1)), 0).otherwise(F.col('week'))) # correction for beginning of January
                     .drop('date_str')
                     # add language
                     .withColumn('language', F.when(F.split('Domain', '\.').getItem(0) != 'www', F.split('Domain', '\.').getItem(0)).otherwise(None))
                     # add extension
                     .withColumn('split', F.split('Domain', '\.'))
                     .withColumn('extension', F.element_at('split', -1))
                     .join(extension_nsc_df, 'extension', 'left')
                     .drop('split')
                    )
  
  # renaming
  if use_case == 'UC3':
    dataframe_renamed = (dataframe_spark
                         .withColumnRenamed('UC3_HeroBanneerSeen', 'HeroBannerSeen')
                         .withColumnRenamed('UC3_CTA1_Clicks', 'CTA1_Clicks')
                         .withColumnRenamed('UC3_CTA2_Clicks', 'CTA2_Clicks')
                        )
  elif use_case == 'UC4':
    dataframe_renamed = (dataframe_spark
                         .withColumnRenamed('UC4_USC_Hero_Seen', 'HeroBannerSeen')
                         .withColumnRenamed('UC4_USC_CC', 'CTA1_Clicks')
                         .withColumnRenamed('UC4_USC_Offer', 'CTA2_Clicks')
                        )
  
  # add use_case and audience
  dataframe_final = dataframe_renamed.withColumn('use_case', F.lit(use_case)).withColumn('audience', F.lit(audience))
  
  return dataframe_final

# COMMAND ----------

def add_basic_kpis_uc3_4(df):
  df_new = (df
            .withColumn('cta1_ctr', F.col('CTA1_Clicks') / F.col('Sessions'))
            .withColumn('cta2_ctr', F.col('CTA2_Clicks') / F.col('Sessions'))
            .withColumn('cta_total_ctr', (F.col('CTA1_Clicks') + F.col('CTA2_Clicks')) / F.col('Sessions'))
            .withColumn('configuration_started', F.col('Configurator'))
            .withColumn('configuration_completed', F.col('Configurator_Completed'))
            .withColumn('tdr_submissions', F.col('TDR_submit'))
            .withColumn('raq_submissions', F.col('RAQ_submit'))
            .withColumn('configuration_started_conversion_rate', F.col('configuration_started') / F.col('Sessions'))
            .withColumn('configuration_completed_conversion_rate', F.col('configuration_completed') / F.col('Sessions'))
            .withColumn('tdr_submit_conversion_rate', F.col('tdr_submissions') / F.col('Sessions'))
            .withColumn('raq_submit_conversion_rate', F.col('raq_submissions') / F.col('Sessions')) 
           )
  
  return df_new

# COMMAND ----------

def calculate_uplift(df, use_case, audience):
  
  # take right selection of data based on use case and audience
  if use_case == 'UC3':
    base_df = df.filter(F.col('use_case') == use_case).filter(F.col('audience') == audience)
  elif use_case == 'UC4':
    base_df = df.filter(F.col('use_case') == use_case)
  
  # determine default vs variant data
  default_filter = base_df.filter(F.col('WebsiteVariant') == 'element1:Default')
  if use_case == 'UC3':
    if audience == 'Audience 1':
      variant_filter = base_df.filter(F.col('WebsiteVariant') == 'element1:configstarted')
    elif audience == 'Audience 2':
      variant_filter = base_df.filter(F.col('WebsiteVariant') == 'element1:configcompleted')
      
  elif use_case == 'UC4':
    variant_filter = base_df.filter(F.col('WebsiteVariant') == 'element1:highestmodel')
    
  default = (default_filter
             .select('Domain', 'date', 'configuration_started_conversion_rate', 'configuration_completed_conversion_rate', 'tdr_submit_conversion_rate', 'raq_submit_conversion_rate')
             .withColumnRenamed('configuration_started_conversion_rate', 'configuration_started_conversion_rate_default')
             .withColumnRenamed('configuration_completed_conversion_rate', 'configuration_completed_conversion_rate_default')
             .withColumnRenamed('tdr_submit_conversion_rate', 'tdr_submit_conversion_rate_default')
             .withColumnRenamed('raq_submit_conversion_rate', 'raq_submit_conversion_rate_default')
            )
  
  variant = (variant_filter
             .select('Domain', 'date', 'configuration_started_conversion_rate', 'configuration_completed_conversion_rate', 'tdr_submit_conversion_rate', 'raq_submit_conversion_rate')
             .withColumnRenamed('configuration_started_conversion_rate', 'configuration_started_conversion_rate_variant')
             .withColumnRenamed('configuration_completed_conversion_rate', 'configuration_completed_conversion_rate_variant')
             .withColumnRenamed('tdr_submit_conversion_rate', 'tdr_submit_conversion_rate_variant')
             .withColumnRenamed('raq_submit_conversion_rate', 'raq_submit_conversion_rate_variant')
            )
  
  join_default_variant = default.join(variant, ['Domain', 'date'], 'full_outer')
  
  uplift = (join_default_variant
            .withColumn('configuration_started_uplift', (F.col('configuration_started_conversion_rate_variant') - F.col('configuration_started_conversion_rate_default')) / F.col('configuration_started_conversion_rate_default'))
            .withColumn('configuration_completed_uplift', (F.col('configuration_completed_conversion_rate_variant') - F.col('configuration_completed_conversion_rate_default')) / F.col('configuration_completed_conversion_rate_default'))
            .withColumn('tdr_submit_uplift', (F.col('tdr_submit_conversion_rate_variant') - F.col('tdr_submit_conversion_rate_default')) / F.col('tdr_submit_conversion_rate_default'))
            .withColumn('raq_submit_uplift', (F.col('raq_submit_conversion_rate_variant') - F.col('raq_submit_conversion_rate_default')) / F.col('raq_submit_conversion_rate_default'))
           )
  
  # add use case and audience
  uplift_final = uplift.withColumn('use_case', F.lit(use_case)).withColumn('audience', F.lit(audience))
  
  return uplift_final

# COMMAND ----------

# def OLD_infinity_extract_data_uc5(url, date):
#   # get token to authenticate to the API of Infinity
#   token = create_token_infinity(
#     username='jasper.penneman@element61.be', 
#     password='2UWzHR^9T3t4'
#   )
  
#   for i in range(0,3):
#     # get data from the Infinity report using the API - do this 3 times just to 'refresh'
#     response = requests.get(url=url, headers = {'Authorization': f'Basic {token}'})
#     data = response.json()
#     time.sleep(10)
    
#   # parse JSON response to usable dataframe
  
#   # OVERALL PART
  
#   # get raw cols first
#   raw_cols_globals = ['Campaign_ID_last_touch_session']
#   for i in data['dimensions'][0]['measures']:
#     raw_cols_globals.append(i['guid'])

#   clean_cols_globals = [col.replace(' ', '_') for col in raw_cols_globals]
#   clean_cols_globals = [col.replace('(', '') for col in clean_cols_globals]
#   clean_cols_globals = [col.replace(')', '') for col in clean_cols_globals]
  
#   # get data
#   values_all_globals = []
#   for global_row in data['dimensions']:
#     values_row = []
#     campaign_id = global_row['value'] # get campaign id
#     values_row.append(campaign_id)
#     for measure in global_row['measures']:
#       values_row.append(measure['value']) # get all other values

#     values_all_globals.append(values_row)

#   globals_pd = pd.DataFrame(values_all_globals, columns = clean_cols_globals)
#   globals_df = spark.createDataFrame(globals_pd).withColumn('Region', F.lit(None))
  
#   # REGION PART
  
#   # get raw cols first
#   raw_cols_regions = ['Campaign_ID_last_touch_session', 'Region']
#   for i in data['dimensions'][0]['dimensions'][0]['measures']:
#     raw_cols_regions.append(i['guid'])

#   clean_cols_regions = [col.replace(' ', '_') for col in raw_cols_regions]
#   clean_cols_regions = [col.replace('(', '') for col in clean_cols_regions]
#   clean_cols_regions = [col.replace(')', '') for col in clean_cols_regions]
  
#   # get data
#   values_all_regions = []
#   for global_row in data['dimensions']:
#     campaign_id = global_row['value'] # get campaign id
#     for region_row in global_row['dimensions']:
#       values_row = []
#       values_row.append(campaign_id)
#       region = region_row['value'] # get region
#       values_row.append(region)
#       for measure in region_row['measures']:
#         values_row.append(measure['value']) # get all other values

#       values_all_regions.append(values_row)

#   regions_pd = pd.DataFrame(values_all_regions, columns = clean_cols_regions)
#   regions_df = spark.createDataFrame(regions_pd)
  
#   final_df = (globals_df
#               .unionByName(regions_df)
#               # add date
#               .withColumn('date_str', F.lit(date))
#               .withColumn('date', F.to_date('date_str', 'yyyy/MM/dd'))
#               .withColumn('year', F.year('date'))
#               .withColumn('quarter', F.quarter('date'))
#               .withColumn('month', F.month('date'))
#               .withColumn('week', F.weekofyear('date'))
#               .withColumn('week', F.when( (F.col('week') > 50) & ((F.col('month') == 1)), 0).otherwise(F.col('week'))) # correction for beginning of January
#               .drop('date_str')
#               # add use case
#               .withColumn('use_case', F.lit('UC5'))
#              )
  
#   return final_df

# COMMAND ----------

def infinity_extract_data_uc5(url, date):
  # get token to authenticate to the API of Infinity
  token = create_token_infinity(
    username='jasper.penneman@element61.be', 
    password='zhMJkJUJ^Z2VW@bV'
  )

  for i in range(0,3):
    # get data from the Infinity report using the API - do this 3 times just to 'refresh'
    response = requests.get(url=url, headers = {'Authorization': f'Basic {token}'})
    data = response.json()
    time.sleep(10)

  # parse JSON response to usable dataframe

  # get raw cols first
  raw_cols = ['ELQ_Campaign_Name', 'Campaign_ID_last_touch_session']
  for i in data['dimensions'][0]['dimensions'][0]['measures']:
    raw_cols.append(i['guid'])

  clean_cols = [col.replace(' ', '_') for col in raw_cols]
  clean_cols = [col.replace('(', '') for col in clean_cols]
  clean_cols = [col.replace(')', '') for col in clean_cols]

  # get data
  values_all = []
  for global_row in data['dimensions']:
    campaign_name = global_row['value'] # get campaign name
    for detail_row in global_row['dimensions']:
      values_row = []
      values_row.append(campaign_name)
      campaign_id = detail_row['value'] # get campaign id
      values_row.append(campaign_id)
      for measure in detail_row['measures']:
        values_row.append(measure['value']) # get all other values

      values_all.append(values_row)

  dataframe_pd = pd.DataFrame(values_all, columns = clean_cols)
  dataframe_spark = spark.createDataFrame(dataframe_pd)

  final_df = (dataframe_spark
              # add date
              .withColumn('date_str', F.lit(date))
              .withColumn('date', F.to_date('date_str', 'yyyy/MM/dd'))
              .withColumn('year', F.year('date'))
              .withColumn('quarter', F.quarter('date'))
              .withColumn('month', F.month('date'))
              .withColumn('week', F.weekofyear('date'))
              .withColumn('week', F.when( (F.col('week') > 50) & ((F.col('month') == 1)), 0).otherwise(F.col('week'))) # correction for beginning of January
              .drop('date_str')
              # add use case
              .withColumn('use_case', F.lit('UC5'))
              # add NSC and audience using ELQ campaign name
              .withColumn('nsc', F.split(dataframe_spark['ELQ_Campaign_Name'], '_').getItem(0))
              .withColumn('audience', F.split(dataframe_spark['ELQ_Campaign_Name'], '_').getItem(5))
             )
  
  return final_df

# COMMAND ----------

def add_basic_kpis_uc5(df):
  df_new = (df
            .withColumn('unique_ctr', F.col('Unique_Email_Click') / F.col('Unique_Email_Send'))
            .withColumn('unique_open_rate', F.col('Unique_Email_Open') / F.col('Unique_Email_Send'))
            .withColumn('unique_click_to_open_rate', F.col('Unique_Email_Click') / F.col('Unique_Email_Open'))
            .withColumn('unique_configuration_started', F.col('Unique_Configurator'))
            .withColumn('unique_configuration_completed', F.col('Unique_Configurator_Completed'))
            .withColumn('unique_tdr_submit', F.col('Unique_TDR_Submits'))
            .withColumn('unique_raq_submit', F.col('Unique_RAQ_Submits')) 
           )
  
  return df_new

# COMMAND ----------

# MAGIC %md
# MAGIC #### Maxymiser

# COMMAND ----------

# mapping domain and NSC
data = [
  ('FR-CH','Switzerland','MS'),
  ('DE-CH','Switzerland','MS'),
  ('IT-CH','Switzerland','MS'),
  ('FR-BE','Belgium','MMB'),
  ('NL-BE','Belgium','MMB'),
  ('AT','Austria','MAG'),
  ('UK','United Kingdom','MMUK'),
  ('CZ','Czech Republic','MMCZ'),
  ('DE','Germany','MMD'),
  ('ES','Spain','MAE'),
  ('FR','France','MAF'),
  ('HR','Croatia','MMCR'),
  ('HU','Hungary','MMH'),
  ('IE','Ireland','MMIRL'),
  ('IT','Italy','MMI'),
  ('LU','Luxembourg','MMB-LUX'),
  ('PL','Poland','MMPOL '),
  ('PT','Portugal','MMP'),
  ('RO','Romania','MMRO'),
  ('SE','Sweden','MMS'),
  ('SI','Slovenia','MMSI'),
  ('SK','Slovakia','MMSK'),
  ('NL','Netherlands','MMNL'),
  ('NO','Norway','MMN'),
  ('DK',' Denmark','MMDK')
]
cols = ['domain','country','nsc']
domain_nsc_df = spark.createDataFrame(data,cols)

# COMMAND ----------

def maxymiser_extract_data_uc3_4(use_case, filepath):
  # read raw csv file
  raw_export = pd.read_csv(filepath, sep='\t')

  # create Spark dataframe
  maxymiser = spark.createDataFrame(raw_export)

  # cleaning: make 1 row per GenerationId and indicate via columns which action was done on this GenerationId
  clean_maxymiser_base = (maxymiser
                          .select('Generation Time and Date', 'Segmentation Rule', 'Element: Element1', 'PC: domain', 'Action Name', 'Generation Id', 'Visitor Id', 'Campaign Name')
                          .withColumn('date', to_date(to_timestamp('Generation Time and Date', 'MM/dd/yyyy HH:mm:ss')))
                          .withColumnRenamed('PC: domain', 'domain')
                          .join(domain_nsc_df, 'domain', 'left')
                          .fillna('Unknown', subset=['nsc', 'country'])
                          .withColumn('ActionCount', F.lit(1))
                          .withColumnRenamed('Generation Id', 'GenerationId')
                          .withColumnRenamed('Visitor Id', 'VisitorId')
                          .withColumnRenamed('Campaign Name', 'CampaignName')
                          .groupBy('date', 'Segmentation Rule', 'Element: Element1', 'nsc', 'GenerationId', 'VisitorId', 'CampaignName')
                          .pivot('Action Name').sum('ActionCount')
                          .withColumnRenamed('null', 'NoAction')
                          .withColumn('use_case', lit(use_case))
                          .withColumn('year', F.year('date'))
                          .withColumn('quarter', F.quarter('date'))
                          .withColumn('month', F.month('date'))
                          .withColumn('week', F.weekofyear('date'))
                          .withColumn('week', F.when( (F.col('week') > 50) & ((F.col('month') == 1)), 0).otherwise(F.col('week'))) # correction for beginning of January
                         )
  
  if use_case == 'UC3':
    clean_maxymiser_final = (clean_maxymiser_base
                             .withColumn('audience', F.when(col('Segmentation Rule') == 'Unsegmented Traffic', F.lit('Audience 1')).when(col('Segmentation Rule') == 'Rule1', F.lit('Audience 2')))
                             .withColumn('WebsiteVariant', F.when(col('Element: Element1') == 'Default', F.lit('element1:Default'))
                                                               .when(col('Element: Element1') == 'ConfigStarted', F.lit('element1:configstarted'))
                                                                   .when(col('Element: Element1') == 'ConfigCompleted', F.lit('element1:configcompleted')))
                             .drop('Segmentation Rule', 'Element: Element1')
                             .fillna(0, subset=['NoAction', 'ConfiguratorComplet', 'ConfiguratorStarted', 'Global_RAQ', 'Global_TDR', 'UC3_USC_Hero_Seen', 'UC3_USC_cta1', 'UC3_USC_cta2'])
                             .withColumn('NoAction', F.when(col('NoAction') > 1, 1).otherwise(col('NoAction')))
                             .withColumn('ConfiguratorComplet', F.when(col('ConfiguratorComplet') > 1, 1).otherwise(col('ConfiguratorComplet')))
                             .withColumn('ConfiguratorStarted', F.when(col('ConfiguratorStarted') > 1, 1).otherwise(col('ConfiguratorStarted')))
                             .withColumn('Global_RAQ', F.when(col('Global_RAQ') > 1, 1).otherwise(col('Global_RAQ')))
                             .withColumn('Global_TDR', F.when(col('Global_TDR') > 1, 1).otherwise(col('Global_TDR')))
                             .withColumn('UC3_USC_Hero_Seen', F.when(col('UC3_USC_Hero_Seen') > 1, 1).otherwise(col('UC3_USC_Hero_Seen')))
                             .withColumn('UC3_USC_cta1', F.when(col('UC3_USC_cta1') > 1, 1).otherwise(col('UC3_USC_cta1')))
                             .withColumn('UC3_USC_cta2', F.when(col('UC3_USC_cta2') > 1, 1).otherwise(col('UC3_USC_cta2')))
                            )
    
  elif use_case == 'UC4':
    clean_maxymiser_final = (clean_maxymiser_base
                             .withColumn('audience', lit(None))
                             .withColumn('WebsiteVariant', F.when(col('Element: Element1') == 'Default', F.lit('element1:Default'))
                                                              .when(col('Element: Element1') == 'highestModel', F.lit('element1:highestmodel')))
                             .drop('Segmentation Rule', 'Element: Element1')
                             .fillna(0, subset=['NoAction', 'ConfiguratorComplet', 'ConfiguratorStarted', 'Global_RAQ', 'Global_TDR', 'UC4_USC_CC', 'UC4_USC_Hero_Seen', 'UC4_USC_Offer'])
                             .withColumn('NoAction', F.when(col('NoAction') > 1, 1).otherwise(col('NoAction')))
                             .withColumn('ConfiguratorComplet', F.when(col('ConfiguratorComplet') > 1, 1).otherwise(col('ConfiguratorComplet')))
                             .withColumn('ConfiguratorStarted', F.when(col('ConfiguratorStarted') > 1, 1).otherwise(col('ConfiguratorStarted')))
                             .withColumn('Global_RAQ', F.when(col('Global_RAQ') > 1, 1).otherwise(col('Global_RAQ')))
                             .withColumn('Global_TDR', F.when(col('Global_TDR') > 1, 1).otherwise(col('Global_TDR')))
                             .withColumn('UC4_USC_CC', F.when(col('UC4_USC_CC') > 1, 1).otherwise(col('UC4_USC_CC')))
                             .withColumn('UC4_USC_Hero_Seen', F.when(col('UC4_USC_Hero_Seen') > 1, 1).otherwise(col('UC4_USC_Hero_Seen')))
                             .withColumn('UC4_USC_Offer', F.when(col('UC4_USC_Offer') > 1, 1).otherwise(col('UC4_USC_Offer')))
                            )
      
  clean_maxymiser_final.display()

  return clean_maxymiser_final
