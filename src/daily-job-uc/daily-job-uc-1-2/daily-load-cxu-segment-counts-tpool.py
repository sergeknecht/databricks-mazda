# Databricks notebook source
# MAGIC %run ./../functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily ingestion of CXU segment counts
# MAGIC
# MAGIC **Goal of this notebook:** Explain and implement the daily ingestion of CXU segment counts using the Segment API.

# COMMAND ----------

# get table with all CXU segments
response = cxu_get_segments()
segments_df = pd.DataFrame.from_records(response['items'])[['name', 'uniqueId']] # convert JSON response to table

# collect correct segment id's for UC1&2
segment_ids = []
for index, row in segments_df.iterrows():
  name_split = row['name'].split('_')
  try:
    if (name_split[1] == 'UC1' or name_split[1] == 'UC2') and row['name'] not in ('MME_UC1_A2_Suppression_Deceased', 'MAG_UC1_A1_Suppression_ExistingPrivateCustomers_withBKUUID', 'MMD_UC1_A1_Suppression_ExistingPrivateCustomers_BKUUID', 'MME_UC2_A1_Suppression_Leads_60days', 'MMB_UC1_A1_FB_Suppression_ExistingPrivateCustomers', 'MMB_UC2_A1_FB_Suppression_Leads', 'MMB-LUX_UC1_A1_FB_Suppression_ExistingPrivateCustomers', 'MMB-LUX_UC2_A1_FB_Suppression_Leads', 'MMCR_UC1_A1_FB_Suppression_ExistingPrivateCustomers', 'MMCR_UC2_A1_FB_Suppression_Leads', 'MMDK_UC1_A1_FB_Suppression_ExistingPrivateCustomers', 'MMDK_UC2_A1_FB_Suppression_Leads', 'MMIRL_UC1_A1_FB_Suppression_ExistingPrivateCustomers', 'MMIRL_UC2_A1_FB_Suppression_Leads', 'MMNL_UC1_A1_FB_Suppression_ExistingPrivateCustomers', 'MMNL_UC2_A1_FB_Suppression_Leads', 'MMP_UC1_A1_FB_Suppression_ExistingPrivateCustomers', 'MMP_UC2_A1_FB_Suppression_Leads', 'MMRO_UC1_A1_FB_Suppression_ExistingPrivateCustomers', 'MMRO_UC2_A1_FB_Suppression_Leads', 'MMSK_UC1_A1_FB_Suppression_ExistingPrivateCustomers', 'MMSK_UC2_A1_FB_Suppression_Leads'): #2nd place in name should be 'UC1' or 'UC2' but exclude some segments (hard-coded)
      segment_ids.append(row['uniqueId'])
  except:
    continue

segments_df = segments_df[segments_df['uniqueId'].isin(segment_ids)]
segments_df["segment_count_cxu"] = -3.0
display(segments_df)


# COMMAND ----------

from multiprocessing.pool import ThreadPool

# task executed in a worker thread
def task(identifier, segment_id):  
    # print(f'Task {identifier} executing segment count: {segment_id}')
    # block for a moment
    # result = dbutils.notebook.run("load_table_2", 120, {"catalog_name": catalog_name, "schema": schema, "table_name": table_name, "scope": scope})
    # return the resy
    try:
      # get refreshed count for the segment
      count = cxu_refresh_segment_count(segment_id) 
      # # if count is -1, then add segment to a list for further processing
      # # else: create new row for the cxu_segment_counts table
      # if count == -1:
      #   # bad_segments_round_1.append(unique_id)        
      #   result = -1
      # else:
      #   # new_count = create_new_count_row(unique_id, segments_df, count)
      #   # add the new row to the overall list
      #   # new_counts.append(new_count)
      #   result = count
    except:
      # if the POST request failed (due to any reason), then add segment to a list for further processing
      # print('failed')
      # print(f'Task {identifier} failed')
      # bad_segments_round_1.append(unique_id)
      return (identifier, -2)
      
    return (identifier, count) # count cn be real number or -1 (still processing count)

# COMMAND ----------

task_params = [ (segment_id, segment_id) for segment_id in segment_ids]

pp.pprint(task_params[0:3])

# COMMAND ----------

# MAGIC %md
# MAGIC it seems that we need to wait a while for the count to be finished. upto now the app runs several loops including 1 with a wait 30 minutes
# MAGIC
# MAGIC this logic can be improved by in a first step get all segment ids and save it to table
# MAGIC then each 30 minutes try a retrieve of all failed segments, but this will allow the cluster to shutdown in between and reduce costs

# COMMAND ----------

print("segments#:" + str(len(task_params)))

# COMMAND ----------

cpu_count = 8

result_counts = []
results_succeeded = 0
task_params_next_run = []

with ThreadPool(cpu_count) as pool:
    results = pool.starmap_async(task, task_params)
    # iterate results
    for result in results.get():
        result_counts.append(result)
        print(f'Got result: {result}')
        identifier, count = result
        if count >= 0:
            results_succeeded += 1

# COMMAND ----------

print("segments counted#:" + str(results_succeeded) + " of " + str(len(segment_ids)))
pp.pprint(result_counts[0:60])

# COMMAND ----------

segment_counts_df = pd.DataFrame.from_records(result_counts, columns=['uniqueId', 'segment_count_cxu']) # convert JSON response to table
segment_counts_df = segment_counts_df[segment_counts_df['segment_count_cxu']>=0]
segment_counts_df

# COMMAND ----------

test = segments_df.set_index('uniqueId').join(segment_counts_df.set_index('uniqueId'), rsuffix='_new')
test['segment_count_cxu'] = test['segment_count_cxu_new'].combine_first(test['segment_count_cxu'])
test.reset_index(inplace=True)
test = test[['name', 'uniqueId', 'segment_count_cxu']]
segments_df = test
segments_df


# COMMAND ----------

task_params = [ (segment_id, segment_id) for segment_id in test[test['segment_count_cxu']<0]['uniqueId']]

pp.pprint(task_params[0:3])
print("segments#:" + str(len(task_params)))

# COMMAND ----------

segments_df_task_params[segments_df_task_params['segment_count_cxu']>=0]

# COMMAND ----------

segments_df_task_params

# COMMAND ----------

# set segment_cxu_count only in cases where the result is not -1
for row in result_counts:
    if row[1] >= 0:
        segments_df_task_params.loc[segments_df_task_params['uniqueId'] == row[0], 'segment_cxu_count'] = row[1]


# COMMAND ----------

dbutils.notebook.exit("we want no further execution")

# COMMAND ----------

# determine batches
nr_full_batches = len(segment_ids) // 10
batches = list(range(1,nr_full_batches+2))

# refresh count for all segments by using batches and waiting period if necessary
new_counts = []
still_minus_1 = []
index_start = 0
for batch in batches:
  print('BATCH: ', batch)
  if batch != batches[-1]:
    segments_batch = segment_ids[index_start:batch*10]
  else:
    segments_batch = segment_ids[index_start:]
  
  bad_segments_round_1 =[]
  bad_segments_round_2 =[]
  bad_segments_round_3 =[]
  
  # ROUND 1: do POST request for all segments in the batch
  for unique_id in segments_batch:
    try:
      # get refreshed count for the segment
      count = cxu_refresh_segment_count(unique_id) 
      
      # if count is -1, then add segment to a list for further processing
      # else: create new row for the cxu_segment_counts table
      if count == -1:
        bad_segments_round_1.append(unique_id)        
      else:
        new_count = create_new_count_row(unique_id, segments_df, count)
        # add the new row to the overall list
        new_counts.append(new_count)
    
    except:
      # if the POST request failed (due to any reason), then add segment to a list for further processing
      print('failed')
      bad_segments_round_1.append(unique_id)
      
  print('bad_segments_round_1: ', bad_segments_round_1)
  
  # ROUND 2: do a second POST request for the segments that had -1 count in the previous POST request
  for unique_id in bad_segments_round_1:
    try:
      # get refreshed count for the segment
      count = cxu_refresh_segment_count(unique_id) 
      
      # if count is -1, then add segment to a list for further processing
      # else: create new row for the cxu_segment_counts table
      if count == -1:
        bad_segments_round_2.append(unique_id)        
      else:
        new_count = create_new_count_row(unique_id, segments_df, count)
        # add the new row to the overall list
        new_counts.append(new_count)
    
    except:
      # if the POST request failed (due to any reason), then add segment to a list for further processing
      print('failed')
      bad_segments_round_2.append(unique_id)
    
  print('bad_segments_round_2: ', bad_segments_round_2)
  
  # ROUND 3: do a third POST request for the segments that still had -1 count in the previous POST request - do this after waiting 30min first
  # note: only if any segments left in bad_segments_round_2
  if len(bad_segments_round_2) > 0:
    time.sleep(1800) # wait 30min
    for unique_id in bad_segments_round_2:
      try:
        # get refreshed count for the segment
        count = cxu_refresh_segment_count(unique_id)

        # if count is -1, then add segment to a list for further processing
        # else: create new row for the cxu_segment_counts table  
        if count == -1:
          bad_segments_round_3.append(unique_id)
          still_minus_1.append(unique_id)

        else:
          new_count = create_new_count_row(unique_id, segments_df, count)
          # add the new count to the overall list
          new_counts.append(new_count)

      except:
        # if the POST request failed (due to any reason), then add segment to a list for further processing
        print('failed')
        bad_segments_round_3.append(unique_id)
        still_minus_1.append(unique_id)
        
  print('bad_segments_round_3: ', bad_segments_round_3)
 
  index_start = batch*10
  
print('still_minus_1: ', still_minus_1)
  
# LAST ROUND: for segments that still have -1: do a final post and add count to table (whether it's -1 are not)
for unique_id in still_minus_1:
    try:
      # get refreshed count for the segment
      count = cxu_refresh_segment_count(unique_id)
      
      # create new row for the cxu_segment_counts table (whether the refreshed count is -1 or not)
      new_count = create_new_count_row(unique_id, segments_df, count)

    except:
      # if the POST request failed (due to any reason): create new row for the cxu_segment_counts table using None as 'refreshed count'
      print('failed')
      new_count = create_new_count_row(unique_id, segments_df, count=None)

    # add the new count to the overall list
    new_counts.append(new_count)

# COMMAND ----------

# create table with new counts
new_counts_pd = pd.DataFrame(new_counts, columns =['name', 'segment_id_cxu', 'nsc', 'use_case', 'segment_count_cxu'])
new_counts_df = spark.createDataFrame(new_counts_pd)

# if first load: put all counts in Delta table without checking history
# else: check history to fix 'failed' counts
try:
  # SUCCES
  # get segments with 'good' count
  success = new_counts_df.filter((F.col('segment_count_cxu').isNotNull()) & (F.col('segment_count_cxu') != -1)).withColumn('status', F.lit('success - count was renewed'))

  # FAIL: if count is -1 or null, then use historic data to find a 'good' count

  # get historic data with 'good' count
  historic_cxu_segment_counts_good = spark.sql("""select * from uc_reporting.cxu_segment_counts where segment_count_cxu is not null and segment_count_cxu != -1""")

  # FAIL 1: COUNT -1
  # get segments with count = -1
  minus = new_counts_df.filter(F.col('segment_count_cxu') == -1)
  minus_segment_ids = [data[0] for data in minus.select('segment_id_cxu').collect()]

  # get historic 'good' counts for these segments and find most recent date
  minus_segments_good = historic_cxu_segment_counts_good.filter(F.col('segment_id_cxu').isin(minus_segment_ids))
  minus_segments_good_date = minus_segments_good.groupBy('name').agg(F.max('date').alias('date'))

  # fix with historic data
  minus_segments_fixed = historic_cxu_segment_counts_good.join(minus_segments_good_date, ['name', 'date']).select('name','segment_id_cxu','nsc','use_case','segment_count_cxu').withColumn('status', F.lit('count was -1, but fixed via historic data'))
  minus_segments_fixed_ids = [data[0] for data in minus_segments_fixed.select('segment_id_cxu').collect()]

  # if not fixed with historic data: keep the -1
  minus_segments_not_fixed = minus.filter(~F.col('segment_id_cxu').isin(minus_segments_fixed_ids)).withColumn('status', F.lit('count was -1 and could not be fixed via historic data'))

  # FAIL 2: COUNT NULL
  # get segments with count = NULL
  null = new_counts_df.filter(F.col('segment_count_cxu').isNull())
  null_segment_ids = [data[0] for data in null.select('segment_id_cxu').collect()]

  # get historic 'good' counts for these segments and find most recent date
  null_segments_good = historic_cxu_segment_counts_good.filter(F.col('segment_id_cxu').isin(null_segment_ids))
  null_segments_good_date = null_segments_good.groupBy('name').agg(F.max('date').alias('date'))

  # fix with historic data
  null_segments_fixed = historic_cxu_segment_counts_good.join(null_segments_good_date, ['name', 'date']).select('name','segment_id_cxu','nsc','use_case','segment_count_cxu').withColumn('status', F.lit('count was null, but fixed via historic data'))
  null_segments_fixed_ids = [data[0] for data in null_segments_fixed.select('segment_id_cxu').collect()]

  # if not fixed with historic data: keep the null
  null_segments_not_fixed = null.filter(~F.col('segment_id_cxu').isin(null_segments_fixed_ids)).withColumn('status', F.lit('count was null and could not be fixed via historic data'))
  
  # concatenate new counts again
  new_counts_concat = (success
                       .unionByName(minus_segments_fixed)
                       .unionByName(minus_segments_not_fixed)
                       .unionByName(null_segments_fixed)
                       .unionByName(null_segments_not_fixed)
                      )
  
except:
  new_counts_concat = new_counts_df.withColumn('status', F.lit('initial load'))

# COMMAND ----------

# add date of today
today = datetime.today()
today_date = today.strftime('%Y-%m-%d')
new_counts_final = (new_counts_concat
                    .withColumn('date_str', F.lit(today_date))
                    .withColumn('date', F.to_date('date_str', 'yyyy-MM-dd'))
                    .withColumn('year', F.year('date'))
                    .withColumn('quarter', F.quarter('date'))
                    .withColumn('month', F.month('date'))
                    .withColumn('week', F.weekofyear('date'))
                    .withColumn('week', F.when( (F.col('week') > 50) & ((F.col('month') == 1)), 0).otherwise(F.col('week'))) # correction for beginning of January
                    .drop('date_str')
                    .withColumn('segment_count_cxu', F.col('segment_count_cxu').cast('double'))
                   )
new_counts_final.display()

# COMMAND ----------

# write to DBFS in Delta format
new_counts_final.write.mode('append').format('delta').save('/uc_reporting/uc_1_2/cxu_segment_counts')

# COMMAND ----------

# MAGIC %sql
# MAGIC --create Delta table
# MAGIC CREATE TABLE IF NOT EXISTS uc_reporting.cxu_segment_counts
# MAGIC USING DELTA
# MAGIC LOCATION '/uc_reporting/uc_1_2/cxu_segment_counts'
