# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ray on Databricks starter notebook
# MAGIC
# MAGIC This notebook illustrates how to: 
# MAGIC * Create a Ray cluster on Databricks 
# MAGIC * Run a simple Ray application 
# MAGIC * Shut down a Ray cluster
# MAGIC
# MAGIC ## Requirements
# MAGIC
# MAGIC Databricks Runtime 12.0 or above.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Install Ray

# COMMAND ----------

# MAGIC %pip install ray[default]>=2.3.0 >/dev/null

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create a Ray cluster
# MAGIC
# MAGIC Set up a Ray cluster with 2 Ray worker nodes. Each worker node is assigned with 4 GPU cores.
# MAGIC Once the cluster is launched, you can click the link "Open Ray Cluster Dashboard in a new tab" to view the Ray cluster dashboard.

# COMMAND ----------

from ray.util.spark import setup_ray_cluster, shutdown_ray_cluster, MAX_NUM_WORKER_NODES


setup_ray_cluster(
  num_worker_nodes=2,
  num_cpus_per_node=4,
  collect_log_to_path="/dbfs/tmp/raylogs",
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Show the resources of the Ray cluster you just created.

# COMMAND ----------

import ray
ray.init()
ray.cluster_resources()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run Ray application
# MAGIC
# MAGIC The following code runs a simple Ray application that creates many Ray tasks on the Ray cluster you just created.

# COMMAND ----------

import ray
import random
import time
import math
from fractions import Fraction

@ray.remote
def pi4_sample(sample_count):
    """pi4_sample runs sample_count experiments, and returns the 
    fraction of time it was inside the circle. 
    """
    in_count = 0
    for i in range(sample_count):
        x = random.random()
        y = random.random()
        if x*x + y*y <= 1:
            in_count += 1
    return Fraction(in_count, sample_count)

SAMPLE_COUNT = 1000 * 1000
start = time.time() 
future = pi4_sample.remote(sample_count = SAMPLE_COUNT)
pi4 = ray.get(future)
end = time.time()
dur = end - start
print(f'Running {SAMPLE_COUNT} tests took {dur} seconds')

pi = pi4 * 4
print(float(pi))

# COMMAND ----------

FULL_SAMPLE_COUNT = 2000 * 1000 * 1000
BATCHES = int(FULL_SAMPLE_COUNT / SAMPLE_COUNT)
print(f'Doing {BATCHES} batches')
results = []
for _ in range(BATCHES):
    results.append(pi4_sample.remote(sample_count = SAMPLE_COUNT))
output = ray.get(results)

pi = sum(output)*4/len(output)
print(float(pi))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Shut down the Ray cluster you just created.

# COMMAND ----------

shutdown_ray_cluster()
