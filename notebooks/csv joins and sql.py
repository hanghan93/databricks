# Databricks notebook source
# MAGIC %md
# MAGIC Write a query that:
# MAGIC 
# MAGIC 1. Returns the top 5 users and their emails by total gross orders (only successful orders) in the last 1 year by active vendor type with ordering by the oldest user on the platform
# MAGIC 2. Vendor ID and vendor type that did the most in amt in non-cancelled orders in the last 3 years
# MAGIC 3. Vendor ID and vendor type with the most amt in any in cancelled orders
# MAGIC 
# MAGIC Python: implement query 1 in Python using only the standard library. No spark, pandas, etc. Imagine you have a CSV for each table with the same headers.

# COMMAND ----------

# MAGIC %python
# MAGIC # import the required modules
# MAGIC from datetime import datetime,timedelta
# MAGIC from collections import Counter,defaultdict
# MAGIC from itertools import groupby
# MAGIC import operator
# MAGIC import csv

# COMMAND ----------

# MAGIC %python
# MAGIC order = csv.DictReader(open('/dbfs/mnt/datalake/practice/FCT_ORDERS.csv'),delimiter=';')
# MAGIC user = csv.DictReader(open('/dbfs/mnt/datalake/practice/DIM_USERS.csv'),delimiter=';')
# MAGIC vendor = csv.DictReader(open('/dbfs/mnt/datalake/practice/DIM_VENDORS.csv'),delimiter=';')

# COMMAND ----------

# MAGIC %python
# MAGIC def merge(order, user, vendor):
# MAGIC     user = list(user)
# MAGIC     vendor = list(vendor)
# MAGIC     matchedlist = []
# MAGIC     ddict = defaultdict(int)
# MAGIC     for orderline in order:
# MAGIC         for userline in user:
# MAGIC                 if (orderline['USER_ID'] == userline['ID']
# MAGIC                     and orderline['STATUS'] == '0' 
# MAGIC                     and datetime.strptime(orderline['PLACED_AT'], '%d-%m-%Y %H:%M:%S') > (datetime.now() - timedelta(days=365))
# MAGIC                     and any(orderline['VENDOR_ID'] == v['ID'] and v['IS_ACTIVE'] == 'true' for v in vendor)):
# MAGIC                     ddict[orderline['USER_ID']] += 1
# MAGIC                     matchedlist.append(userline)
# MAGIC     return ddict,matchedlist

# COMMAND ----------

# MAGIC %python
# MAGIC ddict,ls = merge(order,user,vendor)

# COMMAND ----------

# MAGIC %python
# MAGIC for i in dict(Counter(ddict).most_common(5)):
# MAGIC     for k in ls:
# MAGIC         if int(i) == int(k['ID']):
# MAGIC             print(k['ID'],k['NAME'],k['EMAIL'],k['ADDED_AT'],sep=';')
# MAGIC             break

# COMMAND ----------

# MAGIC %python
# MAGIC orderdf = spark.read.format('csv').options(sep=';',header='true',inferSchema='true').load('/mnt/datalake/practice/FCT_ORDERS.csv')
# MAGIC userdf = spark.read.format('csv').options(sep=';',header='true',inferSchema='true').load('/mnt/datalake/practice/DIM_USERS.csv')
# MAGIC vendordf = spark.read.format('csv').options(sep=';',header='true',inferSchema='true').load('/mnt/datalake/practice/DIM_VENDORS.csv')

# COMMAND ----------

orderdf.createOrReplaceTempView("fct_order")

# COMMAND ----------

userdf.createOrReplaceTempView("dim_user")
vendordf.createOrReplaceTempView("dim_vendor")

# COMMAND ----------

# MAGIC %md
# MAGIC Returns the top 5 users and their emails by total gross orders (only successful orders) in the last 1 year by active vendor type with ordering by the oldest user on the platform

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT du.ID, du.NAME,du.ADDED_AT, COUNT(*) cnt FROM fct_order fo 
# MAGIC INNER JOIN dim_user du ON fo.USER_ID = du.ID
# MAGIC WHERE fo.STATUS = 0 
# MAGIC       AND to_date(fo.PLACED_AT, 'dd-MM-yyyy HH:mm:SS') > DATE_SUB(CURRENT_TIMESTAMP, 365)
# MAGIC       AND EXISTS (SELECT ID FROM dim_vendor dv WHERE dv.IS_ACTIVE = true AND dv.ID = fo.VENDOR_ID)
# MAGIC GROUP BY du.ID, du.NAME,du.ADDED_AT
# MAGIC ORDER BY cnt DESC, to_date(du.ADDED_AT, 'dd-MM-yyyy') ASC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dv.ID, dv.TYPE, SUM(fo.AMT) AMT FROM fct_order fo
# MAGIC INNER JOIN dim_vendor dv ON dv.ID = fo.VENDOR_ID
# MAGIC WHERE fo.STATUS != -1 
# MAGIC       AND to_date(fo.PLACED_AT, 'dd-MM-yyyy HH:mm:SS') >= DATE_SUB(CURRENT_TIMESTAMP, 365*3)
# MAGIC GROUP BY dv.ID,dv.TYPE
# MAGIC ORDER BY AMT DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Vendor ID and vendor type with the most in AMT in cancelled orders
# MAGIC SELECT dv.ID, dv.TYPE, SUM(fo.AMT) AMT FROM fct_order fo
# MAGIC INNER JOIN dim_vendor dv ON dv.ID = fo.VENDOR_ID
# MAGIC WHERE fo.STATUS = -1
# MAGIC GROUP BY dv.ID,dv.TYPE
# MAGIC ORDER BY AMT DESC
# MAGIC LIMIT 1

# COMMAND ----------

