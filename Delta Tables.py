# Databricks notebook source
dbutils.fs.ls("/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/")

# COMMAND ----------

df1=spark.read.format('csv').option('header',True).load('/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/population.csv', name='population.csv')

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.coalesce(1).write.format('json').save('/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/json1')

# COMMAND ----------

dbutils.fs.head('/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/json1/part-00000-tid-146251851664323738-5233d27b-d0d8-43b6-9a21-2435a86dfd91-17-1-c000.json')

# COMMAND ----------

df_test=spark.read.format('json').option('multiline',True).option('escape','"').load('/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/json1/part-00000-tid-146251851664323738-5233d27b-d0d8-43b6-9a21-2435a86dfd91-17-1-c000.json')
df_test.display()

# COMMAND ----------

dbutils.fs.head("/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/json/part-00000-tid-4235068839583056532-b6672d75-1e75-497f-bdda-77d639aad08a-2-1-c000.json")

# COMMAND ----------

df2=spark.read.format('json').option('multiline',True).load("/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/json/part-00000-tid-4235068839583056532-b6672d75-1e75-497f-bdda-77d639aad08a-2-1-c000.json")

# COMMAND ----------

df2=spark.read.format("json").option("multiline",True).load("/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/json/part-00000-tid-4235068839583056532-b6672d75-1e75-497f-bdda-77d639aad08a-2-1-c000.json")
df2.display()

# COMMAND ----------

df2.show()

# COMMAND ----------

dbutils.fs.ls("/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/")

# COMMAND ----------

dbutils.fs.ls("/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/stage")

# COMMAND ----------

df1 = spark.read.format("json").option('multiline',True).load("dbfs:/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/json_day2.json")
df1.show()

# COMMAND ----------

df1.write.format('delta').mode('overwrite').save("/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/stage/")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table stage1
# MAGIC (
# MAGIC id string,
# MAGIC name string,
# MAGIC state string
# MAGIC )
# MAGIC location "/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/stage/"

# COMMAND ----------

# MAGIC %sql
# MAGIC desc formatted stage
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stage

# COMMAND ----------

# MAGIC %sql
# MAGIC create table final2
# MAGIC (
# MAGIC id string,
# MAGIC name string,
# MAGIC state string
# MAGIC )
# MAGIC location '/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/stage/'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from final2

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/syamsundhar957@gmail.com/syam/stage/')

# COMMAND ----------

# MAGIC %sql
# MAGIC desc formatted final

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into final 
# MAGIC using stage 
# MAGIC on final.id=stage.id when matched and final.state != stage.state then update set * 
# MAGIC when not matched then insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from final

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.select('id','name').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into stage1 values('101','srinivas','hyderabad')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stage1

# COMMAND ----------

spark.sql("select * from stage1").show()

# COMMAND ----------


