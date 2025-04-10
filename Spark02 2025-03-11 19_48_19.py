# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, when, lit
from datetime import datetime

# COMMAND ----------

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
display(df)

# COMMAND ----------

df.withColumn('salary',col('salary').cast('Integer')).show()

# COMMAND ----------

df.withColumn('salary',col('salary')*100).show()

# COMMAND ----------

df.withColumn('Cpycolumn',col('salary')*-1).show()

# COMMAND ----------

df.withColumnRenamed('gender','sex')\
    .show(truncate=Falseb)

# COMMAND ----------

df1=df.withColumn('Country',lit('USA'))
df1.printSchema()

# COMMAND ----------

df2 = df.withColumn("Country", lit("USA")) \
   .withColumn("anotherColumn",lit("anotherValue"))
df2.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField 
from pyspark.sql.types import StringType, IntegerType, ArrayType
data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]
        
schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])

df = spark.createDataFrame(data = data, schema = schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

df.filter(df.state=='OH').show()

# COMMAND ----------

df.filter(df.state!='OH').show()

# COMMAND ----------

df.filter(~(df.state=='OH')).show()

# COMMAND ----------

from pyspark.sql.functions import col
df.filter(col('state')=='OH')\
    .show()

# COMMAND ----------

df.filter("gender=='M'").show()

# COMMAND ----------

df.filter("gender!='M'").show()

# COMMAND ----------

df.filter("gender<>'M'").show()

# COMMAND ----------

df.filter((df.state=='OH') & (df.gender=='M'))\
    .show()

# COMMAND ----------

li=['OH','CA','DE']
df.select(df.state.isin(li)).show()

# COMMAND ----------

df.show()

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

distinctDF=df.distinct()
print('Disctint count: '+str(distinctDF.count()))
distinctDF.show(truncate=False)

# COMMAND ----------

df2=df.dropDuplicates()
print('Distinct count:'+str(df2.count()))
df2.show()

# COMMAND ----------

df3=df.dropDuplicates(['department','salary'])
print('Distinct count of department salary: '+str(df3.count()))
df3.show()

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc,desc

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Raman","Finance","CA",99000,40,24000), \
    ("Scott","Finance","NY",83000,36,19000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns= ["employee_name","department","state","salary","age","bonus"]

df = spark.createDataFrame(data = simpleData, schema = columns)

df.printSchema()
df.show(truncate=False)

# COMMAND ----------

df.sort('department','state').show()

# COMMAND ----------

df.sort(col('department'),col('state')).show()

# COMMAND ----------

df.orderBy('department','state').show()

# COMMAND ----------

df.orderBy(col('department'),col('state')).show()

# COMMAND ----------

df.sort(df.department.asc(),df.state.asc()).show()

# COMMAND ----------

df.sort(col('department').asc(),col('state').asc()).show()

# COMMAND ----------

df.orderBy(col('department').asc(),col('state').asc()).show()

# COMMAND ----------

df.sort(df.department.asc(),df.state.desc()).show(truncate=False)
df.sort(col("department").asc(),col("state").desc()).show(truncate=False)
df.orderBy(col("department").asc(),col("state").desc()).show(truncate=False)

df.createOrReplaceTempView("EMP")
spark.sql("select employee_name,department,state,salary,age,bonus from EMP ORDER BY department asc").show(truncate=False)

# COMMAND ----------

