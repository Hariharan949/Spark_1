// Databricks notebook source
// /FileStore/tables/Property_data.csv


// COMMAND ----------

val data = sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

val removeheader = data.filter(line => !line.contains("Price"))

removeheader.take(10)

// COMMAND ----------

val roomrdd = removeheader.map(x => ( x.split(",")(3).toInt, (1,x.split(",")(2).toDouble) ))

roomrdd.take(5)

// COMMAND ----------

// example how it works

roomrdd.map( x => x._1).take(5)

// COMMAND ----------

val reducedrdd = roomrdd.reduceByKey( (x,y) => (x._1 + y._1 , x._2 + y._2) )
reducedrdd.collect()

// COMMAND ----------

val finalrdd = reducedrdd.mapValues( data => data._2 / data._1)
finalrdd.collect()

// COMMAND ----------

for((bedroom, avg) <- finalrdd.collect() ) println(bedroom + " : " + avg)

// COMMAND ----------

finalrdd.saveAsTextFile("PropertyFinal.csv")

// COMMAND ----------


