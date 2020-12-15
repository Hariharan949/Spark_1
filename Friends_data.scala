// Databricks notebook source
// /FileStore/tables/FriendsData.csv

val data = sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

val removeheader = data.filter(line => !line.contains("name"))

removeheader.take(10)

// COMMAND ----------

val friendrdd = removeheader.map(x => ( x.split(",")(2).toInt, (1,x.split(",")(3).toDouble) ))

friendrdd.take(5)

// COMMAND ----------

val reducedrdd = friendrdd.reduceByKey( (x,y) => (x._1 + y._1 , x._2 + y._2) )
reducedrdd.collect()

// COMMAND ----------

val finalrdd = reducedrdd.mapValues( data => data._2 / data._1 )
finalrdd.collect()

// COMMAND ----------

for((age, avg_friends) <- finalrdd.collect() ) println(age + " : " + avg_friends)

// COMMAND ----------

val maxfriendrdd = removeheader.map(x => ( x.split(",")(2).toInt, x.split(",")(3).toDouble ))

maxfriendrdd.take(5)

// COMMAND ----------

val maxrdd = maxfriendrdd.reduceByKey(math.max(_, _))

maxrdd.collect()

// COMMAND ----------


