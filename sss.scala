// Databricks notebook source
// /FileStore/tables/airports.xlsx

// COMMAND ----------

val data = sc.textFile("/FileStore/tables/airports.csv")

// COMMAND ----------

data.map( x => x.split(",")).filter(x => x(3) == "United States").take(4)


// COMMAND ----------

data.map( x =>{ 
  val d = x.split(",")
  d(0) +", "+d(2)}).take(4)

// COMMAND ----------

val numdata= sc.textFile("/FileStore/tables/numbers.csv")

// COMMAND ----------

val g=numdata.map(x => x.toInt)

// COMMAND ----------

g.take(10)


// COMMAND ----------

g.sum

// COMMAND ----------

def isPrime2(i :Int) : Boolean = {
     if (i <= 1)     false
     else if (i == 2)
       true
     else
       !(2 to (i-1)).exists(x => i % x == 0) 
}

// COMMAND ----------

val primedata = g.map(x => (x,isPrime2(x)))

// COMMAND ----------

primedata.take(10)

// COMMAND ----------

primedata.count()

// COMMAND ----------

val namelist =List("Hari 1999","Ravi 1997","Sai 1998")

// COMMAND ----------

val names= namelist.map(x => (x.split(" ")(0),x.split(" ")(1).toInt)).take(2)

// COMMAND ----------

primedata.filter(x => x._2==true).map(x => x._1).sum

// COMMAND ----------

data.take(2)

// COMMAND ----------

val header= data.first
val data1= data.filter(line => line!=header)
data1.take(2)

// COMMAND ----------

data1.map(x => (x.split(",")(3),x.split(",")(11).toLowerCase)).take(2)

// COMMAND ----------

names.take(2)

// COMMAND ----------

val properties= sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

properties.take(3)

// COMMAND ----------

val header= properties.first
val pdata= properties.filter(line => line!=header)
pdata.take(2)

// COMMAND ----------

val prop_data= pdata.map(x => (x.split(",")(3),(1,x.split(",")(2).toDouble)))

// COMMAND ----------


import org.apache.spark.SparkContext._

// COMMAND ----------

val d_data=prop_data.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))

// COMMAND ----------

d_data.collect()

// COMMAND ----------

val f_data = d_data.mapValues(x => x._2/x._1)

// COMMAND ----------

f_data.collect()

// COMMAND ----------

val frnds= sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

val header= frnds.first
val frndsdata= frnds.filter(line => line!=header)
frndsdata.take(2)

// COMMAND ----------

val frnds_data= frndsdata.map(x => (x.split(",")(2),(1,x.split(",")(3).toDouble)))

// COMMAND ----------

val frnds_d=frnds_data.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))

// COMMAND ----------

frnds_d.take(5)

// COMMAND ----------

val avg_ages= frnds_d.mapValues(x=> (x._2/x._1*100).round/100.0)

// COMMAND ----------

avg_ages.collect()

// COMMAND ----------

val max_ages= frndsdata.map(x => (x.split(",")(2),x.split(",")(3).toDouble))

// COMMAND ----------

max_ages.take(2)

// COMMAND ----------

val maxKey = max_ages.reduceByKey((x,y) => math.max(x, y))

// COMMAND ----------

maxKey.collect()

// COMMAND ----------


