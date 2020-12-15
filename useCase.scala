// Databricks notebook source
// /FileStore/tables/ratings-1.csv

// creating an rdd now from external datasets

val data = sc.textFile("FileStore/tables/ratings-1.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

val ratingdata = data.map(x => x.split(",")(2))

ratingdata.collect()

// COMMAND ----------

// count how many times each specific key is repeating 
ratingdata.countByValue()

// COMMAND ----------

// count total elements containing in the ratingdata
ratingdata.count()

// COMMAND ----------

ratingdata.countByValue().foreach(println)

// COMMAND ----------

// /FileStore/tables/airports.text

val data = sc.textFile("FileStore/tables/airports.text")

// COMMAND ----------

data.collect()

// COMMAND ----------

val usa = data.filter(x => x.split(",")(3)=="\"United States\"")
usa.take(2)

// COMMAND ----------

def splitInput(line:String) = {
 
  val dataSplit = line.split(",")
 
  val airportId = dataSplit(1)
 
  val cityName = dataSplit(2)
 
  (airportId, cityName)
}

// COMMAND ----------

// map -> 3rd & 4th index

usa.map(line => line.split(",")(1)).take(2)

// COMMAND ----------

usa.map(line => {
  val splitData = line.split(",")
  splitData(1)+" -- " +splitData(2)
}).collect()

// COMMAND ----------

usa.map(line => {
  val splitData = line.split(",")
  splitData(1)+" -- " +splitData(2)
}).collect()

// COMMAND ----------

// DBTITLE 1,task
val ap =data.filter(x=>(x.split(",")(8)).toDouble%2==0).map(x=>x.split(",")(11))
ap.countByValue

// COMMAND ----------

// DBTITLE 1,task
val filtereddata = data.filter(x=>(x.split(",")(7)).toDouble>40 || x.split(",")(3)=="\"Iceland\"")

// COMMAND ----------


