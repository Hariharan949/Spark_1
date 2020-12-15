// Databricks notebook source

val nasajuly= sc.textFile("/FileStore/tables/nasa_july.tsv")
val nasaaug=sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

val unionRdd=nasajuly.union(nasaaug)
unionRdd.take(5)

// COMMAND ----------

val header=unionRdd.first
unionRdd.filter(line=>line!=header).count()

// COMMAND ----------

def headerRemove(line:String): Boolean = !{line.contains("bytes")}
val nasa = unionRdd.filter(x=>headerRemove(x))
nasa.count()

// COMMAND ----------

nasa.sample(withReplacement = true, fraction=0.20).collect()

// COMMAND ----------

// DBTITLE 1,task
val nasaSplit=nasa.map(x=>x.split("\t"))
nasaSplit.filter(x=>(x(6)).toDouble>1000 || (x(5)).toDouble==0).take(10)

// COMMAND ----------

// DBTITLE 1,task
val nasaj=nasajuly.map(x=>x.split("\t")(0)).filter(x=>x!="host")
val nasaa=nasaaug.map(x=>x.split("\t")(0)).filter(x=>x!="host")
val nasacomm=nasaj.intersection(nasaa)
nasacomm.count()

// COMMAND ----------



// COMMAND ----------

val nasaaug = sc.textFile("FileStore/tables/nasa_august.tsv")
val nasajuly = sc.textFile("FileStore/tables/nasa_july.tsv")

// COMMAND ----------

val aughost = nasaaug.map(x => x.split("\t")(0))
val julyhost = nasajuly.map(x => x.split("\t")(0))
aughost.take(3)
julyhost.take(3)

// COMMAND ----------

aughost.filter(x => x=="host").collect()

// COMMAND ----------


