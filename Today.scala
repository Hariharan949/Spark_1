// Databricks notebook source
// DBTITLE 1,task
val data = List("f","a","f","s","a","regex")

// COMMAND ----------

val datardd = sc.parallelize(data)

// COMMAND ----------

datardd.count()

// COMMAND ----------

datardd.countByValue()

// COMMAND ----------

// DBTITLE 1,task
val data2 = List(1,2,3,4,5)

val data2rdd = sc.parallelize(data2)

// COMMAND ----------

val productRDD = data2rdd.reduce((x,y) => x*y)

productRDD

// COMMAND ----------

// DBTITLE 1,prime task
val numberrdd = sc.textFile("/FileStore/tables/numberData.csv")

// COMMAND ----------

numberrdd.collect()

// COMMAND ----------

val header = numberrdd.first
val numdata = numberrdd.filter(line => line!=header)
val dat = numdata.map(x => x.toInt)

numdata.take(5)

// COMMAND ----------

def prime(num  : Int): Boolean = {
(num > 1) && !(2 to scala.math.sqrt(num).toInt).exists(x =>num % x == 0)
}



// COMMAND ----------

def isPrime2(i :Int) : Boolean = {
     if (i <= 1)
       false
     else if (i == 2)
       true
     else
       !(2 to (i-1)).exists(x => i % x == 0)
   }

// COMMAND ----------

val primes = dat.map(x => (x,prime(x)))


// COMMAND ----------

primes.take(10)

// COMMAND ----------

primes.filter( x => x._2 == true ).map( x => x._1).sum

// COMMAND ----------




// COMMAND ----------

// DBTITLE 1,add and lowercase task
val listData = List("a 2020", "b 1998", "c 1997", "d 2100")

// COMMAND ----------

val datardd = sc.parallelize(listData)

// COMMAND ----------

val keyvalue = datardd.map( x => (x.split(" ")(0) , x.split(" ")(1).toInt))

// COMMAND ----------

keyvalue.collect()

// COMMAND ----------

keyvalue.mapValues(x => x+10).collect()

// COMMAND ----------

val airport =  sc.textFile("/FileStore/tables/airports-1.text")

// COMMAND ----------

// DBTITLE 1,task
val newdata = airport.map( x => (x.split(",")(1) , x.split(",")(11).toLowerCase))

// COMMAND ----------

newdata.take(5)

// COMMAND ----------


