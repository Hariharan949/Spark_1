
val `val` = 10
print(`val`)

// COMMAND ----------

def square(a:Int): Int = {
  a*a
}

square(2)

// COMMAND ----------

val a = "\t\n\u03BB"

// COMMAND ----------

def add(a:Int, b:Int)= a+b

println(add(6,7))

// COMMAND ----------

def square(a:Int): Int = {
  a*a
}

def squar(y:Int, takefunction: Int => Int ): Int = {
  takefunction(y)
}

squar(2,square)

// COMMAND ----------

val new1 = List(1,2,3,4)

new1

// COMMAND ----------

val arr = Array(1,2,3)
arr(0)

// COMMAND ----------

arr(0) = 0
arr

// COMMAND ----------

// DBTITLE 1,First Rdd
// parallelize method

val data = List(1,2,3,4,5)


val creationRDD = sc.parallelize(data) // sc - spark context - object, works with cluster

// COMMAND ----------



creationRDD.collect()

// COMMAND ----------

// get the total partitions for the data
creationRDD.partitions.size

// COMMAND ----------

val rddpartition = sc.parallelize(List(1,2,3,4),2)

// COMMAND ----------

rddpartition.partitions.size

// COMMAND ----------

//count is also an action which returns the no. of elements
rddpartition.count()

// COMMAND ----------

//map -> transformation

// using map to create new rdd from an existing rdd (rddpartition)
val maprdd = rddpartition.map(x=> x*x*x)

//maprdd.take(3)

maprdd.collect()

// COMMAND ----------

maprdd.filter(x => x%2 == 0).collect()

// COMMAND ----------

val mainrdd = sc.parallelize(List("hey","hello","bye","goodbye"))

mainrdd.collect()

// COMMAND ----------

// map vs flatmap

mainrdd.map(x => x.split(",")).collect()

// COMMAND ----------

mainrdd.flatMap(x => x.split(",")).collect()

// COMMAND ----------

val rdd0 = sc.parallelize(Array("one","two","three","one","two"))
val keyrdd = rdd0.map(x => (x,1))


// COMMAND ----------

keyrdd.collect()

// COMMAND ----------

// counting and adding the value for same keys

keyrdd.reduceByKey(_+_).collect()

// COMMAND ----------


