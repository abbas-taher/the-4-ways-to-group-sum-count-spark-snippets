## The 4 Simple Ways to group, sum &amp; count in Spark 2.0 
### Understanding groupBy, reduceByKey &amp; mapValues in Apache Spark by Example 

In this post, I would like to share a few code snippets that can help understand Spark 2.0 API. I am using the Spark Shell to execute the code, but you can also compile the code on Scala IDE for Eclipse and execute it on Hortonworks 2.5 as described in a previous article or Cloudera CDH sandboxes.

For illustration purposes, I am using 6 data pairs that represent sales records each with an id and a value as follows:

     Seq( ("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7) )
     
## Example 1: Using RDDs with groupByKey
     val sales = sc.parallelize( Seq( ("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7) ) )
     val counts = sales.groupByKey()
                       .mapValues(sq => (sq.size,sq.sum)) 

     scala> counts.collect.foreach(println)
            (idA,(2,6))
            (idB,(3,18))
            (idC,(1,7))

In this example the sale values are grouped by the key then each set of values belonging to the same key are counted and totaled to generate the result tuples. For example there are two sales records with a total value of 6 for idA.

## Example 2: Using RDD with reduceByKey
     val sales = sc.parallelize( Seq( ("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7) ) )
     val counts = sales.mapValues((1,_))
                       .reduceByKey {case (a,b) => ((a._1+b._1),(a._2+b._2))}

     scala> counts.collect.foreach(println)
            (idA,(2,6))
            (idB,(3,18))
            (idC,(1,7))

In this example the sale values are mapped and counted (similar to word count) then both the values and the count are totaled separately.

## Example 3: Using DataFrames with groupBy and agg operations 

     val sales = Seq(("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7)).toDF("id", "vl")
     val counts = sales.groupBy($"id")
                       .agg( count($"id"),sum($"vl") )

scala> counts.show
+---+---------+--------+
| id|count(id)|sum(vl)|
+---+---------+--------+
|idA|        2|       6|
|idB|        3|      18|
|idC|        1|       7|
+---+---------+--------+


## Example 4: Using Datasets with SQL select Statement on TempView

     case class Sales(id: String, vl: Int)
     val sales = Seq(("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7)).toDF("id", "vl")
     val countsTB = sales.createOrReplaceTempView("Sales")
     val counts = spark.sql("SELECT id, count(vl), sum(vl) from Sales GROUP BY id")

scala> counts.show
+---+---------+-------+
| id|count(vl)|sum(vl)|
+---+---------+-------+
|idA|        2|      6|
|idB|        3|     18|
|idC|        1|      7|
+---+---------+-------+
