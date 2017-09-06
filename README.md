## The 4 Simple Ways to group, sum &amp; count in Spark 2.0 
### Understanding groupBy, reduceByKey &amp; mapValues in Apache Spark by Example 

In this post, I would like to share a few code snippets that can help understand Spark 2.0 API. I am using the Spark Shell to execute the code, but you can also compile the code on Scala IDE for Eclipse and execute it on Hortonworks 2.5 as described in a previous article or Cloudera CDH sandboxes.

For illustration purposes, the data consisting of key-value pair represents 6 sales records each with an id and a value as follows:

     Seq( ("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7) )
     
## Example 1: Using RDDs with groupByKey
     val sales = sc.parallelize( Seq( ("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7) ) )
     val counts = sales.groupByKey()
                       .mapValues(sq => (sq.size,sq.sum)) 

     scala> counts.collect.foreach(println)
            (idA,(2,6))
            (idB,(3,18))
            (idC,(1,7))

In this example, the sale values are grouped by the key then each set of values belonging to the same key are counted and totaled to generate the result tuples. For example there are 2 sales records with a total value of 6 for idA.

## Example 2: Using RDD with reduceByKey
     val sales = sc.parallelize( Seq( ("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7) ) )
     val counts = sales.mapValues((1,_))
                       .reduceByKey {case (a,b) => ((a._1+b._1),(a._2+b._2))}

     scala> counts.collect.foreach(println)
            (idA,(2,6))
            (idB,(3,18))
            (idC,(1,7))

In this example, the sale values are mapped to a tuple for counting purposes (similar to word count) then both the values and the counts are totaled separately with a reduceByKey operation.

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

In this example, the sale records are first converted to a DataFrame using .toDF method. Following the values are grouped and then counted and summed via DataFrames agg operations.

## Example 4: Using Datasets with SQL SELECT query on TempView

     case class Sales(id: String, vl: Int)
     
     val sales = Seq(("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7)).toDF("id", "vl").as[Sales]
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

In this example, a case class Sales is first defined. Then the  sale records are converted to a DataFrame using .toDF method and the defined case class. Following a temporary view is created with a name Sales. Then a standard Spark SQL SELECT statment is executed to query the Sales View.
