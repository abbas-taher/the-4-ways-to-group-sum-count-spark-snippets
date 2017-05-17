## The 4 Simple Ways to group, sum &amp; count in Spark 2.0 
### Understanding groupBy, reduceByKey &amp; mapValues in Apache Spark by Example 

In this post, I would like to share a few code snippets that can help understand Spark 2.0 API. I am using the Spark Shell to execute the code, but you can also compile the code on Scala IDE for Eclipse and execute it on Hortonworks 2.5 as described in a previous article or Cloudera CDH sandboxes.

For illustration purposes, I am using 6 data pairs that represent sales data each with an id and a value as follows:

     Seq( ("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7) )
     
 
