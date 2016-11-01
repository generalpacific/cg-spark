# CG\_Spark - Implementation of Computational Geometry Algorithms in Apache Spark

This project implements some basic computational geometry algorithms in Apache
Spark.

## Setup
 * Setup Scala: http://www.scala-lang.org/download/install.html
 * Setup Spark: https://spark.apache.org/docs/latest/

## Building
 * ```mvn install```

## Running CG\_Spark

lp - localpath where the jars are located in your system

 * Skyline

```
$ bin/spark-submit --class edu.umn.cs.cgspark.operations.Skyline \
--master local[k] \
--driver-class-path \
lp/cgspark-0.0.1-SNAPSHOT.jar:lp/guava-18.0.jar \
lp/cgspark-0.0.1-SNAPSHOT.jar \
-i lp/<input-filename> -o <output-filename> -l \
-p <partitionsize>
```
 
 * Closest Pair

```
$ bin/spark-submit --class \
edu.umn.cs.cgspark.operations.ClosestPair \
--master local[k] \
--driver-class-path \
lp/cgspark-0.0.1-SNAPSHOT.jar:lp/guava-18.0.jar \
lp/cgspark-0.0.1-SNAPSHOT.jar \
-i lp/<input-filename> -o <output-filename> -l \
-p <partitionsize>
```

 * Command for generating datasets

 ```
 $ java -jar InputCreator.jar \
 <outputfile> <number of points> \
 <distribution-uni|gaus|cor|anti|circle>
 ```

 * Usage information:

-i,--input <arg>       Input File
-l,--isLocal           Run locally instead of as spark job
-o,--output <arg>      Output File
-p,--partition <arg>   Partition Size

