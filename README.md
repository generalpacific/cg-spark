# CG\_Spark - Implementation of Computational Geometry Algorithms in Apache Spark

This project implements some basic computational geometry algorithms in Apache
Spark.

## Setup
 * Setup Scala.
 * Setup Spark.

## Building
 * mvn install

## Running CG\_Spark
'''
bin/spark-submit --class edu.umn.cs.cgspark.operations.Skyline --master local[4] --driver-class-path /Users/prashantchaudhary/Documents/workspace/cgspark/target/cgspark-0.0.1-SNAPSHOT.jar /Users/prashantchaudhary/Documents/workspace/cgspark/target/cgspark-0.0.1-SNAPSHOT.jar
'''
