# Change the following options for different configurations:

# USAGE:
# bin/spark-submit --class org.apache.cgspark.operations.Skyline \
#   --master local[k] \
#   --driver-class-path \
#   <Local-path>/cgspark-0.0.1-SNAPSHOT.jar:<Local-path>/guava-18.0.jar \
#   <Local-path>/cgspark-0.0.1-SNAPSHOT.jar \
#   <input-filename> <output-filename> <islocal-boolean> \
#   <partitionsize>

bin/spark-submit --class org.apache.cgspark.operations.ClosestPair --master local[2] \
  --driver-class-path \
  $HOME/development/cg-spark/target/cgspark-0.0.1-SNAPSHOT.jar:$HOME/guava-15.0.jar \
  ~/development/cg-spark/target/cgspark-0.0.1-SNAPSHOT.jar -i \
  ~/development/cg-spark/input.txt -o output.txt \
  -p 10
