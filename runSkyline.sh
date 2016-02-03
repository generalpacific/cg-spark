# Change the following options for different configurations:

# USAGE:
# bin/spark-submit --class org.apache.cgspark.operations.Skyline \
#   --master local[k] \
#   --driver-class-path \
#   <Local-path>/cgspark-0.0.1-SNAPSHOT.jar:<Local-path>/guava-18.0.jar \
#   <Local-path>/cgspark-0.0.1-SNAPSHOT.jar \
#   <input-filename> <output-filename> <islocal-boolean> \
#   <partitionsize>

bin/spark-submit --class org.apache.cgspark.operations.Skyline --master local[2] \
  --driver-class-path \
  $HOME/target/cgspark-0.0.1-SNAPSHOT.jar:$HOME/guava-15.0.jar \
  ~/cg-spark/target/cgspark-0.0.1-SNAPSHOT.jar ~/development/cg-spark/input.txt output.txt \
  false 10
