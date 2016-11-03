#!/bin/bash
# Change the following options for different configurations:

# USAGE:
# ./runClosestPair.sh -i <inputfile> -o <outputfile> -p <size> -l(islocal)

RUNLOCAL=false
while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    -i|--input)
    INPUT="$2"
    shift # past argument
    ;;
    -o|--output)
    OUTPUT="$2"
    shift # past argument
    ;;
    -l|--isLocal)
    RUNLOCAL=true
    ;;
    -p|--partition)
    PARTITIONSIZE="$2"
    shift
    ;;
    *)
    # unknown option
    ;;
esac
shift # past argument or value
done

echo "Running with following parameters"
echo INPUT FILE       = "${INPUT}"
echo OUTPUT FILE      = "${OUTPUT}"
echo IS LOCAL         = "${RUNLOCAL}"
echo PARTITION SIZE   = "${PARTITIONSIZE}"

cmd="bin/spark-submit --class org.apache.cgspark.operations.ClosestPair --master local[2] \
  --driver-class-path \
  $HOME/development/cg-spark/target/cgspark-0.0.1-SNAPSHOT.jar:$HOME/guava-15.0.jar \
  ~/development/cg-spark/target/cgspark-0.0.1-SNAPSHOT.jar -i \
  $INPUT -o $OUTPUT \
  -p $PARTITIONSIZE"
if [ "$RUNLOCAL" = true ] ; then
  cmd="$cmd -l"
fi

echo "Running final command: $cmd" 
$cmd
