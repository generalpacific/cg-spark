#!/bin/bash

# This script tests the Spark implementation against the local implementation. 
# This script will run the operation with and without spark and verify the
# outputs are equal.

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    -i|--input)
    INPUT="$2"
    shift # past argument
    ;;
    -p|--partition)
    PARTITIONSIZE="$2"
    shift
    ;;
    *)
    ;;
esac
shift # past argument or value
done

echo "Running test against following parameters:"
echo "INPUT $INPUT"
echo "PARTITIONSIZE $PARTITIONSIZE"

OUTPUTFILESPARK=testOutputSpark
OUTPUTFILELOCAL=testOutputLocal

echo "Running Spark implementation for ClosestPair"
./runClosestPair.sh -i $INPUT -o $OUTPUTFILESPARK -p $PARTITIONSIZE
echo "Running Local implementation for ClosestPair"
./runClosestPair.sh -i $INPUT -o $OUTPUTFILELOCAL -l -p $PARTITIONSIZE

echo "Comparing outputs: $OUTPUTFILESPARK(Spark) and $OUTPUTFILELOCAL(local)"
if cmp -s $OUTPUTFILESPARK $OUTPUTFILELOCAL
then
  echo "The files match"
else
  echo "The files are different"
fi
