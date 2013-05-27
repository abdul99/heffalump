CLASSNAME=com.hadooptraining.lab5.TextOutInvertedIndexer

echo Cleaning input and output directories...
hadoop fs -rm -r output
hadoop fs -rm -r input
hadoop fs -mkdir input
echo
echo Copying input files to HDFS...
hadoop fs -copyFromLocal -f data/company-specialties.txt input
echo
echo Running Hadoop task...
command="hadoop jar $DEV_HOME/target/heffalump-1.0.jar $CLASSNAME input/company-specialties.txt output"
echo $command
$command
echo ...Done
echo
echo Here is the output from HDFS...
hadoop fs -ls output
hadoop fs -cat output/part-r-00000
echo
