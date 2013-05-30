echo Cleaning input and output directories...
hadoop fs -rm -r output
hadoop fs -rm -r input
hadoop fs -mkdir input
echo Copying input files to HDFS...
hadoop fs -copyFromLocal -f data/NASA_access_log.txt input
hadoop fs -ls input
echo
echo Running Hadoop task...
# For MR1 use
command="hadoop jar /usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.2.1.jar -input input -output output -mapper logProcessor.py -reducer aggregate -file logProcessor.py"
# For YARN use
#command="hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming-2.0.0-cdh4.2.1.jar -input input -output output -mapper logProcessor.py -reducer aggregate -file logProcessor.py"
echo $command
$command
echo ...Done
echo
echo Here are the files created in the output directory...
hadoop fs -ls output
echo To see your result type:
echo hadoop fs -cat output/part-00000
echo
