CLASSNAME=com.hadooptraining.extra.hbase.HBaseClient

#echo Cleaning input and output directories...
#hadoop fs -rm -r output
#hadoop fs -rm -r input
#hadoop fs -mkdir input
#echo
#echo Copying input files to HDFS...
#hadoop fs -copyFromLocal -f data/NASA_access_log.txt input
#echo
echo Running Hadoop task...
command="hadoop jar $DEV_HOME/target/heffalump-1.0.jar $CLASSNAME input output"
echo $command
$command
echo ...Done
#echo
#echo Here are the files created in the output directory...
#hadoop fs -ls output
#echo
#echo To see your result, type:
#echo hadoop fs -cat output/part-r-00000
#echo
