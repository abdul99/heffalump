CLASSNAME=com.hadooptraining.lab10.LogProcessorWithPartitioner

echo Cleaning input and output directories...
hadoop fs -rm -r output
hadoop fs -rm -r input
hadoop fs -mkdir input
echo
echo Copying input files to HDFS...
hadoop fs -copyFromLocal -f data/access.log input
hadoop fs -ls input
echo
echo Running Hadoop task...
command="hadoop jar $DEV_HOME/target/heffalump-1.0.jar $CLASSNAME input output 10"
echo $command
$command
echo ...Done
echo
echo Here are the files created in the output directory...
hadoop fs -ls output
echo
echo The results are in ten files since we specified ten reducers and thus 10 partitioners.
echo To see your results, type:
echo hadoop fs -cat output/part-r-00000
echo hadoop fs -cat output/part-r-00001
echo ...etc. 
echo
