CLASSNAME=com.hadooptraining.lab6.WordCountWithTools

echo Cleaning input and output directories...
hadoop fs -rm -r output
hadoop fs -rm -r input
hadoop fs -mkdir input
echo
echo Copying input files to HDFS...
hadoop fs -copyFromLocal -f data/humpty.txt input
hadoop fs -ls input
echo
echo Running Hadoop task...
command="hadoop jar $DEV_HOME/target/heffalump-1.0.jar $CLASSNAME input output"
echo $command
$command
echo ...Done
echo
echo Here are the files produced in output folder...
hadoop fs -ls output
echo
echo To see your input file, type:
echo hadoop fs -cat input/humpty.txt
echo
echo To see your output file, type:
echo hadoop fs -cat output/part-r-00000
echo
