CLASSNAME=com.hadooptraining.lab12.LogProcessorDistributed

echo Cleaning input and output directories...
hadoop fs -rm -r output
hadoop fs -rm -r input
hadoop fs -mkdir input
echo
echo Copying distributed caches file to HDFS...
hadoop fs -copyFromLocal -f data/GeoIP.dat
echo Copying input files to HDFS...
hadoop fs -copyFromLocal -f data/access.log input
hadoop fs -ls input
echo
# Create a fat jar file including the GeoIP classes
pushd /tmp
mkdir fatjar
cd fatjar
jar -xf $DEV_HOME/lib/com/maxmind/geoip-api/1.2.11/*.jar
jar -xf $DEV_HOME/target/heffalump-1.0.jar
rm $DEV_HOME/target/heffalump-1.0.jar
jar -cf $DEV_HOME/target/heffalump-1.0.jar *
popd
rm -rf /tmp/fatjar
echo Running Hadoop task...
command="hadoop jar $DEV_HOME/target/heffalump-1.0.jar $CLASSNAME input output 2"
echo $command
$command
echo ...Done
echo
echo Here are the files created in the output directory...
hadoop fs -ls output
echo The results are in two files since we specified two reducers.
echo To see your results type:
echo hadoop fs -cat output/part-r-00000
echo hadoop fs -cat output/part-r-00001
echo
