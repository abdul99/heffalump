package com.hadooptraining.lab12;

import java.io.File;
import java.io.IOException;

import com.hadooptraining.lab8.LogWritable;
import com.maxmind.geoip.Country;
import com.maxmind.geoip.LookupService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *  A mapper that takes  <K1,V1> pair as <Object, LogWritable> and writes a <K2,V2>
 *  pair of <Text, IntWritable>. Mapper uses a GeoIP lookup object that converts any
 *  IP address to a country. The GeoIP database may be downloaded from
 *  http://www.maxmind.com/en/geolocation_landing. You can find the Java APIs to
 *  access the database from https://github.com/maxmind/
 */
public class LogProcessorMapDistributed
        extends Mapper<Object, LogWritable, Text, IntWritable> {

    // Store the cache file locally in a Path[] variable
    Path[] localCachePath;

    // Create and keep a lookup object for querying country from IP
    private LookupService lookupService;

    /**
     * Setup is called once during the map process. Retrieve the local cache to
     * create the lookup service.
     * @param context
     * @throws IOException
     */
    public void setup(Context context) throws IOException{
        Configuration conf = context.getConfiguration();

        // Get the local cache file path from the distributed cache
        localCachePath = DistributedCache.getLocalCacheFiles(conf);

        // Create a lookup object to use when resolving IP addresses
        File lookupDbDir = new File(localCachePath[0].toString());

        // Create a lookup object
        lookupService = new LookupService(lookupDbDir, LookupService.GEOIP_MEMORY_CACHE);
    }

    /**
     * Cleanup is called at the end of each map object
     * @param context
     * @throws IOException
     */
    public void cleanup(Context context) throws IOException{
        // Close the database connection for the lookup service
        lookupService.close();
    }

    /**
     * The map() method receives a LogWritable object, and writes the key-value pair to the
     * context object. The key is simply the user's country found from the GeoIP database
     * and the value is the response size in bytes as found in the LogWritable object.
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, LogWritable value, Context context)
            throws IOException, InterruptedException {

        // Lookup the country from the user's IP address
        Country country = lookupService.getCountry(value.getUserIP().toString());

        // Write the key-value pair to the context object
        context.write(new Text(country.getName()), value.getResponseSize());
    }
}