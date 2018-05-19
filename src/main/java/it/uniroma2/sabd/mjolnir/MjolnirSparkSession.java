package it.uniroma2.sabd.mjolnir;

import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;


public class MjolnirSparkSession {
    public static void main(String[] args) {

        // preparing spark configuration
        SparkConf conf = new SparkConf()
                         .setAppName(MjolnirConstants.APP_NAME)
                         .setMaster(MjolnirConstants.MASTER_LOCAL);

        // retrieving spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        /* SAMPLE READING */
        SampleReader sr = new SampleReader();
        // retrieving sensor records - no distinction on power/energy
        JavaRDD<SensorRecord> sensorRecords = sr.sampleRead(sc);
    }
}
