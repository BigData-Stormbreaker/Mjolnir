package it.uniroma2.sabd.mjolnir;

import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import it.uniroma2.sabd.mjolnir.queries.InstantPowerComputation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class MjolnirSparkSession {
    public static void main(String[] args) {

        // preparing spark configuration
        SparkConf conf = new SparkConf()
                .setAppName(MjolnirConstants.APP_NAME)
                .setMaster(MjolnirConstants.MASTER_LOCAL);

        // retrieving spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // SENSOR RECORDS - no distinction on power/energy
        SampleReader sr = new SampleReader();
        JavaRDD<SensorRecord> sensorRecords = sr.sampleRead(sc);

        // POWER & ENERGY RECORDS RDDs
        JavaRDD<SensorRecord> powerRecords = sensorRecords.filter(new Function<SensorRecord, Boolean>() {
            public Boolean call(SensorRecord sensorRecord) throws Exception {
                return sensorRecord.isPower();
            }
        });
        JavaRDD<SensorRecord> energyRecords = sensorRecords.filter(new Function<SensorRecord, Boolean>() {
            public Boolean call(SensorRecord sensorRecord) throws Exception {
                return sensorRecord.isEnergy();
            }
        });

        JavaRDD<SensorRecord> powerRecordsHouse0 = powerRecords.filter(new Function<SensorRecord, Boolean>() {
            @Override
            public Boolean call(SensorRecord sensorRecord) throws Exception {
                return sensorRecord.getHouseID().equals(0);
            }
        });

        // SAMPLING HOUSE 0 - TODO replace with hdfs
        InstantPowerComputation.getHouseThresholdConsumption(powerRecordsHouse0);

    }
}
