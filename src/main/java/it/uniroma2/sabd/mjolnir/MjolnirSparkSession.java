package it.uniroma2.sabd.mjolnir;

import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import it.uniroma2.sabd.mjolnir.queries.helpers.EnergyConsumption;
import it.uniroma2.sabd.mjolnir.queries.helpers.InstantPowerComputation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


import java.util.ArrayList;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.GENERIC_HOURS;


public class MjolnirSparkSession {
    public static void main(String[] args) {

        // preparing spark configuration
        SparkConf conf = new SparkConf()
                .setAppName(MjolnirConstants.APP_NAME)
                .setMaster(MjolnirConstants.MASTER_LOCAL);

        // retrieving spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // QUERY 1

        int i;

        ArrayList<Integer> result = new ArrayList<Integer>();

        for(i = 0; i < 10; i++) {
            // SENSOR RECORDS - no distinction on power/energy
            SampleReader sr = new SampleReader();
            JavaRDD<SensorRecord> sensorRecords = sr.sampleRead(sc, i);

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

            // CREATE AN RDD WHERE ARE PRESENT RECORDS WITH POWER SUPERIOR TO 350 WATT
            JavaRDD<SensorRecord> overPowerRecords = powerRecords.filter(new Function<SensorRecord, Boolean>() {
                @Override
                public Boolean call(SensorRecord sensorRecord) throws Exception {
                    return sensorRecord.getValue() >= 350;
                }
            });

            // IF THERE MORE THAN 0, THEN WE CAN ADD THE HOUSEID
            if (overPowerRecords.count() > 0) {
                result.add(i);
            }



            // QUERY 2

            EnergyConsumption.getEnergyConsumptionPerTimespan(energyRecords, GENERIC_HOURS);


        }

        System.out.println(result);


//        JavaRDD<SensorRecord> powerRecordsHouse0 = powerRecords.filter(new Function<SensorRecord, Boolean>() {
//            @Override
//            public Boolean call(SensorRecord sensorRecord) throws Exception {
//                return sensorRecord.getHouseID().equals(0);
//            }
//        });



        // SAMPLING HOUSE 0 - TODO replace with hdfs
//        InstantPowerComputation.getHouseThresholdConsumption(powerRecordsHouse0);
    }
}
