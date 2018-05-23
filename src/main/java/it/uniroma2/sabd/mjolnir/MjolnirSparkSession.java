package it.uniroma2.sabd.mjolnir;

import it.uniroma2.sabd.mjolnir.entities.EnergyConsumptionRecord;
import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import it.uniroma2.sabd.mjolnir.queries.helpers.EnergyConsumption;
import it.uniroma2.sabd.mjolnir.queries.helpers.InstantPowerComputation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import java.time.DayOfWeek;


import java.util.ArrayList;
import java.util.Map;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.GENERIC_HOURS;
import static it.uniroma2.sabd.mjolnir.MjolnirConstants.NO_RUSH_HOURS;
import static it.uniroma2.sabd.mjolnir.MjolnirConstants.RUSH_HOURS;
import static java.util.Calendar.*;


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

            // IF THERE ARE MORE THAN 0, THEN WE CAN ADD THE HOUSEID
            if (overPowerRecords.count() > 0) {
                result.add(i);
            }

//            JavaRDD<SensorRecord> firstSpan = EnergyConsumption.getRecordsByTimespan(energyRecords, 0, 6);
//            JavaRDD<SensorRecord> secondSpan = EnergyConsumption.getRecordsByTimespan(energyRecords, 6, 12);
//            JavaRDD<SensorRecord> thirdSpan = EnergyConsumption.getRecordsByTimespan(energyRecords, 12, 18);
//            JavaRDD<SensorRecord> fourthSpan = EnergyConsumption.getRecordsByTimespan(energyRecords, 18, 24);
//
//            JavaPairRDD<Integer, EnergyConsumptionRecord> energyConsumptionfirstTimespan = EnergyConsumption.getEnergyConsumptionPerTimespan(firstSpan, GENERIC_HOURS);
//            EnergyConsumption.getEnergyConsumptionPerTimespan(secondSpan, GENERIC_HOURS);
//            EnergyConsumption.getEnergyConsumptionPerTimespan(thirdSpan, GENERIC_HOURS);
//            EnergyConsumption.getEnergyConsumptionPerTimespan(fourthSpan, GENERIC_HOURS);
//
//            Map<Integer, EnergyConsumptionRecord> integerEnergyConsumptionRecordMap = energyConsumptionfirstTimespan.collectAsMap();


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
