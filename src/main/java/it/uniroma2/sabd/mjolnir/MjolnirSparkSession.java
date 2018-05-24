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
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.time.DayOfWeek;


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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

        for(i = 0; i < 1; i++) {
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

            // QUERY 2

            JavaRDD<SensorRecord> firstSpan = EnergyConsumption.getRecordsByTimespan(energyRecords, 0, 6);
            JavaRDD<SensorRecord> secondSpan = EnergyConsumption.getRecordsByTimespan(energyRecords, 6, 12);
//            JavaRDD<SensorRecord> thirdSpan = EnergyConsumption.getRecordsByTimespan(energyRecords, 12, 18);
//            JavaRDD<SensorRecord> fourthSpan = EnergyConsumption.getRecordsByTimespan(energyRecords, 18, 24);



            JavaRDD<SensorRecord> orderedfirstSpan = firstSpan.sortBy(new Function<SensorRecord, Long>() {
                @Override
                public Long call(SensorRecord sensorRecord) throws Exception {
                    return sensorRecord.getTimestamp();
                }
            }, true, 1);

            LocalDateTime localDate = Instant.ofEpochMilli(orderedfirstSpan.first().getTimestamp() * 1000).atZone(ZoneId.systemDefault()).toLocalDateTime();
            System.out.println(localDate);


            JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionfirstTimespan = EnergyConsumption.getEnergyConsumptionPerTimespan(orderedfirstSpan, GENERIC_HOURS);
//            JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionsecondTimespan = EnergyConsumption.getEnergyConsumptionPerTimespan(secondSpan, GENERIC_HOURS);
//            JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionthirdTimespan = EnergyConsumption.getEnergyConsumptionPerTimespan(thirdSpan, GENERIC_HOURS);
//            JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionfourthTimespan = EnergyConsumption.getEnergyConsumptionPerTimespan(fourthSpan, GENERIC_HOURS);

            System.out.println("First timespan");
            Map<String, EnergyConsumptionRecord> stringEnergyConsumptionRecordMap = energyConsumptionfirstTimespan.collectAsMap();
            for(Map.Entry<String, EnergyConsumptionRecord> entry : stringEnergyConsumptionRecordMap.entrySet()) {
                System.out.println("The plug " + entry.getKey().toString() + " has consumed a mean of " + entry.getValue().getAvgEnergyConsumption().toString()
                + " KWh and has a variance of " + entry.getValue().getStandardDeviation());
            }

//            System.out.println("Second timespan");
//            stringEnergyConsumptionRecordMap = energyConsumptionsecondTimespan.collectAsMap();
//            for(Map.Entry<String, EnergyConsumptionRecord> entry : stringEnergyConsumptionRecordMap.entrySet()) {
//                System.out.println("The plug " + entry.getKey().toString() + " has consumed a mean of " + entry.getValue().getAvgEnergyConsumption().toString()
//                        + " KWh and has a variance of " + entry.getValue().getStandardDeviation());
//            }
//
//            System.out.println("Third timespan");
//            stringEnergyConsumptionRecordMap = energyConsumptionthirdTimespan.collectAsMap();
//            for(Map.Entry<String, EnergyConsumptionRecord> entry : stringEnergyConsumptionRecordMap.entrySet()) {
//                System.out.println("The plug " + entry.getKey().toString() + " has consumed a mean of " + entry.getValue().getAvgEnergyConsumption().toString()
//                        + " KWh and has a variance of " + entry.getValue().getStandardDeviation());
//            }
//
//            System.out.println("Fourth timespan");
//            stringEnergyConsumptionRecordMap = energyConsumptionfourthTimespan.collectAsMap();
//            for(Map.Entry<String, EnergyConsumptionRecord> entry : stringEnergyConsumptionRecordMap.entrySet()) {
//                System.out.println("The plug " + entry.getKey().toString() + " has consumed a mean of " + entry.getValue().getAvgEnergyConsumption().toString()
//                        + " KWh and has a variance of " + entry.getValue().getStandardDeviation());
//            }

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
