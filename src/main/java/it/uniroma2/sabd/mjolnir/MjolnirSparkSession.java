package it.uniroma2.sabd.mjolnir;

import it.uniroma2.sabd.mjolnir.entities.EnergyConsumptionRecord;
import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import it.uniroma2.sabd.mjolnir.helpers.EnergyConsumption;
import it.uniroma2.sabd.mjolnir.helpers.InstantPowerComputation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.*;

import java.util.ArrayList;



public class MjolnirSparkSession {
    public static void main(String[] args) {

        // preparing spark configuration
        SparkConf conf = new SparkConf()
                .setAppName(APP_NAME)
                .setMaster(MASTER_LOCAL);

        // retrieving spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        int houseID;
        ArrayList<Integer> query1Result = new ArrayList<>();
        ArrayList<EnergyConsumptionRecord> query2Result = new ArrayList<>();

        for(houseID = 0; houseID < 1; houseID++) {

            // SENSOR RECORDS - no distinction on power/energy
            SampleReader sr = new SampleReader();
            JavaRDD<SensorRecord> sensorRecords = sr.sampleRead(sc, houseID);

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

            // --------------- QUERY 1 ---------------
            // retrieving houses with instant power consumption more than the given threshold (350W)
            JavaPairRDD<Long, Double> houseInstantOverPowerThreshold = InstantPowerComputation.getHouseThresholdConsumption(powerRecords);
            // adding to house list if some given time the I.P.C. was over threashold
            if (!houseInstantOverPowerThreshold.isEmpty())
                query1Result.add(houseID);

            // --------------- QUERY 2 ---------------

            // TODO - is it actually necessary to order them in order to compute energy?
            //            JavaRDD<SensorRecord> orderedfirstSpan = firstSpan.sortBy(new Function<SensorRecord, Long>() {
            //                @Override
            //                public Long call(SensorRecord sensorRecord) throws Exception {
            //                    return sensorRecord.getTimestamp();
            //                }
            //            }, true, 1);

            //            LocalDateTime localDate = Instant.ofEpochMilli(orderedfirstSpan.first().getTimestamp() * 1000).atZone(ZoneId.systemDefault()).toLocalDateTime();
            //            System.out.println(localDate);

            for (int j = 0; j < DAY_QUARTER_STARTS.length; j++) {
                // retrieving energy records and average consumption by day quarter
                JavaRDD<SensorRecord> energyRecordsQ = EnergyConsumption.getRecordsByTimespan(energyRecords, DAY_QUARTER_STARTS[j], DAY_QUARTER_ENDS[j]);
                JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionQ = EnergyConsumption.getEnergyConsumptionPerTimespan(energyRecordsQ, GENERIC_HOURS_TAG);
                // computing per timespan average
                EnergyConsumptionRecord ecr = new EnergyConsumptionRecord(GENERIC_HOURS_TAG);
                ecr.setHouseID(houseID);
                energyConsumptionQ.foreach(new VoidFunction<Tuple2<String, EnergyConsumptionRecord>>() {
                    @Override
                    public void call(Tuple2<String, EnergyConsumptionRecord> t) throws Exception {
                        ecr.combineMeasures(ecr, t._2);
                    }
                });
                // updating query results
                query2Result.add(ecr);
            }


            // --------------- QUERY 3 ---------------

        }

    }
}
