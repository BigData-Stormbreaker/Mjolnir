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
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class MjolnirSparkSession {
    public static void main(String[] args) {

        // preparing spark configuration
        SparkConf conf = new SparkConf()
                .setAppName(APP_NAME)
                .setMaster(MASTER_LOCAL);

        // retrieving spark context
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // retrieving spark session
        SparkSession sparkSession = new SparkSession(sparkContext.sc());

        int houseID;
        ArrayList<Integer> query1Result = new ArrayList<>();
        HashMap<Integer, ArrayList<EnergyConsumptionRecord>> query2Result = new HashMap<>();
        ArrayList<JavaRDD<Tuple2<Integer, Double>>> query3Result = new ArrayList<>();

        // SENSOR RECORDS - no distinction on power/energy
        SampleReader sr = new SampleReader();
        JavaRDD<SensorRecord> allSensorRecords = sr.sampleRead(sparkContext, 0);


        for (houseID = 0; houseID < 1; houseID++) {

            final Long variable = (long) houseID;

            JavaRDD<SensorRecord> sensorRecords = allSensorRecords.filter(new Function<SensorRecord, Boolean>() {
                @Override
                public Boolean call(SensorRecord sensorRecord) throws Exception {
                    return sensorRecord.getHouseID().equals(variable);
                }
            });

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

            System.out.println(powerRecords.first().toString());

            // --------------- QUERY 1 ---------------
            // retrieving houses with instant power consumption more than the given threshold (350W)
            JavaPairRDD<Long, Double> houseInstantOverPowerThreshold = InstantPowerComputation.getHouseThresholdConsumption(powerRecords);
            // adding to house list if some given time the I.P.C. was over threshold
            if (!houseInstantOverPowerThreshold.isEmpty()) {
                query1Result.add(houseID);
                //redisInstance.rpush(REDIS_DB_HOUSE_QUERY1, String.valueOf(houseID));
            }

            // energy consumption records per house
            HashMap<Integer, ArrayList<EnergyConsumptionRecord>> energyConsumptionDayQ = new HashMap<>();
            for (int j = 0; j < DAY_QUARTER_STARTS.length; j++)
                energyConsumptionDayQ.put(j, new ArrayList<>());

            // energy consumption records (per plug) per house per timespan
            ArrayList<EnergyConsumptionRecord> energyConsumptionDayRushHours = new ArrayList<>();
            ArrayList<EnergyConsumptionRecord> energyConsumptionDayNoRushHours = new ArrayList<>();


            int monthDays = 0;
            for (Long d = TIMESTAMP_START; d <= TIMESTAMP_END; d += SECONDS_PER_DAY) {
                monthDays++;
                // retrieving day
                LocalDateTime localDate = Instant.ofEpochMilli(d * 1000).atZone(ZoneId.systemDefault()).toLocalDateTime();

                // retrieving energy records for the given day
                JavaRDD<SensorRecord> energyRecordsDay = EnergyConsumption.getEnergyRecordsPerDay(energyRecords, d, d + SECONDS_PER_DAY);

                // initializing aux variables to compute avg diff on rush and not rush hours
                ArrayList<EnergyConsumptionRecord> energyConsumptionRecordsRHDay  = new ArrayList<>();
                ArrayList<EnergyConsumptionRecord> energyConsumptionRecordsNRHDay = new ArrayList<>();

                // iterating over day quarters
                for (int j = 0; j < DAY_QUARTER_STARTS.length; j++) {

                    // preparing consumption tag
                    Integer tag = null;
                    if ((NO_RUSH_WEEKEND_START_D <= localDate.getDayOfWeek().getValue() || localDate.getDayOfWeek().getValue() <= NO_RUSH_WEEKEND_END_D) || (j == 0 || j == 3))
                        tag = NO_RUSH_HOURS_TAG;
                    else
                        tag = RUSH_HOURS_TAG;

                    // retrieving energy records and average consumption by day quarter
                    JavaRDD<SensorRecord> energyRecordsQ = EnergyConsumption.getRecordsByTimespan(energyRecordsDay, DAY_QUARTER_STARTS[j], DAY_QUARTER_ENDS[j]);
                    JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionQ = EnergyConsumption.getEnergyConsumptionPerTimespan(energyRecordsQ, tag);

                    // computing per timespan average (iterating over plugs' consumptions)
                    EnergyConsumptionRecord ecr = new EnergyConsumptionRecord(tag);
                    ecr.setHouseID(houseID);
                    Map<String, EnergyConsumptionRecord> stringEnergyConsumptionRecordMap = energyConsumptionQ.collectAsMap();
                    for (Map.Entry<String, EnergyConsumptionRecord> entry : stringEnergyConsumptionRecordMap.entrySet()) {
                        // updating energy consumption record
                        ecr.combineMeasures(ecr, entry.getValue());
                        // storing record for further analysis
                        EnergyConsumptionRecord record = entry.getValue();
                        record.setPlugID(entry.getKey());
                        if (tag.equals(RUSH_HOURS_TAG))
                            energyConsumptionRecordsRHDay.add(record);
                        if (tag.equals(NO_RUSH_HOURS_TAG))
                            energyConsumptionRecordsNRHDay.add(record);
                    }

                    // updating quarters values (query2)
                    energyConsumptionDayQ.get(j).add(ecr);
                }

                // updating day values (query3)
                for (Map.Entry<String, EnergyConsumptionRecord> entry : EnergyConsumption.getMapPlugAvgConsumptionDay(sparkContext, energyConsumptionRecordsRHDay, RUSH_HOURS_TAG).entrySet())
                    energyConsumptionDayRushHours.add(entry.getValue());
                for (Map.Entry<String, EnergyConsumptionRecord> entry : EnergyConsumption.getMapPlugAvgConsumptionDay(sparkContext, energyConsumptionRecordsNRHDay, NO_RUSH_HOURS_TAG).entrySet())
                    energyConsumptionDayNoRushHours.add(entry.getValue());
            }

            // --------------- QUERY 2 ---------------
            query2Result.put(houseID, EnergyConsumption.getAverageAndStdDeviation(energyConsumptionDayQ, monthDays));


            // --------------- QUERY 3 ---------------
            JavaPairRDD<String, EnergyConsumptionRecord> rushHoursRecords = sparkContext.parallelize(energyConsumptionDayRushHours).keyBy(new Function<EnergyConsumptionRecord, String>() {
                @Override
                public String call(EnergyConsumptionRecord energyConsumptionRecord) throws Exception {
                    return energyConsumptionRecord.getPlugID();
                }
            });

            JavaPairRDD<String, EnergyConsumptionRecord> noRushHoursRecords = sparkContext.parallelize(energyConsumptionDayNoRushHours).keyBy(new Function<EnergyConsumptionRecord, String>() {
                @Override
                public String call(EnergyConsumptionRecord energyConsumptionRecord) throws Exception {
                    return energyConsumptionRecord.getPlugID();
                }
            });

            query3Result.add(EnergyConsumption.getPlugsRank(sparkSession, rushHoursRecords, noRushHoursRecords));
        }

    }

}
