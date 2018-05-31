package it.uniroma2.sabd.mjolnir;

import it.uniroma2.sabd.mjolnir.entities.EnergyConsumptionRecord;
import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import it.uniroma2.sabd.mjolnir.helpers.EnergyConsumption;
import it.uniroma2.sabd.mjolnir.helpers.InstantPowerComputation;
import it.uniroma2.sabd.mjolnir.helpers.persistence.HadoopHelper;
import it.uniroma2.sabd.mjolnir.helpers.persistence.RedisHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.*;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class MjolnirSparkSession {

    private static String hdfsAddress;
    private static Integer houseId = -1;

    public static void main(String[] args) throws IOException {

        String[] pair;
        for (String arg : args) {
            pair = arg.split("=");
            if (pair[0].equals(CLI_HDFS)) {
                hdfsAddress = pair[1];
            }
            if (pair[0].equals(CLI_HOUSE_ID)) {
                houseId = Integer.valueOf(pair[1]);
            }
        }

        if (hdfsAddress == null) {
            System.err.println("Usage: specify 'hdfs' parameter");
            System.exit(-1);
        }

        // preparing spark configuration
        SparkConf conf = new SparkConf()
                .setAppName(APP_NAME)
                .setMaster(MASTER_LOCAL);

        // retrieving spark context
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.hadoopConfiguration().set("avro.mapred.ignore.inputs.without.extension", "false");

        // retrieving spark session
        SparkSession sparkSession = new SparkSession(sparkContext.sc());

        int houseID;
        HashMap<Integer, ArrayList<Tuple2<Long, Double>>> query1Result = new HashMap<>();
        HashMap<Integer, ArrayList<EnergyConsumptionRecord>> query2Result = new HashMap<>();
        ArrayList<JavaRDD<Tuple2<String, Double>>> query3Result = new ArrayList<>();

        // SENSOR RECORDS - no distinction on power/energy
        SampleReader sr = new SampleReader();
        // JavaRDD<SensorRecord> allSensorRecords = sr.sampleRead(sparkContext, 0);
        JavaRDD<SensorRecord> allSensorRecords = sr.sampleAvroRead(sparkContext, hdfsAddress, houseId);

        HadoopHelper hadoopHelper = new HadoopHelper(hdfsAddress);

        int maxHouse = HOUSE_NUMBER;

        if (houseId != -1) {
            maxHouse = 1;
        }

        for (houseID = 0; houseID < maxHouse; houseID++) {

            JavaRDD<SensorRecord> sensorRecords;

            final Long finalHouseID = (long) houseID;

            if (houseId == -1) {
                sensorRecords = allSensorRecords.filter(new Function<SensorRecord, Boolean>() {
                    @Override
                    public Boolean call(SensorRecord sensorRecord) throws Exception {
                        return sensorRecord.getHouseID().equals(finalHouseID);
                    }
                });
            } else {
                sensorRecords = allSensorRecords;
            }

            // POWER & ENERGY RECORDS RDDs
            JavaRDD<SensorRecord> powerRecords = sensorRecords.filter(new Function<SensorRecord, Boolean>() {
                public Boolean call(SensorRecord sensorRecord) throws Exception {
                    return sensorRecord.isPower();
                }
            }).cache();
            JavaRDD<SensorRecord> energyRecords = sensorRecords.filter(new Function<SensorRecord, Boolean>() {
                public Boolean call(SensorRecord sensorRecord) throws Exception {
                    return sensorRecord.isEnergy();
                }
            }).cache();

            // --------------- QUERY 1 ---------------
            // retrieving houses with instant power consumption more than the given threshold (350W)
            JavaPairRDD<Long, Double> houseInstantOverPowerThreshold = InstantPowerComputation.getHouseThresholdConsumption(powerRecords);
            // adding to house list if some given time the I.P.C. was over threshold
            if (!houseInstantOverPowerThreshold.isEmpty()) {
                ArrayList<Tuple2<Long, Double>> collect = new ArrayList<Tuple2<Long, Double>>(houseInstantOverPowerThreshold.collect());
                query1Result.put(houseID, collect);
                hadoopHelper.appendHouseOverPowerThresholdRecords(houseID, collect);
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
                ArrayList<EnergyConsumptionRecord> energyConsumptionRecordsPerPlugRHQuarter = new ArrayList<>();
                ArrayList<EnergyConsumptionRecord> energyConsumptionRecordsPerPlugNRHQuarter = new ArrayList<>();

                // iterating over day quarters
                for (int j = 0; j < DAY_QUARTER_STARTS.length; j++) {

                    // preparing consumption tag
                    Integer tag = null;
                    if ((NO_RUSH_WEEKEND_START_D <= localDate.getDayOfWeek().getValue() && localDate.getDayOfWeek().getValue() <= NO_RUSH_WEEKEND_END_D) || (j == 0 || j == 3))
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
                            energyConsumptionRecordsPerPlugRHQuarter.add(record);
                        if (tag.equals(NO_RUSH_HOURS_TAG))
                            energyConsumptionRecordsPerPlugNRHQuarter.add(record);
                    }

                    // updating quarters values (query2)
                    energyConsumptionDayQ.get(j).add(ecr);
                }

                // updating day values (query3)
                for (Map.Entry<String, EnergyConsumptionRecord> entry : EnergyConsumption.combinePlugConsumptions(sparkContext, energyConsumptionRecordsPerPlugRHQuarter, RUSH_HOURS_TAG).collectAsMap().entrySet())
                    energyConsumptionDayRushHours.add(entry.getValue());
                for (Map.Entry<String, EnergyConsumptionRecord> entry : EnergyConsumption.combinePlugConsumptions(sparkContext, energyConsumptionRecordsPerPlugNRHQuarter, NO_RUSH_HOURS_TAG).collectAsMap().entrySet())
                    energyConsumptionDayNoRushHours.add(entry.getValue());
            }

            // --------------- QUERY 2 ---------------
            ArrayList<EnergyConsumptionRecord> averageAndStdDeviation = EnergyConsumption.getAverageAndStdDeviation(energyConsumptionDayQ, monthDays);
            query2Result.put(houseID, averageAndStdDeviation);

            // persist partial results on HDFS
            hadoopHelper.appendHouseQuartersEnergyStats(houseID, averageAndStdDeviation);


            // --------------- QUERY 3 ---------------
            JavaPairRDD<String, EnergyConsumptionRecord> rushHoursRecords = EnergyConsumption.combinePlugConsumptions(sparkContext, energyConsumptionDayRushHours, RUSH_HOURS_TAG);
            JavaPairRDD<String, EnergyConsumptionRecord> noRushHoursRecords = EnergyConsumption.combinePlugConsumptions(sparkContext, energyConsumptionDayNoRushHours, NO_RUSH_HOURS_TAG);

            JavaRDD<Tuple2<String, Double>> plugsRank = EnergyConsumption.getPlugsRank(sparkSession, rushHoursRecords, noRushHoursRecords);
            query3Result.add(plugsRank);
            hadoopHelper.storePlugsRankPerEnergyConsumption(houseID, plugsRank);
        }

        // --------------- INGESTION TO REDIS ---------------
        RedisHelper redisHelper = new RedisHelper();
        redisHelper.storeHouseOverPowerThresholdRecords(query1Result);
        redisHelper.storeHouseQuartersEnergyStats(query2Result);
        redisHelper.storePlugsRankPerEnergyConsumption(query3Result);
    }

}
