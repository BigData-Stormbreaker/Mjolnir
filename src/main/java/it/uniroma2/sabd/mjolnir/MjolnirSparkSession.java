package it.uniroma2.sabd.mjolnir;

import it.uniroma2.sabd.mjolnir.entities.EnergyConsumptionRecord;
import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import it.uniroma2.sabd.mjolnir.helpers.EnergyConsumption;
import it.uniroma2.sabd.mjolnir.helpers.InstantPowerComputation;
import it.uniroma2.sabd.mjolnir.helpers.persistence.RedisHelper;
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

    private static String hdfsAddress;
    private static Integer houseId = -1;

    public static void main(String[] args) {

        String[] pair;
        for (String arg : args) {
            pair = arg.split("=");
            if (pair[0].equals("hdfs")) {
                hdfsAddress = pair[1];
            }
            if (pair[0].equals("houseid")) {
                houseId = Integer.valueOf(pair[1]);
            }
        }

        if (hdfsAddress == null) {
            System.err.println("hdfsAddress is missing");
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
//        JavaRDD<SensorRecord> allSensorRecords = sr.sampleRead(sparkContext, 0);
        JavaRDD<SensorRecord> allSensorRecords = sr.sampleAvroRead(sparkContext, hdfsAddress, houseId);

        int maxHouse = HOUSE_NUMBER;

        if (houseId != -1) {
            maxHouse = 1;
        }

        for (houseID = 0; houseID < maxHouse; houseID++) {

            JavaRDD<SensorRecord> sensorRecords;

            final Long finalHouseID = (long) houseID;

            if (houseID == -1) {

                sensorRecords = allSensorRecords.filter(new Function<SensorRecord, Boolean>() {
                    @Override
                    public Boolean call(SensorRecord sensorRecord) throws Exception {
                        return sensorRecord.getHouseID().equals(finalHouseID);
                    }
                });
            } else {
                sensorRecords = allSensorRecords;
            }

            System.out.println(sensorRecords.count());

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
                query1Result.put(houseID, (ArrayList<Tuple2<Long, Double>>) houseInstantOverPowerThreshold.collect());
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
                ArrayList<EnergyConsumptionRecord> energyConsumptionRecordsPerPlugRHQuarter  = new ArrayList<>();
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
                        //System.out.println(entry.getKey() + " has consumed: " + entry.getValue().getConsumption() +
                        //        " in the day " + localDate.getDayOfMonth() +
                        //        " in the quarter " + j);
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

                    System.out.println("So the total consumption is: " + ecr.getConsumption());

                    // updating quarters values (query2)
                    energyConsumptionDayQ.get(j).add(ecr);
                }

                System.out.println("Fine dei quarter del giorno " + localDate.getDayOfMonth() + " che è un " + localDate.getDayOfWeek().getValue() + ":");
                System.out.println("energyConsumptionRecordsPerPlugRHQuarter ha " + energyConsumptionRecordsPerPlugRHQuarter.size() + " record");
                System.out.println("energyConsumptionRecordsPerPlugNRHQuarter ha " + energyConsumptionRecordsPerPlugNRHQuarter.size() + " record");

                // updating day values (query3)
                for (Map.Entry<String, EnergyConsumptionRecord> entry : EnergyConsumption.combinePlugConsumptions(sparkContext, energyConsumptionRecordsPerPlugRHQuarter, RUSH_HOURS_TAG).collectAsMap().entrySet())
                    energyConsumptionDayRushHours.add(entry.getValue());
                for (Map.Entry<String, EnergyConsumptionRecord> entry : EnergyConsumption.combinePlugConsumptions(sparkContext, energyConsumptionRecordsPerPlugNRHQuarter, NO_RUSH_HOURS_TAG).collectAsMap().entrySet())
                    energyConsumptionDayNoRushHours.add(entry.getValue());

                System.out.println("Consumi orari di punta");
                System.out.println("Ci sono " + energyConsumptionDayRushHours.size() + " elementi");
                for (EnergyConsumptionRecord ecr : energyConsumptionDayRushHours) {
                    System.out.println("Plug: " + ecr.getPlugID() + " = " + ecr.getConsumption());
                }

                System.out.println("Consumi orari non di punta");
                System.out.println("Ci sono " + energyConsumptionDayNoRushHours.size() + " elementi");
                for (EnergyConsumptionRecord ecr : energyConsumptionDayNoRushHours) {
                    System.out.println("Plug: " + ecr.getPlugID() + " = " + ecr.getConsumption());
                }
            }

            // --------------- QUERY 2 ---------------
            query2Result.put(houseID, EnergyConsumption.getAverageAndStdDeviation(energyConsumptionDayQ, monthDays));


            // --------------- QUERY 3 ---------------
            JavaPairRDD<String, EnergyConsumptionRecord> rushHoursRecords = EnergyConsumption.combinePlugConsumptions(sparkContext, energyConsumptionDayRushHours, RUSH_HOURS_TAG);
            JavaPairRDD<String, EnergyConsumptionRecord> noRushHoursRecords = EnergyConsumption.combinePlugConsumptions(sparkContext, energyConsumptionDayNoRushHours, NO_RUSH_HOURS_TAG);


            System.out.println("Negli orari di punta abbiamo: ");
            for (Map.Entry<String, EnergyConsumptionRecord> entry : rushHoursRecords.collectAsMap().entrySet()) {
                System.out.println("Plug id: " + entry.getKey() + " ha consumato in totale: " + entry.getValue().getConsumption());
            }

            System.out.println("Negli orari non di punta abbiamo: ");
            for (Map.Entry<String, EnergyConsumptionRecord> entry : noRushHoursRecords.collectAsMap().entrySet()) {
                System.out.println("Plug id: " + entry.getKey() + " ha consumato in totale: " + entry.getValue().getConsumption());
            }

            query3Result.add(EnergyConsumption.getPlugsRank(sparkSession, rushHoursRecords, noRushHoursRecords));
        }


        // --------------- INGESTION TO REDIS ---------------
        RedisHelper redisHelper = new RedisHelper();
        redisHelper.storeHouseOverPowerThresholdRecords(query1Result);
        redisHelper.storeHouseQuartersEnergyStats(query2Result);
        redisHelper.storePlugsRankPerEnergyConsumption(query3Result);
    }

}
