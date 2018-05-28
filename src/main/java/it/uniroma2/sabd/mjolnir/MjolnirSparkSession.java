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
import org.json4s.jackson.Json;
import redis.clients.jedis.Jedis;
import scala.Tuple2;
import org.json.simple.JSONObject;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
        ArrayList<EnergyConsumptionRecord> query2Result = new ArrayList<>();
        ArrayList<JavaRDD<Tuple2<Integer, Double>>> query3Result = new ArrayList<>();

        RedisHelper redisHelper = new RedisHelper();
        Jedis redisInstance = redisHelper.getRedisInstance();
        redisInstance.del(REDIS_DB_HOUSE_QUERY1);
        redisInstance.del(REDIS_DB_HOUSE_QUERY2);

        // SENSOR RECORDS - no distinction on power/energy
        SampleReader sr = new SampleReader();
        JavaRDD<SensorRecord> allSensorRecords = sr.sampleRead(sparkContext, -1);


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
                redisInstance.rpush(REDIS_DB_HOUSE_QUERY1, String.valueOf(houseID));
            }


            // --------------- QUERY 2 ---------------
            for (int j = 0; j < DAY_QUARTER_STARTS.length; j++) {
                // retrieving energy records and average consumption by day quarter
                JavaRDD<SensorRecord> energyRecordsQ = EnergyConsumption.getRecordsByTimespan(energyRecords, DAY_QUARTER_STARTS[j], DAY_QUARTER_ENDS[j]);
                JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionQ = EnergyConsumption.getEnergyConsumptionPerTimespan(energyRecordsQ, GENERIC_HOURS_TAG);

                // computing per timespan average (iterating over plugs' consumptions)
                EnergyConsumptionRecord ecr = new EnergyConsumptionRecord(GENERIC_HOURS_TAG);

                ecr.setHouseID(houseID);

                Map<String, EnergyConsumptionRecord> stringEnergyConsumptionRecordMap = energyConsumptionQ.collectAsMap();

                for (Map.Entry<String, EnergyConsumptionRecord> entry : stringEnergyConsumptionRecordMap.entrySet()) {
                    ecr.combineMeasures(ecr, entry.getValue());
                }

                JSONObject jsonObject = new JSONObject();
                jsonObject.put("houseID", ecr.getHouseID());
                jsonObject.put("quarter", DAY_QUARTER_STARTS[j].toString() + "_" + DAY_QUARTER_ENDS[j].toString());
                jsonObject.put("avgEnergy", ecr.getAvgEnergyConsumption());
                jsonObject.put("stdDev", ecr.getStandardDeviation());

                redisInstance.rpush(REDIS_DB_HOUSE_QUERY2, jsonObject.toJSONString());

                // updating query results
                query2Result.add(ecr);
            }

            int j = 0;
            for (EnergyConsumptionRecord ecr : query2Result) {
                System.out.print(DAY_QUARTER_STARTS[j] + " to " + DAY_QUARTER_ENDS[j] + " --> ");
                System.out.println("house" + ecr.getHouseID() + ": " + ecr.getAvgEnergyConsumption() + " " + ecr.getVariance());
                j++;
            }

            // --------------- QUERY 3 ---------------
            // retrieving rush hour consumptions
            JavaRDD<SensorRecord> energyRecordsRH = EnergyConsumption.getRecordsByTimespan(energyRecords, RUSH_HOURS_START_H, RUSH_HOURS_END_H, RUSH_HOURS_START_D, RUSH_HOURS_END_D);
            JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionRH = EnergyConsumption.getEnergyConsumptionPerTimespan(energyRecordsRH, RUSH_HOURS_TAG);
            // retrieving the two kind of no-rush hours consumptions
            // - no rush hours working days (0 to 6)
            JavaRDD<SensorRecord> energyRecordsNRH_W_1 = EnergyConsumption.getRecordsByTimespan(energyRecords, NO_RUSH_HOURS_START_H_1, NO_RUSH_HOURS_END_H_1, NO_RUSH_HOURS_START_D, NO_RUSH_HOURS_END_D);
            JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionNRH_W_1 = EnergyConsumption.getEnergyConsumptionPerTimespan(energyRecordsNRH_W_1, NO_RUSH_HOURS_TAG);
            // - no rush hours working days (18 to 24)
            JavaRDD<SensorRecord> energyRecordsNRH_W_2 = EnergyConsumption.getRecordsByTimespan(energyRecords, NO_RUSH_HOURS_START_H_2, NO_RUSH_HOURS_END_H_2, NO_RUSH_HOURS_START_D, NO_RUSH_HOURS_END_D);
            JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionNRH_W_2 = EnergyConsumption.getEnergyConsumptionPerTimespan(energyRecordsNRH_W_2, NO_RUSH_HOURS_TAG);
            // -- combining the two no rush hours timespans during working days
            JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionNRH_W = energyConsumptionNRH_W_1.join(energyConsumptionNRH_W_2).mapValues(new Function<Tuple2<EnergyConsumptionRecord, EnergyConsumptionRecord>, EnergyConsumptionRecord>() {
                @Override
                public EnergyConsumptionRecord call(Tuple2<EnergyConsumptionRecord, EnergyConsumptionRecord> t) throws Exception {
                    EnergyConsumptionRecord ecr = new EnergyConsumptionRecord(NO_RUSH_HOURS_TAG);
                    ecr.combineMeasures(t._1, t._2);
                    return ecr;
                }
            });
            // - no rush hours weekend
            JavaRDD<SensorRecord> energyRecordsNRH_WE = EnergyConsumption.getRecordsByTimespan(energyRecords, NO_RUSH_WEEKEND_START_H, NO_RUSH_WEEKEND_END_H, NO_RUSH_WEEKEND_START_D, NO_RUSH_WEEKEND_END_D);
            JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionNRH_WE = EnergyConsumption.getEnergyConsumptionPerTimespan(energyRecordsNRH_WE, NO_RUSH_HOURS_TAG);
            // joining the two RDDs
            JavaPairRDD<String, EnergyConsumptionRecord> energyConsumptionNRH = energyConsumptionNRH_W.join(energyConsumptionNRH_WE).mapValues(new Function<Tuple2<EnergyConsumptionRecord, EnergyConsumptionRecord>, EnergyConsumptionRecord>() {
                @Override
                public EnergyConsumptionRecord call(Tuple2<EnergyConsumptionRecord, EnergyConsumptionRecord> t) throws Exception {
                    EnergyConsumptionRecord ecr = new EnergyConsumptionRecord(NO_RUSH_HOURS_TAG);
                    ecr.combineMeasures(t._1, t._2);
                    return ecr;
                }
            });

            // retrieving rank of plugs
            JavaRDD<Tuple2<Integer, Double>> plugsRankPerHouse = EnergyConsumption.getPlugsRank(sparkSession, energyConsumptionRH, energyConsumptionNRH);
            query3Result.add(plugsRankPerHouse);

        }

    }
}
