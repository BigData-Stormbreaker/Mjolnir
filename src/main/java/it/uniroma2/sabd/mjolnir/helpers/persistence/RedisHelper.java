package it.uniroma2.sabd.mjolnir.helpers.persistence;

import it.uniroma2.sabd.mjolnir.entities.EnergyConsumptionRecord;
import org.apache.spark.api.java.JavaRDD;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.*;

/**
 * This class can be used in order to handle operations of Redis cluster
 * and ingestion of results from Spark cluster to Redis
 */
public class RedisHelper implements Serializable {

    private JedisPool jedisPool = null;

    /**
     * This method allows the retrieval of the Redis connection resource
     * @return: Jedis, Redis Java client
     */
    public Jedis getRedisInstance() {
        try {
            if (jedisPool == null) {
                jedisPool = new JedisPool(new JedisPoolConfig(), REDIS_HOST, REDIS_PORT);
            }
            return jedisPool.getResource();
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
            return null;
        }
    }

    /**
     * This method can be used in order to store (per house) the couple (timestamp, value) where value is the
     * house total instant power consumption over a given threshold (350 KW by default)
     * @param query: HashMap, the query1 results
     * @return: Integer, the number of records updated
     */
    public Integer storeHouseOverPowerThresholdRecords(HashMap<Integer, ArrayList<Tuple2<Long, Double>>> query) {
        // retrieving pool connection
        Jedis redis = getRedisInstance();
        if (redis == null) return -1;

        Integer updated = 0;
        // saving as a set the records that have an instant power consumption over the threshold
        for (Map.Entry<Integer, ArrayList<Tuple2<Long, Double>>> entry : query.entrySet()) {
            // preparing house identifier string
            String recordPath = REDIS_DB_ROOT + REDIS_DB_HOUSE_QUERY1 + "house#" + String.valueOf(entry.getKey());

            // -> DEL to reset records store
            redis.del(recordPath);

            // -> SET (timestamp, value)
            for (Tuple2<Long, Double> record : entry.getValue()) {
                // preparing and storing csv value
                String value = String.valueOf(record._1) + "," + String.valueOf(record._2);
                redis.sadd(recordPath, value);

                updated++;
            }
        }

        return updated;
    }

    /**
     * This method can be used in order to store (per house) the triple (dayQuarter, avgEnergy, stdDeviation)
     * of the sensor records of energy type, aggregated by day quarters starting at 0,6,12,18
     * @param query: HashMap, the query2 results
     * @return Integer, the number of records updated
     */
    public Integer storeHouseQuartersEnergyStats(HashMap<Integer, ArrayList<EnergyConsumptionRecord>> query) {
        // retrieving pool connection
        Jedis redis = getRedisInstance();
        if (redis == null) return -1;

        Integer updated = 0;
        // saving as a set the energy consumption statistics per quarter
        for (Map.Entry<Integer, ArrayList<EnergyConsumptionRecord>> entry : query.entrySet()) {
            // preparing house identifier string
            String recordPath = REDIS_DB_ROOT + REDIS_DB_HOUSE_QUERY2 + "house#" + String.valueOf(entry.getKey());

            // -> DEL to reset records store
            redis.del(recordPath);

            // -> SET (quarter, avgEnergy, stdDeviation)
            for (int i = 0; i < DAY_QUARTER_STARTS.length; i++) {
                // retrieving record
                EnergyConsumptionRecord record = entry.getValue().get(i);
                // preparing and storing csv value
                String value = String.valueOf(i) + "," + String.valueOf(record.getAvgEnergyConsumption(30)) + "," + String.valueOf(record.getStandardDeviation());
                redis.sadd(recordPath, value);

                updated++;
            }
        }

        return updated;
    }

    /**
     * This method can be used to leverage of Redis capability to maintain a rank by a given score
     * - in this case we are using avg difference of rush and no rush hours avg energy consumptions -
     * raking over plugs
     * @param query: ArrayList of RDD, the query3 results
     * @return Integer, the number of rank records updated
     */
    public Integer storePlugsRankPerEnergyConsumption(ArrayList<JavaRDD<Tuple2<String, Double>>> query) {
        // retrieving pool connection
        Jedis redis = getRedisInstance();
        if (redis == null) return -1;

        Integer updated = 0;
        // preparing house identifier string
        String rankPath = REDIS_DB_ROOT + REDIS_DB_HOUSE_QUERY3;

        // -> DEL to reset rank
        redis.del(rankPath);

        // saving as a set the rank of plugs per energy avg consumption differences
        // between rush and not rush hours
        for (JavaRDD<Tuple2<String, Double>> rdd : query) {
            for (Tuple2<String, Double> record : rdd.collect()) {
                // -> SORTED SET over avg consumption differences
                redis.zadd(rankPath, record._2, record._1);

                updated++;
            }
        }

        return updated;
    }
}
