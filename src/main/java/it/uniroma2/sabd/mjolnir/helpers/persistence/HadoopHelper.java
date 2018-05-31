package it.uniroma2.sabd.mjolnir.helpers.persistence;

import it.uniroma2.sabd.mjolnir.entities.EnergyConsumptionRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.*;


public class HadoopHelper {

    private FileSystem fs = null;

    public HadoopHelper(String hdfsAddress) {
        if (fs == null) {
            Configuration config = new Configuration();
            config.set("fs.default.name", "hdfs://" + hdfsAddress);
            try {
                fs = FileSystem.get(config);
            } catch (IOException e) {
                System.err.println("IOExcpetion for HadoopHelper");
                e.printStackTrace();
            }
        }
    }


    public void appendHouseOverPowerThresholdRecords(Integer houseID, Tuple2<Long, Double> newrecord) throws IOException {

        Integer added = 0;
        // preparing house identifier string
        String recordPath = REDIS_DB_ROOT + REDIS_DB_HOUSE_QUERY1 + "house#" + houseID;
        Path path = new Path(recordPath);
        FSDataOutputStream open = fs.append(path);

        // preparing and storing csv value
        String value = String.valueOf(newrecord._1) + "," + String.valueOf(newrecord._2);
        open.writeChars(value);
    }


    public Integer appendHouseQuartersEnergyStats(Integer houseID, ArrayList<EnergyConsumptionRecord> newrecord) throws IOException {

        Integer added = 0;
        // preparing house identifier string
        String recordPath = REDIS_DB_ROOT + REDIS_DB_HOUSE_QUERY2 + "house#" + houseID;
        Path path = new Path(recordPath);
        FSDataOutputStream open = fs.append(path);

        // -> SET (quarter, avgEnergy, stdDeviation)
        for (int i = 0; i < DAY_QUARTER_STARTS.length; i++) {
            // retrieving record
            EnergyConsumptionRecord record = newrecord.get(i);
            // preparing and storing csv value
            String value = String.valueOf(i) + "," + String.valueOf(record.getAvgEnergyConsumption(30)) + "," + String.valueOf(record.getStandardDeviation());
            open.writeChars(value);
            added++;
        }
        return added;
    }

    public Integer storePlugsRankPerEnergyConsumption(Integer houseID, JavaRDD<Tuple2<String, Double>> newrecord) throws IOException {

        Integer added = 0;
        // preparing house identifier string
        String recordPath = REDIS_DB_ROOT + REDIS_DB_HOUSE_QUERY3 + "house#" + houseID;
        Path path = new Path(recordPath);
        FSDataOutputStream open = fs.append(path);

        // saving as a set the rank of plugs per energy avg consumption differences
        // between rush and not rush hours
        for (Tuple2<String, Double> record : newrecord.collect()) {
            // -> SORTED SET over avg consumption differences
            String value = String.valueOf(record._1) + "," + String.valueOf(record._2);
            open.writeChars(value);
            added++;
        }
        return added;
    }


}
