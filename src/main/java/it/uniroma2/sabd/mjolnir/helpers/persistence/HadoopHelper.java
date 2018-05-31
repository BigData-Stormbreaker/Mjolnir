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

    private String hdfsAddress = null;
    private Long time = null;
    private Configuration config = null;

    public HadoopHelper(String hdfsAddress) {
        if (config == null) {
            config = new Configuration();
            config.set("fs.default.name", "hdfs://" + hdfsAddress);
            time = System.currentTimeMillis();
            this.hdfsAddress = hdfsAddress;
        }
    }


    public void appendHouseOverPowerThresholdRecords(Integer houseID, ArrayList<Tuple2<Long, Double>> records) throws IOException {

        FileSystem fs = FileSystem.get(config);

        Integer added = 0;
        // preparing house identifier string
        String recordPath = "hdfs://" + hdfsAddress + HDFS_ROOT + time + HDFS_HOUSE_QUERY1 + "house#" + houseID;
        Path path = new Path(recordPath);
        if (!fs.exists(path)) {
            fs.createNewFile(path);
        }

        FSDataOutputStream openfile = fs.append(path);

        for (Tuple2<Long, Double> record : records) {
            // preparing and storing csv value
            String value = String.valueOf(record._1) + "," + String.valueOf(record._2) + "\n";
            openfile.writeChars(value);
        }

        openfile.close();
        fs.close();
    }


    public Integer appendHouseQuartersEnergyStats(Integer houseID, ArrayList<EnergyConsumptionRecord> newrecord) throws IOException {

        FileSystem fs = FileSystem.get(config);

        Integer added = 0;
        // preparing house identifier string
        String recordPath = "hdfs://" + hdfsAddress + HDFS_ROOT + time + HDFS_HOUSE_QUERY2 + "house#" + houseID;
        Path path = new Path(recordPath);
        if (!fs.exists(path)) {
            fs.createNewFile(path);
        }
        FSDataOutputStream openfile = fs.append(path);

        // -> SET (quarter, avgEnergy, stdDeviation)
        for (int i = 0; i < DAY_QUARTER_STARTS.length; i++) {
            // retrieving record
            EnergyConsumptionRecord record = newrecord.get(i);
            // preparing and storing csv value
            String value = String.valueOf(i) + "," + String.valueOf(record.getAvgEnergyConsumption(30)) + "," + String.valueOf(record.getStandardDeviation()) + "\n";
            openfile.writeChars(value);
            added++;
        }
        openfile.close();
        fs.close();
        return added;
    }

    public Integer storePlugsRankPerEnergyConsumption(Integer houseID, JavaRDD<Tuple2<String, Double>> newrecord) throws IOException {

        FileSystem fs = FileSystem.get(config);

        Integer added = 0;
        // preparing house identifier string
        String recordPath = "hdfs://" + hdfsAddress + HDFS_ROOT + time + HDFS_HOUSE_QUERY3 + "house#" + houseID;
        Path path = new Path(recordPath);
        if (!fs.exists(path)) {
            fs.createNewFile(path);
        }
        FSDataOutputStream openfile = fs.append(path);

        // saving as a set the rank of plugs per energy avg consumption differences
        // between rush and not rush hours
        for (Tuple2<String, Double> record : newrecord.collect()) {
            // -> SORTED SET over avg consumption differences
            String value = String.valueOf(record._1) + "," + String.valueOf(record._2) + "\n";
            openfile.writeChars(value);
            added++;
        }
        openfile.close();
        fs.close();
        return added;
    }


}
