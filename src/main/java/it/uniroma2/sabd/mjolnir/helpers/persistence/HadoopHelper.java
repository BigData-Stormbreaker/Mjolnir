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

/**
 * This class can be used in order to handle operations of HDFS cluster
 * and ingestion of partial results from Spark cluster to HDFS
 */
public class HadoopHelper {

    private String hdfsAddress = null;
    private Long time = null;
    private Configuration config = null;

    /**
     * This constructor will initialize the HDFS configuration, time and address.
     * @param hdfsAddress, hdfsAddress to connect (e.g.: localhost:9000)
     */
    public HadoopHelper(String hdfsAddress) {
        if (config == null) {
            config = new Configuration();
            config.set("fs.default.name", "hdfs://" + hdfsAddress);
            time = System.currentTimeMillis();
            this.hdfsAddress = hdfsAddress;
        }
    }


    /**
     * This method can be used in order to append (per house) the couple (timestamp, value) where value is the
     * house total instant power consumption over a given threshold (350 KW by default) to HDFS
     * in a unique file (per run)
     * @param houseID, houseID of the house
     * @param records, ArrayList of Tuple2 of timestamp and power value
     * @throws IOException
     */
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


    /**
     * This method can be used in order to append (per house) the triple (dayQuarter, avgEnergy, stdDeviation)
     * of the sensor records of energy type, aggregated by day quarters starting at 0,6,12,18 to HDFS
     * in a unique file (per run)
     * @param houseID
     * @param newrecord
     * @return
     * @throws IOException
     */
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

    /**
     * This method will append the values calculated in the query 3 for the plugs (per house) not ordered
     * to persist such values in a unique file (per run) on HDFS
     * @param houseID
     * @param newrecord
     * @return
     * @throws IOException
     */
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
