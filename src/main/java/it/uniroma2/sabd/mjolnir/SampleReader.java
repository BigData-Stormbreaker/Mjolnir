package it.uniroma2.sabd.mjolnir;

import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import org.apache.arrow.flatbuf.Bool;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.omg.PortableInterceptor.Interceptor;
import scala.Int;
import scala.Serializable;

import java.io.File;
import java.sql.Timestamp;
import java.util.List;

/**
 * This class implements methods for reading proper .csv files or avro files from HDFS
 */
public class SampleReader implements Serializable {

    public SampleReader() {
    }

    /**
     * This method read the plain text dataset and return an RDD of sensorRecords
     * @param sc, JavaSparkContext
     * @param house_id, houseID of the house (or -1 to read the compelte dataset)
     * @return JavaRDD of sensorRecords
     */
    public JavaRDD<SensorRecord> sampleRead(JavaSparkContext sc, Integer house_id) {

        // retrieving data
        // all data
        JavaRDD<String> data;
        if (house_id == -1) {
            data = sc.textFile(getClass().getClassLoader().getResource("d14_filtered.csv").getPath());
        } else { //or per-house data
            data = sc.textFile(getClass().getClassLoader().getResource("house" + house_id).getPath());
        }

        // obtaining an RDD of sensor records
        JavaRDD<SensorRecord> sensorData = data.map(new Function<String, SensorRecord>() {
            public SensorRecord call(String line) throws Exception {
                // splitting csv line
                String[] fields = line.split(",");
                SensorRecord sr = new SensorRecord(
                        Long.valueOf(fields[0]),     //id_record
                        Long.valueOf(fields[1]),     //timestamp
                        Double.valueOf(fields[2]),   //value - measure
                        Integer.valueOf(fields[3]),  //property - cumulative energy
                        //or power snapshot
                        Long.valueOf(fields[4]),     //plug_id
                        Long.valueOf(fields[5]),     //household_id
                        Long.valueOf(fields[6]));    //house_id
                return sr;
            }
        });

        /* DEBUG */
//        final LongAccumulator accumulator = sc.sc().longAccumulator();
//
//        sensorData.foreach(new VoidFunction<SensorRecord>() {
//            public void call(SensorRecord sensorRecord) throws Exception {
//                accumulator.add(1);
//            }
//        });

        return sensorData;
    }

    /**
     * This method read the dataset serialized in AVRO on HDFS and return an RDD of sensorRecords
     * @param sc, JavaSparkContext
     * @param hdfsAddress, HDFS Address
     * @param house_id, houseID of the house (or -1 to read the compelte dataset)
     * @return JavaRDD of sensorRecords
     */
    public JavaRDD<SensorRecord> sampleAvroRead(JavaSparkContext sc, String hdfsAddress, Integer house_id) {

        // prepare SparkSession and set the compression codec
        SparkSession sparkSession = new SparkSession(sc.sc());
        sparkSession.conf().set("spark.sql.avro.compression.codec", "snappy");


        // retrieving data
        // per-house data
        Dataset<Row> load = null;
        if (house_id != -1) {
            load = sparkSession.read()
                    .format("com.databricks.spark.avro")
                    .load("hdfs://" + hdfsAddress + "/ingestNiFi/house" + house_id + "/d14_filtered.csv");
        } else { // or all data
            load = sparkSession.read()
                    .format("com.databricks.spark.avro")
                    .load("hdfs://" + hdfsAddress + "/ingestNiFi/d14_filtered.csv");
        }

        // obtaining an RDD of sensor records
        JavaRDD<SensorRecord> sensorData = load.toJavaRDD().map(new Function<Row, SensorRecord>() {
            public SensorRecord call(Row row) throws Exception {
                // splitting csv line
                SensorRecord sr = new SensorRecord(
                        (Long) row.getAs("id"),               //id_record
                        (Long) row.getAs("timestamp"),        //timestamp
                        (Double) row.getAs("value"),          //value - measure
                        (Integer) row.getAs("property"),      //property - cumulative energy
                        //or power snapshot
                        (Long) row.getAs("plug_id"),          //plug_id
                        (Long) row.getAs("household_id"),     //household_id
                        (Long) row.getAs("house_id"));        //house_id
                return sr;
            }
        });

        /* DEBUG */
//        final LongAccumulator accumulator = sc.sc().longAccumulator();
//
//        sensorData.foreach(new VoidFunction<SensorRecord>() {
//            public void call(SensorRecord sensorRecord) throws Exception {
//                accumulator.add(1);
//            }
//        });

        return sensorData;
    }
}
