package it.uniroma2.sabd.mjolnir;

import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.util.LongAccumulator;
import org.omg.PortableInterceptor.Interceptor;
import scala.Int;
import scala.Serializable;

import java.sql.Timestamp;

public class SampleReader implements Serializable {

    public SampleReader() {}

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
                SensorRecord sr = new SensorRecord(Long.valueOf(fields[0]),     //id_record
                                                   Long.valueOf(fields[1]),     //timestamp
                                                   Double.valueOf(fields[2]),   //value - measure
                                                   Integer.valueOf(fields[3]),  //property - cumulative energy
                                                                                //or power snapshot
                                                   Integer.valueOf(fields[4]),  //plug_id
                                                   Integer.valueOf(fields[5]),  //household_id
                                                   Integer.valueOf(fields[6])); //house_id
                return sr;
            }
        });

        /* DEBUG */
        final LongAccumulator accumulator = sc.sc().longAccumulator();

        sensorData.foreach(new VoidFunction<SensorRecord>() {
            public void call(SensorRecord sensorRecord) throws Exception {
                accumulator.add(1);
            }
        });

        System.out.println("TOTAL RECORDS: " + String.valueOf(accumulator.value()));


        return sensorData;
    }
}
