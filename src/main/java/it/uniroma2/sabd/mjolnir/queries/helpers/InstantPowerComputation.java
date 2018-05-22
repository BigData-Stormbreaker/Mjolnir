package it.uniroma2.sabd.mjolnir.queries.helpers;

import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.INSTANT_POWER_CONSUMPTION_THRESHOLD;

public class InstantPowerComputation implements Serializable {


    public InstantPowerComputation(){ }

    public static JavaPairRDD<Long, Iterable<Tuple2<Integer, SensorRecord>>> getSensorRecordsByTimestamp(JavaRDD<SensorRecord> powerRecords) {
        // retrieving sensors records by timestamp
        JavaPairRDD<Long, Iterable<Tuple2<Integer, SensorRecord>>> houseInstantPowerRecordsByTime = powerRecords.keyBy(new Function<SensorRecord, Long>() {
            // -> key by timestamp
            @Override
            public Long call(SensorRecord instantPowerRecord) throws Exception {
                return instantPowerRecord.getTimestamp();
            }
        }).mapValues(new Function<SensorRecord, Tuple2<Integer, SensorRecord>>() {
            // -> computing <house_id, record>
            @Override
            public Tuple2<Integer, SensorRecord> call(SensorRecord instantPowerRecord) throws Exception {
                return new Tuple2<>(instantPowerRecord.getHouseID(), instantPowerRecord);
            }
        }).groupByKey(); // -> retrieving an iterable per timestamp with all the house sensor records

        return houseInstantPowerRecordsByTime;
    }

    public static JavaPairRDD<Long, Double> getHouseThresholdConsumption(JavaRDD<SensorRecord> housePowerRecords) {
        // retrieving all the aggregated instant power consumption values
        JavaPairRDD<Long, Double> housePowerConsumptionByTime = housePowerRecords.keyBy(new Function<SensorRecord, Long>() {
            // -> key by timestamp
            @Override
            public Long call(SensorRecord sensorRecord) throws Exception {
                return sensorRecord.getTimestamp();
            }
        }).aggregateByKey(0.0,
                // -> computing total consumption per home
                new Function2<Double, SensorRecord, Double>() {
                    @Override
                    public Double call(Double totalValue, SensorRecord sensorRecord) throws Exception {
                        return totalValue + sensorRecord.getValue();
                    }
                },
                // (combine step - Half Life 3 confirmed)
                new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double totalValue, Double interTotalValue) throws Exception {
                        return totalValue + interTotalValue;
                    }
                });

        // retrieving al the records with instant consumption more or equal to the threshold
        return housePowerConsumptionByTime.filter(new Function<Tuple2<Long, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Long, Double> t) throws Exception {
                return t._2 >= INSTANT_POWER_CONSUMPTION_THRESHOLD;
            }
        });
    }
}
