package it.uniroma2.sabd.mjolnir.queries;

import it.uniroma2.sabd.mjolnir.entities.EnergyConsumptionRecord;
import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class EnergyConsumption {


    public static JavaRDD<SensorRecord> getRecordsByTimespan(JavaRDD<SensorRecord> energyRecords, Integer startHour, Integer endHour) {
        // filtering by the given hour timespan
        return energyRecords.filter(new Function<SensorRecord, Boolean>() {
            @Override
            public Boolean call(SensorRecord sensorRecord) throws Exception {
                // computing date from timestamp (assuming system default timezone)
                LocalDateTime localDate = Instant.ofEpochMilli(sensorRecord.getTimestamp() * 1000).atZone(ZoneId.systemDefault()).toLocalDateTime();
                // filtering if record timestamp is in the given timespan (start, end-1m)
                return (startHour <= localDate.getHour() && localDate.getHour() < endHour);
            }
        });
    }

    public static JavaRDD<SensorRecord> getRecordsByTimespan(JavaRDD<SensorRecord> energyRecords, Integer startHour, Integer endHour, Integer startWeekDay, Integer endWeekDay) {
        // filtering by the given hour and week day timespan
        return energyRecords.filter(new Function<SensorRecord, Boolean>() {
            @Override
            public Boolean call(SensorRecord sensorRecord) throws Exception {
                // computing date from timestamp (assuming system default timezone)
                LocalDateTime localDate = Instant.ofEpochMilli(sensorRecord.getTimestamp() * 1000).atZone(ZoneId.systemDefault()).toLocalDateTime();
                // filtering if record timestamp is in the given timespan (start, end-1m)
                return (startHour <= localDate.getHour() && localDate.getHour() < endHour) &&
                       (startWeekDay <= localDate.getDayOfWeek().getValue() && localDate.getDayOfWeek().getValue() <= endWeekDay);
            }
        });
    }

    public static JavaPairRDD<Integer, EnergyConsumptionRecord> getEnergyConsumptionPerTimespan(JavaRDD<SensorRecord> energyRecords, Integer startHour, Integer endHour) {
        // key by the plug identifier (assuming per house RDD as input)
        JavaPairRDD<Integer, SensorRecord> energyByPlug = getRecordsByTimespan(energyRecords, startHour, endHour).keyBy(new Function<SensorRecord, Integer>() {
            @Override
            public Integer call(SensorRecord sensorRecord) throws Exception {
                return sensorRecord.getPlugID();
            }
        });

        // retrieving average by plug
        JavaPairRDD<Integer, EnergyConsumptionRecord> energyAvgByPlug = energyByPlug.aggregateByKey(
                new EnergyConsumptionRecord(),
                // -> computing at runtime variance and updating min/max energy consumption measures
                new Function2<EnergyConsumptionRecord, SensorRecord, EnergyConsumptionRecord>() {
                    @Override
                    public EnergyConsumptionRecord call(EnergyConsumptionRecord energyConsumptionRecord, SensorRecord sensorRecord) throws Exception {
                        energyConsumptionRecord.addNewValue(sensorRecord.getValue());
                        return energyConsumptionRecord;
                    }
                },
                // -> combine step (updating min/max measures, recomputing variance with new min/max)
                // -> the first record is chosen as the 'master' record to update variance
                new Function2<EnergyConsumptionRecord, EnergyConsumptionRecord, EnergyConsumptionRecord>() {
                    @Override
                    public EnergyConsumptionRecord call(EnergyConsumptionRecord energyConsumptionRecord, EnergyConsumptionRecord energyConsumptionRecord2) throws Exception {
                        EnergyConsumptionRecord ecr = new EnergyConsumptionRecord();
                        ecr.combineMeasures(energyConsumptionRecord, energyConsumptionRecord2);
                        return ecr;
                    }
                });

        return energyAvgByPlug;
    }


}
