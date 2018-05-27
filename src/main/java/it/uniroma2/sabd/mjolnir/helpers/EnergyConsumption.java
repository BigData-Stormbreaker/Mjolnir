package it.uniroma2.sabd.mjolnir.helpers;

import it.uniroma2.sabd.mjolnir.entities.EnergyConsumptionRecord;
import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.NO_RUSH_HOURS_TAG;
import static it.uniroma2.sabd.mjolnir.MjolnirConstants.RUSH_HOURS_TAG;
import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.rank;

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

    public static JavaPairRDD<String, EnergyConsumptionRecord> getEnergyConsumptionPerTimespan(JavaRDD<SensorRecord> energyRecords, Integer tag) {

        // key by the plug identifier (assuming per house RDD as input)
        JavaPairRDD<String, SensorRecord> energyByPlug = energyRecords.keyBy(new Function<SensorRecord, String>() {
            @Override
            public String call(SensorRecord sensorRecord) throws Exception {
                return sensorRecord.getHouseholdID().toString() + "_" + sensorRecord.getPlugID().toString();
            }
        });

        // retrieving average by plug
        JavaPairRDD<String, EnergyConsumptionRecord> energyAvgByPlug = energyByPlug.aggregateByKey(
                new EnergyConsumptionRecord(tag),
                // -> computing at runtime variance and updating min/max energy consumption measures
                new Function2<EnergyConsumptionRecord, SensorRecord, EnergyConsumptionRecord>() {
                    @Override
                    public EnergyConsumptionRecord call(EnergyConsumptionRecord energyConsumptionRecord, SensorRecord sensorRecord) throws Exception {
                        // --> update step
                        energyConsumptionRecord.addNewValue(sensorRecord.getValue());
                        // plug tagging for further aggregation
                        if (energyConsumptionRecord.getPlugID() == null)
                            energyConsumptionRecord.setPlugID(sensorRecord.getHouseholdID().toString() + "_" + sensorRecord.getPlugID().toString());
                        return energyConsumptionRecord;
                    }
                },
                // -> combine step (updating min/max measures, recomputing variance with new min/max)
                new Function2<EnergyConsumptionRecord, EnergyConsumptionRecord, EnergyConsumptionRecord>() {
                    @Override
                    public EnergyConsumptionRecord call(EnergyConsumptionRecord energyConsumptionRecord, EnergyConsumptionRecord energyConsumptionRecord2) throws Exception {
                        EnergyConsumptionRecord ecr = new EnergyConsumptionRecord(tag);
                        ecr.combineMeasures(energyConsumptionRecord, energyConsumptionRecord2);
                        return ecr;
                    }
                });

        return energyAvgByPlug;
    }

    public static JavaRDD<Tuple2<Integer, Double>> getPlugsRank(SparkSession sparkSession, JavaPairRDD<String, EnergyConsumptionRecord> rushHoursConsumptions, JavaPairRDD<String, EnergyConsumptionRecord> notRushHoursConsumptions) {

        // performing a join over the two RDDs and computing the difference between the two average consumptions
        JavaPairRDD<String, Double> plugConsumptionAvgDifferences = rushHoursConsumptions.join(notRushHoursConsumptions).mapValues(new Function<Tuple2<EnergyConsumptionRecord, EnergyConsumptionRecord>, Double>() {
            @Override
            public Double call(Tuple2<EnergyConsumptionRecord, EnergyConsumptionRecord> recordsTuple) throws Exception {
                // returning difference by rush / no rush hours consumption
                Double rushValue   = (recordsTuple._1.getTag().equals(RUSH_HOURS_TAG)) ? recordsTuple._1.getAvgEnergyConsumption() : recordsTuple._2.getAvgEnergyConsumption();
                Double noRushValue = (recordsTuple._1.getTag().equals(NO_RUSH_HOURS_TAG)) ? recordsTuple._1.getAvgEnergyConsumption() : recordsTuple._2.getAvgEnergyConsumption();
                return rushValue - noRushValue;
            }
        });

        // retrieving the rank of the plugs basing upon the previously computed consumptions diff.
        // (we need to translate to DataFrame)
        // -> creating the new schema
        String schemaString = "plugID value";
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("plugID", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("value", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        // -> mapping into rows
        JavaRDD<Row> plugsRows = plugConsumptionAvgDifferences.map(new Function<Tuple2<String, Double>, Row>() {
            @Override
            public Row call(Tuple2<String, Double> t) throws Exception {
                // plug unique identifier is houseID_plugID
                return RowFactory.create(t._1, t._2);
            }
        });

        // -> applying schema to RDD
        Dataset<Row> plugsDataset = sparkSession.createDataFrame(plugsRows, schema);
        plugsDataset.show();
        // -> ranking and returning the result
        return plugsDataset.orderBy(desc("value"))
                .toJavaRDD()
                .map(
                        new Function<Row, Tuple2<Integer, Double>>() {
                            @Override
                            public Tuple2<Integer, Double> call(Row row) throws Exception {
                                return new Tuple2<>(row.getInt(0), row.getDouble(1));
                            }
                        });
    }
}
