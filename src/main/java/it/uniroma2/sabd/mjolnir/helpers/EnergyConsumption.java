package it.uniroma2.sabd.mjolnir.helpers;

import it.uniroma2.sabd.mjolnir.entities.EnergyConsumptionRecord;
import it.uniroma2.sabd.mjolnir.entities.SensorRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.*;
import static org.apache.spark.sql.functions.desc;

/**
 * This class collects all the methods necessary to handle operations over energy consumptions
 */
public class EnergyConsumption {

    /**
     * This method can be used in order to retrieve all the records in a given timespan
     * @param energyRecords: RDD of energy records
     * @param startHour: Integer
     * @param endHour: Integer
     * @return JavaRDD<SensorRecord>
     */
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

    /**
     * This method can be used in order to retrieve all the records in a given timespan
     * @param energyRecords: RDD of energy records
     * @param startHour: Integer
     * @param endHour: Integer
     * @param startWeekDay: Integer
     * @param endWeekDay: Integer
     * @return JavaRDD<SensorRecord>
     */
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

    /**
     * This method can be used in order to retrieve all the ricords in a given day of the month
     * and a given timespan of the day
     * @param energyRecords: RDD of energy records
     * @param startHour: Integer
     * @param endHour: Integer
     * @param monthDay: Integer
     * @param weekDay: Integer
     * @return JavaRDD<SensorRecord>
     */
    public static JavaRDD<SensorRecord> getRecordsByDay(JavaRDD<SensorRecord> energyRecords, Integer startHour, Integer endHour, Integer monthDay, Integer weekDay) {
        // filtering by the given hour and week day timespan
        return energyRecords.filter(new Function<SensorRecord, Boolean>() {
            @Override
            public Boolean call(SensorRecord sensorRecord) throws Exception {
                // computing date from timestamp (assuming system default timezone)
                LocalDateTime localDate = Instant.ofEpochMilli(sensorRecord.getTimestamp() * 1000).atZone(ZoneId.systemDefault()).toLocalDateTime();
                // filtering if record timestamp is in the given timespan (start, end, day of month, day of week)
                return (startHour <= localDate.getHour() && localDate.getHour() < endHour) &&
                       (localDate.getDayOfMonth() == monthDay) && (localDate.getDayOfWeek().getValue() == weekDay);
            }
        });
    }

    /**
     * This method can be used in order to retrieve all the ricords in a given day
     * identified by starting and ending timestamps
     * @param energyRecords: RDD of energy records
     * @param startDayTimestamp: Long, timestamp
     * @param endDayTimestamp: Long, timestamp
     * @return JavaRDD<SensorRecord>
     */
    public static JavaRDD<SensorRecord> getEnergyRecordsPerDay(JavaRDD<SensorRecord> energyRecords, Long startDayTimestamp, Long endDayTimestamp) {
        // filtering in a given day
        return energyRecords.filter(new Function<SensorRecord, Boolean>() {
            @Override
            public Boolean call(SensorRecord sensorRecord) throws Exception {
                return (startDayTimestamp <= sensorRecord.getTimestamp() && sensorRecord.getTimestamp() <= endDayTimestamp);
            }
        });
    }

    /**
     * This method can be used to retireve all the average energy consumptions in a predefined timespan
     * @param energyRecords: RDD of energy records
     * @param tag: Integer, kind of hours (e.g. rush hours)
     * @return JavaPairRDD<String, EnergyConsumptionRecord>
     */
    public static JavaPairRDD<String, EnergyConsumptionRecord> getEnergyConsumptionPerTimespan(JavaRDD<SensorRecord> energyRecords, Integer tag) {

        // key by the plug identifier (assuming per house RDD as input)
        JavaPairRDD<String, SensorRecord> energyByPlug = energyRecords.keyBy(new Function<SensorRecord, String>() {
            @Override
            public String call(SensorRecord sensorRecord) throws Exception {
                return sensorRecord.getHouseID() + "_" + sensorRecord.getHouseholdID().toString() + "_" + sensorRecord.getPlugID().toString();
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
//                        System.out.println("Plug " + sensorRecord.getPlugID() +" aggiunge il valore " + sensorRecord.getValue());
                        energyConsumptionRecord.addNewValue(sensorRecord.getValue());
                        // plug tagging for further aggregation
                        if (energyConsumptionRecord.getPlugID() == null)
                            energyConsumptionRecord.setPlugID(sensorRecord.getHouseID() + "_" + sensorRecord.getHouseholdID().toString() + "_" + sensorRecord.getPlugID().toString());
//                        System.out.println("Ora il consumo è " + energyConsumptionRecord.getConsumption());
                        return energyConsumptionRecord;
                    }
                },
                // -> combine step (updating min/max measures, recomputing variance)
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

    /**
     * This method can be used in order to compute, leveraging on SparkSQL, the rank of plugs
     * that are consuming less in no-rush-hours
     * @param sparkSession: SparkSQL Session
     * @param rushHoursConsumptions: PairRDD of EnergyConsumptionRecord by plug unique identifier (rush hours)
     * @param notRushHoursConsumptions: PairRDD of EnergyConsumptionRecord by plug unique identifier (no rush hours)
     * @return JavaRDD<Tuple2<String, Double>>
     */
    public static JavaRDD<Tuple2<String, Double>> getPlugsRank(SparkSession sparkSession, JavaPairRDD<String, EnergyConsumptionRecord> rushHoursConsumptions, JavaPairRDD<String, EnergyConsumptionRecord> notRushHoursConsumptions) {

        // performing a join over the two RDDs and computing the difference between the two average consumptions
        JavaPairRDD<String, Double> plugConsumptionAvgDifferences = rushHoursConsumptions.join(notRushHoursConsumptions).mapValues(new Function<Tuple2<EnergyConsumptionRecord, EnergyConsumptionRecord>, Double>() {
            @Override
            public Double call(Tuple2<EnergyConsumptionRecord, EnergyConsumptionRecord> recordsTuple) throws Exception {
                // returning difference by rush / no rush hours consumption
                Double rushValue   = (recordsTuple._1.getTag().equals(RUSH_HOURS_TAG)) ? recordsTuple._1.getAvgEnergyConsumption(30) : recordsTuple._2.getAvgEnergyConsumption(30);
                Double noRushValue = (recordsTuple._1.getTag().equals(NO_RUSH_HOURS_TAG)) ? recordsTuple._1.getAvgEnergyConsumption(30) : recordsTuple._2.getAvgEnergyConsumption(30);
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
        // -> ranking and returning the result
        return plugsDataset.orderBy(desc("value"))
                .toJavaRDD()
                .map(
                        new Function<Row, Tuple2<String, Double>>() {
                            @Override
                            public Tuple2<String, Double> call(Row row) throws Exception {
                                return new Tuple2<>(row.getString(0), row.getDouble(1));
                            }
                        });
    }

    /**
     * This method can be used to efficiently combine consumption from different plugs given a raw dataset
     * loaded from memory and not already parallelized
     * @param sparkContext: SparkContext
     * @param energyConsumptionRecordsQuarter: ArrayList of EnergyConsumptionRecord of a pre-defined quarter
     * @param rushHoursTag: Integer, tag of the kind of hours (e.g. rush hours)
     * @return JavaPairRDD<String, EnergyConsumptionRecord>
     */
    public static JavaPairRDD<String, EnergyConsumptionRecord> combinePlugConsumptions(JavaSparkContext sparkContext, ArrayList<EnergyConsumptionRecord> energyConsumptionRecordsQuarter, Integer rushHoursTag) {

        return sparkContext.parallelize(energyConsumptionRecordsQuarter).keyBy(new Function<EnergyConsumptionRecord, String>() {
            @Override
            public String call(EnergyConsumptionRecord energyConsumptionRecord) throws Exception {
                return energyConsumptionRecord.getPlugID();
            }
        }).reduceByKey(new Function2<EnergyConsumptionRecord, EnergyConsumptionRecord, EnergyConsumptionRecord>() {
            @Override
            public EnergyConsumptionRecord call(EnergyConsumptionRecord energyConsumptionRecord, EnergyConsumptionRecord energyConsumptionRecord2) throws Exception {
                EnergyConsumptionRecord ecr = new EnergyConsumptionRecord(rushHoursTag);
                ecr.setPlugID(energyConsumptionRecord.getPlugID());
                ecr.combineMeasures(energyConsumptionRecord, energyConsumptionRecord2);
                return ecr;
            }
        });

    }

    /**
     * This method can be used to compute average and standard deviation online while iterating over energy consumptions
     * of houses over the quarters, combining records after parallelized jobs are finished
     * @param energyConsumptionDayPerQ: HashMap of consumption records by day
     * @param monthDays: Integer, number of days to compute over the monthly average
     * @return: ArrayList<EnergyConsumptionRecord>
     */
    public static ArrayList<EnergyConsumptionRecord> getAverageAndStdDeviation(HashMap<Integer, ArrayList<EnergyConsumptionRecord>> energyConsumptionDayPerQ, Integer monthDays) {

        ArrayList<EnergyConsumptionRecord> averageConsumptionsRecords = new ArrayList<>();

        Double stdDev;
        Double mean;
        Double sum;

        // per quarter statistics
        for (int j = 0; j < DAY_QUARTER_STARTS.length; j++) {
            // combining over the entire month
            EnergyConsumptionRecord ecr = new EnergyConsumptionRecord(GENERIC_HOURS_TAG);
            for (int i = 0; i < monthDays; i++) {
                ecr.combineMeasures(ecr, energyConsumptionDayPerQ.get(j).get(i));
            }
            mean = ecr.getAvgEnergyConsumption(30);
            sum = 0.0;
            for (int i = 0; i < monthDays; i++) {
                sum += Math.pow(energyConsumptionDayPerQ.get(j).get(i).getConsumption() - mean, 2.0);
            }
            stdDev = Math.sqrt(sum / 30.0);
            ecr.setStandardDeviation(stdDev);
            averageConsumptionsRecords.add(ecr);
        }

        return averageConsumptionsRecords;
    }
}
