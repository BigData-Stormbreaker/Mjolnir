package it.uniroma2.sabd.mjolnir.entities;

import java.io.Serializable;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.RESET_THRESHOLD_MULTIPLIER;

/**
 * This class can be used in order to handle energy consumption
 * as a record, as it is used across the Spark cluster
 */
public class EnergyConsumptionRecord implements Serializable {

    private Double oldValue = 0.0;

    // aux parameter to compute variance over samples
    private Integer counter = 0;
    //    private Double M = 0.0;
    private Double stdDev = 0.0;

    private Double consumption = 0.0;

    private Integer start = 0;

    // tag value to keep trace of rush hours consumptions
    private Integer tag;

    // house identifier (just for final computation purposes)
    private Integer houseID;
    // plugID
    private String plugID;

    /**
     * Crete a new EnergyConsumptionRecord
     * @param tag: Integer, used to specify the kind of day hours covered (e.g. rush hours)
     */
    public EnergyConsumptionRecord(Integer tag) {
        this.tag = tag;
    }

    /**
     * This method can be used in order to update the current reference cumulative value
     * as well as detecting and handling anomalies in sensor readings
     * @param value: Double, new energy consumption value
     */
    public void addNewValue(Double value) {

        // evaluating if an anomaly occurred
        if (start == 0) {
            oldValue = value;
            start = 1;
        }

        if (value > oldValue) {
            // -> reset occurred
            consumption += (value - oldValue);
        } else if (value <= RESET_THRESHOLD_MULTIPLIER * oldValue) {
           return;
        }
        oldValue = value;
    }

    /**
     * This method can be used in order to combine two different consumption records
     * @param r1: EnergyConsumptionRecord
     * @param r2: EnergyConsumptionRecord
     */
    public void combineMeasures(EnergyConsumptionRecord r1, EnergyConsumptionRecord r2) {
        consumption = r1.getConsumption() + r2.getConsumption();
    }

    /**
     * @return: Double, the computed consumption
     */
    public Double getConsumption() {return consumption;}

    /**
     * @return: Double, the computed avg energy consumption over a given timespan (30d for monthly avg)
     */
    public Double getAvgEnergyConsumption(int timespan) {
        return consumption / timespan;
    }

    /**
     * @return: Double, the computed standard deviation
     */
    public Double getStandardDeviation() { return stdDev; }
    /**
     * @param: Double, custom standard deviation
     */
    public void setStandardDeviation(Double stdDev) { this.stdDev = stdDev; }

    /**
     * @return: Double, the number of samples used to compute the average
     */
    public Integer getCounter() {
        return counter;
    }

    /**
     * This method can be used in order to keep the count of samples
     */
    public void incrementCounter() {
        this.counter += 1;
    }

    /**
     * @return: Integer, used to specify the kind of day hours covered (e.g. rush hours)
     */
    public Integer getTag() { return tag; }

    /**
     * This method can be used in order to specify a house identifier the record refers to
     * @param houseID: Integer
     */
    public void setHouseID(Integer houseID) { this.houseID = houseID; }

    /**
     * @return: Integer, the house identifier
     */
    public Integer getHouseID() { return houseID; }

    /**
     * This method can be used in order to specify a plug identifier the record refers to
     * @param plugID: Integer
     */
    public void setPlugID(String plugID) { this.plugID = plugID; }

    /**
     * @return: String, the plug identifier
     */
    public String getPlugID() { return plugID; }
}
