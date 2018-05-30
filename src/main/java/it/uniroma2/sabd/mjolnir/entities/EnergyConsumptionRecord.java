package it.uniroma2.sabd.mjolnir.entities;

import java.io.Serializable;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.RESET_THRESHOLD_MULTIPLIER;

public class EnergyConsumptionRecord implements Serializable {

    private Double minEnergy = null;
    private Double maxEnergy = null;
    private Double oldValue = 0.0;

    // aux parameter to compute variance over samples
    private Integer counter = 0;
//    private Double M = 0.0;
    private Double S = 0.0;

    private Double consumption = 0.0;

    private Boolean reset = Boolean.FALSE;

    // tag value to keep trace of rush hours consumptions
    private Integer tag;

    // house identifier (just for final computation purposes)
    private Integer houseID;
    // plugID
    private String plugID;

    public EnergyConsumptionRecord(Integer tag) {
        this.tag = tag;
    }


    public void addNewValue(Double value) {

//        Double delta;

        // evaluating if an anomaly occurred
        if (value < oldValue) {
            if (value <= RESET_THRESHOLD_MULTIPLIER * oldValue) {
                // -> reset occurred
//                delta = value;
                oldValue = value;
                reset = Boolean.TRUE;
            }
            else {
                // -> error on detection (ignoring step)
                return;
            }
        } else {
            if (maxEnergy == null) {
                maxEnergy = value;
                minEnergy = value;
            }
            // updating value
            // delta = value - oldValue;
            if (value > maxEnergy) {
                maxEnergy = value;
            }
            if (value < minEnergy) {
                minEnergy = value;
            }

            consumption = maxEnergy - minEnergy;
            incrementCounter();
        }

        // computing variance over samples
//        incrementCounter();
//        Double oldM = M;
//        M = M + (delta - M) / counter;
//        S = S + (delta - M) * (delta - oldM);
//
//        oldValue = value;
    }

    public void combineMeasures(EnergyConsumptionRecord r1, EnergyConsumptionRecord r2) {
        // combining S and aux variables
//        counter = r1.getCounter() + r2.getCounter();
//        M = (r1.getCounter() * r1.getAvgEnergyConsumption() + r2.getCounter() * r2.getAvgEnergyConsumption());
//        S = (r1.getCounter() * (r1.getVariance() + Math.pow(r1.getAvgEnergyConsumption() - M/counter, 2.0)) + r2.getCounter() * (r2.getVariance() + Math.pow(r2.getAvgEnergyConsumption() - M/counter, 2.0)));
        consumption = r1.getConsumption() + r2.getConsumption();
    }

    public Double getConsumption() {return consumption;}

    public Double getAvgEnergyConsumption() {
        return consumption / 30;
    }

    public Double getVariance() {
        return S / (getCounter());
    }

    public Double getStandardDeviation() { return Math.sqrt(getVariance()); }

    public Integer getCounter() {
        return counter;
    }

    public void incrementCounter() {
        this.counter += 1;
    }

    public Integer getTag() { return tag; }

    public void setHouseID(Integer houseID) { this.houseID = houseID; }

    public Integer getHouseID() { return houseID; }

    public void setPlugID(String plugID) { this.plugID = plugID; }

    public String getPlugID() { return plugID; }
}
