package it.uniroma2.sabd.mjolnir.entities;

import java.io.Serializable;

public class EnergyConsumptionRecord implements Serializable {

//    private Double minEnergy = null;
//    private Double maxEnergy = null;
    private Double oldValue = 0.0;

    // aux parameter to compute variance over samples
    private Integer counter = 0;
    private Double M = 0.0;
    private Double S = 0.0;

    // tag value to keep trace of rush hours consumptions
    private Integer tag;

    // house identifier (just for final computation purposes)
    private Integer houseID;
    // plugID
    private Integer plugID;

    public EnergyConsumptionRecord(Integer tag) {
        this.tag = tag;
    }


    public void addNewValue(Double value) {
        // updating value
        Double delta = value - oldValue;

        // TODO - the plug has been activated (or reset)

        // computing variance over samples
        incrementCounter();
        Double oldM = M;
        M = M + (delta - M) / counter;
        S = S + (delta - M) * (delta - oldM);

        oldValue = value;
    }

    public void combineMeasures(EnergyConsumptionRecord r1, EnergyConsumptionRecord r2) {
        // combining S and aux variables
        counter = r1.getCounter() + r2.getCounter();
        M = (r1.getCounter() * r1.M + r2.getCounter() * r2.M) / counter;
        S = (r1.getCounter() * (r1.getVariance() + Math.pow(r1.M - M, 2.0)) + r2.getCounter() * (r2.getVariance() + Math.pow(r2.M - M, 2.0)));
    }

    public Double getAvgEnergyConsumption() {
        return M;
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

    public void setPlugID(Integer plugID) { this.plugID = plugID; }

    public Integer getPlugID() { return plugID; }
}
