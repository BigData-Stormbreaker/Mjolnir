package it.uniroma2.sabd.mjolnir.entities;

import java.io.Serializable;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.RESET_THRESHOLD_MULTIPLIER;

public class EnergyConsumptionRecord implements Serializable {

    //    private Double minEnergy = null;
//    private Double maxEnergy = null;
    private Double oldValue = 0.0;

    // aux parameter to compute variance over samples
    private Integer counter = 0;
    //    private Double M = 0.0;
    private Double S = 0.0;

    private Double consumption = 0.0;

    private Integer start = 0;

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

        // evaluating if an anomaly occurred

//        System.out.println("Arriva " + value);
        if (start == 0) {
//            System.out.println("E' il primo recordo, quindi");
            oldValue = value;
//            System.out.println("oldValue diventa " + oldValue);
            start = 1;
        }

        if (value > oldValue) {
//            System.out.println("value " + value + " è maggiore di " + oldValue);
            // -> reset occurred
            consumption += (value - oldValue);
//            System.out.println("Allora aumento consumption di " + (value - oldValue));
//            System.out.println("ora consumption vale " + consumption);
        }
        oldValue = value;
//        System.out.println("infine oldValue = " + oldValue);
    }

    public void combineMeasures(EnergyConsumptionRecord r1, EnergyConsumptionRecord r2) {
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
