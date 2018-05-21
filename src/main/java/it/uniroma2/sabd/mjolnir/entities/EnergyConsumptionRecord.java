package it.uniroma2.sabd.mjolnir.entities;

import java.io.Serializable;

public class EnergyConsumptionRecord implements Serializable {

    private Double minEnergy = null;
    private Double maxEnergy = null;

    // aux parameter to compute variance
    private Integer counter = 0;
    private Double M = 0.0;
    private Double S = 0.0;

    public void addNewValue(Double value) {
        // updating value
        if (minEnergy == null) {
            minEnergy = value;
            maxEnergy = value;
        } else {
            if (value <= minEnergy) minEnergy = value;
            if (value >= maxEnergy) maxEnergy = value;
        }

        // computing variance
        incrementCounter();
        Double oldM = M;
        M = M + (value - M) / counter;
        S = S + (value - M) * (value - oldM);
    }

    public Double getMinEnergy() {
        return minEnergy;
    }

    public Double getMaxEnergy() {
        return maxEnergy;
    }

    public Double getAvgEnergyConsumption() {
        return (getMaxEnergy() - getMinEnergy()) / 2;
    }

    public Double getVariance() {
        return S / (getCounter() - 1);
    }

    public Integer getCounter() {
        return counter;
    }

    public void incrementCounter() {
        this.counter += 1;
    }
}
