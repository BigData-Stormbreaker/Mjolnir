package it.uniroma2.sabd.mjolnir.entities;

import java.io.Serializable;
import java.sql.Timestamp;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.PROPERTY_ENERGY;
import static it.uniroma2.sabd.mjolnir.MjolnirConstants.PROPERTY_POWER;


public class SensorRecord implements Serializable {

    private Long id;
    private Long timestamp;
    private Double value;
    private Integer property;
    private Long plugID;
    private Long householdID;
    private Long houseID;

    public SensorRecord(Long id, Long timestamp, Double value, Integer property, Long plugID, Long householdID, Long houseID) {
        this.id = id;
        this.timestamp = timestamp;
        this.value = value;
        this.property = property;
        this.plugID = plugID;
        this.householdID = householdID;
        this.houseID = houseID;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Long getPlugID() {
        return plugID;
    }

    public void setPlugID(Long plugID) {
        this.plugID = plugID;
    }

    public Long getHouseholdID() {
        return householdID;
    }

    public void setHouseholdID(Long householdID) {
        this.householdID = householdID;
    }

    public Long getHouseID() {
        return houseID;
    }

    public void setHouseID(Long houseID) {
        this.houseID = houseID;
    }

    public Integer getProperty() {
        return property;
    }

    public void setProperty(Integer property) {
        this.property = property;
    }

    public boolean isPower() {
        return getProperty().equals(PROPERTY_POWER);
    }

    public boolean isEnergy() {
        return getProperty().equals(PROPERTY_ENERGY);
    }
}
