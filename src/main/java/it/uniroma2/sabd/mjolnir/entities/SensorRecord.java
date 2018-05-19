package it.uniroma2.sabd.mjolnir.entities;

import java.sql.Timestamp;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.PROPERTY_ENERGY;
import static it.uniroma2.sabd.mjolnir.MjolnirConstants.PROPERTY_POWER;


public class SensorRecord {

    private Long id;
    private Long timestamp;
    private Double value;
    private Integer property;
    private Integer plugID;
    private Integer householdID;
    private Integer houseID;

    public SensorRecord(Long id, Long timestamp, Double value, Integer property, Integer plugID, Integer householdID, Integer houseID) {
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

    public Integer getPlugID() {
        return plugID;
    }

    public void setPlugID(Integer plugID) {
        this.plugID = plugID;
    }

    public Integer getHouseholdID() {
        return householdID;
    }

    public void setHouseholdID(Integer householdID) {
        this.householdID = householdID;
    }

    public Integer getHouseID() {
        return houseID;
    }

    public void setHouseID(Integer houseID) {
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
