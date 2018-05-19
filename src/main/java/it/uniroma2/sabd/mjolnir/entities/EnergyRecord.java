package it.uniroma2.sabd.mjolnir.entities;

import java.sql.Timestamp;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.PROPERTY_ENERGY;


public class EnergyRecord extends SensorRecord {
    public EnergyRecord(Long id, Long timestamp, Double value, Integer plugID, Integer householdID, Integer houseID) {
        super(id, timestamp, value, PROPERTY_ENERGY, plugID, householdID, houseID);
    }
}
