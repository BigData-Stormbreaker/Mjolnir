package it.uniroma2.sabd.mjolnir.entities;

import java.sql.Timestamp;


import static it.uniroma2.sabd.mjolnir.MjolnirConstants.PROPERTY_POWER;

public class PowerRecord extends SensorRecord {

    public PowerRecord(Long id, Long timestamp, Double value, Integer plugID, Integer householdID, Integer houseID) {
        super(id, timestamp, value, PROPERTY_POWER, plugID, householdID, houseID);
    }
}
