package it.uniroma2.sabd.mjolnir;

import java.util.ArrayList;

public class MjolnirConstants {

    public static final String APP_NAME     = "mjolnir";
    public static final String MASTER_YARN  = "yarn";
    public static final String MASTER_LOCAL = "local";

    public static final String REDIS_HOST   = "redis://localhost";
    public static final Integer REDIS_PORT  = 6379;

    public static final String REDIS_DB_ROOT             = "mjolnir/results/";
    public static final String REDIS_DB_HOUSE_QUERY1     = "/query1/powerthreshold/json";
    public static final String REDIS_DB_HOUSE_QUERY2_AVG = "/query2/consumptions/avg/json";
    public static final String REDIS_DB_HOUSE_QUERY2_STD = "/query2/consumptions/std/json";
    public static final String REDIS_DB_HOUSE_QUERY3     = "/query3/plugsrank/json";
    public static final String REDIS_DB_QUERY3           = "/plugsrank/json";

    public static final Integer PROPERTY_ENERGY = 0;
    public static final Integer PROPERTY_POWER  = 1;

    public static final Long TIMESTAMP_START    = 1377986401L;
    public static final Long TIMESTAMP_END      = 1380578399L;

    public static final Double INSTANT_POWER_CONSUMPTION_THRESHOLD = 350D;

    public static final Integer GENERIC_HOURS_TAG  = 0;
    public static final Integer RUSH_HOURS_TAG     = 2;
    public static final Integer NO_RUSH_HOURS_TAG  = 1;

    public static final Integer[] DAY_QUARTER_STARTS = {0, 6, 12, 18};
    public static final Integer[] DAY_QUARTER_ENDS   = {6, 12, 18, 24};

    public static final Integer RUSH_HOURS_START_H = 6;
    public static final Integer RUSH_HOURS_END_H   = 18;
    public static final Integer RUSH_HOURS_START_D = 1;
    public static final Integer RUSH_HOURS_END_D   = 5;

    public static final Integer NO_RUSH_HOURS_START_H_1   = 0;
    public static final Integer NO_RUSH_HOURS_END_H_1     = 6;
    public static final Integer NO_RUSH_HOURS_START_H_2   = 18;
    public static final Integer NO_RUSH_HOURS_END_H_2     = 24;
    public static final Integer NO_RUSH_HOURS_START_D   = 1;
    public static final Integer NO_RUSH_HOURS_END_D     = 5;

    public static final Integer NO_RUSH_WEEKEND_START_H = 0;
    public static final Integer NO_RUSH_WEEKEND_END_H   = 24;
    public static final Integer NO_RUSH_WEEKEND_START_D = 6;
    public static final Integer NO_RUSH_WEEKEND_END_D   = 7;

    public static final Double RESET_THRESHOLD_MULTIPLIER = 0.5;

}
