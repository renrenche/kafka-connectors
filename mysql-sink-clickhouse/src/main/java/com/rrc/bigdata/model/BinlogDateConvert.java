package com.rrc.bigdata.model;

import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

public class BinlogDateConvert {

    private static final String TIMESTAMP_CLASS = "io.debezium.time.ZonedTimestamp";
    private static final DateTimeFormatter YEAR_DF = DateTimeFormatter.ofPattern("yyyy");
    private static final DateTimeFormatter DATE_DF = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_DF = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter DATE_TIME_DF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter UTC_DATE_TIME_DF = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final DateTimeFormatter UTC_Z_DATE_TIME_DF = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss+08:00");

    public static String dateConvert(String schemaName, Long value) {
        if (StringUtils.isEmpty(schemaName)) {
            return value == null ? "" : value.toString();
        }
        LocalDateTime localDateTime;
        switch (schemaName) {
            case "io.debezium.time.Year":
                localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneId.systemDefault());
                return YEAR_DF.format(localDateTime);

            //  微秒数
            case "io.debezium.time.MicroTimestamp":
                localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(value / 1000), ZoneId.systemDefault());
                return DATE_TIME_DF.format(localDateTime);

            // 毫秒数
            case "io.debezium.time.Timestamp":
            case "org.apache.kafka.connect.data.Timestamp":
                localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneId.systemDefault());
                return DATE_TIME_DF.format(localDateTime);

            //  1970 到时间的天数
            case "io.debezium.time.Date":
            case "org.apache.kafka.connect.data.Date":
                localDateTime = LocalDateTime.parse("1970-01-01 00:00:00", DATE_TIME_DF);
                localDateTime = localDateTime.plus(value, ChronoUnit.DAYS);
                return DATE_DF.format(localDateTime);

            // 毫秒数
            case "io.debezium.time.Time":
            case "org.apache.kafka.connect.data.Time":
                localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneId.systemDefault());
                return TIME_DF.format(localDateTime);

            //  微秒数
            case "io.debezium.time.MicroTime":
                localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(value / 1000), ZoneId.systemDefault());
                return TIME_DF.format(localDateTime);
            default:
                return value.toString();
        }
    }

    public static String dateConvert(String schemaName, String value) {
        if (TIMESTAMP_CLASS.equalsIgnoreCase(schemaName)) {
            LocalDateTime dateTime;
            try {
                dateTime = LocalDateTime.parse(value, UTC_DATE_TIME_DF);
            } catch (DateTimeParseException e) {
                dateTime = LocalDateTime.parse(value, UTC_Z_DATE_TIME_DF);
            }
            dateTime = dateTime.plus(8L, ChronoUnit.HOURS);
            return DATE_TIME_DF.format(dateTime);
        } else {
            return value;
        }
    }

}
