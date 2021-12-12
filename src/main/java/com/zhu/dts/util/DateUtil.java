package com.zhu.dts.util;

import io.debezium.time.Date;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/**
 * @Author ZhuHaiBo
 * @Create 2021/12/9 23:46
 */
public class DateUtil {

    private static final String TIME_ZONE = "Asia/Shanghai";

    private static final String DATETIME_FORMATTER = "yyyy-MM-dd HH:mm:ss";

    public static String handlerDateTime(String time, String fieldType) {
        if (null == time) {
            return "";
        }

        switch (fieldType) {
            case ZonedTimestamp.SCHEMA_NAME:
                return ZonedDateTime.parse(time).withZoneSameInstant(ZoneId.of(TIME_ZONE)).format(DateTimeFormatter.ofPattern(DATETIME_FORMATTER));
            //return LocalDateTime.parse(time, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            case Timestamp.SCHEMA_NAME:
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(time)), ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(DATETIME_FORMATTER));
            case Date.SCHEMA_NAME:
                return LocalDate.ofEpochDay(Long.parseLong(time)).toString();
            default:
                return "";
        }
    }

}
