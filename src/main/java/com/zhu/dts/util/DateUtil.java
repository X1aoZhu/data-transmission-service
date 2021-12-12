package com.zhu.dts.util;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @Author ZhuHaiBo
 * @Create 2021/12/9 23:46
 */
public class DateUtil {

    private static final String TIME_ZONE = "Asia/Shanghai";

    private static final String DATETIME_FORMATTER = "yyyy-MM-dd HH:mm:ss.SSS";

    public static String handlerDateTime(String time) {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(time);
        return zonedDateTime.withZoneSameInstant(ZoneId.of(TIME_ZONE)).format(DateTimeFormatter.ofPattern(DATETIME_FORMATTER));
    }

}
