package com.capgemini.commons;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

public class CalendarUtil {
    public static String calcCalendarCurrentDay(){
        TimeZone zone=TimeZone.getTimeZone("EST");
        Locale locale=Locale.US;
        Calendar cld=Calendar.getInstance(zone,locale);
        SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
        df.setTimeZone(cld.getTimeZone());
        return df.format(cld.getTime());
    }
}
