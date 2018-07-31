package com.example.etl.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public final class DateTimeUtils {
    public static  String  getdatetime(){
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        java.util.Date date = new java.util.Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(calendar.DATE, -0);
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");
        String datetime =dateFormat.format(calendar.getTime());
        return datetime;
    }
    public static  String  getdateminone(){
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        java.util.Date date = new java.util.Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(calendar.DATE, -1);
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");
        String datetime =dateFormat.format(calendar.getTime());
        return datetime;
    }

    public static  String  getmonthminone(){
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        java.util.Date date = new java.util.Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(calendar.MONTH, -1);
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-01");
        String datetime =dateFormat.format(calendar.getTime());
        return datetime;
    }

    public static  int   getmonthminonenum(){
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        java.util.Date date = new java.util.Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(calendar.MONTH, 0);
        int month = calendar.get(Calendar.MONTH);
        return month;
    }
    public static  String  getmonth(){
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        java.util.Date date = new java.util.Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(calendar.MONTH, 0);
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-01");
        String datetime =dateFormat.format(calendar.getTime());
        return datetime;
    }
    public static  String  getlastyearmonth(){
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        java.util.Date date = new java.util.Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(calendar.MONTH, 0);
        calendar.add(calendar.YEAR, -1);
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-01");
        String datetime =dateFormat.format(calendar.getTime());
        return datetime;
    }
    public static  String  getlastyearlastmonth(){
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        java.util.Date date = new java.util.Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(calendar.MONTH, -1);
        calendar.add(calendar.YEAR, -1);
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-01");
        String datetime =dateFormat.format(calendar.getTime());
        return datetime;
    }

    public static  String  getmonthaddone(){
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        java.util.Date date = new java.util.Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(calendar.MONTH, 1);
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-01");
        String datetime =dateFormat.format(calendar.getTime());
        return datetime;
    }
    public static  String  getmonthaddtow(){
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        java.util.Date date = new java.util.Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(calendar.MONTH, 2);
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-01");
        String datetime =dateFormat.format(calendar.getTime());
        return datetime;
    }

    public static  String  getlastweek(){
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        java.util.Date date = new java.util.Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(calendar.DATE, -7);
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");
        String datetime =dateFormat.format(calendar.getTime());
        return datetime;
    }
    public static  String  getyear(){
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        java.util.Date date = new java.util.Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(calendar.DATE, 0);
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy");
        String datetime =dateFormat.format(calendar.getTime());
        return datetime;
    }

}

 