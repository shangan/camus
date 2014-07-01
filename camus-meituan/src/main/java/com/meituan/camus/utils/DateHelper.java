package com.meituan.camus.utils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * A helper class for date and time operations.
 * @author Jianming Jin
 *
 */
public class DateHelper {

	public final static long SECOND_TIME_MILLIS = 1000l;
	public final static long MINUTE_TIME_MILLIS = 60 * SECOND_TIME_MILLIS;
	public final static long HOUR_TIME_MILLIS = 60 * MINUTE_TIME_MILLIS;
	public final static long DAY_TIME_MILLIS = 24 * HOUR_TIME_MILLIS;
	public final static long WEEK_TIME_MILLIS = 7 * DAY_TIME_MILLIS;
	
	private final static long CST_DIFF = 8 * 60 * 60 * 1000l;
	
	
	//---------------------------------------------
	//	Functions related to Date generation
	//---------------------------------------------

	public static Date date(int year, int month, int day)
	{
		Calendar calendar = Calendar.getInstance();
		calendar.clear();
		calendar.set(year, month - 1, day);
		return calendar.getTime();
	}
	
	public static Date date(int yyyymmdd)
	{
		int year = yyyymmdd / 10000;
		int month = (yyyymmdd / 100) % 100;
		int day = yyyymmdd % 100;
		return date(year, month, day);
	}

	
	//---------------------------------------------
	//	Functions related to TimeMillis generation
	//---------------------------------------------

	public static long timeMillis(int year, int month, int day, int hour, int minute, int second)
	{
		Calendar calendar = Calendar.getInstance();
		calendar.clear();
		calendar.set(year, month - 1, day, hour, minute, second);
		return calendar.getTimeInMillis();
	}
	
	public static long timeMillis(int[] time)
	{
		Calendar calendar = Calendar.getInstance();
		calendar.clear();
		calendar.set(time[0], time[1] - 1, time[2], time[3], time[4], time[5]);
		return calendar.getTimeInMillis();
	}
	
	public static long hourStartTimeMillis(long timeMillis)
	{
		return timeMillis / HOUR_TIME_MILLIS * HOUR_TIME_MILLIS;
	}
	
	public static long dayStartTimeMillis(long timeMillis)
	{
		return (timeMillis + CST_DIFF) / DAY_TIME_MILLIS * DAY_TIME_MILLIS - CST_DIFF;
	}

	public static long dayStartTimeMillis(int yyyymmdd)
	{
		if(yyyymmdd < 19700101)
		{
			return 0;
		}
		
		int year = yyyymmdd / 10000;
		int month = (yyyymmdd % 10000) / 100;
		int day = yyyymmdd % 100;
		return timeMillis(year, month, day, 0, 0, 0);
	}

	/** Return the first millisecond of the given date.
	 * @param yyyymmdd The date string. The 2 char month is between
	 *  01 and 12.
	 * @return 00:00:00 of the given date in millisecond.
	 */
	public static long dayStartTimeMillis(String yyyymmdd)
	{
		return dayStartTimeMillis(Integer.valueOf(yyyymmdd));

	}

	public static long todayStartTimeMillis()
	{
		long timeMillis = System.currentTimeMillis();
		return dayStartTimeMillis(timeMillis);
	}


	//---------------------------------------------
	//	Functions related to hour operations
	//---------------------------------------------

	public static int hour()
	{
		Calendar calendar = Calendar.getInstance();
		return calendar.get(Calendar.HOUR_OF_DAY);
	}
	
	public static int hour(long timeMillis)
	{
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timeMillis);
		return calendar.get(Calendar.HOUR_OF_DAY);
	}
	
	//---------------------------------------------
	//	Functions related to day operations
	//---------------------------------------------

	public static int today()
	{
		long timeMillis = System.currentTimeMillis();
		return toDay(timeMillis);
	}
	
	public static String todayString()
	{
		long timeMillis = System.currentTimeMillis();
		return toDayString(timeMillis);
	}

	public static int toDay(long timeMillis)
	{
		String dayString = toDayString(timeMillis);
		return Integer.parseInt(dayString);
	}

	/** Return a string of the format yyyymmdd representing
	 *  the given time.
	 * 
	 * @param timeMillis
	 * @return
	 */
	public static String toDayString(long timeMillis)
	{
		Date date = new Date(timeMillis);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		
		String result = "";
		result += year;
		result += (month < 10) ? ("0" + month) : month;
		result += (day < 10) ? ("0" + day) : day;

		return result;
	}

	public static String toTimeString(long timeMillis){
		Date date = new Date(timeMillis);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(date);
	}
	
	public static int[] toTimeArray(long timeMillis)
	{
		int[] result = new int[6];
		
		Date date = new Date(timeMillis);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		result[0] = calendar.get(Calendar.YEAR);
		result[1] = calendar.get(Calendar.MONTH) + 1;
		result[2] = calendar.get(Calendar.DAY_OF_MONTH);
		result[3] = calendar.get(Calendar.HOUR_OF_DAY);
		result[4] = calendar.get(Calendar.MINUTE);
		result[5] = calendar.get(Calendar.SECOND);

		return result;
	}
	
	public static boolean isDay(int yyyymmdd)
	{
		int year = yyyymmdd / 10000;
		int month = (yyyymmdd % 10000) / 100;
		int day = yyyymmdd % 100;
		return isDay(year, month, day);
	}
	
	public static boolean isDay(int year, int month, int day)
	{
		int dayNumber = getMonthDayNumber(year, month);
		if( (day > dayNumber) || (day < 1) )
		{
			return false;
		}
		
		return true;
	}
	
	public static int getMonthDayNumber(int year, int month)
	{
		switch(month)
		{
		case 1: return 31;
		case 2: return isLeapYear(year) ? 29 : 28;
		case 3: return 31;
		case 4: return 30;
		case 5: return 31;
		case 6: return 30;
		case 7: return 31;
		case 8: return 31;
		case 9: return 30;
		case 10: return 31;
		case 11: return 30;
		case 12: return 31;
		default: return 0;
		}
	}
	
	/**
	 * The valid days belong to [firstDay, lastDay]
	 * @param firstDay
	 * @param lastDay
	 * @return
	 */
	public static List<Integer> daysBetween(int firstDay, int lastDay)
	{
		long startTimeMillis = dayStartTimeMillis(firstDay);
		long endTimeMillis = dayStartTimeMillis(lastDay);
		int dayNumber = (int)((endTimeMillis - startTimeMillis) / DAY_TIME_MILLIS) + 1;
		dayNumber = Math.max(dayNumber, 0);
		
		List<Integer> days = new ArrayList<Integer>(dayNumber);

		for(int i = 0; i < dayNumber; i ++)
		{
			long timeMillis = startTimeMillis + i * DAY_TIME_MILLIS;
			days.add(toDay(timeMillis));
		}

		return days;
	}
	
	public static List<Integer> daysSince(int startDay, int dayNumber)
	{
		List<Integer> days = new ArrayList<Integer>(Math.abs(dayNumber));
		
		long startTimeMillis = dayStartTimeMillis(startDay);

		if(dayNumber > 0)
		{
			for(int i = 0; i < dayNumber; i ++)
			{
				long timeMillis = startTimeMillis + i * DAY_TIME_MILLIS;
				days.add(toDay(timeMillis));
			}
		}
		else if(dayNumber < 0)
		{
			for(int i = 0; i > dayNumber; i --)
			{
				long timeMillis = startTimeMillis + i * DAY_TIME_MILLIS;
				days.add(toDay(timeMillis));
			}
		}
		
		return days;
	}

	/** Return the day after the given date.
	 * @param yyyymmdd A string of the format yyyymmdd.
	 */
	public static String dayAfter(String yyyymmdd)
	{
		long millis = dayStartTimeMillis(yyyymmdd) + DAY_TIME_MILLIS;
		return toDayString(millis);
	}

	public static int dayAfter(int yyyymmdd, int dayNumber)
	{
		long millis = dayStartTimeMillis(yyyymmdd) + DAY_TIME_MILLIS * dayNumber;
		return toDay(millis);
	}

	/** Return the day before the given date.
	 * @param yyyymmdd A string of the format yyyymmdd.
	 */
	public static String dayBefore(String yyyymmdd)
	{
		long millis = dayStartTimeMillis(yyyymmdd) - DAY_TIME_MILLIS;
		return toDayString(millis);
	}

	public static int dayBefore(int yyyymmdd, int dayNumber)
	{
		long millis = dayStartTimeMillis(yyyymmdd) - DAY_TIME_MILLIS * dayNumber;
		return toDay(millis);
	}
	
	//---------------------------------------------
	//	Functions related to Year
	//---------------------------------------------
	
	public static boolean isLeapYear(int year)
	{
		if(year % 4 != 0)
		{
			return false;
		}
		
		if(year % 100 == 0)
		{
			if (year % 400 != 0)
			{
				return false;
			}
		}
		
		return true;
	}
	
	public static boolean isSameDay(long day1, long day2){
		
		return DateHelper.dayStartTimeMillis(day1) == DateHelper.dayStartTimeMillis(day2);
	}

	
	public static void main(String args[])
	{
		System.out.println("Hour: " + hour());
		
		Timestamp ts = new Timestamp(System.currentTimeMillis());
		System.out.println(ts);
		
		System.out.println(isDay(20101301));
		System.out.println(isDay(20100229));
		System.out.println(isDay(20120229));
		
		System.out.println(timeMillis(2011, 3, 28, 0, 0, 0));
		System.out.println(timeMillis(2011, 4, 4, 0, 0, 0));
		System.out.println(new Date(1307259135000l));
		System.out.println(toDayString(System.currentTimeMillis()));
		
		List<Integer> days = daysBetween(20110129, 20110204);
		for(int day : days)
		{
			System.out.print(day + " ");
		}
		System.out.println();

		List<Integer> days1 = daysSince(20110129, 10);
		for(int day : days1)
		{
			System.out.print(day + " ");
		}
		System.out.println();
	}
	
}
