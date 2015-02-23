package net.staplr.master;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import net.staplr.common.feed.Feed;
import net.staplr.common.feed.Feed.Properties;
import net.staplr.logging.LogHandle;

public class ScheduleItem implements Comparable
{
	private DateTime dt_goalTime;
	private Feed f_feed;
	private int ttl;
	
	public ScheduleItem(Feed f_feed, boolean first)
	{
		this.f_feed = f_feed;
		ttl = Integer.valueOf((String)f_feed.get(Feed.Properties.ttl));
		
		if(f_feed.getLastDate() != null)
		{
			dt_goalTime = f_feed.getLastDate();
			
			if(first)
			{
				//System.out.println(f_feed.get(Properties.collection)+" Last Updated: "+f_feed.getLastDate().toString("HH:mm:ss"));
				//System.out.println(f_feed.get(Properties.collection)+" Goal Time:    "+dt_goalTime.toString("HH:mm:ss"));	
			}
			else
			{
				dt_goalTime = (new DateTime().now(DateTimeZone.UTC)).plusMinutes(ttl);
				//System.out.println(f_feed.get(Properties.collection)+" New Goal Time:    "+goalTime.toString("HH:mm:ss"));	
			}
		}
		else
		{
			//System.out.println(f_feed.get(Properties.collection)+" has not been updated before; setting time to now...");
			dt_goalTime = new DateTime().now(DateTimeZone.UTC);
		}
	}
	
	public DateTime getTime()
	{
		return dt_goalTime;
	}
	
	public Feed getFeed()
	{
		return f_feed;
	}
	
	public String toString()
	{
		return f_feed.toJSON().toString();
	}
	
	public int compareTo(Object o_comparison)
	{
		int i_result = 0;
		
		ScheduleItem si_comparison = (ScheduleItem)o_comparison;
		//DateTime dt_comparison = si_comparison.getTime();
		int compareTTL = Integer.valueOf(si_comparison.getFeed().get(Feed.Properties.ttl));
		
		// TEMPORARY: Sort schedule items by goal time initially
		// 			  This is so that the 15, 20 minute waited feeds
		// 			  do not have to wait on the 30 minute feeds to be
		//			  updated. That would cause feeds to be unnecessarily behind
		if(compareTTL == ttl)
		{
			return 0;
		}
		else if(compareTTL > ttl)
		{
			return -1;
		}
		else if (compareTTL < ttl)
		{
			return 1;
		}
		
		return i_result;
	}
}