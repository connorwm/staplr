package net.staplr.master;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import net.staplr.common.feed.Feed.Properties;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class Schedule
{
	private ArrayList<ScheduleItem> items;
	
	public Schedule()
	{
		items = new ArrayList<ScheduleItem>();
	}
	
	public long getWaitTillNext()
	{
		ScheduleItem next = null;
		
		try{
			next = items.get(0);
		} catch (IndexOutOfBoundsException e) {
			//System.out.println("WARNING: No schedule items");
			// Must not be any items
			// We can return zero since this condition is check first before starting to update
			return -1;
		}
		
		DateTime currentTime = new DateTime().now(DateTimeZone.UTC);
		//System.out.println("Current UTC Time: "+currentTime.toString("HH:mm:ss")+" "+currentTime.getMillis());
		//System.out.println("Next Time:        "+next.getTime().toString()+" "+next.getTime().getMillis());
		//System.out.println("Wait time: "+(next.getTime().getMillis() - currentTime.getMillis()));
		return (next.getTime().getMillis() - currentTime.getMillis());
	}
	
	public ScheduleItem advance()
	{
		if(items.size() > 0)
		{
			//System.out.println("Advancing! ("+items.get(0).toString()+")");
			
			ScheduleItem item = items.remove(0); // Remove it so we can insert at the end
			ScheduleItem newItem = new ScheduleItem(item.getFeed(), false);
			items.add(newItem); // Put it back in with the next goaltime
			
			return item; // Return it to be updated because it still is the next one
		}
		else return null;
	}
	
	public void addItem(ScheduleItem item)
	{
		// This will all need to be updated within a year
		// Just saying :P 
		// 11/24/2012 9:30 PM

		// Just was biotch!
		// Haha about 6 months off
		// 6/29/2013 9:41 PM
		
		items.add(item);		
	}
	
	public void sort()
	{
		Collections.sort(items);
		
		//System.out.println("First item: "+items.get(0).getFeed().get(Properties.collection));
	}
	
	public int getItemCount()
	{
		return items.size();
	}
}