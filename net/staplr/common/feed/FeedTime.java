package net.staplr.common.feed;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FeedTime
{
	private String raw;
	private Date date;
	private DateFormat format;
	
	public FeedTime(String raw, String str_format)
	{		
		this.raw = raw;
		format = new SimpleDateFormat(str_format);
	}
	
	public Date convert()
	{
		// Clean up the format thanks to Atom feeds
		// 2012-12-28T18:30:02Z
		if(raw.endsWith("Z")) {
			raw = raw.substring(0, raw.length()-1);
			raw = raw.replace('T', ' ');
		}

		try{
			date = format.parse(raw);
		} catch (Exception e) {
			System.out.println("Unparsable date ["+raw+"] for ["+format.format(new Date())+"]:");
			e.printStackTrace();
		}
		
		return date;
	}
}