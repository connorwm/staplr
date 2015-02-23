package net.staplr.logging;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Entry implements Comparable
{
	public enum Type{
		Error,
		Status,
		Statistic,
		Warning
	}
	
	private String str_source;
	private Type t_type;
	private String str_dateTime;
	protected String str_message;
	
	public Entry(String str_source, Type t_type, String str_message)
	{
		this.str_source = str_source;
		this.t_type = t_type;
		this.str_message = str_message;
		str_dateTime = new Date().toString();
	}

	public int compareTo(Object e_entry)
	{
		SimpleDateFormat sdf_formatter = new SimpleDateFormat();
		Date d_this = null;
		Date d_other = null;
		
		try{
			d_this = sdf_formatter.parse(str_dateTime);
			d_other = sdf_formatter.parse(((Entry)e_entry).getDateTime());
		} catch (Exception e) {
			System.err.println("Failed to compare log entries: " + e.toString());
		}
		
		if(str_dateTime.equals(((Entry)e_entry).getDateTime())) return 0;
		else if(d_this.before(d_other)) return -1;
		else if(d_this.after(d_other)) return 1;
		else return 0;
	}
	
	public String getDateTime()
	{
		return str_dateTime;
	}
	
	public String toString()
	{
		String str_asString = str_dateTime+"\t["+str_source+"]: ";
		String[] arr_lines = str_message.split("\r\n|\n|\r");
		
		if(arr_lines.length > 1)
		{
			str_asString += arr_lines[0]+"\r\n";
			
			for(int i_lineIndex = 1; i_lineIndex < arr_lines.length; i_lineIndex++)
			{
				str_asString += "\t\t\t\t"+arr_lines[i_lineIndex];
				
				if(i_lineIndex+1 != arr_lines.length) str_asString += "\r\n";
			}
		} else {
			str_asString += str_message;
		}
		
		return str_asString;
	}
}