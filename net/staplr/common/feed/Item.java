package net.staplr.common.feed;


import java.util.Date;

import net.staplr.common.message.Message;

public class Item implements Comparable
{
	private Message msg_message;
	private Date d_timeSent;
	
	public Item(Message msg_message)
	{
		this.msg_message = msg_message;
		d_timeSent = new Date();
	}
	
	public int compareTo(Object item_other)
	{
		Date d_this = d_timeSent;
		Date d_other = null;
		
		try{
			d_other = ((Item)item_other).getTimeSent();
		} catch (Exception e) {
			System.err.println("Could not get time sent from comparing Item");
			return 0;
		} 
		
		if(d_timeSent.equals(((Item)item_other).getTimeSent())) return 0;
		else if(d_this.before(d_other)) return -1;
		else if(d_this.after(d_other)) return 1;
		else return 0;
	}
	
	public Date getTimeSent()
	{
		return d_timeSent;
	}
	
	public Message getMessage()
	{
		return msg_message;
	}
	
	public void setMessage(Message msg_message)
	{
		this.msg_message = msg_message;
	}
}