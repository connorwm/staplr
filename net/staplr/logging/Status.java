package net.staplr.logging;

public class Status extends Entry
{	
	public Status(String str_source, String str_message)
	{
		super(str_source, Type.Status, str_message);
	}
}