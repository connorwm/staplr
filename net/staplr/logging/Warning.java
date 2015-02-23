package net.staplr.logging;

public class Warning extends Entry
{
	public Warning(String str_source, String str_message)
	{
		super(str_source, Entry.Type.Warning, "WARNING: " + str_message);
	}
	
	public String toString()
	{
		return super.toString();
	}
}