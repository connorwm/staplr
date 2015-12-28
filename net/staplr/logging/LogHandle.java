package net.staplr.logging;

import net.staplr.logging.Entry.Type;

public class LogHandle
{
	private String str_name;
	private Log l_log;
	
	public LogHandle(String str_name, Log l_log)
	{
		this.str_name = str_name;
		this.l_log = l_log;
	}
	
	public void write(Type t_level, String str_message)
	{
		l_log.write(new Entry(str_name, t_level, str_message));
	}
	
	public void write(String str_message)
	{
		l_log.write(new Entry(str_name, Type.Status, str_message));
	}
	
	public Log getLog()
	{
		return l_log;
	}
}