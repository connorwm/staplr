package net.staplr.logging;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class Error extends Entry
{
	private Exception e_exception;
	
	public Error(String str_source, String str_message)
	{
		super(str_source, Entry.Type.Error, "ERROR: "+str_message);
	}
	
	public Error(String str_source, String str_message, Exception e_exception)
	{
		super(str_source, Entry.Type.Error, str_message);
		this.e_exception = e_exception;
	}
	
	public String toString()
	{
		if(e_exception != null)
		{
			String str_exception = "";
			PrintWriter pw_exception = null;
			try {
				pw_exception = new PrintWriter(str_exception);
			} catch (FileNotFoundException e) {
				//TODO nothing since this will never happen with a dumb string :P
				// June 13, 2013 12:14 AM
			}
			e_exception.printStackTrace(pw_exception);
			
			super.str_message += System.lineSeparator()+str_exception;
		}
		
		return super.toString();
	}
}