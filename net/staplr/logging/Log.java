package net.staplr.logging;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import net.staplr.logging.Entry.Type;

public class Log
{
	public enum Options
	{
		ConsoleOutput,
		FileOutput
	}
	
	private boolean[] b_options;	
	private File f_logFile;
	private Writer w_logWriter;
	private boolean b_hasError;
	private String str_baseDirectory = "/var/www/master/";
	private String str_fileName;
	
	private int i_currentDay;
	private DateTime dt_local;
	
	public Log()
	{
		b_options = new boolean[Options.values().length];
		b_hasError = false;
		
		dt_local = new DateTime(DateTimeZone.getDefault());
		i_currentDay = dt_local.getDayOfMonth();
				
		for(int i_optionIndex = 0; i_optionIndex < Options.values().length; i_optionIndex++)
		{
			b_options[i_optionIndex] = false;
		}
		
		str_fileName = "staplr-" + dt_local.toString("M.d.y") + ".log";
	}
	
	public synchronized void write(Entry e_entry)
	{		
		if(i_currentDay != dt_local.getDayOfMonth())
		{
			// We are on a new day
			// A new log file is necessary
			
			str_fileName = "staplr-" + dt_local.toString("M.d.y") + ".log";
			i_currentDay = dt_local.getDayOfMonth();
		}
		
		if(b_options[Options.ConsoleOutput.ordinal()]) {
			if(Error.class.isInstance(e_entry))	System.err.println(e_entry.toString());
			else if (Warning.class.isInstance(e_entry)) System.out.println(e_entry.toString());
			else	System.out.println(e_entry.toString());
		}
		if(b_options[Options.FileOutput.ordinal()])	{
			open();

			try {
				w_logWriter.write(e_entry.toString()+"\r\n");
				w_logWriter.flush();
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}

			close();
		}
		
		if(Error.class.isInstance(e_entry)) 
		{
			b_hasError = true;
		}
	}
	
	
	public void open()
	{
		f_logFile = new File(str_baseDirectory + str_fileName);
		
		// Create the file if it is the first time
		try {
			f_logFile.createNewFile();
		} catch (IOException excep_create) {
			excep_create.printStackTrace(System.err);
		}
		
		try {
			w_logWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f_logFile, true)));
		} catch (FileNotFoundException e) {
			e.printStackTrace(System.err);
		}
	}
	
	public void close()
	{
		try {
			w_logWriter.close();
		} catch (IOException excep_io) {
			excep_io.printStackTrace(System.err);
		}
	}
	
	public void setOption(Options option, boolean b_value)
	{
		synchronized (b_options)
		{
			b_options[option.ordinal()] = b_value;
		}
	}

	public boolean isEnabled(Options option)
	{
		synchronized (b_options)
		{
			return b_options[option.ordinal()];
		}
	}
}