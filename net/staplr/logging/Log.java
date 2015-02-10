package net.staplr.logging;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;

import net.staplr.logging.Entry.Type;

public class Log
{
	public enum Options
	{
		ConsoleOutput,
		FileOutput,
		SaveOnlyOnError
	}
	
	private boolean[] b_options;	
	private ArrayList<Entry> arr_entry;
	private File f_logFile;
	private Writer w_logWriter;
	private boolean b_hasError;
	private String str_baseDirectory = "/var/www/master/";
	private String str_fileName;
	
	public Log()
	{
		b_options = new boolean[Options.values().length];
		arr_entry = new ArrayList<Entry>();
		b_hasError = false;
		
		for(int i_optionIndex = 0; i_optionIndex < Options.values().length; i_optionIndex++)
		{
			b_options[i_optionIndex] = false;
		}
	}
	
	public Log(String str_fileName)
	{
		this();
		
		this.str_fileName = str_fileName;
	}
	
	public synchronized void write(Entry e_entry)
	{		
		arr_entry.add(e_entry);
		
		if(b_options[Options.ConsoleOutput.ordinal()]) {
			if(Error.class.isInstance(e_entry))	System.err.println(e_entry.toString());
			else if (Warning.class.isInstance(e_entry)) System.out.println(e_entry.toString());
			else	System.out.println(e_entry.toString());
		}
		if(b_options[Options.FileOutput.ordinal()])	{
			synchronized (w_logWriter)
			{
				open();
				
				try {
					w_logWriter.write(e_entry.toString()+"\r\n");
					w_logWriter.flush();
				} catch (IOException e) {
					e.printStackTrace(System.err);
				}
				
				close();
			}
			
		}
		
		if(Error.class.isInstance(e_entry)) 
		{
			b_hasError = true;
		}
	}
	
	
	public void open()
	{
		f_logFile = new File(str_baseDirectory + str_fileName);
		
		b_options[Options.FileOutput.ordinal()] = true;
		try {
			w_logWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f_logFile, true)));
		} catch (FileNotFoundException e) {
			e.printStackTrace(System.err);
		}
	}
	
	public void close()
	{
		if(b_options[Options.SaveOnlyOnError.ordinal()] && b_hasError)
		{
			for(int i_entryIndex = 0; i_entryIndex < arr_entry.size(); i_entryIndex++)
			{
				try 
				{
					w_logWriter.write(arr_entry.get(i_entryIndex).toString()+"\r\n");
					w_logWriter.flush();
				} 
				catch (IOException e) 
				{
					e.printStackTrace(System.err);
				}
			}
		}
	}
	
	public String toString()
	{
		String str_asString = new String();
		
		for(int i_entryIndex = 0; i_entryIndex < arr_entry.size(); i_entryIndex++)
		{
			str_asString += arr_entry.toString() + "\r\n";
		}
		
		return str_asString;
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