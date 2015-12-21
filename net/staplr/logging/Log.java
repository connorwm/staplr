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
	
	public enum Instance
	{
		Server,
		Client
	}
	
	private String str_windowsServerBaseDirectory = "./";
	private String str_linuxServerBaseDirectory = "/var/www/master/";
	private String str_clientBaseDirectory = "./";
	
	private boolean[] b_options;	
	private File f_logFile;
	private Writer w_logWriter;
	private boolean b_hasError;
	private String str_baseDirectory;
	private String str_fileName;
	
	private int i_currentDay;
	
	/**Instance of the the logger across all classes.<br />
	 * Log handles can be used to write to the log from multiple threads safely.  
	 * @param instance Server or Client
	 */
	public Log(Instance instance)
	{
		// Determine which operating system we are running on
		// Server base directory will be dependent on this 
		String localOS = System.getProperty("os.name").toLowerCase();
		
		if(instance == Instance.Client) str_baseDirectory = str_clientBaseDirectory;
		else 
		{
			if(localOS.indexOf("win") >= 0) str_baseDirectory = str_windowsServerBaseDirectory;
			else if(localOS.indexOf("linux") >= 0) str_baseDirectory = str_linuxServerBaseDirectory;
			else 
			{
				str_baseDirectory = "./"; // Indeterminate where else to go so play it safe
				System.err.println("Indeterminate OS: Will be logging in the current directory");
			}
		}
		
		b_options = new boolean[Options.values().length];
		b_hasError = false;
		
		i_currentDay = DateTime.now(DateTimeZone.getDefault()).getDayOfMonth();
				
		for(int i_optionIndex = 0; i_optionIndex < Options.values().length; i_optionIndex++)
		{
			b_options[i_optionIndex] = false;
		}
		
		str_fileName = "staplr-" + DateTime.now(DateTimeZone.getDefault()).toString("M.d.y") + ".log";
	}
	
	/**Writes a given Entry into the log in a thread-safe manner.
	 * @param e_entry Entry to write: Status, Warning, or Error
	 */
	public synchronized void write(Entry e_entry)
	{		
		if(i_currentDay != DateTime.now(DateTimeZone.getDefault()).getDayOfMonth())
		{
			// We are on a new day
			// A new log file is necessary
			
			str_fileName = "staplr-" + DateTime.now(DateTimeZone.getDefault()).toString("M.d.y") + ".log";
			i_currentDay = DateTime.now(DateTimeZone.getDefault()).getDayOfMonth();
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
	
	/**Used by write() to open the log file for writing.<br />
	 * Opens the file through the class' BufferedWriter and will create the file if it does not exist.
	 */
	private void open()
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
	
	/**Used by write() to close the log file.<br/>
	 * Closes the file through the class' BufferedWriter.
	 */
	private void close()
	{
		try {
			w_logWriter.close();
		} catch (IOException excep_io) {
			excep_io.printStackTrace(System.err);
		}
	}
	
	/**Sets an option for the logger.
	 * @param option ConsoleOutput, FileOutput
	 * @param b_value True or False
	 */
	public void setOption(Options option, boolean b_value)
	{
		synchronized (b_options)
		{
			b_options[option.ordinal()] = b_value;
		}
	}

	/**Returns the Boolean condition of an option. 
	 * @param option ConsoleOutput, FileOutput
	 * @return Boolean enabled state
	 */
	public boolean isEnabled(Options option)
	{
		synchronized (b_options)
		{
			return b_options[option.ordinal()];
		}
	}
}