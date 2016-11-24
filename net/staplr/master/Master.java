package net.staplr.master;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.staplr.common.Settings;
import net.staplr.common.Settings.Setting;
import net.staplr.logging.Log;
import net.staplr.logging.Entry.Type;
import net.staplr.logging.LogHandle;

public class Master implements Runnable
{
	private ExecutorService es_components;
	private Communication c_communication;
	private Feeds f_feeds;
	private Settings s_settings;
	private Log l_main;
	private LogHandle lh_master;
	private DatabaseExecutor dx_executor;
	
	private boolean b_shouldRun = false;
	
	public Master()
	{
		l_main = new Log(Log.Instance.Server);
		l_main.setOption(Log.Options.ConsoleOutput, true);
		l_main.setOption(Log.Options.FileOutput, true);
		
		lh_master = new LogHandle("mas", l_main);
		
		lh_master.write("======================================================================================");
		lh_master.write("		  _________ __                 __        ");
		lh_master.write("		 /   _____//  |______  ______ |  |_______");
		lh_master.write("		 \\_____  \\\\   __\\__  \\ \\____ \\|  |\\_  __ \\");
		lh_master.write("		 /        \\|  |  / __ \\|  |_> >  |_|  | \\/");
		lh_master.write("		/_______  /|__| (____  /   __/|____/__| ");
		lh_master.write("		        \\/           \\/|__|              ");
		lh_master.write("");
		lh_master.write("	                             _    _                                     _ _   ");
		lh_master.write("	 ___ ___ _ _ _ ___ ___ ___ _| |  | |_ _ _    _____ ___ ___ ___ ___    _| | |_ ");
		lh_master.write("	| . | . | | | | -_|  _| -_| . |  | . | | |  |     | . |   | . | . |  | . | . |");
		lh_master.write("	|  _|___|_____|___|_| |___|___|  |___|_  |  |_|_|_|___|_|_|_  |___|  |___|___|");
		lh_master.write("	|_|                                  |___|                |___|               ");
		lh_master.write("");
		lh_master.write("");
		lh_master.write("======================================================================================");
		
		s_settings = new Settings(l_main);
		es_components = Executors.newCachedThreadPool();
		
		if(s_settings.loaded())
		{
			dx_executor = new DatabaseExecutor(s_settings, l_main);
			f_feeds = new Feeds(s_settings, dx_executor, l_main);
			c_communication = new Communication(s_settings, Integer.valueOf((String)s_settings.get(Setting.servicePort)), Integer.valueOf((String)s_settings.get(Setting.masterPort)), l_main, f_feeds);
			
			b_shouldRun = true;
		}
		else
		{
			lh_master.write(Type.Error, "Failed to Start: settings not loaded");
		}
	}
	
	public static void main(String[] args)
	{
		  Master master = new Master();
		  
		  if(args.length == 1)
		  {
			  master.startup(args[0]);
		  }  else {
			  System.err.println("Not enough parameters (1 needed)");
			  System.err.println("Parameter 1: first or join");
		  }
	}
	
	public void startup(String str_startType)
	{		
		if(dx_executor.connect())
		{
			if(str_startType.equals("first"))
			{
				lh_master.write("Starting as first");

				if(f_feeds.downloadFeeds())
				{
					f_feeds.buildSchedule();

					this.run();
				} else {
					lh_master.write(Type.Error, "Failed to download feeds; will not run");
				}
			} else if (str_startType.equals("join")) {
				lh_master.write("Starting as join");

				s_settings.set(Settings.Setting.masterPort, String.valueOf(new Random().nextInt(65535)));
				this.run();
				
				if(!c_communication.joinMasters())
				{
					lh_master.write(Type.Error, "Failed to join other masters");
				}
			} else {
				lh_master.write(Type.Error, "Invalid arg[0]: should be join or first");
			}
		}
	}
	
	public void run()
	{
		
		lh_master.write("Launching components...");

		es_components.submit(f_feeds);
		es_components.submit(c_communication);

		lh_master.write("Components launched");
	}
	
	
	public boolean shouldRun()
	{
		return b_shouldRun;
	}
}