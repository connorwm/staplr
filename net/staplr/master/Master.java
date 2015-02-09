package net.staplr.master;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.staplr.common.Credentials.Properties;
import net.staplr.common.DatabaseAuth;
import net.staplr.common.Communicator;
import net.staplr.common.Settings;
import net.staplr.common.Settings.Setting;
import net.staplr.common.feed.Feed;
import net.staplr.common.message.Message;
import net.staplr.common.message.MessageExecutor;
import net.staplr.logging.Entry;
import net.staplr.logging.Log;
import net.staplr.logging.Entry.Type;
import net.staplr.logging.LogHandle;
import net.staplr.slave.Slave;

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
		l_main = new Log("master.log");
		l_main.setOption(Log.Options.ConsoleOutput, true);
		lh_master = new LogHandle("mas", l_main);
		
		s_settings = new Settings(l_main);
		es_components = Executors.newCachedThreadPool();
		
		if(s_settings.loaded())
		{
			dx_executor = new DatabaseExecutor(s_settings, l_main);
			f_feeds = new Feeds(s_settings, dx_executor, l_main);
			c_communication = new Communication(s_settings, Integer.valueOf((String)s_settings.get(Setting.servicePort)), Integer.valueOf((String)s_settings.get(Setting.masterCommunicationPort)), l_main, f_feeds);
			
			b_shouldRun = true;
		}
		else
		{
			lh_master.write("Failed to Start: settings not loaded");
		}
	}
	
	public static void main(String[] args)
	{
		  Master master = new Master();
		  
		  if(args.length == 1)
		  {
			  master.startup(args[0]);
		  }  else {
			  System.out.println("Not enough parameters (1 needed)");
			  System.out.println("Parameter 1: first or join");
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
					lh_master.write("Failed to download feeds; will not run");
				}
			} else if (str_startType.equals("join")) {
				lh_master.write("Starting as join");

				s_settings.set(Settings.Setting.masterCommunicationPort, String.valueOf(new Random().nextInt(65535)));
				this.run();
				c_communication.joinMasters();
			} else {
				lh_master.write(Type.Error, "Invalid arg[0]");
			}
		}
		else
		{
			lh_master.write("Could not fully connect to databases");
		}
	}
	
	public void run()
	{
		lh_master.write("Connecting to databases...");		
		if(dx_executor.connect())
		{			
			lh_master.write("Connected; now launching components...");
			
			es_components.submit(f_feeds);
			es_components.submit(c_communication);
			
			lh_master.write("Components launched");
		} else {
			lh_master.write(Entry.Type.Error, "Could not fully connect to databases: "+dx_executor.getLastError());
		}
	}
	
	
	public boolean shouldRun()
	{
		return b_shouldRun;
	}
}