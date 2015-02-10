package net.staplr.master;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import net.staplr.common.Settings;
import net.staplr.common.Settings.Setting;
import net.staplr.common.feed.Feed;
import net.staplr.logging.Entry;
import net.staplr.logging.Entry.Type;
import net.staplr.logging.Log;
import net.staplr.logging.Log.Options;
import net.staplr.logging.LogHandle;
import net.staplr.slave.Slave;

public class SlaveMaster implements Runnable
{
	public Schedule sch_schedule;
	private DatabaseExecutor dx_executor;
	private Settings s_settings;
	private LogHandle lh_sm;
	private Log l_main;
	
	private boolean b_shouldRun;
	private ArrayList<Slave> arr_slave;
	private ArrayList<Future<?>> arr_slaveFuture;
	private ExecutorService es_slaves;
	
	public SlaveMaster(DatabaseExecutor dx_executor, Settings s_settings, Log l_main)
	{
		this.arr_slave = arr_slave;
		this.dx_executor = dx_executor;
		this.s_settings = s_settings;
		this.l_main = l_main;
		lh_sm = new LogHandle("sm", l_main);
		
		arr_slave = new ArrayList<Slave>();
		arr_slaveFuture = new ArrayList<Future<?>>();
		sch_schedule = new Schedule();
		b_shouldRun = true;
		
		try{
			es_slaves = Executors.newFixedThreadPool(Integer.valueOf((String)s_settings.get(Setting.maxSlaveCount)));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public void run() 
	{
		lh_sm.write("Beginning slave labor");
		
		while(b_shouldRun)
		{
			//System.out.println("About to wait: "+sch_schedule.getWaitTillNext());
			long waittime = sch_schedule.getWaitTillNext();
			
			if(sch_schedule.getItemCount() == 0)
			{
				lh_sm.write("No schedule items");
				waittime = 60*1000; // 60 seconds X 1000 milliseconds per second
			}
			
			if(waittime > 0)
			{
				lh_sm.write("Waiting " + (waittime * 1000 * 60) + " minutes...");
				
				try{
					Thread.sleep(waittime);
				} catch (Exception e) {}
				finally
				{
					lh_sm.write("Finished waiting; resuming...");
				}
			}
			else
			{
				lh_sm.write(Type.Warning, "Currently " + (Math.abs(waittime) * 1000 * 60) + " minutes behind schedule");
				//lh_sm.write((sch_schedule.getItemCount() > 0)+" - "+(arr_slave.size() < Integer.valueOf((String)s_settings.get(Setting.maxSlaveCount))));
			}
			
			// Recalculate how many slaves are available if necessary
			if(arr_slave.size() > 0)
			{
				lh_sm.write("Cleaning up finished slaves...");
				ArrayList<Integer> arr_doneSlaves = new ArrayList<Integer>();
				
				for(int i_slave = 0; i_slave < arr_slaveFuture.size(); i_slave++)
				{
					if(arr_slaveFuture.get(i_slave).isDone())
					{
						arr_doneSlaves.add(i_slave);
					}
				}
				
				lh_sm.write(arr_doneSlaves.size()+" slaves to be cleaned up");
				
				// Now actually delete them
				int startingSize = arr_doneSlaves.size();
				
				// We can only execute this loop if there are any done slaves
				// Else it will get stuck infinitely
				if(startingSize > 1)
				{
					while(arr_slave.size() > (startingSize - arr_doneSlaves.size()))
					{
						for(int i_slave = 0; i_slave < arr_doneSlaves.size(); i_slave++)
						{
							if(arr_slaveFuture.get(i_slave).isDone())
							{
								lh_sm.write("\tRemoving "+i_slave+"/"+arr_slave.size());
								arr_slave.remove(i_slave);
								arr_slaveFuture.remove(i_slave);
								break;
							}
						}
					}
					
					lh_sm.write("Active slaves: "+arr_slave.size());
				}
			}
			
			// Now we can get an accurate measure of how many are available
			if(sch_schedule.getItemCount() > 0 && arr_slave.size() < Integer.valueOf((String)s_settings.get(Setting.maxSlaveCount)))
			{			
				ScheduleItem si_current = sch_schedule.advance();
				
				lh_sm.write("Beginnning work on "+si_current.getFeed().get(Feed.Properties.name)+"; "+si_current.getTime().toString());
				
				Slave slv_newSlave = new Slave(si_current.getFeed(), dx_executor, s_settings, l_main);				
				
				Future<?> ftr_newSlave = es_slaves.submit(slv_newSlave);
				arr_slave.add(slv_newSlave);
				arr_slaveFuture.add(ftr_newSlave);
			}
		}
	}
}