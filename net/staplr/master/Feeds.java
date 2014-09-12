package net.staplr.master;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.staplr.common.Settings;
import net.staplr.common.feed.Feed;
import net.staplr.slave.Slave;
import net.staplr.logging.Log;
import net.staplr.logging.LogHandle;

public class Feeds implements Runnable
{
	private ExecutorService es_components;
	private SlaveMaster sm_master;
	public List<Feed> arr_feed;
	private ArrayList<String> arr_lostFeeds;
	private int i_redistributeNumber;
	private Map<String,ArrayList<String>> map_assignments;
	
	private DatabaseExecutor dx_executor;
	private Settings s_settings;
	private LogHandle lh_feeds;
	
	public Feeds(Settings s_settings, DatabaseExecutor dx_executor, Log l_main)
	{
		this.s_settings = s_settings;
		this.dx_executor = dx_executor;
		lh_feeds = new LogHandle("feed", l_main);
		
		arr_feed = Collections.synchronizedList(new ArrayList<Feed>());
		map_assignments = new ConcurrentHashMap<String, ArrayList<String>>();
		
		es_components = Executors.newCachedThreadPool();
		sm_master = new SlaveMaster(dx_executor, s_settings, l_main);
	}
	
	public void run()
	{
		es_components.submit(sm_master);
	}
	
	public boolean downloadFeeds()
	{
		boolean b_result = false;
		
		lh_feeds.write("Getting feeds");
		arr_feed = dx_executor.getAllFeeds();
		lh_feeds.write("Got "+arr_feed.size()+" feeds");
		
		ArrayList<String> arr_localMappings = new ArrayList<String>();
		
		// Establish original feed assignment
		for(int i_feedIndex = 0; i_feedIndex < arr_feed.size(); i_feedIndex++)
		{
			arr_localMappings.add(arr_feed.get(i_feedIndex).get(Feed.Properties.collection)+":"+arr_feed.get(i_feedIndex).get(Feed.Properties.name));
		}
		
		map_assignments.put("127.0.0.1", arr_localMappings);
		
		if(arr_feed.size() > 0)
			b_result = true;
		
		return b_result;
	}
	
	public void buildSchedule()
	{		
		lh_feeds.write("Building schedule...");
		
		for(int i_feedIndex = 0; i_feedIndex < arr_feed.size(); i_feedIndex++)
		{
			ScheduleItem si_new = new ScheduleItem(arr_feed.get(i_feedIndex), true);
			sm_master.sch_schedule.addItem(si_new);
		}
		
		lh_feeds.write("Added "+sm_master.sch_schedule.getItemCount()+" items to schedule");
		
		sm_master.sch_schedule.sort();
	}
	
	public Map<String,ArrayList<String>> getAssignments()
	{
		return map_assignments;
	}
	
	public ArrayList<String> getLostFeeds()
	{
		return arr_lostFeeds;
	}
	
	public void setLostFeeds(ArrayList<String> arr_lostFeeds)
	{
		this.arr_lostFeeds = arr_lostFeeds;
	}
	
	public int getRedistributeNumber()
	{
		return i_redistributeNumber;
	}
	
	public void setRedistributeNumber(int i_number)
	{
		i_redistributeNumber = i_number;
	}
}