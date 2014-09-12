package net.staplr.common;

import java.util.Random;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.staplr.common.Credentials.Properties;
import net.staplr.common.feed.Feed;
import net.staplr.common.message.Message;
import net.staplr.common.message.MessageExecutor;
import net.staplr.common.message.Message.Value;
import net.staplr.common.Settings;
import net.staplr.logging.Log;
import net.staplr.logging.Entry;
import net.staplr.logging.LogHandle;
import net.staplr.master.Feeds;
import net.staplr.master.Master;
import FFW.Network.*;

public class Communicator implements Runnable
{
	private DefaultSocketConnection sc_listener;
	private Settings s_settings;
	private boolean b_run;
	
	private ExecutorService es_workerPool;
	private ArrayList<Worker> arr_worker;
	private ArrayList<DefaultSocketConnection> arr_client;
	
	private LogHandle lh_communication;
	private Log l_main;
	private Type t_type;
	private Feeds f_feeds;
	
	public enum Type
	{
		Service,
		Master
	}
	
	public Communicator(Settings s_settings, int i_port, Type t_type, Log l_main, Feeds f_feeds)
	{
		this.s_settings = s_settings;
		sc_listener = new DefaultSocketConnection(i_port);
		this.t_type = t_type;		
		lh_communication = new LogHandle("comr", l_main);
		this.l_main = l_main;
		this.f_feeds = f_feeds;
		
		es_workerPool = Executors.newCachedThreadPool();
		b_run = true;
		arr_worker = new ArrayList<Worker>();
		arr_client = new ArrayList<DefaultSocketConnection>();
		
		if(t_type == Type.Service)
		{
			System.out.println("Starting as service on port "+i_port);
		}
		
		// TODO
		if(sc_listener.getLastError() != null){
			lh_communication.write(Entry.Type.Error, sc_listener.getLastError().toString());
		}
	}
	
	public void run()
	{
		if(sc_listener.isBound())
		{
			lh_communication.write("Bound on port "+sc_listener.getPort());
			
			while(b_run)
			{
				addClient(sc_listener.accept(), false);
			}
		} else {
			lh_communication.write("ERROR: Not bound");
		}
	}
	
	public synchronized void stop()
	{
		b_run = false;
		
		// TODO Cleanup time!
	}
	
	public Worker addClient(DefaultSocketConnection sc_newClient, boolean b_isJoining)
	{
		Worker w_new = null;
		
		synchronized (arr_client) 
		{
			arr_client.add(sc_newClient);
		}
		synchronized (arr_worker) 
		{
			arr_worker.add(new Worker(sc_newClient, s_settings, l_main, b_isJoining, t_type, f_feeds, this));
			es_workerPool.submit(arr_worker.get(arr_worker.size()-1));
			
			w_new = arr_worker.get(arr_worker.size()-1);
		}
		
		int i_runningWorkers = 0;
		
		for(int i_workerIndex = 0; i_workerIndex < arr_client.size(); i_workerIndex++)
		{
			if(arr_client.get(i_workerIndex).isConnected()) i_runningWorkers++;
		}
		
		lh_communication.write("Added; running: "+i_runningWorkers);
		
		return w_new;
	}
	
	public MessageExecutor connect(Credentials credentials)
	{
		DefaultSocketConnection sc_new = new DefaultSocketConnection((String)credentials.get(Credentials.Properties.location), Integer.valueOf((String)credentials.get(Credentials.Properties.port)));
		
		sc_new.connect();
		
		if(sc_new.isConnected())
		{
			lh_communication.write("Connected to master "+credentials.get(Credentials.Properties.location));
			Worker w_new = addClient(sc_new, true);

			Message msg_key = new Message(Message.Type.Request, Value.Authorization);
			msg_key.addItem("key", (String)credentials.get(Properties.key));

			w_new.mx_executor.send(msg_key);

			return w_new.mx_executor;
		}
		 else {
			 lh_communication.write("ERROR: Failed to connect to "+sc_new.getAddress());
		}
		
		return null;
	}
	
	/**Returns a mapping of the addresses and ports of the connected masters
	 * @return Map of the addresses to the ports
	 * @author murphyc1
	 */
	public Map<String, Integer> getConnectedMasters()
	{
		Map<String, Integer> map_masterAddresses = new HashMap<String, Integer>();
		
		for(int i_client = 0; i_client < arr_client.size(); i_client++)
		{
			String str_address = arr_client.get(i_client).getAddress();
			str_address = str_address.substring(1, str_address.indexOf(":"));
			
			map_masterAddresses.put(str_address, arr_client.get(i_client).getPort());
		}
		
		return map_masterAddresses;
	}
	
	public synchronized void redistributeFeeds(String str_address)
	{
		lh_communication.write("Redistributing feeds from "+str_address);
		
		ArrayList<String> arr_lostFeeds = f_feeds.getAssignments().get(str_address);
		String str_feeds = "";
		
		for(String str_feed : arr_lostFeeds)
		{
			str_feeds += str_feed+"\r\n";
		}
		
		lh_communication.write("Removing remnants of connection...");
		
		// Remove remnants of the connection
		Object o_toRemove = null;
		
		for(Worker w_worker : arr_worker) 
		{
			if(w_worker.getClientAddress() == str_address) 
			{
				o_toRemove = w_worker;
				break;
			}
		}
		
		arr_worker.remove(o_toRemove);
		o_toRemove = null;
		
		for(DefaultSocketConnection sc_client : arr_client) 
		{
			if(sc_client.getAddress() == str_address) 
			{
				o_toRemove = sc_client;
				break;
			}
		}
		
		arr_worker.remove(o_toRemove);
		
		lh_communication.write("Completed removal");
		lh_communication.write("Lost feeds from "+str_address+": \r\n"+str_feeds);
		
		// Determine course of action depending on how many other masters there are
		if(arr_worker.size() == 0)
		{
			lh_communication.write("Assigning lost feeds to self; no other masters active");
			
			// Put together a feed object with as little known variables as possible (2)
			ArrayList<Feed> arr_lostFeedsPartial = new ArrayList<Feed>();
			
			for(int i_feedIndex = 0; i_feedIndex < arr_lostFeeds.size(); i_feedIndex++)
			{
				Feed f_feed = new Feed();
				f_feed.set(Feed.Properties.collection, arr_lostFeeds.get(i_feedIndex).substring(0, arr_lostFeeds.get(i_feedIndex).indexOf(":")));
				f_feed.set(Feed.Properties.name, arr_lostFeeds.get(i_feedIndex).substring(arr_lostFeeds.get(i_feedIndex).indexOf(":")+1));
				
				arr_lostFeedsPartial.add(f_feed);
			}
			
			f_feeds.arr_feed.addAll(arr_lostFeedsPartial);
			
			lh_communication.write("Now responsible for "+f_feeds.arr_feed.size());
		}
		else
		{
			lh_communication.write("Contacting other masters to distribute lost feeds");
			
			/*
			 * To reorganize the lost feeds:
			 * Each master will send a random number to other masters
			 * The master with the highest number distributes the feeds
			 * 
			 * Since this cannot be accomplished on this thread we will save
			 * the lost feeds in the Feeds object
			 */
			
			f_feeds.setLostFeeds(arr_lostFeeds);
			
			Message msg_redistributeNumber = new Message(Message.Type.Request, Value.RedistributeNumber);
			msg_redistributeNumber.addItem("number", String.valueOf((new Random()).nextInt(101)));
			
			broadcast(msg_redistributeNumber);
		}
		
		lh_communication.write("Feed distribution now resolved");
	}
	
	public void broadcast(Message msg_broadcast)
	{
		lh_communication.write("Broadcasting to all other masters:\r\n"+msg_broadcast.toString());
		
		for(Worker w_worker : arr_worker)
		{
			w_worker.mx_executor.send(msg_broadcast);
		}
		
		lh_communication.write("Sent to "+arr_worker.size()+" masters");
	}
	
	public ArrayList<Worker> getWorkers()
	{
		return arr_worker;
	}
	
	public int getConnectionCount()
	{
		return arr_client.size();
	}
}