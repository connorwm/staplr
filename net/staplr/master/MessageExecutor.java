package net.staplr.master;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.swing.JTextField;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import net.staplr.common.Credentials;
import net.staplr.common.DatabaseAuth;
import net.staplr.common.FormIntermediary;
import net.staplr.common.Communicator;
import net.staplr.common.DatabaseAuth.Properties;
import net.staplr.common.Settings;
import net.staplr.common.Settings.Setting;
import net.staplr.common.feed.Feed;
import net.staplr.common.message.Message;
import net.staplr.common.message.MessageEnsurer;
import net.staplr.common.message.Message.Type;
import net.staplr.common.message.Message.Value;
import net.staplr.common.TextFieldLogger;
import net.staplr.logging.Entry;
import net.staplr.logging.Log;
import net.staplr.logging.LogHandle;
import FFW.Network.DefaultSocketConnection;

public class MessageExecutor extends net.staplr.common.message.MessageExecutor
{
	private DefaultSocketConnection sc_client;
	private Settings s_settings;
	private LogHandle lh_worker;
	private ArrayList<Message> msg_inbox;
	public ArrayList<Message> msg_outbox;
	private Communicator c_communicator;
	private Feeds f_feeds;
	
	public MessageExecutor(DefaultSocketConnection sc_client, LogHandle lh_worker,
			MessageEnsurer me_ensurer, ArrayList<Message> msg_inbox,
			ArrayList<Message> msg_outbox, Settings s_settings,
			Communicator c_communicator, Feeds f_feeds) 
	{
		super(sc_client, lh_worker, me_ensurer, msg_inbox, msg_outbox, s_settings);
		
		this.sc_client = sc_client;
		this.lh_worker = lh_worker;
		this.msg_inbox = msg_inbox;
		this.msg_outbox = msg_outbox;
		this.s_settings = s_settings;
		this.c_communicator = c_communicator;
		this.f_feeds = f_feeds;
	}
	
	@Override
	public void executeAsPer(Message msg_message)
	{
		if(msg_message.getType() == Type.Request)
		{
			if(msg_message.getValue() == Value.Ping)
			{
				Message msg_response = new Message(Type.Response, Value.Pong);
				super.send(msg_response);
			}
			if(msg_message.getValue() == Value.Sync)
			{
				if(c_communicator.getConnectionCount() > 1)
				{
					lh_worker.write("Received request for Sync; building message with masters");
					
					Message msg_sync = new Message(Type.Response, Value.Sync);
					JSONArray json_masters = new JSONArray();
					Map<String, Integer> map_connectedMasters = c_communicator.getConnectedMasters();

					for(String str_address : map_connectedMasters.keySet())
					{
						JSONObject json_master = new JSONObject();
						
						// Make sure we don't give the client it's own address as "another" master
						if(!str_address.equals(sc_client.getAddress()))
						{							
							json_master.put("address", str_address);
							json_master.put("port", map_connectedMasters.get(str_address));
							
							json_masters.add(json_master);
						}	
					}
					
					msg_sync.addItem("masters", json_masters);
					
					lh_worker.write("Sending addresses of "+json_masters.size()+" masters");
					super.send(msg_sync);
					lh_worker.write("Sent Sync response");
				}
				else
				{
					lh_worker.write("No need for Sync; no other masters");
					
					// Theres no others so just say theres none
					Message msg_sync = new Message(Type.Response, Value.Sync);
					msg_sync.addItem("masters", "none");
					
					super.send(msg_sync);
					lh_worker.write("Sent Sync response");
				}
			}
			if(msg_message.getValue() == Value.FeedSync)
			{
				String[] arr_feeds = null;
				JSONArray json_feeds = null;
				Map<String,ArrayList<String>> map_assignments = f_feeds.getAssignments();
				
				lh_worker.write("FeedSyncing with "+sc_client.getAddress());
				
				try {
					if(msg_message.get("feeds") == null)
					{
						lh_worker.write(Entry.Type.Error, "feeds is null");
					}
					
					json_feeds = (JSONArray) msg_message.get("feeds");
					arr_feeds = new String[json_feeds.size()];
					
					for(int i_feedIndex = 0; i_feedIndex < json_feeds.size(); i_feedIndex++)
					{
						arr_feeds[i_feedIndex] = json_feeds.get(i_feedIndex).toString();
					}
				} catch (Exception e) {
					// Well that must not have been right
					lh_worker.write(Entry.Type.Error, "Invalid array: could not cast");
					respondInvalid(msg_message);
				}
				
				if(arr_feeds != null)
				{
					// Ensure that the mapping assignment array is already initiated
					if(map_assignments.get(sc_client.getAddress()) == null)
					{
						map_assignments.put(sc_client.getAddress(), new ArrayList<String>());
					}
					
					// Now we can add the mappings					
					for(int i_feedIndex = 0; i_feedIndex < arr_feeds.length; i_feedIndex++)
					{
						map_assignments.get(sc_client.getAddress()).add(arr_feeds[i_feedIndex]);
					}
					
					lh_worker.write("Updated "+arr_feeds.length+" feeds");
				}
			}
			if(msg_message.getValue() == Value.FeedDistribute)
			{
				Message msg_feedDistribute = new Message(Type.Response, Value.Feeds);
				ArrayList<Feed> arr_feeds = new ArrayList<Feed>();
				List<Feed> arr_masterFeeds = f_feeds.arr_feed;
			
				lh_worker.write("Distributing "+(arr_feeds.size()/(c_communicator.getConnectionCount()+1)+" feeds to "+sc_client.getAddress()));
				
				for(int i_feedIndex = 0; i_feedIndex < (arr_masterFeeds.size()/(c_communicator.getConnectionCount()+1)); i_feedIndex++)
				{
					arr_feeds.add(arr_masterFeeds.remove(i_feedIndex));
				}
				
				
				msg_feedDistribute.addFeeds("feeds", arr_feeds);
				super.send(msg_feedDistribute);			
				
				Message msg_test = new Message(Type.Request, Value.Ping);
				super.send(msg_test);
				
				// Rebuild the schedule since now we have different feeds
				f_feeds.buildSchedule();
				
				lh_worker.write("Finished distributing");
			}
		}
		else if (msg_message.getType() == Type.Response) 
		{
			if(msg_message.getValue() == Value.Ping)
			{
				Message msg_response = new Message(Type.Response, Value.Pong);
				super.send(msg_response);
			}
			if(msg_message.getValue() == Value.Sync)
			{
				lh_worker.write("Received Sync response; connecting to\r\nother masters...");
				JSONArray json_masters = null;
				JSONObject json_message = msg_message.toJSON();
				
				try
				{
					json_masters = (JSONArray) json_message.get("masters");
				} 
				catch (Exception e) 
				{
					if(json_message.get("masters").equals("none"))
					{
						lh_worker.write("No other masters to join; requesting current master for FeedDistribute");
						
						Message msg_feedDistribute = new Message(Type.Request, Value.FeedDistribute);
						super.send(msg_feedDistribute);
					}
					else
					{
						e.printStackTrace();
						super.respondInvalid(msg_message);
					}
				}
				
				if(json_masters != null)
				{			
					// TODO update the shit out of this
					// 7/10/14 1:49 AM What does this even do? LOL
					MessageExecutor[] mx_executors = new MessageExecutor[json_masters.size()];
					
					lh_worker.write("Received "+mx_executors.length+" addresses; attempting to connect to them...");
					
					for(int i_masterIndex = 0; i_masterIndex < json_masters.size(); i_masterIndex++)
					{
						JSONObject json_master = (JSONObject)json_masters.get(i_masterIndex);
						Credentials c_credential = new Credentials();
						
						System.out.println("\tGot the object");
						
						try{
							c_credential.set(Credentials.Properties.location, String.valueOf(json_master.get("address")));
							c_credential.set(Credentials.Properties.port, String.valueOf((long)json_master.get("port")));
						} catch (Exception e) {
							e.printStackTrace();
						}
						System.out.println("\tSet the properties:\r\n"+c_credential.toString());

						for(int i_credentialIndex = 0; i_credentialIndex < s_settings.c_credentials.size(); i_credentialIndex++)
						{
							Credentials c_credentialIndex = s_settings.c_credentials.get(i_credentialIndex);
							
							if(c_credentialIndex.get(Credentials.Properties.location).equals(c_credential.get(Credentials.Properties.location)))
							{
								if(c_credentialIndex.get(Credentials.Properties.port).equals(c_credential.get(Credentials.Properties.port)))
								{
									c_credential.set(Credentials.Properties.key, c_credentialIndex.get(Credentials.Properties.key));
									
									lh_worker.write("Connecting to\r\n"+c_credential.toString());
									
									MessageExecutor mx_executor = (net.staplr.master.MessageExecutor)c_communicator.connect(c_credential);

									if(mx_executor != null)
									{
										lh_worker.write("Connected; initiating FeedDistribute");
										
										Message msg_feedDistribute = new Message(Type.Request, Value.FeedDistribute);
										mx_executor.send(msg_feedDistribute);
									}
									
									break;
								}
							}
						}
					}
					
					
				}
			}
			if(msg_message.getValue() == Value.Feeds)
			{
				lh_worker.write("Received Feeds to Parse");
				
				JSONObject json_message = msg_message.toJSON();
				JSONArray json_feeds = null;
				
				try
				{
					json_feeds = (JSONArray)json_message.get("feeds");
				}
				catch (Exception e)
				{
					e.printStackTrace();
					lh_worker.write("Invalid Feeds message; sending response");
					super.respondInvalid(msg_message);
				}
				finally
				{
					List<Feed> lst_feeds = f_feeds.arr_feed;
					ArrayList<String> str_feeds = new ArrayList<String>(); // This is for the future feed sync
					int i_startCount = lst_feeds.size();
					
					for(int i_feedIndex = 0; i_feedIndex < json_feeds.size(); i_feedIndex++)
					{
						JSONObject json_feed = (JSONObject) json_feeds.get(i_feedIndex);
						Feed f_feed = new Feed();
						
						for(int i_propertyIndex = 0; i_propertyIndex < Feed.Properties.values().length; i_propertyIndex++)
						{
							if(json_feed.containsKey(Feed.Properties.values()[i_propertyIndex].toString()))
							{
								f_feed.set(Feed.Properties.values()[i_propertyIndex], String.valueOf(json_feed.get(Feed.Properties.values()[i_propertyIndex].toString())));
							}
						}
						
						lst_feeds.add(f_feed);
						str_feeds.add(f_feed.get(Feed.Properties.collection)+":"+f_feed.get(Feed.Properties.name));
					}
					
					lh_worker.write("Added "+(lst_feeds.size()-i_startCount)+" feeds");
					f_feeds.buildSchedule();
					
					// Now feed sync so all masters know what feeds you have
					Message msg_feedSync = new Message(Type.Request, Value.FeedSync);
					
					JSONArray json_feedList = new JSONArray();
					
					for (int i_feed = 0; i_feed < str_feeds.size(); i_feed++)
					{
						json_feedList.add(str_feeds.get(i_feed));
					}
					
					msg_feedSync.addItem("feeds", json_feedList);
					
					c_communicator.broadcast(msg_feedSync);
				}
			}
		}
	}
}