package net.staplr.master;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
		//////////////////////////////////////////////////////////////////////////////////////////////////////
		//
		//	REQUEST
		//
		//////////////////////////////////////////////////////////////////////////////////////////////////////
		if(msg_message.getType() == Type.Request)
		{
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	PING
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Value.Ping)
			{
				Message msg_response = new Message(Type.Response, Value.Pong);
				super.send(msg_response);
			}
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	SYNC
			//////////////////////////////////////////////////////////////////////////////////////////////////
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
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	FEED SYNC
			//////////////////////////////////////////////////////////////////////////////////////////////////
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
					
					lh_worker.write("Preparing FeedSync Response...");
					json_feeds = new JSONArray();
					
					for(int i_feedIndex = 0; i_feedIndex < map_assignments.get("127.0.0.1").size(); i_feedIndex++)
					{
						json_feeds.add((String)map_assignments.get("127.0.0.1").get(i_feedIndex));
					}
					
					Message msg_feedSync = new Message(Type.Response, Value.FeedSync);
					msg_feedSync.addItem("feeds", json_feeds);
					
					lh_worker.write("Sending...");
					super.send(msg_feedSync);
				}
			}
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	FEED DISTRIBUTE
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Value.FeedDistribute)
			{
				Message msg_feedDistribute = new Message(Type.Response, Value.Feeds);
				ArrayList<Feed> arr_feeds = new ArrayList<Feed>();
				List<Feed> arr_masterFeeds = f_feeds.arr_feed;
			
				lh_worker.write("Distributing "+(arr_masterFeeds.size()/(c_communicator.getConnectionCount()+1)+"/"+arr_masterFeeds.size()+" feeds to "+sc_client.getAddress()));
				
				for(int i_feedIndex = 0; i_feedIndex <= (arr_masterFeeds.size()/(c_communicator.getConnectionCount()+1)); i_feedIndex++)
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
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	CONNECTION CHECK
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Value.ConnectionCheck)
			{
				String str_address = (String)msg_message.get("address");
				
				if(str_address != null && str_address.length() > 0)
				{
					Set<String> map_masters = c_communicator.getConnectedMasters().keySet();
					String str_connectionStatus = "";
					
					lh_worker.write("Checking connection status of master at "+str_address);
					
					if(map_masters.contains(str_address))
					{
						lh_worker.write("Still connected to master "+str_address);
						str_connectionStatus = "up";
					}
					else
					{
						lh_worker.write(Entry.Type.Error, "Not connected to master");
						str_connectionStatus = "down";
					}
					
					Message msg_connectionCheckResponse = new Message(Type.Response, Value.ConnectionCheck);
					msg_connectionCheckResponse.addItem("address", str_address);
					msg_connectionCheckResponse.addItem("status", str_connectionStatus);
					
					super.send(msg_connectionCheckResponse);
				}
				else
				{
					lh_worker.write(Entry.Type.Error, "Invalid ConnectionCheck: no address specified");
					respondInvalid(msg_message);
				}
			}
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	REDISTRIBUTE NUMBER
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Value.RedistributeNumber)
			{
				int i_number = -1;
				String str_number = (String)msg_message.get("number");
				String str_address = (String)msg_message.get("address");
				
				if(str_number != null)
				{
					if(str_address != null)
					{
						try
						{
							i_number = Integer.valueOf(str_number);
						}
						catch (Exception e)
						{
							lh_worker.write(Entry.Type.Error, "Invalid redistribute number: could not be converted to an integer");
							respondInvalid(msg_message);
						}

						if(i_number != -1)
						{
							// Check to make sure no other master has used this number
							Map<String, FeedRedistribution> map_feedRedistribution = c_communicator.getParent().getFeedRedistributionMap();
							Map<String, Integer> map_redistributionNumbers = map_feedRedistribution.get(str_address).getRedistributeNumbers();

							if(c_communicator.getParent().getFeedRedistributionMap().values().contains(i_number))
							{
								lh_worker.write("Redistribute number has already been used ("+i_number+")");

								Message msg_alreadyAssigned = new Message(Type.Response, Value.DuplicateNumber);
								
								Iterator<String> itr_assignments = map_redistributionNumbers.keySet().iterator();
								String str_currentAddress = "";

								// Find out who else has it
								while(itr_assignments.hasNext())
								{
									str_currentAddress = itr_assignments.next();

									if(map_redistributionNumbers.get(str_address) == i_number)
									{
										// Now str_currentAddress has the address of the other master that has the duplicate number
										// We can now send them a duplicate message
										break;
									}
								}

								lh_worker.write("Sending already used response...");
								super.send(msg_alreadyAssigned);

								lh_worker.write("Notifying other master...");

								if(str_address != "127.0.0.1")
								{
									if(!c_communicator.sendTo(str_currentAddress, msg_alreadyAssigned))
									{
										lh_worker.write(Entry.Type.Error, "Failed to send to master at "+str_currentAddress+": does not exist/not connected");
									}
									else
									{
										lh_worker.write("Sent redistribute message to "+str_currentAddress);
									}
								}
								else
								{
									lh_worker.write("Duplicate number also my own; sending out redistribution number again...");
									c_communicator.getParent().redistributeFeeds(str_currentAddress);
								}
							}
							else
							{
								lh_worker.write("Valid redistribute number from "+sc_client.getAddress()+": "+i_number);
								map_feedRedistribution.get(str_address).getRedistributeNumbers().put(sc_client.getAddress(), i_number);

								// Now check to see if all numbers have been accounted for


								if(map_feedRedistribution.get(str_address).allNumbersReceived())
								{
									lh_worker.write("All redistribute numbers received");

									if(!map_feedRedistribution.get(str_address).IAmRedistributionLeader())
									{
										lh_worker.write("Master at "+map_feedRedistribution.get(str_address).getAddressOfHighest()+" has highest number");
									}
									else
									{
										lh_worker.write("I have the highest number");
										lh_worker.write("Gathering missing feeds...");

										ArrayList<String> arr_missingFeeds = new ArrayList<String>();

										
									}
								}
							}					
						}
					}
					else
					{
						lh_worker.write("No address given in message");
					}
				}
				else
				{
					lh_worker.write("No redistribution number given in message");
					respondInvalid(msg_message);
				}
			}
		}
		//////////////////////////////////////////////////////////////////////////////////////////////////////
		//
		//	RESPONSE
		//
		//////////////////////////////////////////////////////////////////////////////////////////////////////
		else if (msg_message.getType() == Type.Response) 
		{
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	PING
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Value.Ping)
			{
				Message msg_response = new Message(Type.Response, Value.Pong);
				super.send(msg_response);
			}
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	SYNC
			//////////////////////////////////////////////////////////////////////////////////////////////////
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
						lh_worker.write(Entry.Type.Error, "Invalid masters list received in Sync response");
						super.respondInvalid(msg_message);
					}
				}
				
				if(json_masters != null)
				{			
					MessageExecutor[] mx_executors = new MessageExecutor[json_masters.size()];
					
					lh_worker.write("Received "+mx_executors.length+" addresses; attempting to connect to them...");
					
					for(int i_masterIndex = 0; i_masterIndex < json_masters.size(); i_masterIndex++)
					{
						JSONObject json_master = (JSONObject)json_masters.get(i_masterIndex);
						Credentials c_credential = new Credentials();
						
						try{
							c_credential.set(Credentials.Properties.location, String.valueOf(json_master.get("address")));
							c_credential.set(Credentials.Properties.port, String.valueOf((Long)json_master.get("port")));
						} catch (Exception e) {
							e.printStackTrace();
						}

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
										lh_worker.write("Connected; initiating FeedDistribute...");
										
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
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	FEEDS
			//////////////////////////////////////////////////////////////////////////////////////////////////
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
					e.printStackTrace(System.err);
					lh_worker.write(Entry.Type.Error, "Invalid Feeds message; sending response");
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
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	FEED SYNC
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Value.FeedSync)
			{
				String[] arr_feeds = null;
				JSONArray json_feeds = null;
				Map<String,ArrayList<String>> map_assignments = f_feeds.getAssignments();
				
				lh_worker.write("FeedSyncing with "+sc_client.getAddress() + " to update assignments");
				
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
					// Clear previous mappings
					map_assignments.put(sc_client.getAddress(), new ArrayList<String>());
					
					// Now we can add the mappings					
					for(int i_feedIndex = 0; i_feedIndex < arr_feeds.length; i_feedIndex++)
					{
						map_assignments.get(sc_client.getAddress()).add(arr_feeds[i_feedIndex]);
					}
					
					lh_worker.write("Updated "+arr_feeds.length+" feeds");
				}
				else
				{
					lh_worker.write(Entry.Type.Error, "Failed to update feeds for "+sc_client.getAddress());
				}
			}
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	CONNECTION CHECK
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Value.ConnectionCheck)
			{
				String str_address = (String)msg_message.get("address");
				String str_connectionStatus = (String)msg_message.get("status");
				
				if(str_address != null && str_connectionStatus != null)
				{
					
				}
				else
				{
					lh_worker.write(Entry.Type.Error, "Invalid ConnectionCheck Response, missing information:\n\tAddress: "+str_address+"\n\tStatus: "+str_connectionStatus);
					respondInvalid(msg_message);
				}
			}
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	DUPLICATE NUMBER
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Value.DuplicateNumber)
			{
				String str_address = (String)msg_message.get("address");
				
				if(str_address != null)
				{
					lh_worker.write("My redistribution number is duplicate; sending a new one");
					c_communicator.getParent().redistributeFeeds(str_address);
				}
				else
				{
					lh_worker.write(Entry.Type.Error, "DuplicateNumber message is invalid: missing address of downed master");
					respondInvalid(msg_message);
				}
			}
		}
	}
}