package net.staplr.service;

import java.awt.TextArea;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import net.staplr.common.DatabaseAuth;
import net.staplr.common.FormIntermediary;
import net.staplr.common.DatabaseAuth.Properties;
import net.staplr.common.Settings;
import net.staplr.common.Settings.Setting;
import net.staplr.common.message.Message;
import net.staplr.common.message.MessageEnsurer;
import net.staplr.common.message.Message.Type;
import net.staplr.common.message.Message.Value;
import net.staplr.common.TextFieldLogger;
import net.staplr.logging.Entry;
import net.staplr.logging.LogHandle;
import net.staplr.master.Feeds;
import FFW.Network.DefaultSocketConnection;

public class MessageExecutor extends net.staplr.common.message.MessageExecutor
{
	private DefaultSocketConnection sc_client;
	private Settings s_settings;
	private LogHandle lh_worker;
	private ArrayList<Message> msg_inbox;
	public ArrayList<Message> msg_outbox;
	private FormIntermediary fi_main;
	private Feeds f_feeds;
	
	public MessageExecutor(DefaultSocketConnection sc_client, LogHandle lh_worker,
			MessageEnsurer me_ensurer, ArrayList<Message> msg_inbox,
			ArrayList<Message> msg_outbox, Settings s_settings, Feeds f_feeds) 
	{
		super(sc_client, lh_worker, me_ensurer, msg_inbox, msg_outbox, s_settings);
		
		this.sc_client = sc_client;
		this.lh_worker = lh_worker;
		this.msg_inbox = msg_inbox;
		this.msg_outbox = msg_outbox;
		this.s_settings = s_settings;
		this.f_feeds = f_feeds;
	}
	
	@Override
	public void executeAsPer(Message msg_message)
	{
		//////////////////////////////////////////////////////////////////////////////////////////////////
		//
		//	REQUEST
		//
		//////////////////////////////////////////////////////////////////////////////////////////////////
		if(msg_message.getType() == Message.Type.Request)
		{
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	SETTINGS
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Message.Value.Settings)
			{
				Message msg_settings = new Message(Type.Response, Value.Settings);
				
				JSONObject json_databaseAuth = new JSONObject();
				DatabaseAuth auth_settings = s_settings.map_databaseAuth.get("settings");
				
				json_databaseAuth.put("database", (String)auth_settings.get(Properties.database));
				json_databaseAuth.put("location", (String)auth_settings.get(Properties.location));
				json_databaseAuth.put("port", (String)auth_settings.get(Properties.port));
				json_databaseAuth.put("username", (String)auth_settings.get(Properties.username));
				json_databaseAuth.put("password", (String)auth_settings.get(Properties.password));
				
				msg_settings.addItem("databaseAuth", json_databaseAuth);
				
				msg_settings.addItem("masterKey", String.valueOf(s_settings.get(Setting.masterKey)));
				msg_settings.addItem("masterCommunicationPort", String.valueOf(s_settings.get(Setting.masterPort)));
				msg_settings.addItem("servicePort", String.valueOf(s_settings.get(Setting.servicePort)));
				
				super.send(msg_settings);
				msg_message.setRespondedTo();
			}
			
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	UPDATE SETTINGS
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Message.Value.UpdateSettings)
			{
				lh_worker.write("Updating settings...");
				
				DatabaseAuth auth_settingsDB = new DatabaseAuth();
				JSONObject json_auth = (JSONObject)msg_message.get("databaseAuth");
				
				// Grab database authorization properties from JSON node in message and put those properties into our configuration
				for(int i_propertyIndex = 0; i_propertyIndex < DatabaseAuth.Properties.values().length; i_propertyIndex++)
				{
					if(json_auth.containsKey(Properties.values()[i_propertyIndex].toString()))
					{
						auth_settingsDB.set(Properties.values()[i_propertyIndex], json_auth.get(Properties.values()[i_propertyIndex].toString()));
					}
				}
				
				// Set the Settings object's properties to the properties specified by the message
				s_settings.setSettingsDBAuth(auth_settingsDB);
				
				// Now set the other properties in Settings to those specified by the message
				for(int i_settingIndex = 0; i_settingIndex < Settings.Setting.values().length; i_settingIndex++)
				{
					if(msg_message.get(Settings.Setting.values()[i_settingIndex].toString()) != null)
					{
						s_settings.set(Setting.values()[i_settingIndex], (String)msg_message.get(Settings.Setting.values()[i_settingIndex].toString()));
					}
				}
				
				lh_worker.write("\tComplete");
			}
			
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	LOGS
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Message.Value.Logs)
			{
				Message msg_logContents = new Message(Type.Response, Value.Logs);
				InputStream is_log = null;
				BufferedReader br_log = null;
				String str_logLine = null;
				String str_logFile = lh_worker.getLog().getLogFilePath();
				String str_logContents = "";
				
				// Open log file and load the contents into a stream
				if(str_logFile != null)
				{
					// Try and open the log
					try 
					{
						is_log = new FileInputStream(str_logFile);
					} 
					catch (Exception excep_openLog) 
					{
						lh_worker.write(Entry.Type.Error, "Cannot open master log file: "+excep_openLog.toString());
						str_logContents = "-- Error when Opening Log --";
					}
					finally 
					{
						// If log file was opened, read it line by line into the string
						if(is_log != null)
						{
							try 
							{
								br_log = new BufferedReader(new InputStreamReader(is_log));
								
								while ((str_logLine = br_log.readLine()) != null)
								{
									str_logContents += str_logLine +"\r\n";
								}
							}
							catch (Exception excep_readLog)
							{
								lh_worker.write(Entry.Type.Error, "Problem while reading log file: "+excep_readLog.toString());
								
								str_logContents += "-- Error Encountered while Reading Log --";
							}
							
							// Close log buffered reader
							try
							{
								br_log.close();
							} 
							catch (Exception excep_readerClose) 
							{
								lh_worker.write(Entry.Type.Error, "Failed to close log buffered reader: "+excep_readerClose.toString());
							}
							
							// Close log file input stream
							try
							{
								is_log.close();
							} 
							catch (Exception excep_streamClose) 
							{
								lh_worker.write(Entry.Type.Error, "Failed to close log input stream: "+excep_streamClose.toString());
							}
						}
					}
				}
				
				// Place contents into response message
				// In the event that an error was encountered during reading the contents will contain that error
				msg_logContents.addItem("contents", str_logContents);
				
				super.send(msg_logContents);
			}
			
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	GARBAGE COLLECTION
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Value.GarbageCollection)
			{
				lh_worker.write("Running garbage collection...");
				System.gc();
				lh_worker.write("\tComplete");
			}
			
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	FEEDS
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Message.Value.Feeds)
			{
				Message msg_feedList = new Message(Type.Response, Value.Feeds);
				JSONArray json_feeds = new JSONArray();
				
				lh_worker.write("Received feeds request");
				
				// Put all feeds this master is responsible for and place them into an array
				for(String str_feed : f_feeds.getAssignments().get("127.0.0.1"))
				{
					json_feeds.add(str_feed);
				}
				
				// Add array of feed names to message to send back to client
				msg_feedList.addItem("feeds", json_feeds);
				lh_worker.write("Sending back list of "+json_feeds.size()+" feeds");
				
				super.send(msg_feedList);
			}
			
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	DISCONNECT
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Message.Value.Disconnect)
			{
				lh_worker.write("Client has requested disconnect; disconnecting...");
				
				try
				{
					sc_client.close();
				}
				catch(Exception excep_disconnect)
				{
					lh_worker.write(Entry.Type.Error, "Exception occurred while disconnecting: "+excep_disconnect.toString());
				}
				
				if(sc_client.isClosed())
				{
					lh_worker.write("Disconnected successfully");
				}
			}
		}
		//////////////////////////////////////////////////////////////////////////////////////////////////
		//
		//	RESPONSE
		//
		//////////////////////////////////////////////////////////////////////////////////////////////////
		else if(msg_message.getType() == Message.Type.Response)
		{
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	SETTINGS
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Message.Value.Settings)
			{
				TextFieldLogger tf_logger = fi_main.getLogger();
				
				tf_logger.log("Received settings; parsing...", TextFieldLogger.StandardStyle);
				
				try{
					JSONObject json_databaseAuth = (JSONObject)msg_message.get("databaseAuth");

					((JTextField) fi_main.get("db_database")).setText((String)json_databaseAuth.get("database"));
					((JTextField) fi_main.get("db_location")).setText((String)json_databaseAuth.get("location"));
					((JTextField) fi_main.get("db_port")).setText((String)json_databaseAuth.get("port"));
					((JTextField) fi_main.get("db_username")).setText((String)json_databaseAuth.get("username"));
					((JTextField) fi_main.get("db_password")).setText((String)json_databaseAuth.get("password"));

					((JTextField) fi_main.get("servicePort")).setText((String)msg_message.get("servicePort"));
					((JTextField) fi_main.get("masterCommunicationPort")).setText((String)msg_message.get("masterCommunicationPort"));
					((JTextField) fi_main.get("masterKey")).setText((String)msg_message.get("masterKey"));
				
					tf_logger.log("Settings parsed successfully", TextFieldLogger.SuccessStyle);
				}
				catch (Exception excep_parseSettings)
				{
					tf_logger.log("Encountered error while parsing received settings: "+excep_parseSettings.toString(), TextFieldLogger.ErrorStyle);
				}
			}
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	LOGS
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Message.Value.Logs)
			{
				lh_worker.write("Received logs response");
	
				String str_logContents = null;
				TextArea ta_log = (TextArea) fi_main.get("log");
				
				if(ta_log != null)
				{
					if(msg_message.get("contents") != null)
					{
						str_logContents = (String) msg_message.get("contents");
						
						lh_worker.write("Updating log contents...");
						ta_log.setText(str_logContents);
						ta_log.setCaretPosition(str_logContents.length() - 1);
						lh_worker.write("\tComplete");
					}
					else
					{
						lh_worker.write(Entry.Type.Error, "Logs response does not have contents");
						super.respondInvalid(msg_message);
					}
					
				} else lh_worker.write(Entry.Type.Error, "Could not get log TextArea");
			}
			//////////////////////////////////////////////////////////////////////////////////////////////////
			//	FEEDS
			//////////////////////////////////////////////////////////////////////////////////////////////////
			if(msg_message.getValue() == Message.Value.Feeds)
			{
				lh_worker.write("Received feeds response");
				
				JTextArea ta_feeds = (JTextArea)fi_main.get("ta_feeds");
				String str_feeds = "";
				JSONArray json_feeds = null;
				
				try
				{
					json_feeds = (JSONArray)msg_message.get("feeds");
				}
				catch(Exception excep_getFeeds)
				{
					lh_worker.write(Entry.Type.Error, "Failed to get feeds array from message: "+excep_getFeeds);
				}
				
				if(json_feeds  != null)
				{
					lh_worker.write("Parsing...");
					
					String[] arr_feeds = null;
					Object[] arr_rawFeeds = null;
					
					try {
						arr_rawFeeds = json_feeds.toArray();
						arr_feeds = new String[arr_rawFeeds.length];
						
						for(int i_feed = 0; i_feed < arr_rawFeeds.length; i_feed++)
						{
							arr_feeds[i_feed] = (String)arr_rawFeeds[i_feed];
						}
					}
					catch (Exception excep_parseFeeds)
					{
						lh_worker.write(Entry.Type.Error, "Exception occurred while parsing feeds list: "+excep_parseFeeds.toString());
					}
					
					if(arr_feeds != null)
					{
						Arrays.sort(arr_feeds);
						
						for(String str_feed : arr_feeds)
						{
							if(str_feed != null) 
							{
								String str_collection = "";
								boolean b_parsed = false;						

								try{
									str_collection = str_feed.substring(0, str_feed.indexOf(":"));
									str_feed = str_feed.substring(str_feed.indexOf(":")+1);

									b_parsed = true;
								} 
								catch (Exception excep_parseFeedNames)
								{
									lh_worker.write("Failure parsing received feed list: possible misformatted feed: '"+str_feed+"': "+excep_parseFeedNames.toString());
								}

								if(b_parsed) str_feeds += (str_collection+"\t\t\t"+str_feed+"\n");
							}
						}
					}					
					
					lh_worker.write("Finished parsing");
				
					ta_feeds.setText(str_feeds);
					lh_worker.write("Updated feeds TextArea with received feed names");
				}
			}
		}
	}
	
	public void setFormIntermediary(FormIntermediary fi_main)
	{
		this.fi_main = fi_main;
	}
}