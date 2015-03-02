package net.staplr.service;

import java.awt.TextArea;
import java.awt.TextField;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import javax.swing.JTextArea;
import javax.swing.JTextField;

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
import net.staplr.logging.Log;
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
		if(msg_message.getType() == Message.Type.Request)
		{
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
			if(msg_message.getValue() == Message.Value.UpdateSettings)
			{
				DatabaseAuth auth_settingsDB = new DatabaseAuth();
				JSONObject json_auth = (JSONObject)msg_message.get("databaseAuth");
				
				for(int i_propertyIndex = 0; i_propertyIndex < DatabaseAuth.Properties.values().length; i_propertyIndex++)
				{
					if(json_auth.containsKey(Properties.values()[i_propertyIndex].toString()))
					{
						auth_settingsDB.set(Properties.values()[i_propertyIndex], json_auth.get(Properties.values()[i_propertyIndex].toString()));
					}
				}
				
				s_settings.setSettingsDBAuth(auth_settingsDB);
				
				for(int i_settingIndex = 0; i_settingIndex < Settings.Setting.values().length; i_settingIndex++)
				{
					if(msg_message.get(Settings.Setting.values()[i_settingIndex].toString()) != null)
					{
						s_settings.set(Setting.values()[i_settingIndex], (String)msg_message.get(Settings.Setting.values()[i_settingIndex].toString()));
					}
				}
			}
			if(msg_message.getValue() == Message.Value.Logs)
			{
				Message msg_logContents = new Message(Type.Response, Value.Logs);
				String str_logType = String.valueOf(msg_message.get("log"));
				InputStream is_log = null;
				BufferedReader br = null;
				String str_logLine = null;
				String str_logFile = null;
				String str_logContents = "";
				
				if(str_logType == "master")
				{
					str_logFile = "master.log";	
				}
				else if(str_logType == "listener")
				{
					str_logFile = "listener.log";
				}
				else if(str_logType == "ensurer")
				{
					str_logFile = "ensurer.log";
				}
				else if(str_logType == "settings")
				{
					str_logFile = "settings.log";
				}
				else
				{
					lh_worker.write("Unknown request for log: '"+str_logType+"'");
					str_logContents = "-- Unknown Log '"+str_logType+"' --";
				}
				
				msg_logContents.addItem("type", str_logType);
				
				if(str_logFile != null)
				{
					try {
						is_log = this.getClass().getResourceAsStream(str_logFile);
					} catch (Exception e) {
						lh_worker.write("ERROR: Cannot open master log file\r\n"+e.toString());
						str_logContents = "-- Error when Opening Log --";
					} 
					finally {
						try {
							br = new BufferedReader(new InputStreamReader(is_log));
							
							while ((str_logLine = br.readLine()) != null)
							{
								str_logContents += str_logLine;
							}
							
							br.close();
							is_log.close();
						}
						catch (Exception e)
						{
							lh_worker.write("ERROR: During reading log file:"+e.toString());
							
							str_logContents += "-- Error Encountered While Reading --";
						
							try{
								br.close();
							} catch (Exception closeException) {}
							try{
								is_log.close();
							} catch (Exception closeException) {}
						}
					}
				}
				
				msg_logContents.addItem("contents", str_logContents);
				super.send(msg_logContents);
			}
			if(msg_message.getValue() == Message.Value.Disconnect)
			{
				lh_worker.write("Attempting to disconnect");
				sc_client.disconnect();
			}
			if(msg_message.getValue() == Message.Value.GarbageCollection)
			{
				System.gc();
			}
			if(msg_message.getValue() == Message.Value.Feeds)
			{
				Message msg_feedList = new Message(Type.Response, Value.Feeds);
				JSONArray json_feeds = new JSONArray();
				
				for(String str_feed : f_feeds.getAssignments().get("127.0.0.1"))
				{
					json_feeds.add(str_feed);
				}
				
				msg_feedList.addItem("feeds", json_feeds);
				
				super.send(msg_feedList);
			}
			if(msg_message.getValue() == Message.Value.Disconnect)
			{
				lh_worker.write("Client has requested disconnect; disconnecting...");
				
				try
				{
					sc_client.close();
				} 
				catch(Exception e)
				{
					lh_worker.write("Failed to disconnect");
				}
				
				if(sc_client.isClosed())
				{
					lh_worker.write("Disconnected successfully");
				}
			}
		}
		else if(msg_message.getType() == Message.Type.Response)
		{
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
				catch (Exception e)
				{
					tf_logger.log("Encountered error while parsing settings: "+e.toString(), TextFieldLogger.ErrorStyle);
					e.printStackTrace();
				}
			}
			if(msg_message.getValue() == Message.Value.Logs)
			{
				String str_logType = String.valueOf(msg_message.get("type"));
				String str_logContents = String.valueOf(msg_message.get("contents"));
				TextArea ta_log = null;
				
				if(str_logType == "master")
				{
					ta_log = (TextArea) fi_main.get("log_master");
				}
				else if(str_logType == "listener")
				{
					ta_log = (TextArea) fi_main.get("log_listener");
				}
				else if(str_logType == "ensurer")
				{
					ta_log = (TextArea) fi_main.get("log_ensurer");
				}
				else if(str_logType == "settings")
				{
					ta_log = (TextArea) fi_main.get("log_settings");
				}
				else
				{
					lh_worker.write("ERROR: Unknown log type received back: '"+str_logType+"'");
					super.respondInvalid(msg_message);
				}
				
				if(ta_log != null)
				{
					ta_log.setText(str_logContents);
					ta_log.setCaretPosition(str_logContents.length()-1);
				}
			}
			if(msg_message.getValue() == Message.Value.Feeds)
			{
				lh_worker.write("Received feeds response!");
				
				JTextArea ta_feeds = (JTextArea)fi_main.get("ta_feeds");
				String str_feeds = "";
				JSONArray json_feeds = null;
				
				try
				{
					json_feeds = (JSONArray)msg_message.get("feeds");
				}
				catch(Exception e)
				{
					lh_worker.write("Failed to convert?");
					e.printStackTrace();
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
					catch (Exception e)
					{
						lh_worker.write("Failure parsing received feed list: possible erraneous message");
						e.printStackTrace();
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
								catch (Exception e)
								{
									lh_worker.write("Failure parsing received feed list: possible misformatted feed: '"+str_feed+"'");
									e.printStackTrace();
								}

								if(b_parsed) str_feeds += (str_collection+"\t\t\t"+str_feed+"\n");
							}
						}
					}					
					
					lh_worker.write("Finished parsing");
				
					ta_feeds.setText(str_feeds);
					lh_worker.write("Set feeds");
				}
				else
				{
					lh_worker.write("Feeds was null after retrieval from message");
				}
			}
		}
	}
	
	public void setFormIntermediary(FormIntermediary fi_main)
	{
		this.fi_main = fi_main;
	}
}