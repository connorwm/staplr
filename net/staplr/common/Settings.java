package net.staplr.common;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import net.staplr.common.DatabaseAuth.Properties;
import net.staplr.logging.Entry;
import net.staplr.logging.Log;
import net.staplr.logging.Log.Options;
import net.staplr.logging.LogHandle;
import net.staplr.master.MasterCredentials;
import net.staplr.processing.Processor;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;


/**Universal Settings Object
 * @author murphyc1
 */
public class Settings
{
	public static enum Setting
	{
		downloaderTimeout,
		version,
		maxSlaveCount,
		masterKey,
		masterCommunicationPort,
		servicePort,
		agentName
	}

	private Object[] o_settings;
	public ArrayList<Credentials> c_credentials;
	public Map<String, DatabaseAuth> map_databaseAuth;
	private DatabaseAuth auth_settingsDB;
	private ArrayList<String> arrlist_ignoreWords;
	
	private LogHandle lh_settings;
	private boolean b_loadStatus;
	
	/**Instantiates Universal Settings Object
	 * @author connorwm
	 * @param l_main - Log file object
	 */
	public Settings(Log l_main)
	{		
		lh_settings = new LogHandle("stng", l_main);
		
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = null;
		Document document = null;
		o_settings = new String[Setting.values().length];
		c_credentials = new ArrayList<Credentials>();
		arrlist_ignoreWords = new ArrayList<String>();
		map_databaseAuth = new ConcurrentHashMap<String, DatabaseAuth>();
		auth_settingsDB = new DatabaseAuth();
		
		map_databaseAuth.put("settings", auth_settingsDB);
		
		b_loadStatus = true;
		
		try{
			documentBuilder = dbf.newDocumentBuilder();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				document = documentBuilder.parse("./settings.xml");
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if(document == null)
				{
					//System.out.println("Document is null :P");
					lh_settings.write(Entry.Type.Error, "Fatal Error: settings document is null");
					b_loadStatus = false;
				} else {
					Element rootElement = document.getDocumentElement();
					NodeList childList = rootElement.getChildNodes();
					
					for(int childIndex = 0; childIndex < childList.getLength(); childIndex++)
					{
						Node child = childList.item(childIndex);
						
						if(child.getNodeName().equals("databaseAuth")) {
							NodeList databaseAuthTypeList = child.getChildNodes();
							
							// Parse through database auth properties of each type (feeds, entries, etc)
							for(int databaseAuthNodeListIndex = 0; databaseAuthNodeListIndex < databaseAuthTypeList.getLength(); databaseAuthNodeListIndex++)
							{
								Node databaseAuthTypeNode = databaseAuthTypeList.item(databaseAuthNodeListIndex);
								NodeList databaseAuthTypePropertyList = databaseAuthTypeNode.getChildNodes();
								
								// Now go through individual properties
								for(int databaseAuthPropertyIndex = 0; databaseAuthPropertyIndex < databaseAuthTypePropertyList.getLength(); databaseAuthPropertyIndex++)
								{
									Node databaseAuthPropertyNode = databaseAuthTypePropertyList.item(databaseAuthPropertyIndex);
									
									if(databaseAuthPropertyNode.getNodeName() != "#text")
									{
										for(int settingIndex = 0; settingIndex < DatabaseAuth.Properties.values().length; settingIndex++)
										{
											if(databaseAuthPropertyNode.getNodeName().equals(DatabaseAuth.Properties.values()[settingIndex].toString()))
											{
												if(databaseAuthTypeNode.getNodeName().equals("settings"))
												{
													auth_settingsDB.set(DatabaseAuth.Properties.values()[settingIndex], databaseAuthPropertyNode.getTextContent());
													lh_settings.write(DatabaseAuth.Properties.values()[settingIndex].toString()+" = '"+databaseAuthPropertyNode.getTextContent()+"'");
												}
												else if(databaseAuthTypeNode.getNodeName().equals("feeds"))
												{
													if(map_databaseAuth.get("feeds") == null)
													{
														DatabaseAuth auth_feedsDB = new DatabaseAuth();
														map_databaseAuth.put("feeds", auth_feedsDB);
													}
													
													map_databaseAuth.get("feeds").set(DatabaseAuth.Properties.values()[settingIndex], databaseAuthPropertyNode.getTextContent());
													lh_settings.write(DatabaseAuth.Properties.values()[settingIndex].toString()+" = '"+databaseAuthPropertyNode.getTextContent()+"'");
												}
												else if(databaseAuthTypeNode.getNodeName().equals("entries"))
												{
													if(map_databaseAuth.get("entries") == null)
													{
														DatabaseAuth auth_entriesDB = new DatabaseAuth();
														map_databaseAuth.put("entries", auth_entriesDB);
													}
													
													map_databaseAuth.get("entries").set(DatabaseAuth.Properties.values()[settingIndex], databaseAuthPropertyNode.getTextContent());
													lh_settings.write(DatabaseAuth.Properties.values()[settingIndex].toString()+" = '"+databaseAuthPropertyNode.getTextContent()+"'");
												}
												else if(databaseAuthTypeNode.getNodeName().equals("service"))
												{
													if(map_databaseAuth.get("service") == null)
													{
														DatabaseAuth auth_serviceDB = new DatabaseAuth();
														map_databaseAuth.put("service", auth_serviceDB);
													}
													
													map_databaseAuth.get("service").set(DatabaseAuth.Properties.values()[settingIndex], databaseAuthPropertyNode.getTextContent());
													lh_settings.write(DatabaseAuth.Properties.values()[settingIndex].toString()+" = '"+databaseAuthPropertyNode.getTextContent()+"'");
												}
												else if(databaseAuthTypeNode.getNodeName().equals("statistics"))
												{
													if(map_databaseAuth.get("statistics") == null)
													{
														DatabaseAuth auth_statisticsDB = new DatabaseAuth();
														map_databaseAuth.put("statistics", auth_statisticsDB);
													}
													
													map_databaseAuth.get("statistics").set(DatabaseAuth.Properties.values()[settingIndex], databaseAuthPropertyNode.getTextContent());
													lh_settings.write(DatabaseAuth.Properties.values()[settingIndex].toString()+" = '"+databaseAuthPropertyNode.getTextContent()+"'");
												}
												else if(databaseAuthTypeNode.getNodeName().equals("associations"))
												{
													if(map_databaseAuth.get("associations") == null)
													{
														DatabaseAuth auth_associationsDB = new DatabaseAuth();
														map_databaseAuth.put("associations", auth_associationsDB);
													}
													
													map_databaseAuth.get("associations").set(DatabaseAuth.Properties.values()[settingIndex], databaseAuthPropertyNode.getTextContent());
													lh_settings.write(DatabaseAuth.Properties.values()[settingIndex].toString()+" = '"+databaseAuthPropertyNode.getTextContent()+"'");
												}
												break;
											}
										}
									}
								}								
							}
						} else {
							for(int settingIndex = 0; settingIndex < Setting.values().length; settingIndex++)
							{
								if(child.getNodeName().equals(Setting.values()[settingIndex].toString()))
								{
									o_settings[settingIndex] = child.getTextContent();
									lh_settings.write(Setting.values()[settingIndex].toString()+"='"+child.getTextContent()+"'");
									break;
								}
							}
						}
					}
				}
				
				//----------------------------------------------
				//	Connect to Settings Database
				//----------------------------------------------
				
				if(auth_settingsDB.isComplete())
				{
					Mongo m_settings = null;
					DB db_settings = null;
					DBCollection col_backend = null;
					DBObject dbo_settings = null;
					DBObject dbo_searchCriteria = new BasicDBObject();
					dbo_searchCriteria.put("_purpose", "master");
					
					lh_settings.write("Connecting to settings database...");
					
					try
					{
						m_settings = new Mongo((String)auth_settingsDB.get(DatabaseAuth.Properties.location), Integer.parseInt((String)auth_settingsDB.get(DatabaseAuth.Properties.port)));
						db_settings = m_settings.getDB("settings");
					}
					catch (Exception e) 
					{
						lh_settings.write(Entry.Type.Error, "Failed to Connect:\r\n"+e.toString());
						b_loadStatus = false;
					} 
					finally
					{
						lh_settings.write("Connected; looking for document...");
						
						if(db_settings.authenticate((String)auth_settingsDB.get(Properties.username), ((String)auth_settingsDB.get(Properties.password)).toCharArray()))
						{
							// Load
							col_backend = db_settings.getCollection("backend");
							dbo_settings = col_backend.findOne(dbo_searchCriteria);
							
							if(dbo_settings == null)
							{
								lh_settings.write(Entry.Type.Error, "Could not find Master settings document");
								b_loadStatus = false;
							}
							else
							{
								lh_settings.write("Found settings document in database");
								
								for(int i_objectIndex = 0; i_objectIndex < dbo_settings.keySet().size(); i_objectIndex++)
								{
									String str_key = (String)dbo_settings.keySet().toArray()[i_objectIndex];
									
									if(str_key.equals("databaseAuth"))
									{
										DBObject dbo_databaseAuthList = (DBObject)dbo_settings.get(str_key);
										
										for(int i_authIndex = 0; i_authIndex < dbo_databaseAuthList.keySet().size(); i_authIndex++)
										{
											DBObject dbo_databaseAuth = (DBObject)dbo_databaseAuthList.get((String)dbo_databaseAuthList.keySet().toArray()[i_authIndex]);
											String str_name = (String)dbo_databaseAuthList.keySet().toArray()[i_authIndex];
											DatabaseAuth auth_new = map_databaseAuth.get(str_name);
											lh_settings.write("Auth - "+str_name);
											
											// By default, the settings.xml file that is stored locally will have the passwords matched
											// to the database name. The settings database ensures that the locations and specifics
											// of the database can easily be updated and moved.
											if(auth_new == null)
											{
												auth_new = new DatabaseAuth();
												lh_settings.write(Entry.Type.Warning, "\tNOTICE: No password for this database was loaded from local settings; password unknown");
											}
											
											for(int i_authChildIndex = 0; i_authChildIndex < dbo_databaseAuth.keySet().size(); i_authChildIndex++)
											{
												String str_authChild = (String)dbo_databaseAuth.keySet().toArray()[i_authChildIndex];
												
												for(int i_authPropertyIndex = 0; i_authPropertyIndex < DatabaseAuth.Properties.values().length; i_authPropertyIndex++)
												{
													if(str_authChild.equals(DatabaseAuth.Properties.values()[i_authPropertyIndex].toString()))
													{
														auth_new.set(DatabaseAuth.Properties.values()[i_authPropertyIndex], dbo_databaseAuth.get(str_authChild));
														lh_settings.write("\t"+DatabaseAuth.Properties.values()[i_authPropertyIndex]+"='"+dbo_databaseAuth.get(str_authChild)+"'");
														break;
													}
												}
											}
											
											map_databaseAuth.put(str_name, auth_new);
										}
									}
									else if(str_key.equals("masters"))
									{
										BasicDBList dbo_masters = (BasicDBList) dbo_settings.get("masters");
										
										for(int i_masterIndex = 0; i_masterIndex < dbo_masters.size(); i_masterIndex++)
										{
											DBObject dbo_master = (DBObject)dbo_masters.get(i_masterIndex);
											Credentials c_credential = new Credentials();
											lh_settings.write("Master");
											
											for(int i_masterChildIndex = 0; i_masterChildIndex < dbo_master.keySet().size(); i_masterChildIndex++)
											{
												String str_child = (String)dbo_master.keySet().toArray()[i_masterChildIndex];
												
												for(int i_masterPropertyIndex = 0; i_masterPropertyIndex < Credentials.Properties.values().length; i_masterPropertyIndex++)
												{
													if(Credentials.Properties.values()[i_masterPropertyIndex].toString().equals(str_child))
													{
														c_credential.set(Credentials.Properties.values()[i_masterPropertyIndex], (String)dbo_master.get(str_child));
														lh_settings.write("\t"+Credentials.Properties.values()[i_masterPropertyIndex].toString()+"='"+dbo_master.get(str_child)+"'");
														break;
													}
												}
											}
											
											c_credentials.add(c_credential);
										}
									}
									else
									{
										for(int i_settingsIndex = 0; i_settingsIndex < Properties.values().length; i_settingsIndex++)
										{
											if(str_key.equals(Setting.values()[i_settingsIndex].toString()))
											{
												lh_settings.write(str_key+"='"+dbo_settings.get(str_key)+"'");
												
												o_settings[i_settingsIndex] = dbo_settings.get(str_key);
												break;
											}
										}
									}
								}
								
								lh_settings.write("Loading Processor ignore words...");
								if(!loadProcessorIgnoreWords()) b_loadStatus = false;
							}
						}
						else
						{
							lh_settings.write(Entry.Type.Error, "Authentication Failed:\r\n"+db_settings.getLastError().toString());
							b_loadStatus = false;
						}
					}
				}
				else
				{
					b_loadStatus = false;
				}
			}
		}
	}
	
	/**Gets a setting
	 * @param setting
	 * @return Object
	 * @author murphyc1
	 */
	public synchronized Object get(Setting setting)
	{
		return o_settings[setting.ordinal()];
	}
	
	/**Sets a setting
	 * @param setting
	 * @param value
	 * @author murphyc1
	 */
	public synchronized void set(Setting setting, String value)
	{
		o_settings[setting.ordinal()] = value;
	}
	
	/**Adds a database authorization for use by assets of program
	 * @param auth_settingsDB
	 * @author murphyc1
	 */
	public void setSettingsDBAuth(DatabaseAuth auth_settingsDB)
	{
		this.auth_settingsDB = auth_settingsDB;
	}
	
	/**Checks to see if the settings loaded correctly (able to be successfully downloaded)
	 * @return Boolean value of whether or not the settings loaded successfully
	 * @author murphyc1
	 */
	public boolean loaded()
	{
		return b_loadStatus;
	}
	
	/**Loads ignore words from the exclusions file (exclusions.txt)
	 * @return Boolean value of whether or not the settings were loaded successfully
	 */
	private boolean loadProcessorIgnoreWords()
	{
		BufferedReader br_exclusionList = null;
		boolean b_opened = false;
		boolean b_loadedWords = true;
		
		try{
			br_exclusionList = new BufferedReader(new InputStreamReader(new FileInputStream("exclusions.txt")));
			b_opened = true;
		} 
		catch (IOException ioe_open)
		{
			lh_settings.write(Entry.Type.Error, "Could not open exclusions.txt to read ignore words");
			b_loadedWords = true;
		}
		
		if(b_opened)
		{
			String str_line = "";
			
			try 
			{
				while((str_line = br_exclusionList.readLine()) != null)
				{
					arrlist_ignoreWords.add(str_line);
				}
			} 
			catch (IOException e) 
			{
				lh_settings.write(Entry.Type.Error, "Could not parse/read all ignore words in exclusions file");
				b_loadedWords = false;
			}
			
			try {
				if(br_exclusionList.ready())	br_exclusionList.close();
			}
			catch (Exception e) {}
		}
		
		return b_loadedWords;
	}
	
	public ArrayList<String> getProcessorIgnoreWords()
	{
		return arrlist_ignoreWords;
	}
}