package net.staplr.common;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import net.staplr.master.MasterCredentials;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**Loads o_settings from settings.xml
 * @author murphyc1
 */
public class CopyOfSettings
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
	public ArrayList<MasterCredentials> mc_credentials;
	public DatabaseAuth auth_feedsDB;
	public DatabaseAuth auth_entriesDB;
	public DatabaseAuth auth_serviceDB;
	public DatabaseAuth auth_statisticsDB;
	public MasterCredentials mc_self;
	
	public CopyOfSettings(String str_location)
	{		
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = null;
		Document document = null;
		o_settings = new String[Setting.values().length];
		mc_credentials = new ArrayList<MasterCredentials>();
		mc_self = new MasterCredentials();
		
		auth_feedsDB = new DatabaseAuth();
		auth_entriesDB = new DatabaseAuth();
		auth_serviceDB = new DatabaseAuth();
		auth_statisticsDB = new DatabaseAuth();
		
		try{
			documentBuilder = dbf.newDocumentBuilder();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(str_location == null) str_location = "./settings.xml";
				
				System.out.println("Settings location: '"+str_location+"'");
				
				document = documentBuilder.parse(str_location);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if(document == null)
				{
					//System.out.println("Document is null :P");
					System.out.println("ERROR: Settings document is null");
				} else {
					Element rootElement = document.getDocumentElement();
					NodeList childList = rootElement.getChildNodes();
					
					for(int childIndex = 0; childIndex < childList.getLength(); childIndex++)
					{
						Node child = childList.item(childIndex);

						if(child.getNodeName().equals("databaseAuth")) {
							System.out.println("\tDatabase Auth");
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
									System.out.println("\t\t"+databaseAuthPropertyNode.getNodeName());
									
									if(databaseAuthPropertyNode.getNodeName() != "#text")
									{
										for(int settingIndex = 0; settingIndex < DatabaseAuth.Properties.values().length; settingIndex++)
										{
											System.out.println("\t\tComparing '"+databaseAuthPropertyNode.getNodeName()+"' to '"+DatabaseAuth.Properties.values()[settingIndex].toString()+"'");
											if(databaseAuthPropertyNode.getNodeName().equals(DatabaseAuth.Properties.values()[settingIndex].toString()))
											{
												System.out.println("\t\t\tFound");
												if(databaseAuthTypeNode.getNodeName().equals("feeds"))
												{								
													System.out.println("'"+databaseAuthPropertyNode.getTextContent()+"'");
													auth_feedsDB.set(DatabaseAuth.Properties.values()[settingIndex], databaseAuthPropertyNode.getTextContent());
													System.out.println("\t\t\t-feeds");
												}
												if(databaseAuthTypeNode.getNodeName().equals("entries"))
												{
													auth_entriesDB.set(DatabaseAuth.Properties.values()[settingIndex], databaseAuthPropertyNode.getTextContent());
													System.out.println("\t\t\t-entries");
												}
												if(databaseAuthTypeNode.getNodeName().equals("service"))
												{
													auth_serviceDB.set(DatabaseAuth.Properties.values()[settingIndex], databaseAuthPropertyNode.getTextContent());
													System.out.println("\t\t\t-service");
												}
												if(databaseAuthTypeNode.getNodeName().equals("statistics"))
												{
													auth_statisticsDB.set(DatabaseAuth.Properties.values()[settingIndex], databaseAuthPropertyNode.getTextContent());
													System.out.println("\t\t\t-statistics");
												}
												
												break;
											}
										}
									}
								}								
							}
						} else if (child.getNodeName().equals("masters")) {
							System.out.println("\tMasters");
							if(child.hasChildNodes())
							{
								Node mastersNode = child;
								
								for(int i_masterIndex = 0; i_masterIndex < mastersNode.getChildNodes().getLength(); i_masterIndex++)
								{
									if(mastersNode.getChildNodes().item(i_masterIndex).hasChildNodes())
									{
										System.out.println("\t\tMaster["+i_masterIndex+"]");
										Node masterNode = mastersNode.getChildNodes().item(i_masterIndex);
										MasterCredentials masterCredentials = new MasterCredentials();
										
										for(int i_masterChildIndex = 0; i_masterChildIndex < masterNode.getChildNodes().getLength(); i_masterChildIndex++)
										{
											Node masterPropertyNode = masterNode.getChildNodes().item(i_masterChildIndex);
											System.out.print("\t\t\t"+masterPropertyNode.getTextContent()+"=");
											
											for(int i_masterPropertyIndex = 0; i_masterPropertyIndex < MasterCredentials.Properties.values().length; i_masterPropertyIndex++)
											{
												if(masterPropertyNode.getNodeName().equals(MasterCredentials.Properties.values()[i_masterPropertyIndex].toString()))
												{
													System.out.print(MasterCredentials.Properties.values()[i_masterPropertyIndex].toString());
													masterCredentials.set(MasterCredentials.Properties.values()[i_masterPropertyIndex], masterPropertyNode.getTextContent());
													break;
												}
											}
											
											System.out.print("\r\n");
										}
										
										mc_credentials.add(masterCredentials);
									}	
								}
							}
						} else if (child.getNodeName().equals("self")) {
							System.out.println("\tSelf");
							
							for(int i_childIndex = 0; i_childIndex < child.getChildNodes().getLength(); i_childIndex++)
							{
								Node childNode = child.getChildNodes().item(i_childIndex);
								
								for(int i_credentialPropertyIndex = 0; i_credentialPropertyIndex < MasterCredentials.Properties.values().length; i_credentialPropertyIndex++)
								{
									if(childNode.getNodeName() != "#text")
									{
										System.out.print("\t\t"+childNode.getNodeName());
										if(childNode.getNodeName().equals(MasterCredentials.Properties.values()[i_credentialPropertyIndex].toString()))
										{
											System.out.println("='"+childNode.getTextContent()+"'");
											mc_self.set(MasterCredentials.Properties.values()[i_credentialPropertyIndex], childNode.getTextContent());
											
											break;
										} else {
											System.out.println("=!");
										}
									}
								}
							}
						} else {
							System.out.println("\tMisc");
							for(int settingIndex = 0; settingIndex < Setting.values().length; settingIndex++)
							{
								if(child.getNodeName().equals(Setting.values()[settingIndex].toString()))
								{
									System.out.println("\t\t"+Setting.values()[settingIndex].toString());
									o_settings[settingIndex] = child.getTextContent();
									break;
								}
							}
						}
					}
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
}