package net.staplr.slave;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import net.staplr.common.Settings;
import net.staplr.common.Settings.Setting;
import net.staplr.common.feed.FeedDocument;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import FFW.Utility.Error;
import FFW.Utility.ErrorProne;

/**Downloads the feed acquired from the location given by Coordinator
 * @param None
 * @author murphyc1
 */
public class Downloader implements ErrorProne
{
	private Settings settings;
	private HttpURLConnection con_connection;
	private String str_location;
	private BufferedReader br_connectionReader;
	private String str_rawDocument;
	private String feedDateFormat;
	private Document doc_feed;
	private FeedDocument fd_feed;
	
	private Error lastError;
	
	/**Initiates new Downloader for a feed source
	 * @author murphyc1
	 * @param str_location
	 */
	public Downloader(String str_location, Settings settings, String feedDateFormat)
	{
		this.str_location = str_location;		
		this.settings = settings;
		this.feedDateFormat = feedDateFormat;
	}
	
	/**Sets in motion the Downloader thread
	 * @author murphyc1	
	 */
	public boolean run()
	{
		// Errors in any method are printed via LastError constructor when they happen
		if(buildConnection())
		{
			if(connect())
			{
				if(download())
				{
					if(buildDocument()) return true;
					else return false;
				}
			}
		}
		
		return false;
	}
	
	private boolean buildConnection()
	{
		try {
			System.out.println("Going to connect to:\""+str_location+"\"");
			con_connection = (HttpURLConnection)new URL(str_location).openConnection();
			con_connection.setRequestMethod("GET");
			con_connection.setDoInput(true);
			con_connection.setDoOutput(false);
			con_connection.addRequestProperty("User-Agent", settings.get(Setting.agentName)+" "+settings.get(Setting.version));
			con_connection.setReadTimeout(6*1000);
		} catch (IOException excep_io) {
			lastError = new Error("con_connection", Error.Type.Connection, excep_io.toString());			
			return false;
		} catch (Exception excep_general) {
			lastError = new Error("con_connection", Error.Type.Connection, excep_general.toString());			
			return false;
		} 
		
		return true;
	}
	
	private boolean connect()
	{
		try{
			con_connection.connect();
		} catch (IOException e) {
			lastError = new Error("con_connection", Error.Type.Connection, e.toString());
			return false;
		} finally {
			int connectionStatus = 0;
			
			try{
				connectionStatus = con_connection.getResponseCode();
			} catch (Exception e) {
				lastError = new Error("con_connection", Error.Type.Connection, e.toString());
				return false;
			} finally {
				if(connectionStatus == HttpURLConnection.HTTP_ACCEPTED || connectionStatus == HttpURLConnection.HTTP_OK)
				{
					System.out.println("\tConnected");
					try{
						br_connectionReader = new BufferedReader(new InputStreamReader(con_connection.getInputStream()));
					} catch (Exception e) {
						lastError = new Error("br_connectionReader", Error.Type.Initiation, e.toString());
					} finally {
						System.out.println("\tCreated Reader");
					}
				} else {
					lastError = new Error("con_connection", Error.Type.Connection, "Status "+connectionStatus);
					return false;
				}
			}
		}
		
		return true;
	}
	
	private boolean download()
	{
		String line = null;
		
		try{
			while((line = br_connectionReader.readLine()) != null)
			{
				str_rawDocument += (line + "\n");
			}
		} catch (IOException excep_io) {
			lastError = new Error("con_connection", Error.Type.Receive, excep_io.toString());
			return false;
		} finally {
			//System.out.println("\tGot Content");
			
			if(str_rawDocument == null) {
				//System.out.println("\tBut its null");
				return false;
			}
			
			//System.out.println("First Line: '"+str_rawDocument.substring(0, str_rawDocument.indexOf('\n')));
			
			// Remove BOM if present
			// If not removed the XML parser will throw an error and not parse
			if(!str_rawDocument.startsWith("<") && str_rawDocument.contains("<"))
			{
				str_rawDocument = str_rawDocument.substring(str_rawDocument.indexOf("<"));
			}
			
			/*
			String str_encoding = con_connection.getContentEncoding();
			System.out.println("\t"+str_encoding);
			
			if(str_encoding == null)
			{
				System.out.println("Encoding is null");
				str_encoding = con_connection.getHeaderField("Content-Encoding");
				Map<String,List<String>> headers = con_connection.getHeaderFields();
				
				
				System.out.println("\t\t"+str_encoding);
				for(int headerIndex = 0; headerIndex < headers.size(); headerIndex++)
				{
					System.out.println("\t\t"+headers.keySet().toArray()[headerIndex]);
					for(int headerValueIndex = 0; headerValueIndex < headers.get(headers.keySet().toArray()[headerIndex]).size(); headerValueIndex++)
					{
						System.out.println("\t\t\t"+headers.get(headers.keySet().toArray()[headerIndex]).get(headerValueIndex));
					}
				}
				
				try{
					List<String> str_rawContentTypes = headers.get("Content-Type");
					
					for(int contentTypeIndex = 0; contentTypeIndex < str_rawContentTypes.size(); contentTypeIndex++)
					{
						String str_contentTypeRaw = str_rawContentTypes.get(contentTypeIndex);
						
						if(str_contentTypeRaw.contains("charset"))
						{
							try{
								System.out.println("Going to try it on '"+str_contentTypeRaw+"'");
								
								str_encoding = str_contentTypeRaw.substring(str_contentTypeRaw.indexOf("=")+1);
						
								System.out.println("Result: '"+str_encoding+"'");
							} catch (Exception e) {
								System.out.println("ERROR: '=' not found in Content-Type's header");
								e.printStackTrace();
								break;
							}
						}
					}
				} catch (Exception e) {
					System.out.println("ERROR: Unable to find Content-Type header in response");
					e.printStackTrace();
				}
			}
			
			if(str_encoding != null)
			{
				str_encoding = str_encoding.toUpperCase();
				
				if(str_encoding.equals("UTF-8"))
				{
					System.out.println("Substring dat bitch");
					System.out.println("'"+str_rawDocument+"'");
					str_rawDocument = str_rawDocument.substring(4); // The BOM for UTF-8 shows up as null in Java. So remove that word!
					
					System.out.println("'"+str_rawDocument.substring(0, (int)str_rawDocument.length()/4)+"'");
				}
			} else if (str_rawDocument.startsWith("null")) {
				System.out.println("What to do: '"+str_rawDocument+"'");
			}*/
		}
		
		
		return true;
	}
	
	private boolean buildDocument()
	{
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = null;
		
		try{
			documentBuilder = dbf.newDocumentBuilder();
		} catch (Exception e) {
			lastError = new Error("dbf", Error.Type.Initiation, e.toString());
			return false;
		} finally {
			try {
				//System.out.println("\tLength: "+str_rawDocument.length());
				doc_feed = documentBuilder.parse(new InputSource(new StringReader(str_rawDocument)));
			} catch (Exception e) {
				lastError = new Error("documentBuilder", Error.Type.Parse, e.toString());
				return false;
			} finally {
				//System.out.println("\tBuilt document");
				fd_feed = new FeedDocument(doc_feed, feedDateFormat);
			}
		}
		
		return true;
	}
	
	public FeedDocument getFeedDocument()
	{
		return fd_feed;
	}

	
	public Error getLastError() {
		return lastError;
	}
}
	