package net.staplr.common.feed;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;


import net.staplr.logging.Entry.Type;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import FFW.Utility.EasyDate;

public class Entry implements Comparable<Entry>
{
	public static enum Properties
	{
		source,
		title,
		description,
		id,
		pubDate,
		updated,
		summary,
		published,
		timestamp
	}
	
	private String[] properties;
	private ArrayList<String> category;
	private ArrayList<Link> link;
	private ArrayList<Author> author;
	
	private String feedDateFormat;
	
	public Entry(Node node, String feedDateFormat)
	{
		NodeList childNodes = node.getChildNodes();
		NodeList categoryList = ((Element)node).getElementsByTagName("category");
		
		properties = new String[Properties.values().length];
		category = new ArrayList<String>();
		link = new ArrayList<Link>();
		author = new ArrayList<Author>();
		
		this.feedDateFormat = feedDateFormat;
		
		for(int childNodeIndex = 0; childNodeIndex < childNodes.getLength(); childNodeIndex++)
		{
			Node childNode = childNodes.item(childNodeIndex);
			
			if(!childNode.getNodeName().equals("link") && !childNode.getNodeName().equals("author"))
			{
				for(int propertyIndex = 0; propertyIndex < properties.length; propertyIndex++)
				{
					if(Properties.values()[propertyIndex].toString().equals(childNode.getNodeName()))
					{
						properties[propertyIndex] = childNode.getTextContent();
						break;
					}
				}
			} 
			else if (childNode.getNodeName().equals("link"))
			{
				Link newLink = parseLink(childNode);
				
				if(newLink != null) link.add(newLink);
			}
			else if (childNode.getNodeName().equals("author"))
			{
				Author newAuthor = parseAuthor(childNode);
				
				if(newAuthor != null) author.add(newAuthor);
			}
		}
		
		for(int categoryIndex = 0; categoryIndex < categoryList.getLength(); categoryIndex++)
		{
			Node categoryNode = categoryList.item(categoryIndex);
			category.add(categoryNode.getNodeValue());
		}
		
		// Set the universal timestamp - format and title does not vary
		DateTime dt_timestamp = getEntryDateTime(this);
		
		if(dt_timestamp != null) {
			properties[Properties.timestamp.ordinal()] = String.valueOf(dt_timestamp.getMillis()/1000);
		} else {
			properties[Properties.timestamp.ordinal()] = "0";
		}
	}
	
	private Link parseLink(Node node)
	{
		Link newLink = new Link();
		NamedNodeMap linkProperties = node.getAttributes();
		
		if(linkProperties.getLength() > 0)
		{			
			for(int linkPropertyIndex = 0; linkPropertyIndex < Link.Properties.values().length; linkPropertyIndex++)
			{
				//System.out.println("\t Checking for '"+Link.Properties.values()[linkPropertyIndex].toString()+"'");
				
				if(linkProperties.getNamedItem(Link.Properties.values()[linkPropertyIndex].toString()) != null)
				{
					newLink.set(Link.Properties.values()[linkPropertyIndex], linkProperties.getNamedItem(Link.Properties.values()[linkPropertyIndex].toString()).getNodeValue());
				} else {
					//System.out.println("\t doesn't have '"+Link.Properties.values()[linkPropertyIndex].toString()+"'");
				}
			}
			
			return newLink;
		} else if (node.getTextContent() != null) {
			// Link like so: <link>http://somewhere/</link>
			newLink.set(Link.Properties.href, node.getTextContent());
		
			return newLink;
		}
		return null;
	}
	
	private Author parseAuthor(Node node)
	{
		Author newAuthor = new Author();
		NodeList authorProperties = node.getChildNodes();
		
		if(authorProperties != null)
		{
			//System.out.println("Properties aren't null either ("+authorProperties.getLength()+")");
			for(int authorChildNodeIndex = 0; authorChildNodeIndex < authorProperties.getLength(); authorChildNodeIndex++)
			{
				Node authorChildNode = authorProperties.item(authorChildNodeIndex);
				//System.out.println("Author node child: "+authorChildNode.getNodeName());
				
				if(!authorChildNode.getNodeName().equals("#text"))
				{
					for(int authorPropertyIndex = 0; authorPropertyIndex < Author.Properties.values().length; authorPropertyIndex++)
					{
						if(authorChildNode.getNodeName().equals(Author.Properties.values()[authorPropertyIndex].toString()))
						{
							newAuthor.set(Author.Properties.values()[authorPropertyIndex], authorChildNode.getTextContent());
							break;
						}
					}
				}
			}
			
			return newAuthor;
		}
		
		return null;
	}
	
	/**Parses the entry's date
	 * @author murphyc1
	 * @param e_entry
	 * @return DateTime object of the entry
	 */
	private DateTime getEntryDateTime(Entry e_entry)
	{
		DateTime datetime = null;
		DateTimeFormatter dtf = DateTimeFormat.forPattern(feedDateFormat);
		//ArrayList<Entry> e_entries = fd_feedDocument.getEntries();
		//Arrays.sort(e_entries.toArray()); // Sorted oldest to newest
		//Entry e_entry = e_entries.get(e_entries.size()-1);
		
		if(e_entry.get(Entry.Properties.pubDate) != null)
		{
			datetime = parseDate(e_entry.get(Entry.Properties.pubDate), dtf);
		}
		
		if(datetime == null)
		{
			if(e_entry.get(Entry.Properties.published) != null)
			{
				datetime = parseDate(e_entry.get(Entry.Properties.published), dtf);
			}
		}
		
		return datetime;
	}
	
	/**Tries to parse the given date to a DateTime object using the DateTimeFormatter. Prints to the Log given in the FeedParser object.
	 * @author murphyc1
	 * @param str_date
	 * @param dtf
	 * @return
	 */
	private DateTime parseDate(String str_date, DateTimeFormatter dtf)
	{
		DateTime d_date = null;
		
		try{
			d_date = dtf.parseDateTime(str_date);
		} catch (Exception e) {
			//log_slave.write(Type.Error, "Could not parse date '"+str_date+"' for feed's specified format");
		}
		
		return d_date;
	}
	
	public String get(Properties property)
	{
		return properties[property.ordinal()];
	}
	
	public void set(Properties property, String str_value)
	{
		properties[property.ordinal()] = str_value;
	}
	
	public ArrayList<String> getCategories()
	{
		return category;
	}
	
	public ArrayList<Link> getLinks()
	{
		return link;
	}
	
	public ArrayList<Author> getAuthors()
	{
		return author;
	}
	
	public Long getTimestamp()
	{		
		return Long.parseLong(properties[Properties.timestamp.ordinal()]);
	}
	
	public int compareTo(Entry compareEntry)
	{
		Long compareTimestamp = compareEntry.getTimestamp();
		Long thisTimestamp = Long.valueOf(properties[Properties.timestamp.ordinal()]);
		
		if(compareTimestamp != null && thisTimestamp != null)
		{
			if(thisTimestamp > compareTimestamp) return 1;
			else if(thisTimestamp < compareTimestamp) return -1;
			else return 0;
		} else {
			return 0;
		}
	}
}