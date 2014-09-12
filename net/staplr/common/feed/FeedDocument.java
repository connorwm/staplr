package net.staplr.common.feed;

import java.util.ArrayList;
import java.util.Map;


import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**Class holding the feed's properties and entries
 * @param Document document
 * @author murphyc1
 */
public class FeedDocument
{
	public static enum Properties
	{
		id,
		guid,
		title,
		subtitle,
		url,
		description,
		ttl,
		summary,
		type,
		rights,
		copyright,
		icon,
		lastBuildDate,
		pubDate,
		updated,
		generator,
		language,
		timestamp
	}
	
	private String[] properties;
	private ArrayList<Entry> entries;
	private ArrayList<Link> link;
	private ArrayList<Author> author;
	private BasicDBList dbl_links;
	private BasicDBList dbl_authors;
	
	/**Builds a FeedDocument to hold the XML's data
	 * @author murphyc1
	 * @param document
	 */
	public FeedDocument(Document document, String feedDateFormat)
	{	
		entries = new ArrayList<Entry>();
		properties = new String[Properties.values().length];
		link = new ArrayList<Link>();
		author = new ArrayList<Author>();
		
		Element rootElement = document.getDocumentElement();
		NodeList entryList = null;
		Node channelNode = null;
		NodeList channelChildNodes = null;
		
		// Determine Feed Type
		if(rootElement.getTagName().equals("feed"))
		{
			properties[Properties.type.ordinal()] = "Atom "+rootElement.getAttribute("version");
			
			entryList = rootElement.getElementsByTagName("entry");
			
			// Get Entries
			for(int entryIndex = 0; entryIndex < entryList.getLength(); entryIndex++)
			{
				entries.add(new Entry(entryList.item(entryIndex), feedDateFormat));
			}
			
			// Get Everything Else
			for(int nodeIndex = 0; nodeIndex < rootElement.getChildNodes().getLength(); nodeIndex++)
			{
				if(!rootElement.getChildNodes().item(nodeIndex).getNodeName().equals("#text")) // Sometimes treats text as a node; don't bother trying to parse it
				{
					Node node = rootElement.getChildNodes().item(nodeIndex);
					//System.out.println("I have a node "+node.getNodeName());
					
					if(node.getNodeName().equals("author"))
					{
						//System.out.println("Found an author!");
						Author newAuthor = parseAuthor(node);
						
						if(author != null) author.add(newAuthor);
					} else if (node.getNodeName().equals("link")) {
						//System.out.println("Found a link!");
						Link newLink = parseLink(node);
						
						if(newLink != null) link.add(newLink);
					} else {
						for(int propertyIndex = 0; propertyIndex < FeedDocument.Properties.values().length; propertyIndex++)
						{
							if(node.getNodeName().equals(FeedDocument.Properties.values()[propertyIndex].toString()))
							{
								//System.out.println("Setting '"+FeedDocument.Properties.values()[propertyIndex].toString()+"' as '"+node.getTextContent()+"'");
								properties[propertyIndex] = node.getTextContent();
								break;
							}
						}
					}
				}
			}
		}
		if(rootElement.getTagName().equals("rss"))
		{
			properties[Properties.type.ordinal()] = "RSS "+rootElement.getAttribute("version");
		
			if(rootElement.hasChildNodes())
			{
				channelNode = document.getElementsByTagName("channel").item(0);
				channelChildNodes = channelNode.getChildNodes();

				for(int nodeIndex = 0; nodeIndex < channelChildNodes.getLength(); nodeIndex++)
				{
					Node node = channelChildNodes.item(nodeIndex);

					if(node.getNodeName().equals("item"))
					{
						//System.out.println("Added:\n\t"+channelChildNodes.item(nodeIndex).getNodeName());
						entries.add(new Entry(node, feedDateFormat));
					} else if(!node.getNodeName().equals("#text")) {
						//System.out.println("Checking '"+node.getNodeName()+"'");
						
						if(node.getNodeName().equals("author"))
						{
							//System.out.println("Found an author!");
							Author newAuthor = parseAuthor(node);
							
							if(author != null) author.add(newAuthor);
						}
						else if (node.getNodeName().equals("link"))
						{
							//System.out.println("Found a link!");
							Link newLink = parseLink(node);
							
							if(newLink != null) link.add(newLink);
						} else {
							for(int propertyIndex = 0; propertyIndex < FeedDocument.Properties.values().length; propertyIndex++)
							{
								if(node.getNodeName().equals(FeedDocument.Properties.values()[propertyIndex].toString()))
								{
									//System.out.println("Setting '"+FeedDocument.Properties.values()[propertyIndex].toString()+"' as '"+node.getTextContent()+"'");
									properties[propertyIndex] = node.getTextContent();
									break;
								}
							}
						}
					}
				}
			}
		}
		
		// Add links to document
		dbl_links = new BasicDBList();
		//System.out.println("Link size: "+link.size());
		for(int linkIndex = 0; linkIndex < link.size(); linkIndex++)
		{
			DBObject dbo_link = new BasicDBObject();
			Link l_link = link.get(linkIndex);
			//System.out.println("\t"+l_link.toString());
			
			for(int linkPropertyIndex = 0; linkPropertyIndex < Link.Properties.values().length; linkPropertyIndex++)
			{
				dbo_link.put(Link.Properties.values()[linkPropertyIndex].toString(), l_link.get(Link.Properties.values()[linkPropertyIndex]));
			}
			
			dbl_links.add(dbo_link);
		}
		
		// Add authors to document
		dbl_authors = new BasicDBList();
		for(int authorIndex = 0; authorIndex < author.size(); authorIndex++)
		{
			DBObject dbo_author = new BasicDBObject();
			Author l_author = author.get(authorIndex);
			
			for(int authorPropertyIndex = 0; authorPropertyIndex < Author.Properties.values().length; authorPropertyIndex++)
			{
				dbo_author.put(Author.Properties.values()[authorPropertyIndex].toString(), l_author.get(Author.Properties.values()[authorPropertyIndex]));
			}
			
			dbl_authors.add(dbo_author);
		}
		
//		//System.out.println("Properties:");
//		for(int propertyIndex = 0; propertyIndex < properties.length; propertyIndex++)
//		{
//			//System.out.println("\t"+properties[propertyIndex]);
//		}
	}
	
	/**Takes an XML node and converts it into a Link object
	 * @author murphyc1
	 * @param node
	 * @return Link
	 */
	private Link parseLink(Node node)
	{
		Link newLink = new Link();
		NamedNodeMap linkProperties = node.getAttributes();
		
		if(linkProperties.getLength() > 0)
		{
			//System.out.println("Link properties are there!");
			
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
			//System.out.println("And it has text content");
			// Link like so: <link>http://somewhere/</link>
			newLink.set(Link.Properties.href, node.getTextContent());
		
			return newLink;
		}
		return null;
	}
	
	/**Takes an XML node and converts it into a Author object
	 * @author murphyc1
	 * @param node
	 * @return Author
	 */
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
	
	/**Set a specific property (accepts Object)
	 * @author murphyc1
	 * @param property
	 * @param value
	 */
	public void set(Properties property, String value)
	{
		properties[property.ordinal()] = value;
	}
	
	/**Gets a specific property
	 * @author murphyc1
	 * @param property
	 * @return Object
	 */
	public Object get(Properties property)
	{
		return properties[property.ordinal()];
	}
	
	/**Returns the ArrayList of the document's entries
	 * @author murphyc1
	 * @return ArrayList
	 */
	public ArrayList<Entry> getEntries()
	{
		return entries;
	}
	
	public BasicDBList getLinks()
	{
		return dbl_links;
	}
	
	public BasicDBList getAuthors()
	{
		return dbl_authors;
	}
}