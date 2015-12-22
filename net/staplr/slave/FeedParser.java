package net.staplr.slave;

import org.bson.Document;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import net.staplr.common.feed.Entry;
import net.staplr.common.feed.Feed;
import net.staplr.common.feed.FeedDocument;
import net.staplr.logging.Entry.Type;
import net.staplr.logging.LogHandle;

public class FeedParser
{
	private FeedDocument fd_feedDocument;
	private Feed f_feed;
	private Document doc_feed;
	private LogHandle lh_slave;
	
	public FeedParser(FeedDocument fd_feedDocument, Feed f_feed, Document doc_feed, LogHandle lh_slave)
	{
		this.fd_feedDocument = fd_feedDocument;
		this.f_feed = f_feed;
		this.doc_feed = doc_feed;
		this.lh_slave = lh_slave;
	}
	
	public void parse()
	{
		for(int feedDocumentPropertyIndex = 0; feedDocumentPropertyIndex < FeedDocument.Properties.values().length; feedDocumentPropertyIndex++)
		{
			String str_property = FeedDocument.Properties.values()[feedDocumentPropertyIndex].toString();
			FeedDocument.Properties p_property = FeedDocument.Properties.values()[feedDocumentPropertyIndex];

			//log_slave.write("Property "+str_property+": "+doc_feed.get(str_property)+"|"+fd_feedDocument.get(p_property));

			// If the new document has a property that the old one does not
			if(doc_feed.get(str_property) == null && fd_feedDocument.get(p_property) != null)
			{
				//log_slave.write("Didn't have, adding "+str_property);
				doc_feed.put(str_property, (String)fd_feedDocument.get(p_property));
			}
			// If the old document has a property that the new one does not
			else if (doc_feed.get(str_property) != null && fd_feedDocument.get(p_property) == null) 
			{
				if(str_property != "location" && str_property != "ttl")
				{
					//log_slave.write("Removing "+str_property);
					doc_feed.remove(str_property);
				}
			} 
			// If the old document has a property that the new one has a different version of
			else if (doc_feed.get(str_property) != null && fd_feedDocument.get(p_property) != null && String.valueOf(doc_feed.get(str_property)) != String.valueOf(fd_feedDocument.get(p_property)))
			{
				//log_slave.write("Updating "+str_property);
				doc_feed.put(FeedDocument.Properties.values()[feedDocumentPropertyIndex].toString(), (String)fd_feedDocument.get(FeedDocument.Properties.values()[feedDocumentPropertyIndex]));
			}

			// Add authors if non-existent
			if(doc_feed.get("link") == null && fd_feedDocument.getLinks().keySet().size() > 0)
			{
				//log_slave.write("Had no links, adding all");
				doc_feed.put("link", fd_feedDocument.getLinks());
			}
			// Update if different
			else if(doc_feed.get("link") != null && fd_feedDocument.getLinks() != doc_feed.get("link"))
			{
				//log_slave.write("Adding a missing link");
				doc_feed.put("link", fd_feedDocument.getLinks());
			}
			// Remove if null
			else if(doc_feed.get("link") != null && fd_feedDocument.getLinks() == null)
			{
				//log_slave.write("New had no links but old did, removing all");
				doc_feed.remove("link");
			}

			// Add authors if non-existent
			if(doc_feed.get("author") == null && !fd_feedDocument.getAuthors().toMap().isEmpty())
			{
				//log_slave.write("Had no authors, adding all");
				doc_feed.put("author", fd_feedDocument.getAuthors());
			}
			// Update if different
			else if(doc_feed.get("author") != null && fd_feedDocument.getAuthors() != doc_feed.get("author"))
			{
				//log_slave.write("Updating authors");
				doc_feed.put("author", fd_feedDocument.getAuthors());
			}
			// Remove if null
			else if(doc_feed.get("author") != null && fd_feedDocument.getAuthors() == null)
			{
				//log_slave.write("New had no authors but the old did, removing all");
				doc_feed.remove("author");
				// 9/29/13 12:59 AM
				// For some reason author was zdnet
			}
		}
		
		setTimestamp();
	}
	
	private void setTimestamp()
	{
		// Timestamp sacrifices a few handfuls of bytes for optimization and code-saving
		// (Seconds since UNIX epoch)
		lh_slave.write("Creating timestamp for Feed and FeedDocument");
		DateTimeFormatter dtf = DateTimeFormat.forPattern((String)f_feed.get(Feed.Properties.dateFormat));
		long l_timestamp = 0L;
		String[] str_dates = {String.valueOf(fd_feedDocument.get(FeedDocument.Properties.pubDate)),
				String.valueOf(fd_feedDocument.get(FeedDocument.Properties.updated)),
				String.valueOf(fd_feedDocument.get(FeedDocument.Properties.lastBuildDate))};

		for(int i_dateIndex = 0; i_dateIndex < str_dates.length; i_dateIndex++)
		{
			String str_date = str_dates[i_dateIndex];

			if(str_date != null)
			{
				l_timestamp = parseDate(str_date, dtf);

				if(l_timestamp != 0L) {
					lh_slave.write("Found usable time");
					break;
				}
			}
		}

		if(l_timestamp == 0L)
		{
			lh_slave.write("Could not find a date in the feed's documents. Checking in entries...");

			if(fd_feedDocument.getEntries().size() > 0)
			{
				l_timestamp = findMostRecentEntryTimestamp(fd_feedDocument, f_feed);
			}
			else
			{
				// TODO No other way?
				lh_slave.write(Type.Error, "No dates found in FeedDocument or its entries: unable to create timestamp.");
			}
		}

		if(l_timestamp != 0L)
		{
			lh_slave.write("Adding timestamp...");

			String str_timestamp = String.valueOf(l_timestamp);
			fd_feedDocument.set(FeedDocument.Properties.timestamp, str_timestamp);
			f_feed.set(Feed.Properties.timestamp, str_timestamp);

			lh_slave.write("Successfully added timestamp");
		}
	}
	
	/**Finds the most recent entry in the the FeedDocument
	 * @author murphyc1
	 * @param fd_feedDocument
	 * @param f_feed
	 * @return DateTime object of the most recent entry
	 */
	private Long findMostRecentEntryTimestamp(FeedDocument fd_feedDocument, Feed f_feed)
	{
//		DateTimeFormatter dtf = DateTimeFormat.forPattern(f_feed.get(Feed.Properties.dateFormat));
//		ArrayList<Entry> e_entries = fd_feedDocument.getEntries();
//		Arrays.sort(e_entries.toArray()); // Sorted oldest to newest
//		Entry e_entry = e_entries.get(e_entries.size()-1);
//		
//		if(e_entry.get(Entry.Properties.pubDate) != null)
//		{
//			l_mostRecent = parseDate(e_entry.get(Entry.Properties.pubDate), dtf);
//		}
//
//		if(e_entry.get(Entry.Properties.published) != null && l_mostRecent == 0L)
//		{
//			l_mostRecent = parseDate(e_entry.get(Entry.Properties.published), dtf);
//		}
//		
//		if(l_mostRecent == 0L)
//		{
//			log_slave.write("Could not find a date in the entries");
//		}
		
		long l_mostRecentTimestamp = 0L;
		DateTimeFormatter dtf =  DateTimeFormat.forPattern(f_feed.get(Feed.Properties.dateFormat));
		
		for(int i_entryIndex = 0; i_entryIndex < fd_feedDocument.getEntries().size(); i_entryIndex++)
		{
			Entry e_entry = fd_feedDocument.getEntries().get(i_entryIndex);
			String str_date = "";
			
			if(e_entry.get(Entry.Properties.pubDate) != null)
			{
				str_date = e_entry.get(Entry.Properties.pubDate);
			}
			else if(e_entry.get(Entry.Properties.published) != null)
			{
				str_date = e_entry.get(Entry.Properties.published);
			}
			
			if(str_date != "")
			{
				long l_timestamp = parseDate(str_date, dtf);
				
				if(l_timestamp > l_mostRecentTimestamp) l_mostRecentTimestamp = l_timestamp;
			}
		}
		
		return l_mostRecentTimestamp;
	}
	
	/**Tries to parse the given date to a DateTime object using the DateTimeFormatter. Prints to the Log given in the FeedParser object.
	 * @author murphyc1
	 * @param str_date
	 * @param dtf
	 * @return 
	 */
	private Long parseDate(String str_date, DateTimeFormatter dtf)
	{
		long l_timestamp = 0L;
		
		try{
			DateTime dt_date = dtf.parseDateTime(str_date);
			l_timestamp = (dt_date.getMillis()/1000);
		} catch (Exception e) {
			lh_slave.write(Type.Error, "Could not parse date '"+str_date+"' for feed's specified format "+f_feed.get(Feed.Properties.dateFormat));
		}
		
		return l_timestamp;
	}
}