package net.staplr.processing;

import java.util.ArrayList;
import java.util.Collections;

import org.joda.time.DateTime;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

import net.staplr.logging.LogHandle;
import net.staplr.logging.Entry.Type;
import net.staplr.master.DatabaseExecutor;

public class Result
{
	public ArrayList<Keyword> arr_keyword;
	public ArrayList<Term> arr_term;
	private DatabaseExecutor dx_executor;
	private LogHandle lh_processor;
	
	public Result(DatabaseExecutor dx_executor, LogHandle lh_processor)
	{
		this.dx_executor = dx_executor;
		this.lh_processor = lh_processor;
		
		arr_keyword = new ArrayList<Keyword>();
		arr_term = new ArrayList<Term>();
	}
	
	public void post(String str_entryFeed, String str_entryID)
	{
		lh_processor.write("Sorting keywords");
		Collections.sort(arr_keyword, new KeywordComparator());
		lh_processor.write("Sorted keywords");
		
		lh_processor.write("Picking top keywords..."+(arr_keyword.size() - 1));
		
		int i_currentKeywordOccurences = arr_keyword.get(arr_keyword.size()-1).getOccurences();
		ArrayList<Keyword> arr_topKeyword = new ArrayList<Keyword>();
		
		for(int i_keyword = (arr_keyword.size() - 1); i_keyword > 0; i_keyword--)
		{
			//System.out.println(arr_keyword.get(i_keyword).toString()+"["+arr_keyword.get(i_keyword).getOccurences()+":"+i_currentKeywordOccurences+"]");
			if(arr_keyword.get(i_keyword).getOccurences() < i_currentKeywordOccurences)
			{
				i_currentKeywordOccurences = arr_keyword.get(i_keyword).getOccurences();
				
				//System.out.println("\tDropping occurence count");
				if(arr_topKeyword.size() >= 10)
				{
					//System.out.println("\t\tHave enough; breaking");
					// We have at least 5 keywords and the rest have less occurrences so
					// we do not need them
					break;
				}
				else
				{
					//System.out.println("\t\tAdding because we don't even have 10");
					arr_topKeyword.add(arr_keyword.get(i_keyword));
				}
			}
			else if(arr_keyword.get(i_keyword).getOccurences() == i_currentKeywordOccurences && arr_topKeyword.size() <= 10)
			{
				//System.out.println("\tAdding because its the same count and we don't even have 10");
				arr_topKeyword.add(arr_keyword.get(i_keyword));
				i_currentKeywordOccurences = arr_keyword.get(i_keyword).getOccurences();
			}
		}
		
		arr_keyword = arr_topKeyword;
		
		lh_processor.write("Picked "+arr_keyword.size()+" keywords");
		
		DateTime dt_now = new DateTime(1970, 1, 1, 0, 0, 0, 0);
		dt_now = dt_now.plusSeconds((int)((new DateTime()).getMillis() / 1000));
		
		for(Keyword k_word : arr_keyword)
		{
			DBCollection col_word = getCollection(k_word.toString());
			
			if(col_word != null)
			{
				DBObject dbo_association = new BasicDBObject();
				
				dbo_association.put("entryID", str_entryID);
				dbo_association.put("entryFeed", str_entryFeed);
				dbo_association.put("timestamp", (dt_now.getMillis() / 1000)); // TEMP
				dbo_association.put("occurences",  k_word.getOccurences());
				
				// TEMP: Do not post as of yet
//				WriteResult wr_result = col_word.insert(dbo_association);
//				
//				if(wr_result.getN() == 1) lh_processor.write("Successfully associated '"+k_word.toString()+"' with "+str_entryFeed+":"+str_entryID);
//				else lh_proccessor.write("Failed to associate '"+k_word.toString()+"' with "+str_entryFeed+":"+str_entryID);
				
				//TEMP: For debug only:
				System.out.println("Posted: "+k_word.toString());
			}
			else
			{
				lh_processor.write("Failed to get collection for '"+k_word.toString()+"'");
			}
		}
		
		// TEMP: Do not post for now until bugs have been worked out
		// TEMP: Allow it
		postTrendData(arr_keyword);
	}
	
	private DBCollection getCollection(String str_name)
	{
		DBCollection collection = null;
		
		if(!dx_executor.db_associations.collectionExists(str_name))
		{
			collection = dx_executor.db_associations.createCollection(str_name, null);
		} else {
			collection = dx_executor.db_associations.getCollection(str_name);
		}
		
		return collection;
	}
	
	private void postTrendData(ArrayList<Keyword> arr_keywords)
	{
		DBCollection col_trends = getCollection("_trends");
		DateTime dt_now = new DateTime();
		String str_currentTrendDocument = dt_now.toString("M-d-yyyy");
		DBObject dbo_query = new BasicDBObject();
		DBObject dbo_currentTrendDocument = null;
		
		dbo_query.put("_date", str_currentTrendDocument);
		
		DBCursor dbc_result = col_trends.find(dbo_query);
		
		if(dbc_result.count() == 1)
		{
			lh_processor.write("Found trend document");
			dbo_currentTrendDocument = dbc_result.next();
		}
		else if(dbc_result.count() == 0)
		{
			lh_processor.write("Trend document not found; will create one");
			
			dbo_currentTrendDocument = new BasicDBObject();
			dbo_currentTrendDocument.put("_date", str_currentTrendDocument);
		}
		else
		{
			lh_processor.write(Type.Error, "Multiple trend documents found for "+str_currentTrendDocument+"; administrative action necessary");
		}
		
		if(dbo_currentTrendDocument != null)
		{
			for(Keyword k_keyword : arr_keywords)
			{
				if(dbo_currentTrendDocument.containsField(k_keyword.toString()))
				{
					int i_occurences = -1;
					
					try
					{
						i_occurences = (int)dbo_currentTrendDocument.get(k_keyword.toString());
					}
					catch (Exception e) 
					{
						lh_processor.write(Type.Error, "Could not parse occurence count in "+str_currentTrendDocument+" for "+k_keyword.toString());
						i_occurences = 0;
					}
					
					i_occurences += 1;
					
					dbo_currentTrendDocument.put(k_keyword.toString(), i_occurences);					
				}
				else
				{
					dbo_currentTrendDocument.put(k_keyword.toString(), 1);		
				}
			}
			
			if(dbc_result.count() == 1)
			{
				WriteResult wr_updateOccurences = col_trends.update(dbo_query, dbo_currentTrendDocument);
				
				if(wr_updateOccurences.getN() == 1)
				{
					lh_processor.write("Successfully updated "+str_currentTrendDocument+" trend document");
				}
				else
				{
					lh_processor.write(Type.Error, "Failed to update trends entry for "+str_currentTrendDocument);
				}
			}
			else
			{
				WriteResult wr_addTrendDocument = col_trends.insert(dbo_currentTrendDocument);
				
				if(wr_addTrendDocument.getN() == 1)
				{
					lh_processor.write("Successfully inserted "+str_currentTrendDocument+" trend document");
				}
				else
				{
					lh_processor.write(Type.Error, "Failed to insert trends document for "+str_currentTrendDocument);
				}
			}
			
		}
	}
}