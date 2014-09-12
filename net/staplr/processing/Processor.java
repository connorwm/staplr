package net.staplr.processing;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import net.staplr.logging.Log;
import net.staplr.logging.LogHandle;
import net.staplr.logging.Log.Options;
import net.staplr.master.DatabaseExecutor;

public class Processor implements Runnable
{
	private ArrayList<String> arr_ignoreWords;
	private Result result;
	private BufferedReader br;
	private String str_document;
	private String str_charset;
	private String str_entryID;
	private String str_entryFeed;
	private LogHandle lh_processor;
	
	public Processor(String str_document, String str_charset, ArrayList<String> arr_ignoreWords, String str_entryID, String str_entryFeed, DatabaseExecutor dx_executor, Log l_main)
	{
		this.arr_ignoreWords = arr_ignoreWords;
		
		// Sample document strings for debugging purposes
		//this.str_document = "<img alt=\"Pollution\" src=\"http://rack.2.mshcdn.com/media/ZgkyMDEzLzA3LzA5L2QzL3BvbGx1dGlvbi5jYjNkOC5qcGcKcAl0aHVtYgk1NzV4MzIzIwplCWpwZw/3b532848/f4b/pollution.jpg\" /><div style=\"float: right; width: 50px;\"><a href=\"http://twitter.com/share?via=Mashable&text=Architecture+Innovation+Would+Turn+Air+Pollution+Into+Biofuel&src=http%3A%2F%2Fmashable.com%2F2013%2F07%2F09%2Fair-pollution-biofuel%2F%3Futm_campaign%3DMash-Product-RSS-Pheedo-All-Partial%26utm_cid%3DMash-Product-RSS-Pheedo-All-Partial%26utm_medium%3Dfeed%26utm_source%3Drss\" style=\"margin: 10px;\"><img alt=\"Feed-tw\" border=\"0\" src=\"http://rack.3.mshcdn.com/assets/feed-tw-df3e816c4e85a109d6e247013aed8d66.jpg\" /></a><a href=\"http://www.facebook.com/sharer.php?u=http%3A%2F%2Fmashable.com%2F2013%2F07%2F09%2Fair-pollution-biofuel%2F%3Futm_campaign%3DMash-Product-RSS-Pheedo-All-Partial%26utm_cid%3DMash-Product-RSS-Pheedo-All-Partial%26utm_medium%3Dfeed%26utm_source%3Drss&src=sp\" style=\"margin: 10px;\"><img alt=\"Feed-fb\" border=\"0\" src=\"http://rack.1.mshcdn.com/assets/feed-fb-fdab25e3700868c9621fb03b7fd07c38.jpg\" /></a></div><div><div></div></div>\n<p>What if air pollution could be converted into something useful, like biofuel?</p>\n<p>That&#8217;s the idea behind a new concept by Royal College of Art student Chang-Yeob Lee. Lee wants to transform London&#8217;s BT Tower &#8212; one of the city&#8217;s tallest buildings &#8212; into a structure capable of capturing air pollution in the area and then transforming it into usable biofuel</p>\n<p><strong><div class=\"see-also\">SEE ALSO: <a href=\"http://mashable.com/2013/03/05/data-visualization-projects/?utm_campaign=Mash-Product-RSS-Pheedo-All-Partial&amp;utm_cid=Mash-Product-RSS-Pheedo-All-Partial&amp;utm_medium=feed&amp;utm_source=rss\" data-crackerjax=\"#post-slider\">10 Fascinating Data Visualization Projects</a>\n</div></strong></p>\n<p>\"The project is about a new infrastructure gathering resources from pollutants in the city atmosphere, which could be another valuable commodity in the age of depleting resources,\" Lee told <a href=\"http://www.dezeen.com/2013/06/24/synthetechecology-by-chang-yeob-lee/\">architecture and design magazine <em>Dezeen</em></a>. <a href=\"http://mashable.com/2013/07/09/air-pollution-biofuel/?utm_campaign=Mash-Product-RSS-Pheedo-All-Partial&amp;utm_cid=Mash-Product-RSS-Pheedo-All-Partial&amp;utm_medium=feed&amp;utm_source=rss\">Read more...</a></p>More about <a href=\"/category/biofuel/?utm_campaign=Mash-Product-RSS-Pheedo-All-Partial&amp;utm_cid=Mash-Product-RSS-Pheedo-All-Partial&amp;utm_medium=feed&amp;utm_source=rss\">Biofuel</a>, <a href=\"/category/pollution/?utm_campaign=Mash-Product-RSS-Pheedo-All-Partial&amp;utm_cid=Mash-Product-RSS-Pheedo-All-Partial&amp;utm_medium=feed&amp;utm_source=rss\">Pollution</a>, and <a href=\"/tech/?utm_campaign=Mash-Product-RSS-Pheedo-All-Partial&amp;utm_cid=Mash-Product-RSS-Pheedo-All-Partial&amp;utm_medium=feed&amp;utm_source=rss\">Tech</a>";
		//this.str_document = "<a>tech tech</a> click here for some tech. popular tech is the new Apple iWatch that really doesn't have any buzz to it at all. The buzz Apple is creating over a watch is rediculous. A real watch is made of steel not silicon.";
		this.str_document = str_document;
		this.str_charset = str_charset;
		this.str_entryID = str_entryID;
		this.str_entryFeed = str_entryFeed;
		
		lh_processor = new LogHandle("proc", l_main);
		
		result = new Result(dx_executor, lh_processor);
		
		try {
			br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(this.str_document.getBytes()), this.str_charset));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args)
	{
		ArrayList<String> arrlist_ignoreWords = new ArrayList<String>();
		BufferedReader br_exclusionList = null;
		boolean b_opened = false;
		Log l_proc = new Log();
		l_proc.setOption(Options.ConsoleOutput, true);
		
		try{
			br_exclusionList = new BufferedReader(new InputStreamReader(new FileInputStream("exclusions.txt")));
			b_opened = true;
		} 
		catch (IOException ioe_open)
		{
			ioe_open.printStackTrace();
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
				e.printStackTrace();
			}
			
			Processor p = new Processor(null, "UTF-8", arrlist_ignoreWords, "a", "b", null, l_proc);
			p.run();
		}
	}
	
	public void run()
	{
		String str_line = new String(); 
		int i_nextIndex = 0;
		
		try{
			while((str_line = br.readLine()) != null)
			{
				while(str_line.contains("&#8217;"))
				{
					str_line = str_line.replace("&#8217;", "'");
				}
				
				while(str_line.length() > 0)
				{
					// Check if it is an HTML tag
					if(str_line.startsWith("<"))
					{
						parseLessThanSymbol(str_line);
					}

					i_nextIndex = findNextIndex(str_line);

					// Now get our word and trim the line
					String str_word = str_line.substring(0, i_nextIndex);

					if(str_line.indexOf("<") == i_nextIndex)
					{
						if(str_line.contains(">")) str_line = str_line.substring(str_line.indexOf(">")+1);
						else str_line = str_line.substring(str_line.indexOf("<")+1);
					}
					else
					{
						str_line = str_line.substring(i_nextIndex+=1);
					}
					
					// Clean up word first
					str_word = str_word.toLowerCase();
					
					if(str_word.endsWith("'s"))
					{
						str_word = str_word.replace("'s", "");
					}

					// Now make sure its not an ignore word
					if(arr_ignoreWords.contains(str_word))
					{
						str_word = "";
					}
					
					// And that it does not contain any symbols that are non-alphanumeric besides a hyphen
					if(containsSymbols(str_word))
					{
						str_word = "";
					}
					
					if(isNumber(str_word))
					{
						str_word = "";
					}

					// Now check if we already have it
					if(str_word != null && str_word.length() > 1)
					{
						Keyword kw_new = new Keyword(str_word);
						boolean b_found = false;

						for(int i_keywordIndex = 0; i_keywordIndex < result.arr_keyword.size(); i_keywordIndex++)
						{
							if(result.arr_keyword.get(i_keywordIndex).toString().equals(str_word))
							{
								result.arr_keyword.get(i_keywordIndex).inc();
								b_found = true;
								break;
							}
						}

						if(!b_found)
						{
							result.arr_keyword.add(kw_new);
						}
					}

					trimLine(str_line);
				}
				
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
//		System.out.println("------- Results ------");
//		System.out.println("Keywords:	"+result.arr_keyword.size());
//		System.out.println("");
//		
//		Collections.sort(result.arr_keyword, new KeywordComparator());
//		
//		for(int i_keywordIndex = 0; i_keywordIndex < result.arr_keyword.size(); i_keywordIndex++)
//		{
//			System.out.println(result.arr_keyword.get(i_keywordIndex).toString()+": "+result.arr_keyword.get(i_keywordIndex).getOccurences());
//		}
		
		//TEMP: Until all bugs have been worked out do not flood database with garbage
		//NOTE: As of now the entries will not be posted 
		result.post(str_entryFeed, str_entryID);
	}
	
	/**
	 * Parse a < in a line in the article and then truncates the string as necessary to find the next term in the line
	 * @author murphyc
	 * @param str_line The current line in the news article
	 */
	private void parseLessThanSymbol(String str_line)
	{
		// If the next character is NOT a space then it is an HTML tag
		if(str_line.toCharArray()[1] != ' ')
		{
			// Skip over the contents of the tag and go the end of it if possible
			if(str_line.contains(">"))
			{
				str_line = str_line.substring(str_line.indexOf(">")+1);
			} 
			// If this statement triggers there must be a mistake in the page
			// In that case just get to the next space to try and find another term
			else if(str_line.contains(" "))
			{
				str_line = str_line.substring(str_line.indexOf(" ")+1);
			}
			// In the rare case there is not a space nor a < or > there must not
			// be anything else we want
			else
			{
				str_line = "";
			}
		}
		// If the next character is not a space then it is just a less/greater-than symbol
		else
		{
			str_line = str_line.substring(str_line.indexOf(" ")+1);
		}
	}
	
	/**
	 * Finds the next index of a term that needs to be parse
	 * @author murphyc
	 * @param str_line The current line in the news article
	 * @return The next index of a term needed to parse
	 */
	private int findNextIndex(String str_line)
	{
		// What's next? A space, a period, or a <
		int[] arr_nextIndices = {
				str_line.indexOf(" "),
				str_line.indexOf("."),
				str_line.indexOf(","),
				str_line.indexOf("<"),
				str_line.indexOf(":"),
				str_line.indexOf("?"),
				str_line.indexOf("\"")
		};
		int i_nextIndex = -2;
		
		
		Arrays.sort(arr_nextIndices);
		
		for(int i_index: arr_nextIndices)
		{
			if(i_index != -1)
			{
				i_nextIndex = i_index;
				break;
			}
		}
		
		if(i_nextIndex == -2) i_nextIndex = 0; // Must be the last word then
		
		return i_nextIndex;
	}

	/**
	 * Trims and removes any non-alphanumeric characters from the left side of the current line string
	 * @author murphyc
	 * @param str_line The current line in the news article
	 */
	private void trimLine(String str_line)
	{
		for(int i_lineIndex = 0; i_lineIndex < str_line.length(); i_lineIndex++)
		{
			int i_char = (int)str_line.toCharArray()[i_lineIndex];
			
			if(i_char >= 48 && i_char <= 59) // Is a number
			{
				break;
			}
			else if (i_char >= 65 && i_char <= 90) // Is an upper-case letter
			{
				break;
			}
			else if(i_char >= 97 && i_char <= 122) // Is a lower-case letter
			{
				break;
			}
			else
			{
				str_line = str_line.substring(1);
			}
		}
	}
	
	/**
	 * Checks to see if a words contains any symbols EXCEPT for hyphens
	 * @param str_word Word to be examined
	 * @return True or False
	 */
	private boolean containsSymbols(String str_word)
	{
		for(char c_character : str_word.toCharArray())
		{
			boolean b_containsSymbols = true;
			
			if(c_character >= 48 && c_character <= 57)
			{
				b_containsSymbols = false;
			}
			else if(c_character >= 65 && c_character <= 90)
			{
				b_containsSymbols = false;
			}
			else if(c_character >= 97 && c_character <= 122)
			{
				b_containsSymbols = false;
			}
			else if(c_character == 45) // Hyphen
			{
				b_containsSymbols = false;
			}
			
			if(b_containsSymbols)
			{
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Checks to see if a string is an integer
	 * @param str_number Number to check
	 * @return True or False
	 */
	private boolean isNumber(String str_number)
	{
		int i_number = -1;
		
		try{
			i_number = Integer.valueOf(str_number);
		} catch (Exception e) {}
		
		return (i_number != -1);
	}
}