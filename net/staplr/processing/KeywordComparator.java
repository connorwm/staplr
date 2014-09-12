package net.staplr.processing;

import java.util.Comparator;

public class KeywordComparator implements Comparator<Keyword>
{
	public KeywordComparator()
	{
		
	}
	
	public int compare(Keyword kw_1, Keyword kw_2)
	{
		return kw_1.compareTo(kw_2);
	}
}