package net.staplr.processing;

import java.lang.Comparable;

public class Keyword implements Comparable<Keyword>
{
	private int i_occurences;
	private String str_keyword;
	
	public Keyword(String str_keyword)
	{
		this.str_keyword = str_keyword;
		i_occurences = 1;
	}
	
	public void inc()
	{
		i_occurences++;
	}
	
	public int getOccurences()
	{
		return i_occurences;
	}
	
	public String toString()
	{
		return str_keyword;
	}

	public int compareTo(Keyword kw_other) 
	{
		int result = 0;
		
		if(i_occurences < kw_other.getOccurences())
		{
			result = -1;
		}
		else if (i_occurences > kw_other.getOccurences())
		{
			result = 1;
		}
		
		return result;
	}
}