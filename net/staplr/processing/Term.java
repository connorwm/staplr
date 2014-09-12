package net.staplr.processing;

public class Term
{
	private int i_occurences;
	private String str_term;
	
	public Term()
	{
		i_occurences = 0;
		str_term = new String();
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
		return str_term;
	}
}