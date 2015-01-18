package net.staplr.master;

import java.util.Iterator;
import java.util.Map;

public class FeedRedistribution
{
	private String str_address;
	private Map<String, Integer> map_redistributeNumbers;
	
	public FeedRedistribution(String str_address)
	{
		this.str_address = str_address;
	}
	
	public boolean allNumbersReceived()
	{
		boolean b_allNumbersReceived = true;
		
		for(int i_masterIndex = 0; i_masterIndex < map_redistributeNumbers.size(); i_masterIndex++)
		{
			if(map_redistributeNumbers.values().toArray()[i_masterIndex] == null)
			{
				b_allNumbersReceived = false;
				break;
			}
		}
		
		return b_allNumbersReceived;
	}
	
	public String getAddressOfHighest()
	{
		String str_addressOfHighest = "";
		int i_highestNumber = -1;
		
		Map<String, Integer> map_redistributionNumbers = map_redistributeNumbers;
		Iterator<String> it_redistributionNumbers = map_redistributionNumbers.keySet().iterator();
		
		while(it_redistributionNumbers.hasNext())
		{
			String str_address = it_redistributionNumbers.next();
			
			if(map_redistributionNumbers.get(str_address) > i_highestNumber)
			{
				i_highestNumber = map_redistributionNumbers.get(str_address);
				str_addressOfHighest = str_address.toString();
			}
		}
		
		return str_addressOfHighest;
	}
	
	public boolean IAmRedistributionLeader()
	{
		return (getAddressOfHighest() == "127.0.0.1");
	}
	
	public Map<String, Integer> getRedistributeNumbers()
	{
		return map_redistributeNumbers;
	}
	
	public String getAddress()
	{
		return str_address;
	}
}