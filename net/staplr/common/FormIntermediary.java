package net.staplr.common;
import net.staplr.common.TextFieldLogger;
import java.awt.Component;
import java.util.HashMap;
import java.util.Map;

public class FormIntermediary
{
	private Map<String, Component> map_components;
	private TextFieldLogger tf_logger;
	
	public FormIntermediary(TextFieldLogger tf_logger)
	{
		this.tf_logger = tf_logger;
		
		map_components = new HashMap<String, Component>();
	}
	
	public Component get(String str_name)
	{
		return map_components.get(str_name);
	}
	
	public void put(String str_name, Component component)
	{
		map_components.put(str_name, component);
	}
	
	public TextFieldLogger getLogger()
	{
		return tf_logger;
	}
}