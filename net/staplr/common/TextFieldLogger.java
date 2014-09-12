package net.staplr.common;

import java.awt.Color;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.text.DefaultCaret;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;

public class TextFieldLogger
{
	private JTextPane textPane;
	private StyledDocument document;
	
	public static SimpleAttributeSet StandardStyle;
	public static SimpleAttributeSet ErrorStyle;
	public static SimpleAttributeSet SuccessStyle;
	
	public TextFieldLogger(JTextPane textField)
	{
		this.textPane = textField;
		document = (StyledDocument)this.textPane.getDocument();
		
		ErrorStyle = new SimpleAttributeSet();
		ErrorStyle.addAttribute(StyleConstants.CharacterConstants.Foreground, Color.decode("#990000"));
		
		SuccessStyle = new SimpleAttributeSet();
		SuccessStyle.addAttribute(StyleConstants.CharacterConstants.Foreground, Color.decode("#009900"));
		
		StandardStyle = new SimpleAttributeSet();
		StandardStyle.addAttribute(StyleConstants.CharacterConstants.Foreground, Color.decode("#666666"));
		
		((DefaultCaret)textField.getCaret()).setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE);
	}
	
	public void log(String text, SimpleAttributeSet style)
	{
		SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy [HH:mm:ss]");
		Date timeNow = new Date();
		
		format.setTimeZone(TimeZone.getDefault());
		
		try{
			document.insertString(document.getLength(), format.format(timeNow)+":"+text+"\r\n", style);
		} catch (Exception e) {	}
	}
}