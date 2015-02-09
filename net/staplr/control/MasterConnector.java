package net.staplr.control;

import java.awt.EventQueue;

import javax.swing.JFrame;

import net.staplr.common.Credentials;
import net.staplr.common.Communicator.Type;
import net.staplr.common.TextFieldLogger;
import net.staplr.common.CopyOfSettings.Setting;
import net.staplr.common.Credentials.Properties;
import net.staplr.common.Communicator;
import net.staplr.logging.Log;
import net.staplr.logging.Log.Options;
import net.staplr.master.Master;
import net.staplr.service.MessageExecutor;
import net.staplr.common.Settings;

import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.RowSpec;
import com.jgoodies.forms.factories.FormFactory;

import javax.swing.JLabel;

import java.awt.Font;
import java.awt.Choice;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

import javax.swing.JButton;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.JScrollPane;
import javax.swing.ScrollPaneConstants;

public class MasterConnector 
{
	private Settings s_settings;
	private TextFieldLogger tf_logger;
	private Communicator c_communicator;
	private Thread t_listener;
	private MessageExecutor mx_executor;
	private Log l_masterConnector;
	
	// --------------------------------------- //
	private JLabel lbl_title;
	private JButton btn_connect;
	private Choice cmbo_master;
	private JFrame frame;
	private JTextField txt_masterKey;
	private JLabel lbl_status;
	private JLabel lblLog;
	private JTextPane txt_log;
	private JScrollPane scrl_log;
	// --------------------------------------- //

	/**
	 * @wbp.parser.entryPoint
	 */
	public MasterConnector(Settings s_settings, Log l_masterConnector) {
		this.s_settings = s_settings;
		this.l_masterConnector = l_masterConnector;
		
		initialize();
	}
	
	public static void main(String[] args)
	{
		Log l_masterConnector = new Log("connector.log");
		l_masterConnector.setOption(Options.ConsoleOutput, true);
		l_masterConnector.setOption(Options.FileOutput, true);
		
		Settings settings = new Settings(l_masterConnector);
		
		if(settings.loaded()) new MasterConnector(settings, l_masterConnector);
		else System.out.println("Error: Failed to load settings for MasterConnector");
	}

	private void initialize() {
		frame = new JFrame();
		frame.setBounds(100, 100, 519, 231);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(new FormLayout(new ColumnSpec[] {
				FormFactory.RELATED_GAP_COLSPEC,
				ColumnSpec.decode("default:grow"),
				FormFactory.RELATED_GAP_COLSPEC,
				FormFactory.DEFAULT_COLSPEC,},
			new RowSpec[] {
				FormFactory.RELATED_GAP_ROWSPEC,
				FormFactory.DEFAULT_ROWSPEC,
				FormFactory.RELATED_GAP_ROWSPEC,
				FormFactory.DEFAULT_ROWSPEC,
				FormFactory.RELATED_GAP_ROWSPEC,
				FormFactory.DEFAULT_ROWSPEC,
				FormFactory.RELATED_GAP_ROWSPEC,
				FormFactory.DEFAULT_ROWSPEC,
				FormFactory.RELATED_GAP_ROWSPEC,
				RowSpec.decode("default:grow"),}));
		
		initFormComponents();
		
		// Now show the form
		frame.setVisible(true);
		
		// ------------------------------------------
		// Settings load
		// ------------------------------------------
		
		tf_logger.log("Loading settings...", TextFieldLogger.StandardStyle);
		
		if(s_settings.loaded())
		{
			tf_logger.log("Loaded settings", TextFieldLogger.SuccessStyle);
			
			// ------------------------------------------
			// Form population
			// ------------------------------------------
						
			for(int i_masterIndex = 0; i_masterIndex < s_settings.c_credentials.size(); i_masterIndex++)
			{
				Credentials c_credentials = s_settings.c_credentials.get(i_masterIndex);
				cmbo_master.add(c_credentials.get(Properties.location)+":"+c_credentials.get(Properties.port));
			}
			
			// ------------------------------------------
			// Thread startup
			// ------------------------------------------

			c_communicator = new Communicator(null, s_settings, 1998, Type.Service, l_masterConnector, null);
			t_listener = new Thread(c_communicator);
			
			t_listener.start();
		}
		else
		{
			tf_logger.log("Failed to load settings", TextFieldLogger.ErrorStyle);
		}
	}

	private void initFormComponents()
	{
		lbl_title = new JLabel("Connect to Master");
		lbl_title.setFont(new Font("Arial", Font.PLAIN, 16));
		frame.getContentPane().add(lbl_title, "2, 2");
		
		cmbo_master = new Choice();
		frame.getContentPane().add(cmbo_master, "2, 4");
		
		btn_connect = new JButton("Connect");
		btn_connect.addMouseListener(new MouseListener() {
			public void mouseClicked(MouseEvent me) {
			}
			public void mouseEntered(MouseEvent me) {
			}
			public void mouseExited(MouseEvent me) {
			}
			public void mousePressed(MouseEvent me) {
			}
			public void mouseReleased(MouseEvent me) {
				if(me.getButton() == MouseEvent.BUTTON1)
				{
					String str_selectedMaster = cmbo_master.getSelectedItem();
					String str_location = null;
					String str_port = null;
					
					try{
						str_location = str_selectedMaster.substring(0, str_selectedMaster.indexOf(":"));
					} 
					catch (Exception e)
					{
						e.printStackTrace();
					}
					finally
					{
						try
						{
							str_port = str_selectedMaster.substring(str_selectedMaster.indexOf(":")+1);
						}
						catch(Exception e)
						{
							e.printStackTrace();
						}
						finally
						{
							for(int i_masterIndex = 0; i_masterIndex < s_settings.c_credentials.size(); i_masterIndex++)
							{
								Credentials c_credential = s_settings.c_credentials.get(i_masterIndex);
								
								if(((String)c_credential.get(Properties.location)).equals(str_location))
								{
									if(String.valueOf(c_credential.get(Properties.port)).equals(str_port))
									{
										tf_logger.log("Attempting to connect to "+str_selectedMaster, TextFieldLogger.StandardStyle);
										
										if(txt_masterKey.getText().length() > 0)
										{
											c_credential.set(Properties.key, txt_masterKey.getText());
										}
										
										
										if((mx_executor = (MessageExecutor)c_communicator.connect(c_credential)) != null)
										{
											tf_logger.log("Connection Success", TextFieldLogger.SuccessStyle);
											
											new MasterServiceWindow(mx_executor);
										}
										else
										{
											tf_logger.log("Failed to Connect", TextFieldLogger.ErrorStyle);
										}
										
										break;
									}
								}
							}
						}
					}
				}
			}
		});
		
		frame.getContentPane().add(btn_connect, "4, 4");
		
		txt_masterKey = new JTextField();
		frame.getContentPane().add(txt_masterKey, "2, 6, fill, default");
		txt_masterKey.setColumns(10);
		
		lblLog = new JLabel("Log");
		frame.getContentPane().add(lblLog, "2, 8");
		
		scrl_log = new JScrollPane();
		scrl_log.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
		scrl_log.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
		frame.getContentPane().add(scrl_log, "2, 10, fill, fill");
		
		txt_log = new JTextPane();
		txt_log.setEditable(false);
		scrl_log.setViewportView(txt_log);
		tf_logger = new TextFieldLogger(txt_log);
	}
}
