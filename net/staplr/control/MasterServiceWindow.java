package net.staplr.control;

import java.awt.EventQueue;
import javax.swing.JFrame;
import net.staplr.common.FormIntermediary;
import net.staplr.common.TextFieldLogger;
import net.staplr.service.MessageExecutor;
import net.staplr.common.message.Message;
import net.staplr.common.message.Message.Type;
import net.staplr.common.message.Message.Value;

import javax.swing.JMenuBar;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JSplitPane;
import javax.swing.JTextField;
import java.awt.BorderLayout;
import javax.swing.JTabbedPane;
import javax.swing.JTextPane;
import javax.swing.JScrollPane;
import javax.swing.JPanel;
import java.awt.GridLayout;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.RowSpec;
import com.jgoodies.forms.factories.FormFactory;
import javax.swing.JLabel;
import java.awt.Font;
import javax.swing.JPasswordField;
import javax.swing.ScrollPaneConstants;
import javax.swing.JTextArea;

import org.json.simple.JSONObject;
import javax.swing.JButton;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import javax.swing.JScrollBar;
import java.awt.event.MouseAdapter;
import java.awt.TextArea;
import javax.swing.SwingConstants;

public class MasterServiceWindow {
	private MessageExecutor mx_executor;
	private FormIntermediary fi_main;
	private MouseListener ml_main;
	private TextFieldLogger tf_logger;
	//-----------------------------------------------
	private JFrame frmMasterService;
	private JScrollPane scrl_log;
	private JTabbedPane tab_main;
	private JTextPane txt_log;
	private JMenuBar menuBar;
	private JMenuItem mntmSaveToClipboard;
	private JMenu mnPane;
	private JMenuItem mntmRefresh;
	private JMenu mnConnection;
	private JMenuItem mntmDisconnect;
	private JPanel pnl_settings;
	private JLabel lblDatabaseAuth;
	private JLabel lblLocation;
	private JLabel lblPort;
	private JLabel lblUsername;
	private JLabel lblPassword;
	private JLabel lblDatabase;
	private JLabel lblMiscellaneous;
	private JLabel lblKey;
	private JLabel lblCommunicationsPort;
	private JLabel lblServicePort;
	private JTextField txtLocation;
	private JTextField txtPort;
	private JTextField txtUsername;
	private JTextField txtDatabase;
	private JTextField txtKey;
	private JTextField txtCommunicationPort;
	private JScrollPane scrl_settings;
	private JTextField txtServicePort;
	private JTextField txtPassword;
	private JButton btnUpdateSettings;
	private JScrollPane scrl_masterLogs;
	private JTabbedPane tab_logs;
	private JScrollPane scrl_operations;
	private JPanel pnl_operations;
	private JLabel lblInstance;
	private JButton btnGarbageCollection;
	private JLabel lblServlet;
	private JButton btnRestart;
	private JScrollPane scrl_masterLog;
	private JScrollPane scrl_settingsLog;
	private JScrollPane scrl_listenerLog;
	private JScrollPane scrl_ensurerLog;
	private TextArea ta_settingsLog;
	private TextArea ta_ensurerLog;
	private TextArea ta_listenerLog;
	private TextArea ta_masterLog;
	private JScrollPane scrl_feeds;
	private JTextArea ta_feeds;

	public MasterServiceWindow(MessageExecutor mx_executor) {
		initFormComponents();
		
		frmMasterService.setVisible(true);
		
		this.mx_executor = mx_executor;
		tf_logger = new TextFieldLogger(txt_log);
		
		fi_main = new FormIntermediary(tf_logger);
		fi_main.put("db_database", txtDatabase);
		fi_main.put("db_location", txtLocation);
		fi_main.put("db_port", txtPort);
		fi_main.put("db_username", txtUsername);
		fi_main.put("db_password", txtPassword);
		fi_main.put("masterCommunicationPort", txtCommunicationPort);
		fi_main.put("servicePort", txtServicePort);
		fi_main.put("masterKey", txtKey);
		fi_main.put("log_master", ta_masterLog);
		fi_main.put("log_listener", ta_listenerLog);
		fi_main.put("log_ensurer", ta_ensurerLog);
		fi_main.put("log_settings", ta_settingsLog);
		fi_main.put("ta_feeds", ta_feeds);
		
		mx_executor.setFormIntermediary(fi_main);
		
		ml_main = new MouseListener()
		{
			public void mouseClicked(MouseEvent me) {}
			public void mouseEntered(MouseEvent me) {}
			public void mouseExited(MouseEvent me) {}
			public void mousePressed(MouseEvent me) 
			{
				if(me.getSource() == btnUpdateSettings)
				{
					sendUpdateSettingsRequest();
				}
				if(me.getSource() == mntmRefresh)
				{
					refreshPane();
				}
				if(me.getSource() == mntmDisconnect)
				{
					disconnect();
				}
				if(me.getSource() == btnGarbageCollection)
				{
					sendGCRequest();
				}
			}
			public void mouseReleased(MouseEvent me) {}
		};
		
		btnUpdateSettings.addMouseListener(ml_main);
		mntmRefresh.addMouseListener(ml_main);
		
		// ----------------------------------------------------
		// Request data from Master to populate forms
		// ----------------------------------------------------
		
		// Request Settings
		tf_logger.log("Requesting settings...", TextFieldLogger.StandardStyle);
		Message msg_settingsRequest = new Message(Type.Request, Value.Settings);
		mx_executor.send(msg_settingsRequest);
		tf_logger.log("Sent", TextFieldLogger.StandardStyle);
		
		// Request all other
		sendLogsRequest();
		sendFeedsRequest();
	}
	
	private void initFormComponents()
	{
		// Main Form
		frmMasterService = new JFrame();
		frmMasterService.setTitle("Master Service");
		frmMasterService.setBounds(100, 100, 501, 494);
		frmMasterService.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		// Menu Bar
		menuBar = new JMenuBar();
		frmMasterService.setJMenuBar(menuBar);
		
		// Connection Menu
		mnConnection = new JMenu("Connection");
		menuBar.add(mnConnection);
		
		mntmDisconnect = new JMenuItem("Disconnect");
		mntmDisconnect.addMouseListener(ml_main);
		mnConnection.add(mntmDisconnect);
		
		// Pane Menu
		mnPane = new JMenu("Pane");
		menuBar.add(mnPane);
		
		mntmSaveToClipboard = new JMenuItem("Save to Clipboard");
		mnPane.add(mntmSaveToClipboard);
		
		mntmRefresh = new JMenuItem("Refresh");
		mnPane.add(mntmRefresh);
		
		// Main Tab Group (left side)
		tab_main = new JTabbedPane(JTabbedPane.LEFT);
		frmMasterService.getContentPane().add(tab_main, BorderLayout.NORTH);
		
		// --------------- Settings --------------------- //
		
		// Scrollpane
		scrl_settings = new JScrollPane();
		scrl_settings.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
		tab_main.addTab("Settings", null, scrl_settings, null);
		
		// Panel
		pnl_settings = new JPanel();
		pnl_settings.setLayout(new FormLayout(new ColumnSpec[] {
				FormFactory.RELATED_GAP_COLSPEC,
				FormFactory.DEFAULT_COLSPEC,
				FormFactory.RELATED_GAP_COLSPEC,
				FormFactory.DEFAULT_COLSPEC,
				FormFactory.RELATED_GAP_COLSPEC,
				ColumnSpec.decode("default:grow"),},
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
				FormFactory.DEFAULT_ROWSPEC,
				FormFactory.RELATED_GAP_ROWSPEC,
				FormFactory.DEFAULT_ROWSPEC,
				FormFactory.RELATED_GAP_ROWSPEC,
				FormFactory.DEFAULT_ROWSPEC,
				FormFactory.RELATED_GAP_ROWSPEC,
				FormFactory.DEFAULT_ROWSPEC,
				FormFactory.RELATED_GAP_ROWSPEC,
				FormFactory.DEFAULT_ROWSPEC,
				FormFactory.RELATED_GAP_ROWSPEC,
				FormFactory.DEFAULT_ROWSPEC,
				FormFactory.RELATED_GAP_ROWSPEC,
				FormFactory.DEFAULT_ROWSPEC,
				FormFactory.RELATED_GAP_ROWSPEC,
				FormFactory.DEFAULT_ROWSPEC,}));
		scrl_settings.setViewportView(pnl_settings);
		
		// Label Database Auth
		lblDatabaseAuth = new JLabel("Database Auth");
		lblDatabaseAuth.setFont(new Font("Tahoma", Font.BOLD, 11));
		pnl_settings.add(lblDatabaseAuth, "2, 2");
		
		// Label Location
		lblLocation = new JLabel("Location");
		pnl_settings.add(lblLocation, "2, 4");
		
		// TextField Location
		txtLocation = new JTextField();
		pnl_settings.add(txtLocation, "6, 4, fill, default");
		txtLocation.setColumns(10);
		
		// Label Port
		lblPort = new JLabel("Port");
		pnl_settings.add(lblPort, "2, 6");
		
		// TextField Port
		txtPort = new JTextField();
		pnl_settings.add(txtPort, "6, 6, fill, default");
		txtPort.setColumns(10);
		
		// Label Username
		lblUsername = new JLabel("Username");
		pnl_settings.add(lblUsername, "2, 8");
		
		// TextField Username
		txtUsername = new JTextField();
		pnl_settings.add(txtUsername, "6, 8, fill, default");
		txtUsername.setColumns(10);
		
		// Label Password
		lblPassword = new JLabel("Password");
		pnl_settings.add(lblPassword, "2, 10");
		
		// TextField Password
		txtPassword = new JTextField();
		pnl_settings.add(txtPassword, "6, 10, fill, default");
		txtPassword.setColumns(10);
		
		// Label Database
		lblDatabase = new JLabel("Database");
		pnl_settings.add(lblDatabase, "2, 12");
		
		// TextField Database
		txtDatabase = new JTextField();
		pnl_settings.add(txtDatabase, "6, 12, fill, default");
		txtDatabase.setColumns(10);
		
		// Label Miscellaneous
		lblMiscellaneous = new JLabel("Miscellaneous");
		lblMiscellaneous.setFont(new Font("Tahoma", Font.BOLD, 11));
		pnl_settings.add(lblMiscellaneous, "2, 14");
		
		// Label Key
		lblKey = new JLabel("Key");
		pnl_settings.add(lblKey, "2, 16");
		
		// TextField Key
		txtKey = new JTextField();
		pnl_settings.add(txtKey, "6, 16, fill, default");
		txtKey.setColumns(10);
		
		// Label Communications Port
		lblCommunicationsPort = new JLabel("Com. Port");
		pnl_settings.add(lblCommunicationsPort, "2, 18");
		
		// TextField Communications Port
		txtCommunicationPort = new JTextField();
		pnl_settings.add(txtCommunicationPort, "6, 18, fill, default");
		txtCommunicationPort.setColumns(10);
		
		// Label Service Port
		lblServicePort = new JLabel("Service Port");
		pnl_settings.add(lblServicePort, "2, 20");
		
		// TextField Service Port
		txtServicePort = new JTextField();
		pnl_settings.add(txtServicePort, "6, 20, fill, default");
		txtServicePort.setColumns(10);
		
		// Button Update Settings
		btnUpdateSettings = new JButton("Update Settings");
		pnl_settings.add(btnUpdateSettings, "6, 24");
		
		// ScrollPane  for main Log
		scrl_log = new JScrollPane();
		scrl_log.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
		
		frmMasterService.getContentPane().add(scrl_log, BorderLayout.CENTER);
		
		// Feeds Pane
		scrl_feeds = new JScrollPane();
		scrl_feeds.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
		tab_main.addTab("Feeds", null, scrl_feeds, null);
		
		ta_feeds = new JTextArea();
		ta_feeds.setText("Select\r\nPane > Refresh\r\nto find the master's current feeds\r\n");
		ta_feeds.setEditable(false);
		scrl_feeds.setViewportView(ta_feeds);
		ta_feeds.setColumns(10);
		
		// TextPane Log
		txt_log = new JTextPane();
		txt_log.setEditable(false);
		scrl_log.setViewportView(txt_log);

		scrl_masterLogs = new JScrollPane();
		tab_main.addTab("Logs", scrl_masterLogs);
		
		tab_logs = new JTabbedPane(JTabbedPane.TOP);
		scrl_masterLogs.setViewportView(tab_logs);
		
		scrl_masterLog = new JScrollPane();
		tab_logs.addTab("Master", null, scrl_masterLog, null);
		
		scrl_settingsLog = new JScrollPane();
		tab_logs.addTab("Settings", null, scrl_settingsLog, null);
		
		scrl_listenerLog = new JScrollPane();
		tab_logs.addTab("Listener", null, scrl_listenerLog, null);
		
		scrl_ensurerLog = new JScrollPane();
		tab_logs.addTab("Ensurer", null, scrl_ensurerLog, null);
		
		ta_settingsLog = new TextArea();
		ta_settingsLog.setEditable(false);
		scrl_settingsLog.setViewportView(ta_settingsLog);
		
		ta_ensurerLog = new TextArea();
		ta_ensurerLog.setEditable(false);
		scrl_ensurerLog.setViewportView(ta_ensurerLog);
		
		ta_listenerLog = new TextArea();
		ta_listenerLog.setEditable(false);
		scrl_listenerLog.setViewportView(ta_listenerLog);
		
		ta_masterLog = new TextArea();
		ta_masterLog.setEditable(false);
		scrl_masterLog.setViewportView(ta_masterLog);
		
		scrl_operations = new JScrollPane();
		
		tab_main.addTab("Operations", scrl_operations);
		
		pnl_operations = new JPanel();
		pnl_operations.setLayout(new FormLayout(new ColumnSpec[] {
				FormFactory.RELATED_GAP_COLSPEC,
				FormFactory.DEFAULT_COLSPEC,
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
				FormFactory.DEFAULT_ROWSPEC,
				FormFactory.RELATED_GAP_ROWSPEC,
				FormFactory.DEFAULT_ROWSPEC,}));
		scrl_operations.setViewportView(pnl_operations);
		
		lblInstance = new JLabel("Instance");
		lblInstance.setFont(new Font("Tahoma", Font.BOLD, 11));
		pnl_operations.add(lblInstance, "2, 2");
		
		btnGarbageCollection = new JButton("Garbage Collection");
		btnGarbageCollection.addMouseListener(ml_main);
		pnl_operations.add(btnGarbageCollection, "4, 4");
		
		lblServlet = new JLabel("Servlet");
		lblServlet.setFont(new Font("Tahoma", Font.BOLD, 11));
		pnl_operations.add(lblServlet, "2, 10");
		
		btnRestart = new JButton("Restart");
		pnl_operations.add(btnRestart, "4, 12");
	}

	private void sendFeedsRequest()
	{
		tf_logger.log("Sending feeds request...", TextFieldLogger.StandardStyle);
		Message msg_requestFeeds = new Message(Type.Request, Value.Feeds);
		mx_executor.send(msg_requestFeeds);
		tf_logger.log("Sent", TextFieldLogger.StandardStyle);
	}
	
	private void sendGCRequest()
	{
		tf_logger.log("Requesting garbage collection...", TextFieldLogger.StandardStyle);
		Message msg_gc = new Message(Type.Request, Value.GarbageCollection);
		mx_executor.send(msg_gc);
		tf_logger.log("Sent", TextFieldLogger.StandardStyle);
	}
	
	private void sendUpdateSettingsRequest()
	{
		tf_logger.log("Sending updated settings...", TextFieldLogger.StandardStyle);
		
		Message msg_updatedSettings = new Message(Type.Request, Value.UpdateSettings);
		
		JSONObject json_databaseAuth = new JSONObject();
		json_databaseAuth.put("db_database", txtDatabase.getText());
		json_databaseAuth.put("db_location", txtLocation.getText());
		json_databaseAuth.put("db_port", txtPort.getText());
		json_databaseAuth.put("db_username", txtUsername.getText());
		json_databaseAuth.put("db_password", txtPassword.getText());
		
		msg_updatedSettings.addItem("databaseAuth", json_databaseAuth);
		
		msg_updatedSettings.addItem("masterCommunicationPort", txtCommunicationPort.getText());
		msg_updatedSettings.addItem("servicePort", txtServicePort.getText());
		msg_updatedSettings.addItem("masterKey", txtKey.getText());
		
		mx_executor.send(msg_updatedSettings);
	}

	private void refreshPane()
	{
		String str_currentTab = tab_main.getTitleAt(tab_main.getSelectedIndex());
		
		switch(str_currentTab)
		{
		case "Settings":
			tf_logger.log("Refreshing settings...", TextFieldLogger.StandardStyle);
			Message msg_settingsRequest = new Message(Type.Request, Value.Settings);
			mx_executor.send(msg_settingsRequest);
			break;
		case "Feeds":
			tf_logger.log("Refreshing feeds...", TextFieldLogger.StandardStyle);
			Message msg_feedRequest = new Message(Type.Request, Value.Feeds);
			mx_executor.send(msg_feedRequest);
			break;		
		default:
			break;
		};
	}
	
	private void sendLogsRequest()
	{
		Message msg_logRequest = new Message(Type.Request, Value.Logs);
		
		tf_logger.log("Requesting Master Logs...", TextFieldLogger.StandardStyle);
		msg_logRequest.addItem("log", "master");
		mx_executor.send(msg_logRequest);
		
		tf_logger.log("Requesting Listener Logs...", TextFieldLogger.StandardStyle);
		msg_logRequest.addItem("log", "listener");
		mx_executor.send(msg_logRequest);
		
		tf_logger.log("Requesting Ensurer Logs...", TextFieldLogger.StandardStyle);
		msg_logRequest.addItem("log", "ensurer");
		mx_executor.send(msg_logRequest);
		
		tf_logger.log("Requesting Settings Logs...", TextFieldLogger.StandardStyle);
		msg_logRequest.addItem("log", "settings");
		mx_executor.send(msg_logRequest);
	}

	private void disconnect()
	{
		Message msg_disconnect = new Message(Type.Request, Value.Disconnect);
		mx_executor.send(msg_disconnect);
		
		frmMasterService.setVisible(false);
	}
}
