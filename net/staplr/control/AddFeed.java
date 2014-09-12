package net.staplr.control;

import java.awt.Color;
import java.awt.EventQueue;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import javax.swing.JFrame;

import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.RowSpec;
import com.jgoodies.forms.factories.FormFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteResult;

import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.JButton;

import net.staplr.common.DatabaseAuth;
import net.staplr.common.Settings;

import javax.swing.JTextArea;

public class AddFeed {

	private JFrame frmAddFeed;
	private JTextField txtName;
	private JTextField txtCollection;
	private JTextField txtSource;
	private JTextField txtDateFormat;
	private JTextField txtTTL;
	private JButton btnCheckDateFormat;
	private JButton btnAdd;
	private JLabel lblDateFormat;
	private JTextArea txtDateFormatDesc;
	private JScrollPane scrlDateFormatDesc;
	//---------------------------------------
	private MouseListener ml_listener;
	private Settings s_settings;
	

	public AddFeed(Settings s_settings) {
		this.s_settings = s_settings;
		
		initialize();
		
		ml_listener = new MouseListener()
		{
			public void mouseClicked(MouseEvent me) {
			}
			public void mouseEntered(MouseEvent me) {
			}
			public void mouseExited(MouseEvent me) {
			}
			public void mousePressed(MouseEvent me) {
				if(me.getSource() == btnCheckDateFormat)
				{					
					System.out.println("Check Date Format");
					
					try{
						new SimpleDateFormat(txtDateFormat.getText());
					}
					catch (Exception e)
					{
						lblDateFormat.setForeground(Color.red);
					}
					finally
					{
						lblDateFormat.setForeground(Color.black);
					}
				}
				if(me.getSource() == btnAdd)
				{
					System.out.println("Add Feed");
					add();
				}
			}
			public void mouseReleased(MouseEvent me) {
			}
		};
		
		btnCheckDateFormat.addMouseListener(ml_listener);
		btnAdd.addMouseListener(ml_listener);
	}

	private void initialize() {
		frmAddFeed = new JFrame();
		frmAddFeed.setResizable(false);
		frmAddFeed.setTitle("Add Feed");
		frmAddFeed.setBounds(100, 100, 753, 226);
		frmAddFeed.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		frmAddFeed.getContentPane().setLayout(new FormLayout(new ColumnSpec[] {
				FormFactory.RELATED_GAP_COLSPEC,
				FormFactory.DEFAULT_COLSPEC,
				FormFactory.RELATED_GAP_COLSPEC,
				ColumnSpec.decode("150dlu"),
				FormFactory.RELATED_GAP_COLSPEC,
				FormFactory.DEFAULT_COLSPEC,},
			new RowSpec[] {
				FormFactory.RELATED_GAP_ROWSPEC,
				RowSpec.decode("default:grow"),
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
		
		JLabel lblName = new JLabel("Name");
		frmAddFeed.getContentPane().add(lblName, "2, 2, right, default");
		
		txtName = new JTextField();
		frmAddFeed.getContentPane().add(txtName, "4, 2, fill, default");
		txtName.setColumns(10);
		
		txtDateFormatDesc = new JTextArea();
		txtDateFormatDesc.setEditable(false);
		txtDateFormatDesc.setText("Symbol  \tMeaning                 \t\tPresentation  \tExamples\r\n \r\n G       \tera                          \t\ttext          \t\tAD\r\n C       \tcentury of era (>=0)       number        \t20\r\n Y       \tyear of era (>=0)           \tyear          \t\t1996\r\n\r\n x       \tweekyear                     \tyear          \t\t1996\r\n w       \tweek of weekyear           number        \t27\r\n e       \tday of week                  \tnumber        \t2\r\n E       \tday of week                  \ttext          \t\tTuesday; Tue\r\n\r\n y       \tyear                        \t\t year          \t\t1996\r\n D       \tday of year                 \tnumber        \t189\r\n M       \tmonth of year                \tmonth         \tJuly; Jul; 07\r\n d       \tday of month                 \tnumber        \t10\r\n\r\n a       \thalfday of day               \ttext          \t\tPM\r\n K       \thour of halfday (0~11)  number       \t\t0\r\n h      \tclockhour of halfday (1~12)  number      12\r\n\r\n H      \thour of day (0~23)          number        \t0\r\n k       \tclockhour of day (1~24) number        \t24\r\n m       minute of hour               \t number        \t30\r\n s       second of minute            \t number       \t55\r\n S       fraction of second           \t number        \t978\r\n\r\n z       time zone                   \t\ttext          \t\tPacific Standard Time; PST\r\n Z       time zone offset/id          \tzone          \t\t-0800; -08:00; America/Los_Angeles\r\n\r\n '       escape for text              \t\tdelimiter\r\n ''      single quote                \t\tliteral       \t\t'");
		
		scrlDateFormatDesc = new JScrollPane(txtDateFormatDesc);
		frmAddFeed.getContentPane().add(scrlDateFormatDesc, "6, 2, 1, 9, fill, fill");
		
		JLabel lblSource = new JLabel("Source");
		frmAddFeed.getContentPane().add(lblSource, "2, 4, right, default");
		
		txtSource = new JTextField();
		frmAddFeed.getContentPane().add(txtSource, "4, 4, fill, default");
		txtSource.setColumns(10);
		
		JLabel lblCollection = new JLabel("Collection");
		frmAddFeed.getContentPane().add(lblCollection, "2, 6, right, default");
		
		txtCollection = new JTextField();
		txtCollection.setText("");
		frmAddFeed.getContentPane().add(txtCollection, "4, 6, fill, default");
		txtCollection.setColumns(10);
		
		lblDateFormat = new JLabel("Date Format");
		frmAddFeed.getContentPane().add(lblDateFormat, "2, 8, right, default");
		
		txtDateFormat = new JTextField();
		txtDateFormat.setText("");
		frmAddFeed.getContentPane().add(txtDateFormat, "4, 8, fill, default");
		txtDateFormat.setColumns(10);
		
		JLabel lblTtl = new JLabel("TTL");
		frmAddFeed.getContentPane().add(lblTtl, "2, 10, right, default");
		
		txtTTL = new JTextField();
		frmAddFeed.getContentPane().add(txtTTL, "4, 10, fill, default");
		txtTTL.setColumns(10);
		
		btnCheckDateFormat = new JButton("Check Date Format");
		frmAddFeed.getContentPane().add(btnCheckDateFormat, "4, 12, left, default");
		
		btnAdd = new JButton("Add");
		frmAddFeed.getContentPane().add(btnAdd, "4, 14, left, default");
		
		frmAddFeed.setVisible(true);
	}
	
	private void add()
	{
		Mongo m_feeds = null;
		Mongo m_statistics = null;
		DB db_feeds = null;
		DB db_statistics = null;
		DBCollection col_requested = null;
		//EEE, dd MM YYYY HH:mm:ss Z
		
		// -------------------------------------------------
		// Add Feed
		// -------------------------------------------------
		
		try
		{
			m_feeds = new Mongo((String)s_settings.map_databaseAuth.get("feeds").get(DatabaseAuth.Properties.location).toString(), Integer.parseInt((String)s_settings.map_databaseAuth.get("feeds").get(DatabaseAuth.Properties.port)));
			db_feeds = m_feeds.getDB("feeds");
		}
		catch (Exception e)
		{
			// TODO let them know it failed
		}
		finally
		{
			
			if(db_feeds.authenticate((String)s_settings.map_databaseAuth.get("feeds").get(DatabaseAuth.Properties.username),
					s_settings.map_databaseAuth.get("feeds").get(DatabaseAuth.Properties.password).toString().toCharArray()))
			{
				if(!db_feeds.collectionExists(txtCollection.getText()))
				{
					db_feeds.createCollection(txtCollection.getText(), null);
				}
				
				DBObject dbo_feed = new BasicDBObject();
				dbo_feed.put("name", txtName.getText());
				
				col_requested = db_feeds.getCollection(txtCollection.getText());
				WriteResult wr_insertFeed = col_requested.insert(dbo_feed);
				
				if(wr_insertFeed.getCachedLastError() != null)
				{
					// TODO Error?
				}
			}
			else
			{
				// TODO Let me know it failed
			}
		}
		
		// ---------------------------------------------------
		// Add Statstics Object
		// ---------------------------------------------------
		
		try
		{
			m_statistics = new Mongo((String)s_settings.map_databaseAuth.get("statistics").get(DatabaseAuth.Properties.location).toString(), Integer.parseInt((String)s_settings.map_databaseAuth.get("statistics").get(DatabaseAuth.Properties.port)));
			db_statistics = m_statistics.getDB("statistics");
		}
		catch (Exception e)
		{
			// TODO let them know it failed
		}
		finally
		{
			
			if(db_statistics.authenticate((String)s_settings.map_databaseAuth.get("statistics").get(DatabaseAuth.Properties.username),
					s_settings.map_databaseAuth.get("statistics").get(DatabaseAuth.Properties.password).toString().toCharArray()))
			{
				if(!db_statistics.collectionExists(txtCollection.getText()))
				{
					db_statistics.createCollection(txtCollection.getText(), null);
				}
				
				DBObject dbo_statistics = new BasicDBObject();
				dbo_statistics.put("collection", txtCollection.getText());
				dbo_statistics.put("name", txtName.getText());
				dbo_statistics.put("ttl", txtTTL.getText());
				dbo_statistics.put("dateFormat", txtDateFormat.getText());
				dbo_statistics.put("url", txtSource.getText());
				
				col_requested = db_statistics.getCollection("feeds");
				WriteResult wr_insertFeed = col_requested.insert(dbo_statistics);
				
				if(wr_insertFeed.getCachedLastError() != null)
				{
					// TODO Error?
				}
				else
				{
					this.frmAddFeed.dispose();
				}
			}
			else
			{
				// TODO Let me know it failed
			}
		}
	}
}
