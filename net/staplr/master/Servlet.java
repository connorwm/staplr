package net.staplr.master;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.staplr.common.Settings.Setting;
import net.staplr.logging.Log;
import net.staplr.logging.Log.Options;

public class Servlet extends HttpServlet
{
	private Log log_servlet;
	private Master master;
	private Thread t_master;
	
	public void init(ServletConfig config)
	{
		log_servlet = new Log("servlet.log");
		log_servlet.setOption(Options.ConsoleOutput, false);
		
		master = new Master();
		t_master = new Thread(master, "Master");
		
		// TODO Rewrite servlet for updated code
	}
}