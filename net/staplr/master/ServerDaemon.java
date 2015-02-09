package net.staplr.master;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

public class ServerDaemon implements Daemon {

    private Thread myThread; 
    private boolean stopped = false;
    
    private Master master;
    
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception 
    {
        String[] args = daemonContext.getArguments(); 
        
        master = new Master();
        master.startup(args[0]);
        
        myThread = new Thread()
        {    
            @Override
            public synchronized void start() {
            	ServerDaemon.this.stopped = false;
                super.start();
            }

            @Override
            public void run() {             
            }
        };
    }

    public void start() throws Exception 
    {
        myThread.start();
    }

    public void stop() throws Exception 
    {
        stopped = true;
        
        try{
            myThread.join(1000);
        }catch(InterruptedException e){
            System.err.println(e.getMessage());
            throw e;
        }
    }
    
    public void destroy() 
    {
        myThread = null;
    }
}