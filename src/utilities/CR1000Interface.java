package utilities;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.campbellsci.pakbus.DataCollectClient;
import com.campbellsci.pakbus.DataCollectModeDateToNewest;
import com.campbellsci.pakbus.DataCollectModeRecordNoToNewest;
import com.campbellsci.pakbus.DataCollectTran;
import com.campbellsci.pakbus.Datalogger;
import com.campbellsci.pakbus.GetTableDefsClient;
import com.campbellsci.pakbus.GetTableDefsTran;
import com.campbellsci.pakbus.LoggerDate;
import com.campbellsci.pakbus.Network;
import com.campbellsci.pakbus.Record;
import com.campbellsci.pakbus.TableDef;

/**
 * 
 * @author jes244
 *
 */
public class CR1000Interface implements GetTableDefsClient, DataCollectClient
{
	// Variables
	private int rec_index;
	private boolean complete;
	private Datalogger my_cr1000;
	private Network network;
	private Socket socket;
	private SegaLogger log;
	private boolean debug;
	// Public variables
	public Record[] records;
	public TableDef[] tables;
	
	public CR1000Interface(){
		//Empty constructor
	}
	public boolean init_cr1000_interface(String ip_address, short pb_address,String directory, boolean debug) 
	{
		try{
			if(log == null)
				log = new SegaLogger(directory + "/logs/cr1000-interface.txt");
			this.debug = debug;
			// Create connection and network
			socket = new Socket(ip_address, 6785);
			network = new Network((short)4079, socket.getInputStream(), socket.getOutputStream());
			// Create station
			my_cr1000 = new Datalogger(pb_address);
			// Add datalogger to the network
			network.add_station(my_cr1000);
		}catch(UnknownHostException e){
			StringWriter errors = new StringWriter();
        	e.printStackTrace(new PrintWriter(errors));
    		writeToLog(errors.toString());
    		return false;
			
		} catch (IOException e) {
			StringWriter errors = new StringWriter();
        	e.printStackTrace(new PrintWriter(errors));
    		writeToLog(errors.toString());
    		return false;
		}
		
		return true;
	}

	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	public void get_tables() throws Exception
	{
		// Variables
		int active_links = 0;

		// Reset the tables
		tables = new TableDef[0];
		// Add a transaction to get the table definitions
		my_cr1000.add_transaction(new GetTableDefsTran(this));
		complete = false;
		while (!complete || active_links > 0) {
			active_links = network.check_state();
			sleep_thread(1);
		}
	}
	
	/**
	 * 
	 */
	public void on_complete(GetTableDefsTran transaction, int outcome)
	{
		// Variables
		int count = 0;

		// Did the transaction succeed?
		if (outcome == GetTableDefsTran.outcome_success) {
			// Get the number of tables
			count = my_cr1000.get_tables_count();
			// Create array of tables
			tables = new TableDef[count - 2];
			// Store the tables
			for (int i = 2; i < count; i++)
				tables[i - 2] = my_cr1000.get_table(i);
		} else {
			writeToLog("Get table definitions transaction did not succeed. Error code " + outcome + ".");
		}
		complete = true;
	}

	/**
	 * 
	 * @param table - table to fetch from
	 * @param rn - record number
	 * @return
	 * @throws Exception
	 */
	public void get_records(TableDef table, long rn) throws Exception 
	{
		
		// Variables
		int active_links = 0;

		// Reset the index
		rec_index = 0;
		// Reset the records
		records = new Record[0];
		// Add a transaction to collect the records based on if any
		// records have been received or not
		my_cr1000.add_transaction(new DataCollectTran(table.name, this,
					new DataCollectModeRecordNoToNewest(rn)));
		
		complete = false;
		while (!complete || active_links > 0) {
			 active_links = network.check_state(complete);	         
	         Thread.sleep(100);
		}
	}
	/**
	 * 
	 * @param table
	 * @param rn
	 * @return
	 * @throws Exception
	 */
	public void get_records_by_timestamp(TableDef table, long timestamp) throws Exception 
	{
		// Variables
		int active_links = 0;
		
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(timestamp);
		LoggerDate logger_date = new LoggerDate(cal);
		// Reset the index
		rec_index = 0;
		// Reset the records
		records = new Record[0];
		// Add a transaction to collect the records based on if any
		// records have been received or no
		my_cr1000.add_transaction(new DataCollectTran(table.name, this,
				new DataCollectModeDateToNewest(logger_date)));
		
		complete = false;
		while (!complete || active_links > 0) {
			active_links = network.check_state();
			Thread.sleep(100);
		}
	}
	
	/**
	 * 
	 * @param table
	 * @param rn
	 * @return
	 * @throws Exception
	 */
	public void get_records(TableDef table,int days_of_data) throws Exception 
	{
		// Variables
		int active_links = 0;
		
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1 * days_of_data);
		LoggerDate logger_date = new LoggerDate(cal);
		// Reset the index
		rec_index = 0;
		// Reset the records
		records = new Record[0];
		// Add a transaction to collect the records based on if any
		// records have been received or no
		my_cr1000.add_transaction(new DataCollectTran(table.name, this,
				new DataCollectModeDateToNewest(logger_date)));
		
		complete = false;
		while (!complete || active_links > 0) {
			active_links = network.check_state();
			Thread.sleep(100);
		}
	}

	/**
	 * 
	 */
	public void on_complete(DataCollectTran transaction, int outcome)
	{
		// Did the transaction succeed?
		if (outcome != DataCollectTran.outcome_success){
			if(outcome != 11)
				writeToLog("Data collection transaction did not succeed. Error code " + outcome + ".");
			//If the outcome was 11 we don't really care - it just means that that table hasn't gotten any data yet
			//so we don't need to log it
		}
			
		complete = true;
	}

	/**
	 * 
	 */
	public boolean on_records(DataCollectTran transaction, List<Record> rs)
	{
		// This method returns 18 records at a time, I don't know why
		// Extend the records array
		records = Arrays.copyOf(records, records.length + rs.size());
		// Store the records
		for (Record r : rs) 
			records[rec_index++] = r;
		// Clear the list of records received
		rs.clear();
		return true;
	}
	
	/**
	 * Sleep the current thread for x amount of seconds.
	 * 
	 * @param sec Seconds to sleep for.
	 */
	private void sleep_thread(int sec)
	{
		try {
			TimeUnit.SECONDS.sleep(sec);
		} catch (InterruptedException e) {
			writeToLog("Thread interruption during sleep.");
		}
	}
	
	public synchronized void writeToLog(Object obj){
		if(debug){
			print_to_console(obj.toString());
		}
		log.write(obj);
	}
	public synchronized void print_to_console(String s){
		System.out.println(s);
	}
}
