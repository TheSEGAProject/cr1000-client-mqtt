package edu.nau.wnrl;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;
import java.util.TimeZone;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.campbellsci.pakbus.Record;
import com.campbellsci.pakbus.TableDef;
import com.campbellsci.pakbus.ValueBase;
import com.rbnb.sapi.ChannelMap;
import com.rbnb.sapi.SAPIException;
import com.rbnb.sapi.Source;

import utilities.CR1000Interface;
import utilities.DataGeneratorCodec;
import utilities.SegaLogger;
import utilities.TableInfo;

/**
 * 
 * @author jes244
 * @author jdk85 - edited and updated
 *
 *
 */
@Deprecated
public class CR1000Client
{
	/** Default IP address for the CR1000 */
	private static String	DEFAULT_CR1000_ADDRESS = "192.168.13.110";
	/** Default root directory for configuration, script, and log files - no trailing slash' */
	private static String	DEFAULT_DIRECTORY="/opt/RBNB";
	/** Default RBNB port value */
	private static String	DEFAULT_PORT = "3333";
	/** Default CR1000 PakBus Address */
	private static short 	DEFAULT_PB_ADDRESS = 6;
	/** Default days of data to fetch from the CR1000 on init */
	private static int		DEFAULT_DAYS_OF_DATA = 3;
	/** IP address for the CR1000 */
	private String cr1000_address = null;
	/** The OS-level root directory for configuration, script, and log files */
	private String root_directory = null;
	/** The RBNB port for the localhost connections */
	private String port = null;
	/** The CR1000 PakBus Address */
	private short pb_address = -1;
	/** When the client initially fetches data from the CR1000, days_of_data is used as a multiplier to specify how many days worth of data to fetch */
	private int days_of_data = -1;
	/** When true, all incoming data from the WiSARD as well as status information is written to the console */
	private boolean debug = false;
	/** When true, user is prompted to enter timestamp and tables will be fetched from then */
	private boolean load = false;

	// Variables
	/** RBNB cache size */
	private int cache_size = 360; //6 hours at minute sampling rate
	/** RBNB archive*/
	private int archive_size = 4320; // 3 days at minute sampling rate

	private ArrayList<TableInfo> table_info_list = new ArrayList<TableInfo>();
	/** Final reconnect logic value - try to reconnect every 30 seconds for 3 days*/
	private final int max_reconnect_attempts = 8640, reconnect_sleep_time = 30;
	/** Total number of reconnect attempts by CR1000 or RBNB */
	private int rbnb_reconnect_attempts = 0,cr1000_reconnect_attempts = 0;
	/** CR100 interface defines functions used by the cr1000 */
	private CR1000Interface cr1000;
	
	/** SegaLog used to write log to disk */
	private SegaLogger log;
	/** Default RBNB source - used to flush CR1000 data to local RBNB */
	private Source src;
	/** Default RBNB synchronization source - used to provide sync information for the RDF */
	private Source sync_src;
	/** The number of seconds to add to convert CR1000 timestamps into ms since 1970*/
	private static final int seconds_to_add = 631152000;
	/** The timezone used to calculated the timezone offset from UTC (should probably be an argument option) */
	private static final TimeZone tz = TimeZone.getTimeZone("US/Arizona");
	/** Date format that represents date as 'M/d/Y - HH:mm:ss:SSS' */
	private static final SimpleDateFormat sdf = new SimpleDateFormat("M/d/y - HH:mm:ss:SSS");
	/**
	 * Used to run as a standalone application.
	 * 
	 * @param args Arguments are passed on to {@link #CR1000Client(String[],Options,CommandLineParser)}
	 */
	static public void main(String args[])
	{
		// Check input parameters
		CommandLineParser parser = new GnuParser();
		
		Options options = new Options();
		options.addOption("h", "help", false, "print help");					
		options.addOption("a", "addr", true, "IP address of the CR1000");
		options.addOption("p", "pbaddr", true, "PakBbus address of the CR1000");
		options.addOption("o", "port", true, "RBNB port - defaults to 3333");
		options.addOption("a", "days", true, "number of days of days to collect on intial request to the cr1000");
		options.addOption("i", "dir", true, "root directory for scripts, logs, config files, etc");
		options.addOption("d", "debug", false, "if set, debug mode is enabled");
		options.addOption("l", "load",false,"if set, script will prompt to load from timestamp (ms since 1970)");
		
		(new CR1000Client(args,options,parser)).execute();
	}
	
	/**
	 * Preferred constructor with command line parsing for program arguments
	 * @param args
	 * @param options
	 * @param parser
	 */
	public CR1000Client(String[] args, Options options, CommandLineParser parser)
	{
		try{
			//Create a new CommandLine object from the parser that was passed to the constructor
			CommandLine line = parser.parse( options, args );		
			
		    // Print help message
		    if(line.hasOption("help")) {
		        print_help(options);
		        System.exit(0);
		    }
		    // Parse the options
		    //If the address flag is set but no argument was provided, assign the default value
		    if(!line.hasOption("addr") || (cr1000_address = line.getOptionValue("addr")) == null) {
		    	cr1000_address = DEFAULT_CR1000_ADDRESS;
		    	System.out.println("WARNING: Using default CR1000 IP address: " + DEFAULT_CR1000_ADDRESS);
		    }
		    //If the PakBus address was provided and the value is not null, parse the argument as a short
		    if(line.hasOption("pbaddr") && line.getOptionValue("pbaddr") != null){
		    	try{
		    		pb_address = Short.parseShort(line.getOptionValue("pbaddr"));		    
		    	}catch(NumberFormatException e){
		    		System.out.println("ERROR parsing 'pbaddr' argument - must be an 'short' value");
		    		System.exit(0);
		    	}		    	
		    }
		    //Otherwise assign the default PakBus address value
		    else{
		    	pb_address = DEFAULT_PB_ADDRESS;
		    	System.out.println("WARNING: Using default CR1000 PakBus Address: " + DEFAULT_PB_ADDRESS);
		    }

		    //If the 'days_of_data' flag was provided and the value is not null, parse the argument as an int
		    if(line.hasOption("days") && line.getOptionValue("days") != null){
		    	try{
		    		days_of_data = Integer.parseInt(line.getOptionValue("days"));		    
		    	}catch(NumberFormatException e){
		    		System.out.println("ERROR parsing 'days' argument - must be an integer value");
		    		System.exit(0);
		    	}		    	
		    }
		    //Otherwise set the default value
		    else{
		    	days_of_data = DEFAULT_DAYS_OF_DATA;
		    	System.out.println("WARNING: Using default days of data: " + DEFAULT_DAYS_OF_DATA);
		    }
		    //If the directory flag is set but no argument was provided, assign the default value
		    if(!line.hasOption("dir") || (root_directory = line.getOptionValue("dir")) == null) {
		    	root_directory = DEFAULT_DIRECTORY;
		    	System.out.println("WARNING: Using default root directory: " + DEFAULT_DIRECTORY);
		    }
		    
		    //If the port flag is set but no argument was provided, assign the default value
		    if(!line.hasOption("port") || (port = line.getOptionValue("port")) == null) {
		    	port = DEFAULT_PORT;
		    	System.out.println("WARNING: Using default port: " + DEFAULT_PORT);
		    }
		    
		    //If the debug flag was set, the debugging option is true
		    if(line.hasOption("debug")){
		    	debug = true;
		    }
		    
		    if(line.hasOption("load")){
		    	load = true;
		    }
		}		
		catch(ParseException e) {
		    System.err.println( "Argument error: " + e.getMessage() );
		    System.exit(0);
		}		
		
		init_cr1000_client();
	}
	
	/**
	 * Default constructor. - should never be called
	 * 
	 *
	 */
	public CR1000Client()
	{
		//Assign all default values for input arguments
		System.out.println("WARNING: Default constructor called - using all default values");
		cr1000_address = DEFAULT_CR1000_ADDRESS;
		pb_address = DEFAULT_PB_ADDRESS;
		root_directory = DEFAULT_DIRECTORY;
		days_of_data = DEFAULT_DAYS_OF_DATA;
		port = DEFAULT_PORT;
		init_cr1000_client();
		
	}
	/**
	 * Initializes the CR1000 client
	 * Establishes cache and archive size, resets the last retrieved record number,
	 * sets up first run booleans and connects the Source
	 */
	public void init_cr1000_client(){
		try{
			//Initialize log and write configuration information
			log = new SegaLogger(root_directory + "/logs/cr1000-log.txt");
		}catch(IOException e){
			System.exit(0);
		}
		writeToLog("Adding shutdown hook...");
		Runtime.getRuntime().addShutdownHook(new ShutdownHook());
		writeToLog("\tOK");
		
		//Print out intialization info
		writeToLog("[CR1000 IP Addr]: " + cr1000_address + "\r\n"
				+  "[PackBus Addr]: " + pb_address + "\r\n"
				+  "[RBNB Port]: " + port + "\r\n"
				+  "[Root Directory]: " + root_directory + "\r\n"
				+  "[Inital Fetch Length (days)]: " + days_of_data + "\r\n"
				+  "[Debug]: " + debug + "\r\n"
				+  "[Load]: " + load + "\r\n"
				+  "[Cache Size]: " + cache_size + "\r\n"
				+  "[Archive Size]: " + archive_size + "\r\n"				
				);

	}
	/**
	 * Print application help message.
	 *
	 * @param options object containing application command line options.
	 */
	public static void print_help(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("CR1000Client", options);
	}
	/**
	 * This is the main execution function for the CR1000 Client
	 * 
	 */
	public void execute()
	{
		// Initialize variables
		int index;
		long data_interval = 0;
		
		//Create source channel map for flushing CR1000 data
		ChannelMap src_map = new ChannelMap();
		//Create a synchronization source for providing frame flush data (number of channels flushed/expected by sink)
		ChannelMap sync_map = new ChannelMap();
		
		//Add a number of channels channel and a frame number channel to the sync source
		try {
			sync_map.Add("chan");
			sync_map.Add("frame");
		} catch (SAPIException e1) {
			writeToLog("Error adding sync channels to channel map 'sync_map'");
		}
		
		//Temp variable used to store the sample timestamp
		long sample_timestamp = 0;
		
		//TableInfo stores the last record number for each table
		TableInfo table_info;
		
		boolean reinit_cr1000_client = true;		
		
		double flush_time;
		int flush_count = 0;
		long frame_count = 0;
		long start_timestamp = 0;
		//Store channel names to provide number of channels flushed
		ArrayList<String> channel_names = new ArrayList<String>();
		
		// Load the table info (last known record numbers) from disk
		if(!load){
			load_table_info();
		}
		else{
			//Reinit table info if load flag is set
			table_info_list = new ArrayList<TableInfo>();
			save_table_info();
			//Prompt the user to confirm that they're cleaned the archive for the WiSARD packets
			System.out.println("You have enabled the 'load' flag.\r\n" +
					"This option will load all of the CR1000 data points\r\n" +
					"in each table that are newer than the timestamp specified.\r\n\r\n" +
					"Please input a timestamp (ms since 1970) to begin the search \r\nfor data points that are " +
					"newer than the specified timestamp.\r\n\r\n" +
					"Timestamp (ms since 1970): ");
			
			Scanner scan = new Scanner(System.in);
			String val = scan.next();
			try{
				start_timestamp = Long.parseLong(val);
			}catch(NumberFormatException e){
				System.out.println("Number format exception - please enter a valid timestamp (ms since 1970) of type 'long'");
				System.exit(-1);
			}
			
			System.out.println("Timestamp entered: " + start_timestamp + " (" + sdf.format(new Date(start_timestamp)) + ")");
			System.out.println("Is this the correct starting timestamp to load persistent storage?" +
					"\r\n\r\nContinue? [Y/n]: ");
			val = scan.next();
			
			if(val.equals("Y")){
				System.out.println("\r\n\r\nLoading data starting after " + sdf.format(new Date(start_timestamp)));
				scan.close();
			}
			else{
				System.out.println("User cancelled operation. Exiting...");
				scan.close();
				System.exit(-1);
			}
		}
		
			while (true) {	
				//If error occured or this is the first time through - initialize the client
				if(reinit_cr1000_client){
					//Connect/reconnect to RBNB
					reconnect_rbnb();
					// Update the tables
					update_tables();					
					reinit_cr1000_client = false;
				}
				
				try{
					//Store explicit timestamp for the entire frame (unless first run)
					flush_time = (double)System.currentTimeMillis()/1000.;
					
					// Table loop
					for (TableDef table : cr1000.tables) {
						
						// Update data_interval to the fastest sampling rate
						if (table.interval < data_interval || data_interval == 0)
							data_interval = table.interval;
						
						//Init a TableInfo object
						table_info = get_table_info(table.name);
						
						
						
						// Request records
						if(table_info == null && !load){							
							//Create a new TableInfo obj
							table_info = new TableInfo(table.name,0,true);
							
							// Initialize data_interval
							if (data_interval == 0)
								data_interval = table.interval;
							//Add the new table obj to the list
							table_info_list.add(table_info);
							
							writeToLog("\t*Executing intial fetch for " + table.name);
							//TODO: handle the case where these need to be broken into multiple requests
							cr1000.get_records(table,days_of_data);
							
						}
						else if(load){
							
								
								if(table_info == null){
									//Create a new TableInfo obj
									table_info = new TableInfo(table.name,0,true);
									
									// Initialize data_interval
									if (data_interval == 0)
										data_interval = table.interval;
									//Add the new table obj to the list
									table_info_list.add(table_info);
									
									writeToLog("\t*Executing intial fetch for " + table.name);
								}
								
								
								cr1000.get_records_by_timestamp(table,start_timestamp);
							
							
							
						}
						else{
							//Otherwise, fetch starting with the last record number that was stored 
							cr1000.get_records(table,table_info.getLast_record_no());
							
						}
						
						for (Record record : cr1000.records) {
							// Check for new records
							if (record.get_record_no() > table_info.getLast_record_no() && record.get_values_count() > 0) {								
								
								
								
								// Update the last_record_no
								table_info.setLast_record_no(record.get_record_no());
								sample_timestamp = (long)(record.get_time_stamp().get_secs_since_1990() + seconds_to_add)*1000 - tz.getRawOffset();
								
								float floatValue;
								for (ValueBase value : record.get_values()) {
									// Create the value pair							
									try{
										//Set floatValue and check for NaN or unknown format
										if(Float.isNaN(floatValue = value.to_float())){									
											floatValue = Float.NaN;			
										}										
									}catch(NumberFormatException e){
										floatValue = Float.NaN;
									}
	
									//Add table name to channel list
									if(!channel_names.contains(table.name + "/" + value.get_name())){
										channel_names.add(table.name + "/" + value.get_name());										
									}
									// Clear the ChannelMap
									src_map = new ChannelMap();											
									// Add channels
									index = src_map.Add(table.name + "/" + value.get_name());
									//If this is the first run, use the sample timestamp as the RBNB timestamp
									if(table_info.isFirst_run()){
										//writeToLog("\t*First run table: putting time as " + sample_timestamp/1000.0);
										//This is guaranteed to be in sequential order
										src_map.PutTime(sample_timestamp/1000.0, 0);
									}
									else{
										//Put explicit timestamp
										src_map.PutTime(flush_time, 0.);
									}
									
									// Put data
									src_map.PutDataAsByteArray(index,DataGeneratorCodec.encodeValuePair((byte)ChannelMap.TYPE_FLOAT32, sample_timestamp, floatValue));
									
									flush_count += src.Flush(src_map);
									
								}
								
							}
						}
						
						if(table_info.isFirst_run()){
							// Set the first_run flag to false, records were received
							table_info.setFirst_run(false);
							writeToLog("\t*Intial fetch completed for " + table.name);
						}
						
						
						//Save the  updated TableInfo array to disk
						save_table_info();						
						
						
					}
					
					//Reset the load variable
					load = false;
					
					if (flush_count != 0){

						frame_count++;
						
						if(debug){
							System.out.println(sdf.format(new Date()) + " - Flushed " + flush_count + " data points for " + channel_names.size() + " channels. Frame " + frame_count);
						}
						
						//Send sync channel info
						sync_map.PutTime(flush_time, 0.);
						//Add channel count
						sync_map.PutDataAsInt32(0, new int[]{channel_names.size()});
						//Add frame number
						sync_map.PutDataAsInt64(1, new long[]{frame_count});
						sync_src.Flush(sync_map);
						
					}
					flush_count = 0;					
					channel_names.clear();
					// Sleep for half the fastest sampling rate					
					Thread.sleep(data_interval/(2000000));		
				}catch(SAPIException e){
					writeToLog("SAPIException in main while-loop");	
					writeToLog("\tERROR: \t" + e.getLocalizedMessage());
					if(!validateReconnectAttempt(e)){
						writeToLog("Reconnect validation for CR1000 failed, operation will not be resumed.\r\n Exiting...");
						System.exit(0);
					}
					else{
						//Try again to connect
						reinit_cr1000_client = true;
					}
				}catch(InterruptedException e){
					writeToLog("InterruptedException in main while-loop");
					System.exit(1);
				}catch (Exception e) {				
					if (e.getClass().equals(SocketException.class)) {
						// Most likely a network issue
						writeToLog("Communication error. Was the datalogger reprogrammed?");
						//TODO: reset table info here?
						table_info_list = new ArrayList<TableInfo>();
						//Try again to connect
						reinit_cr1000_client = true;
					} else {
						writeToLog("\t*Unhandled exception occurred in main while-loop");
						StringWriter errors = new StringWriter();
			        	e.printStackTrace(new PrintWriter(errors));
			        	writeToLog(errors.toString());
			        	System.exit(-1);
					}
				}
			}
		

	}
	
	/**
	 * Safely shut down RBNB source
	 * 
	 * 
	 */
	private void disconnect_rbnb(){
		writeToLog("Disconnecting...");
		if(src != null && src.VerifyConnection()){
			writeToLog("\tOK - CR1000 Source");
			src.Detach();
		}
		if(sync_src != null && sync_src.VerifyConnection()){
			writeToLog("\tOK - CR1000 Sync Source");
			sync_src.CloseRBNBConnection();
		}
	}
	private void reconnect_rbnb(){
		//Block until sources are connected to localhost - otherwise we can't do anything
		try{
			while(!connect_rbnb()){
				if(rbnb_reconnect_attempts++ < max_reconnect_attempts){
					writeToLog("Reconnecting in " + reconnect_sleep_time + " seconds\tAttempt " + rbnb_reconnect_attempts + "/" + max_reconnect_attempts);
					Thread.sleep(reconnect_sleep_time * 1000);
				}
				else{
					writeToLog("Maximum number of reconnect attempts reached (" + rbnb_reconnect_attempts + ")\r\nExiting...");
					System.exit(0);
				}
			}
		} catch (InterruptedException e) {
			writeToLog("Interrupted while reconnecting...");
		}
	}
	
	/**
	 * Attempt connect source to localhost
	 * 
	 * @return true if connection is successful
	 */
	private boolean connect_rbnb(){
		//Connect sources/sinks
		try{
			//Make sure source is already detached
			disconnect_rbnb();
			
			writeToLog("Connecting CR1000 source...");			
			//Create or recreate the source
			src = new Source(cache_size, "append", archive_size);			
			src.OpenRBNBConnection("127.0.0.1:" + port, "cr1000");
			writeToLog("\tOK");
			
			writeToLog("Connecting CR1000 Sync source...");		
			//Create or recreate the syncsource
			sync_src = new Source(10, "none", 0);			
			sync_src.OpenRBNBConnection("127.0.0.1:" + port, "cr1000_sync");
			writeToLog("\tOK");
				
			rbnb_reconnect_attempts = 0;
			return true;
		}catch(SAPIException e){
			writeToLog("Error connecting source - make sure RBNB is running on the localhost");	
			writeToLog("\tERROR: \t" + e.getLocalizedMessage());
			if(!validateReconnectAttempt(e)){
				writeToLog("Reconnect validation for CR1000 client failed, operation will not be resumed.\r\n Exiting...");
				System.exit(0);
			}
		}
		return false;
	}
	
	
	
	/**
	 * Detach source, reopen RBNBConnection, and flush the channel map
	 */
	
	@SuppressWarnings("unused")
	@Deprecated
	private void reattachThenFlush(ChannelMap src_map){
		try{
			src.Detach();
			src.OpenRBNBConnection("127.0.0.1:" + port, "cr1000");
			src.Flush(src_map);
			if (debug){
				System.out.println(sdf.format(new Date(System.currentTimeMillis())) + " - Data flushed for " + src_map.NumberOfChannels() + " channels");
			}
		}catch(SAPIException e){
			StringWriter errors = new StringWriter();
        	e.printStackTrace(new PrintWriter(errors));
        	writeToLog(errors.toString());
			writeToLog("Problem with reattachThenFlush() - probably should start debugging this");
		}
	}



	
		
	/**
	 * Loads the TableInfo ArrayList object from file
	 * 
	 */
	private void load_table_info(){
		try{
			File file = new File(root_directory + "/cr1000_config/table_info_list.data");
			if(file.getParentFile().mkdirs()){
				writeToLog("Creating directories for CR1000 folder " + file.getParentFile().getAbsolutePath());
			}
			// Read from disk using FileInputStream
	    	FileInputStream fileInput = new FileInputStream(root_directory + "/cr1000_config/table_info_list.data");
	    	// Read object using ObjectInputStream
	    	ObjectInputStream objectInput = new ObjectInputStream(fileInput);
	
	    	// Read an object
	    	Object obj = objectInput.readObject();
	    	objectInput.close();
	    	fileInput.close();
	    	if (obj instanceof ArrayList<?>)
	    	{
	    		// Cast object to an ArrayList<TableInfo>
				@SuppressWarnings("unchecked")
				ArrayList<TableInfo> table_info_temp = (ArrayList<TableInfo>) obj;	    		
	    	
	    		if(!table_info_list.equals(table_info_temp) && table_info_temp.size() >= 1){
	    			table_info_list = table_info_temp;
		    		writeToLog("Successfully loaded info for the following tables:\r\n");
		    		for(TableInfo ti : table_info_list){
		    			writeToLog("\t" + ti.getChannel_name() + "\t" + ti.getLast_record_no());	    			
		    		}
	    		}
	    		else{
	    			writeToLog("No TableInfo loaded");
	    			
	    		}
	    	}
	    	else 
	    		writeToLog("*** table_info_list.data NOT an instance of ArrayList");
		}catch(FileNotFoundException e){
    		writeToLog("File table_info_list.data not found, file will be created");
    		save_table_info();
    	}catch(IOException e){
    		StringWriter errors = new StringWriter();
        	e.printStackTrace(new PrintWriter(errors));
    		writeToLog(errors.toString());
    	}catch(ClassNotFoundException e){
    		StringWriter errors = new StringWriter();
        	e.printStackTrace(new PrintWriter(errors));
    		writeToLog(errors.toString());
    	}
	}
	
	/**
	 * Write table_info_list to disk
	 */
	private void save_table_info(){
		try{
			File file = new File(root_directory + "/cr1000_config/table_info_list.data");
			if(file.getParentFile().mkdirs()){
				writeToLog("Creating directories for CR1000 folder " + file.getParentFile().getAbsolutePath());
			}
			// Write to disk with FileOutputStream
			FileOutputStream fileOutput = new FileOutputStream(root_directory + "/cr1000_config/table_info_list.data");
			
			// Write object with ObjectOutputStream
			ObjectOutputStream objectOutput = new ObjectOutputStream (fileOutput);
	
			// Write object out to disk
			objectOutput.writeObject(table_info_list);
			objectOutput.flush();
			objectOutput.close();
			fileOutput.close();
		}catch(IOException e){
			StringWriter errors = new StringWriter();
        	e.printStackTrace(new PrintWriter(errors));
    		writeToLog(errors.toString());
		}catch(Exception e){
			StringWriter errors = new StringWriter();
        	e.printStackTrace(new PrintWriter(errors));
    		writeToLog(errors.toString());
		}
	}
	
	/**
	 * Updates the tables from the CR1000.
	 */
	private void update_tables()
	{
		
		// Create CR1000 interface
		cr1000 = new CR1000Interface();
		writeToLog("Connecting to CR1000...");
		try {
			while(!cr1000.init_cr1000_interface(cr1000_address,pb_address,root_directory,debug)){				
				if(cr1000_reconnect_attempts++ < max_reconnect_attempts){
					writeToLog("Attempting to connect CR1000 in " + reconnect_sleep_time + " seconds\tAttempt " + cr1000_reconnect_attempts + "/" + max_reconnect_attempts);
					Thread.sleep(reconnect_sleep_time * 1000);
				}
				else{
					writeToLog("Maximum number of reconnect attempts reached (" + cr1000_reconnect_attempts + ")\r\nExiting...");
					System.exit(0);
				}
			}
		} catch (InterruptedException e) {
			writeToLog("Interrupted while connecting sources...");			
		}	
		writeToLog("\tOK");
		
		// Fetch the tables
		try {
			writeToLog("Fetching tables...");
			cr1000.get_tables();
			writeToLog("\tOK");
		} catch (Exception e) {
			// Most likely a network issue
			writeToLog("CR1000 is available but request to fetch tables threw exception");
		}

	}
	/**
	 * Search for TableInfo object using string name
	 * 
	 * @param s table name
	 * @return TableInfo object if table info exists, otherwise return null
	 */
	private TableInfo get_table_info(String s){
		for(TableInfo t : table_info_list){
			if(t.getChannel_name().equals(s)){
				return t;
			}
		}
		
		return null;
	}
	
	
	/**
	 * This method determines whether or not a nested SAPI exception requires a reconnect to recover
	 * 
	 * @param error
	 * @param attempts
	 * @return true if a reconnect attempt is needed
	 */
	public boolean validateReconnectAttempt(Throwable e){
		//Get the full error
		StringWriter errors = new StringWriter();
		e.printStackTrace(new PrintWriter(errors));
		String fullError = errors.toString();
		
		//Get the main nested exception
		String error = 	e.getLocalizedMessage();	

		if(error.contains("Nesting java.io.InterruptedIOException")){
    		//DO NOT try to reconnect
		}
		else if(error.contains("This operation requires a connection.")){
			//We've already disconnected
			//DO NOT try to reconnect
		}
		else if(error.contains("Nesting java.lang.InterruptedException")){
			//This can get thrown on a shutdown or restart during a fetch timeout
			//DO NOT try to reconnect
		}
		else if(error.contains("Nesting java.lang.IllegalStateException")){		
			//This can happen when source channels already exist and the application tries to reconnect
			//DO NOT try to reconnect if there is already an existing (and running) client of the same name
			if(!fullError.contains("Cannot reconnect to existing client handler")){				
				return true;
			}
		
		}
		else if(error.contains("Nesting java.net.SocketException")){
			//This can happen when RBNB goes down while operating		
			return true;				
		}
		else if(error.contains("Nesting java.net.ConnectException")){
			//This can happen when RBNB is down during initial connection
			return true;		
		}
		else if(error.contains("Nesting java.net.NoRouteToHostException")){
			//This can happen when server connection is unavailable - most likely powered off or in the process of rebooting
			return true;			
		}
		else if(error.contains("Nesting java.io.EOFException")){
			//This can happen when RBNB is down during initial connection
			return true;		
		}

    	return false;
    }
	
	/**
	 * Log Object to file. If the debug 
	 * variable set set to 'true', another synchronized
	 * method print_to_console is used to print the 
	 * obj to the console. 
	 * 
	 * @param obj - The object (typically a string) 
	 * to be written to the log file
	 * @see print_to_console(String s)
	 */
	public void writeToLog(Object obj){
		if(debug){
			print_to_console(obj.toString());
		}
		log.write(obj);
	}
	
	/**
	 * Synchronized method for printing a string to
	 * the console using System.out.println()
	 * 
	 * @param s - String object that is printed to the
	 * console/screen
	 */
	public void print_to_console(String s){
		System.out.println(s);		
	}
	
	/**
	 * ShutdownHook class used to gracefully exit RBNB 
	 * when the client is shutdown
	 *
	 */
	private class ShutdownHook extends Thread {		
		@Override
		public void run(){
			writeToLog("CR1000 client shutdown hook activated...");			
			disconnect_rbnb();
			save_table_info();
			
		}
	}
	
}
