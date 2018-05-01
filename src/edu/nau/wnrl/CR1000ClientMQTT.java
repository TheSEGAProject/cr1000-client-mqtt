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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import utilities.CR1000Interface;
import utilities.DataGeneratorCodec;
import utilities.Message;
import utilities.SegaLogger;
import utilities.SendMail;
import utilities.TableInfo;

import com.campbellsci.pakbus.Record;
import com.campbellsci.pakbus.TableDef;
import com.campbellsci.pakbus.ValueBase;
import com.rbnb.sapi.ChannelMap;

/* 
 * @author jes244
 * @author jdk85 - edited and updated
 *
 */
public class CR1000ClientMQTT implements MqttCallback
{
	/** Default IP address for the CR1000 */
	private static String	DEFAULT_CR1000_ADDRESS = "192.168.13.110";
	/** Default root directory for configuration, script, and log files - no trailing slash' */
	private static String	DEFAULT_DIRECTORY="/opt/RBNB";
	/** Default CR1000 PakBus Address */
	private static short 	DEFAULT_PB_ADDRESS = 6;
	/** Default days of data to fetch from the CR1000 on init */
	private static int		DEFAULT_DAYS_OF_DATA = 3;
	/** IP address for the CR1000 */
	private String cr1000_address = null;
	/** The OS-level root directory for configuration, script, and log files */
	private String root_directory = null;
	/** The CR1000 PakBus Address */
	private short pb_address = -1;
	/** When the client initially fetches data from the CR1000, days_of_data is used as a multiplier to specify how many days worth of data to fetch */
	private int days_of_data = -1;
	/** When true, status information is written to the console */
	private boolean debug = false;
	/** When true, user is prompted to enter timestamp and tables will be fetched from then */
	private boolean load = false;

	/** Date format that represents date as 'M/d/Y - HH:mm:ss' */
	private static final SimpleDateFormat sdf = new SimpleDateFormat("M/d/y - HH:mm:ss");
	
	private ArrayList<TableInfo> table_info_list = new ArrayList<TableInfo>();
	/** Final reconnect logic value - try to reconnect every 30 seconds for 3 days*/
	private final int max_reconnect_attempts = 8640, reconnect_sleep_time = 30;
	/** Total number of reconnect attempts by CR1000 */
	private int cr1000_reconnect_attempts = 0;
	/** CR100 interface defines functions used by the cr1000 */
	private CR1000Interface cr1000;


	/** Handles connection to mqtt. Flushes packets from the packet queue*/
	private Thread mqtt_thread;
	/** Handles ethernet communication with the CR1000*/
	private Thread cr1000_thread;

	/**MessageQueue for incoming packets from the CR1000*/
	private volatile Queue<Message> pkt_queue;


	//TODO: describe these variables
	private String common_name;


	//MQTT variables	
	private String pubTopic = common_name + "/data/cr1000";
	private static int qos = 2;
	private String broker = "tcp://localhost:1883";
	private String pubID = common_name + "/data_publisher/cr1000";
	private MqttClient pubClient;
	private AtomicBoolean reconnecting_mqtt = new AtomicBoolean(false);
	private AtomicBoolean reconnect_mqtt = new AtomicBoolean(false);

	private Thread.UncaughtExceptionHandler thread_exception_handler;


	/** SegaLog used to write log to disk */
	private SegaLogger log;
	/** SendMail client used to send email/text alerts */
	private SendMail sendmail;

	/** The number of seconds to add to convert CR1000 timestamps into ms since 1970*/
	private static final int seconds_to_add = 631152000;
	/** The timezone used to calculated the timezone offset from UTC (should probably be an argument option) */
	private static final TimeZone tz = TimeZone.getTimeZone("US/Arizona");//TODO: use utc probably
	
	
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
		options.addOption("a", "days", true, "number of days of days to collect on intial request to the cr1000");
		options.addOption("i", "dir", true, "root directory for scripts, logs, config files, etc");
		options.addOption("d", "debug", false, "if set, debug mode is enabled");
		options.addOption("n", "name", true, "the common name of the garden");
		options.addOption("l", "load",false,"if set, script will prompt to load from timestamp (ms since 1970)");

		(new CR1000ClientMQTT(args,options,parser)).execute();
	}

	/**
	 * Preferred constructor with command line parsing for program arguments
	 * @param args
	 * @param options
	 * @param parser
	 */
	public CR1000ClientMQTT(String[] args, Options options, CommandLineParser parser)
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

			//Parse the options	    
			if(!line.hasOption("name") || (common_name = line.getOptionValue("name")) == null) {

				System.out.println("WARNING: You must include the common name (hostname) of the garden server"
						+ "\r\n\t(e.g., 'arboretum', 'blackpoint', etc.");
				System.exit(0);
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
	public CR1000ClientMQTT()
	{
		//Assign all default values for input arguments
		System.out.println("WARNING: Default constructor called - using all default values");
		cr1000_address = DEFAULT_CR1000_ADDRESS;
		pb_address = DEFAULT_PB_ADDRESS;
		root_directory = DEFAULT_DIRECTORY;
		days_of_data = DEFAULT_DAYS_OF_DATA;
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
			sendmail = new SendMail(root_directory + "/mail/cr1000",common_name);
		}catch(IOException e){
			System.exit(0);
		}
		


		pubTopic = common_name + "/data/cr1000";
		qos = 2;
		broker = "tcp://localhost:1883";
		pubID = common_name + "/data_publisher/cr1000";

		//Print out intialization info
		writeToLog("[CR1000 IP Addr]: " + cr1000_address + "\r\n"
				+  "[PackBus Addr]: " + pb_address + "\r\n"
				+  "[Root Directory]: " + root_directory + "\r\n"
				+  "[Inital Fetch Length (days)]: " + days_of_data + "\r\n"
				+  "[Debug]: " + debug + "\r\n"
				+  "[Load]: " + load + "\r\n"
				+  "[Publisher Topic]: " + pubTopic + "\r\n"
				+  "[Quality of Service (QoS)]: " + qos + "\r\n"
				+  "[MQTT Broker]: " + broker + "\r\n"
				+  "[Publisher ID]: " + pubID + "\r\n"
				);

		
		
		writeToLog("Adding shutdown hook...");
		Runtime.getRuntime().addShutdownHook(new ShutdownHook());
		writeToLog("\tOK - Shutdown hook added");
		
		
		writeToLog("Creating uncaught exception handler...");
		thread_exception_handler = new Thread.UncaughtExceptionHandler() {				
			@Override
			public void uncaughtException(Thread t, Throwable e) {
//				String msg = e.getLocalizedMessage();
//				writeToLog("Caught: " + msg + "\r\n                          from thread: " + t.getName());
				writeToLog("\tERROR: UNCAUGHT EXCEPTION:",e);
				//Send this and exit - once we figure out what type of errors to expect, start handling them here
				sendmail.sendAlert(common_name + "(" + t.getName() + ")",SendMail.AlertType.CR1000CLIENT, e);
				writeToLog("===== " + SendMail.AlertType.CR1000CLIENT + " ALERT SENT =====");
				System.exit(-1); 

			}
		};
		writeToLog("\tOK - Uncaught exception handler created");	
		
		
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
		// Populate the packet queue from file
		if((pkt_queue = load_packet_queue(false)) == null){
			writeToLog("*Packet queue is null - exiting");
			System.exit(-1);
		}


		//Init the rbnb thread here for normal operation
		init_mqtt_thread();	


		//Init the cr1000 thread
		init_cr1000_thread();
	}


	/**
	 * Initializes the mqtt thread
	 * Checks to see if the thread is already running first.
	 * Adds the exception handler for reconnecting to RBNB
	 * Sets name to "mqtt_thread" and calls the start
	 * function on the mqtt_thread variable
	 * @param none
	 * @return none
	 */
	private void init_mqtt_thread(){
		interrupt_thread(mqtt_thread);		
		mqtt_thread = new Thread(new Runnable(){
			@Override
			public void run(){
				mqtt_client();
			}
		});
		mqtt_thread.setUncaughtExceptionHandler(thread_exception_handler);
		mqtt_thread.setName("mqtt_thread");
		mqtt_thread.start();		
	}

	/**
	 * Initializes the CR1000 thread
	 * Checks to see if the thread is already running first.
	 * Sets name to "cr1000_thread" and calls the start
	 * function on the cr1000_thread variable
	 */
	private void init_cr1000_thread(){
		interrupt_thread(cr1000_thread);
		cr1000_thread = new Thread(new Runnable(){
			@Override
			public void run(){
				cr1000_client();
			}
		});
		cr1000_thread.setUncaughtExceptionHandler(thread_exception_handler);
		cr1000_thread.setName("cr1000_thread");
		cr1000_thread.start();
	}



	public void mqtt_client(){
		try{
			connect_mqtt();
		}catch(MqttException e){
			writeToLog("MqttException on init, make sure that the local mosquitto broker is running. Exiting...",e);
			System.exit(-1);
		}

		try{
			MqttMessage message;

			Message msg;
			//Watchdog variable to prevent 
			long watchdog;
			
			int flush_count = 0;
			// Loop				
			while (!mqtt_thread.isInterrupted()) {
				
				//Check if we need to reconnect
				if(reconnect_mqtt.get()){
					reconnect_mqtt();
				}
				
				synchronized(pkt_queue){
					watchdog = System.currentTimeMillis();
					while (!pkt_queue.isEmpty()) {
						msg = pkt_queue.peek();
						if(msg != null){
							try{
								message = new MqttMessage(msg.data);
								message.setQos(qos);
								pubClient.publish(pubTopic + "/" + msg.table_name + "/" + msg.value_name,message);

								pkt_queue.remove();
								flush_count += 1;
								//if(debug)print_to_console(" * Data flushed - pkt_queue size: " + pkt_queue.size());							
							}catch(MqttException e){
								if(!reconnecting_mqtt.get()){
									writeToLog("\tMQTT client caught MqttException",e);	
									reconnect_mqtt.set(true);
									break;
								}
							}

							
						}
						//If we've spun for over 10 minutes - log and break;
						if((System.currentTimeMillis() - watchdog) > 600000){
							writeToLog(" * ERROR: Packet queue flush watchdog expired with " + pkt_queue.size() + " packets");
							if(sendmail.sendText(common_name,SendMail.AlertType.CR1000CLIENT,"ERROR: Packet queue flush watchdog expired with " + pkt_queue.size() + " packets")){
								writeToLog("===== " + SendMail.AlertType.CR1000CLIENT + " TEXT SENT =====");
							}
							else{
								writeToLog("===== " + SendMail.AlertType.CR1000CLIENT + " TEXT FAILED TO SEND =====");
							}
							System.exit(-1); 
						}
					}

				}
				
				if (flush_count != 0){
					if(debug){
						System.out.println(sdf.format(new Date()) + " - Flushed " + flush_count + " data points");
					}	
				}
				
				flush_count = 0;	

				Thread.sleep(1000); //keep from busy looping

			}
		}catch(InterruptedException e){			
			writeToLog("\tmqtt_thread interrupted during sleep");
		}catch(Exception e){
			throw new RuntimeException(e);
		}

	}

	public void cr1000_client(){
		long data_interval = 0;


		//Temp variable used to store the sample timestamp
		long sample_timestamp = 0;

		//TableInfo stores the last record number for each table
		TableInfo table_info;

		boolean reinit_cr1000_client = true;		

		long start_timestamp = 0;


		load_table_info();


		while (!cr1000_thread.isInterrupted()) {	
			//If error occured or this is the first time through - initialize the client
			if(reinit_cr1000_client){
				// Update the tables
				update_tables();					
				reinit_cr1000_client = false;
			}
			
			try{

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

								synchronized(pkt_queue){
									pkt_queue.add(new Message(DataGeneratorCodec.encodeValuePair((byte)ChannelMap.TYPE_FLOAT32, sample_timestamp, floatValue),System.currentTimeMillis(),table.name,value.get_name()));
								}
								
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


				// Sleep for half the fastest sampling rate					
				Thread.sleep(data_interval/(2000000));		
			}catch(InterruptedException e){
				writeToLog("\tcr1000_thread interrupted");
				break;
			}catch(Exception e) {				
				if (e.getClass().equals(SocketException.class)) {
					// Most likely a network issue
					writeToLog("Communication error. Was the datalogger reprogrammed?",e);
					table_info_list = new ArrayList<TableInfo>();
					//Try again to connect
					reinit_cr1000_client = true;
				} else {
					writeToLog("\t*Unhandled exception occurred in main while-loop",e);
					throw new RuntimeException(e);
				}
			}
		}


	}



	private void disconnect_mqtt() throws MqttException{
		if(pubClient != null && pubClient.isConnected()){
			pubClient.disconnect();
			writeToLog("\tOK - Disconnected publisher " + pubID);						
		}

	}

	private boolean validate_mqtt(){
		if(pubClient != null && pubClient.isConnected()){
			return true;
		}
		return false;
	}

	private void connect_mqtt() throws MqttException{
		pubTopic = common_name + "/data/cr1000";
		qos = 2;
		broker = "tcp://localhost:1883";
		pubID = common_name + "/data_publisher/cr1000";

		//Make the persistence directory if it doesn't exist
		File file = new File(root_directory + "/cr1000_config/mqtt_persistence/test_file");
		if(file.getParentFile().mkdirs()){
			writeToLog("Creating mqtt persistence directory " + file.getParentFile().getAbsolutePath());
		}


		MqttDefaultFilePersistence pubPersistence = new MqttDefaultFilePersistence(root_directory + "/cr1000_config/mqtt_persistence");

		pubClient = new MqttClient(broker, pubID, pubPersistence);


		MqttConnectOptions connOpts = new MqttConnectOptions();
		connOpts.setCleanSession(false); //set to false to maintain session in client and broker

		writeToLog("Connecting publisher to broker " + broker + " as client " + pubID + "...");
		pubClient.connect(connOpts); 
		writeToLog("\tOK - " + pubID + " connected to " + broker);
	}

	private void reconnect_mqtt(){
		reconnecting_mqtt.set(true);
		boolean first_attempt = true;
		try {
			disconnect_mqtt();//force disconnect in case one client hasn't cleanly disconnected
		} catch (MqttException e) {
			//do nothing here, it shouldn't work anyway if we ended up here
		}
		
		//try to reconnect
		do{
			try{
				Thread.sleep(30000);
				connect_mqtt();
			}catch(MqttException e){
				//if the broker isn't up, we won't be able to connect, send an alert if it is the first time this is happening
				if(first_attempt){
					sendmail.sendAlert(common_name,SendMail.AlertType.CR1000CLIENT, e);
					writeToLog("===== " + SendMail.AlertType.CR1000CLIENT + " ALERT SENT =====");
				}				
				first_attempt = false;			
			
			} catch (InterruptedException e) {
				writeToLog("\tMqtt reconnect attempt interrupted");
				break;
			}
			
		}while(!validate_mqtt());
		
		reconnect_mqtt.set(false);
		reconnecting_mqtt.set(false);
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
		}catch(Exception e){
			writeToLog("\tERROR: Unable to write table info to disk",e);
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
			writeToLog("Interrupted while connecting to the CR1000...");
			return;
		}
		writeToLog("\tOK - Connected to CR1000");

		// Fetch the tables
		try {
			writeToLog("Fetching tables...");
			cr1000.get_tables();
			writeToLog("\tOK - Fetched tables");
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


	public String getRoot_directory() {
		return root_directory;
	}
	public void setRoot_directory(String root_directory) {
		this.root_directory = root_directory;
	}


	/**
	 * Loads the packet queue from disk
	 * @param load_empty - set true to 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Queue<Message> load_packet_queue(boolean load_empty){
		Queue<Message> temp_queue;
		try{
			writeToLog("Loading CR1000 packet queue from disk");	
			File file = new File(getRoot_directory() + "/cr1000_config/packet-queue");
			if(file.getParentFile().mkdirs()){
				writeToLog("Creating cr1000 configuration directory " + file.getParentFile().getAbsolutePath());
			}
			if(!file.exists()){
				pkt_queue = new LinkedList<Message>();
				save_packet_queue();
			}else{
				File target = new File(getRoot_directory() + "/cr1000_config/packet-queue_backup");
				Files.copy(file.toPath(),target.toPath(),StandardCopyOption.REPLACE_EXISTING);
			}

			if(!load_empty){
				// Read from disk using FileInputStream
				FileInputStream fileInput = new FileInputStream(file);	

				// Read object using ObjectInputStream
				ObjectInputStream objectInput = new ObjectInputStream(fileInput);

				// Read an object
				Object obj = objectInput.readObject();
				objectInput.close();
				fileInput.close();
				if (obj instanceof LinkedList<?>)
				{
					temp_queue = (LinkedList<Message>)obj;	    		

					if(!temp_queue.isEmpty()){
						writeToLog("\tOK - Successfully loaded " + temp_queue.size() + " queued CR1000 packets from disk");	    			
					}
					else{
						writeToLog("\tOK - Loaded empty CR1000 packet queue from disk"); 

					}	    		
					return temp_queue;
				}
				else{
					writeToLog("\tERROR: CR1000 packet queue not of type 'LinkedList<?>'");
					writeToLog("\tERROR: CR1000 packet queue type: " + obj.getClass().getName() + " " + obj.getClass().toString());
				}
			}
			else{
				//Load a fresh (empty) packet queue
				pkt_queue = new LinkedList<Message>();
				save_packet_queue();
			}
		}catch(IOException e){
			writeToLog("\tERROR Unable to load CR1000 packet queue from disk (IOException)",e);
		} catch (ClassNotFoundException e) {
			writeToLog("\tERROR Unable to load CR1000 packet queue from disk (ClassNotFoundException)",e);
		}

		return null;
	}

	/**
	 * 
	 */
	public void save_packet_queue(){
		try {
			// Write to disk with FileOutputStream
			FileOutputStream fileOutput = new FileOutputStream(getRoot_directory() + "/cr1000_config/packet-queue");	
			// Write object with ObjectOutputStream
			ObjectOutputStream objectOutput = new ObjectOutputStream(fileOutput);	
			// Write object out to disk
			objectOutput.writeObject(pkt_queue);
			objectOutput.flush();
			objectOutput.close();
			fileOutput.close();
			writeToLog("\tOK - CR1000 packet queue written to file");
		} catch (IOException e) {			
			writeToLog("\tERROR: Unable to write CR1000 packet queue to disk");
			e.printStackTrace();
		}

	}



	/**
	 * 
	 * @param thread
	 */
	private void interrupt_thread(Thread thread){
		//If the thread is already running, interrupt it
		if(thread != null && (thread.isAlive() || !thread.isInterrupted()) ){
			writeToLog("Interrupting " + thread.getName() + "...");
			thread.interrupt(); 
			try {
				//Give up to 1 minutes for the thread to end after being interrupted
				thread.join(60*1000);
			} catch (InterruptedException e) {
				writeToLog("\tInterruptedException while joining " + thread.getName());
			}
			writeToLog("\tOK - Interrupted " + thread.getName());
			thread = null;


		}
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
	 * 
	 * @param obj
	 * @param e
	 */
	public void writeToLog(Object obj,Throwable e){		
		StringWriter errors = new StringWriter();
		e.printStackTrace(new PrintWriter(errors));
		if(debug){
			print_to_console(obj.toString());
			print_to_console(errors.toString());
		}
		log.write(obj);
		log.write("\r\n" + errors.toString());
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
			interrupt_thread(cr1000_thread);				
			interrupt_thread(mqtt_thread);	
			
			
			writeToLog("Saving packet queue with " + pkt_queue.size() + " packets...");
			save_packet_queue();

			writeToLog("\tOK - Table info written to file");
			
			
			try {
				disconnect_mqtt();
			} catch (MqttException e) {
				writeToLog("MqttException thrown while disconnecting in shutdown hook",e);
			}
			writeToLog("Writing table info to file...");
			save_table_info();
			writeToLog("\tOK - Table info written to file");

		}
	}

	@Override
	public void connectionLost(Throwable arg0) {
		writeToLog("Connection to MQTT lost",arg0);
		reconnect_mqtt.set(true);

	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
		// TODO Auto-generated method stub

	}

}
