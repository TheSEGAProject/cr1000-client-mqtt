package utilities;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
/**
 * SegaLogger Class. Can be used as a plugin to easily create log files
 * 
 * Example use:
		\code
		SegaLogger tester = new SegaLogger("TestLogFile");
		tester.write("This will automatically be time stamped when written");
		tester.write("So will this");
		tester.close(); //this usually isn't necessary since most logs won't need closing
		\endcode
 * @author jdk85
 *
 */
public class SegaLogger implements java.io.Serializable{

	/** Required by java.io.Serializable */
	private static final long serialVersionUID = -7242211923105835326L;
	/** Stores FileWriter for creating log */
	private transient FileWriter logFile; 
	/** Stores BufferedWriter to actually write log entries with*/
    private transient BufferedWriter log; 
    /** Used to write the date format for the log*/
    private SimpleDateFormat dateFormatter = new SimpleDateFormat("MM/dd/yyyy - h:mm:ss a"); 
    /** Stores the name of the log file to be written to*/
    private String filename; 
    /** File object used to write to the log */
    private File logFileObj;
    /**
	 * SegaLogger Constructor - Takes in file name and initializes the log file
	 * @param String filename - Name of the log file (.txt)
     * @throws IOException 
	 * @returns none
	 */
    public SegaLogger(String filename) throws IOException{
    	createLogFile(filename);
    }
    
    /**
     * 
     * @param filename
     * @throws IOException 
     */
    public void createLogFile(String filename) throws IOException{
    	try{
    		if(!filename.contains(".txt"))logFileObj = new File(filename + ".txt");
    		else logFileObj = new File(filename);
    		//Store filename locally
			this.filename = filename;			
			if(logFileObj.getParentFile().mkdirs()){
				System.out.println("Creating log directory " + logFileObj.getParentFile().getAbsolutePath());
			}
			logFileObj.createNewFile();
			//Create FileWriter object used for log	
			
			logFile = new FileWriter(logFileObj,true);
			
			//Create BufferedWriter object used to writing to log file
			log = new BufferedWriter(logFile);
			
			//Write initial creation information to log
			log.write("============================================================================\r\n");
			log.write("Log Created: " + dateFormatter.format(new Date(System.currentTimeMillis())) + "\r\n");
			log.write("============================================================================\r\n\r\n");
			//Flush info to log to write without having to close file
			log.flush();
			
		}catch(IOException e){
			System.out.println("Error creating log file ");
			e.printStackTrace();
			throw new IOException(e);
		}catch(Exception e){
			System.out.println("Error creating log file " );
			e.printStackTrace();
		}
    }
    /**
	 * write(Object o)  - Takes in object to be written to log, will take the toString() of the object
	 * @param Object o - data that gets written to log 
	 * @returns none
	 */
    public void write(Object o){
    	try{
    		if(!logFileObj.exists()){
    			createLogFile(filename);
    		}
    		
    		log.write(dateFormatter.format(new Date(System.currentTimeMillis())) + ": ");
    		log.write(o.toString() + "\r\n");
    		log.flush();
    		
    	}catch(IOException e){
    		System.out.println("Error writing to " + filename);
    		e.printStackTrace();
    	}catch(Exception e){
    		System.out.println("Error writing to " + filename);
    		e.printStackTrace();
    	}
    	
    }
    /**
	 * close(void)  - Shuts down BufferedWriter and FileWriter
	 * @param none
	 * @returns none
	 */
    public void close(){
    	try {
			log.close();
		}catch (IOException e) {
			System.out.println("Error closing log file " + filename);
			e.printStackTrace();
		}catch (Exception e) {
			System.out.println("Error closing log file " + filename);
			e.printStackTrace();
		}
    }
}
