package utilities;


/**
 * A class to hold information about a message.
 * 
 * @author jes244
 *
 */
public class Message implements java.io.Serializable, Comparable<Message>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1409503657656669825L;
	// Variables
	public byte[] data;
	public long timestamp;
	public String table_name,value_name;
	/**
	 * Default constructor.
	 * 
	 * @param b The data of the Message.
	 * @param t The timestamp.
	 */
	public Message(byte[] b, long t, String table_name, String value_name)
	{
		// Initialization
		data = b;
		timestamp = t;
		this.table_name = table_name;
		this.value_name = value_name;
	}
	
	@Override
	public int compareTo(Message m){
		//This shouldn't actually be called
		//Get the timestamp of the message being compared
		long cts = m.timestamp;
		
		if(this.timestamp > cts){
			return 1;
		}
		else if (this.timestamp == cts){
			return 0;
		}
		else{
			return -1;
		}
	}
	
	@Override
	public boolean equals(Object m){
		
	    if (m == null) return false;
	    if (!(m instanceof Message))return false;
	    if (m == this) return true;
	    
	    Message msg = (Message)m;
	    if(this.timestamp == msg.timestamp && this.data.equals(msg.data)){
	    	return true;
	    }
	    else
	    	return false;

	}	
	
}
