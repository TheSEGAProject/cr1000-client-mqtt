package utilities;

public class TableInfo implements java.io.Serializable{

	/** Generated serial version ID */
	private static final long serialVersionUID = 2631657826044988131L;
	private String channel_name;
	private long last_record_no;
	private boolean first_run;
	
	public TableInfo(){
		
	}
	/**
	 * 
	 * @param channel_name
	 * @param last_record_no
	 * @param first_run
	 */
	public TableInfo(String channel_name, long last_record_no, boolean first_run){
		this.setChannel_name(channel_name);
		this.setLast_record_no(last_record_no);
		this.setFirst_run(first_run);
	}

	/**
	 * @return the channel_name
	 */
	public String getChannel_name() {
		return channel_name;
	}

	/**
	 * @param channel_name the channel_name to set
	 */
	public void setChannel_name(String channel_name) {
		this.channel_name = channel_name;
	}

	/**
	 * @return the last_record_no
	 */
	public long getLast_record_no() {
		return last_record_no;
	}

	/**
	 * @param last_record_no the last_record_no to set
	 */
	public void setLast_record_no(long last_record_no) {
		this.last_record_no = last_record_no;
	}

	/**
	 * @return the first_run
	 */
	public boolean isFirst_run() {
		return first_run;
	}

	/**
	 * @param first_run the first_run to set
	 */
	public void setFirst_run(boolean first_run) {
		this.first_run = first_run;
	}
	
	
	
}
