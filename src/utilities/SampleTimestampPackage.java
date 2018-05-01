package utilities;

public class SampleTimestampPackage implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3438563973136656479L;
	/** Used to store RBNB timestamp*/
	private long rbnb_timestamp = -1;
	
	/** Used to sample timestamp */
	private long sample_timestamp = -1;
	
	/** Used to sample data point */
	private Object sample_data = null;
		/**
		 * 
		 * @param rbnb_timestamp
		 * @param sample_timestamp
		 * @param sample_data
		 */
		public SampleTimestampPackage(long rbnb_timestamp, long sample_timestamp, Object sample_data) {
			this.setRbnb_timestamp(rbnb_timestamp);
			this.setSample_timestamp(sample_timestamp);
			this.setSample_data(sample_data);
		}
		
		public long getRbnb_timestamp() {
			return rbnb_timestamp;
		}
		public void setRbnb_timestamp(long rbnb_timestamp) {
			this.rbnb_timestamp = rbnb_timestamp;
		}
		public long getSample_timestamp() {
			return sample_timestamp;
		}
		public void setSample_timestamp(long sample_timestamp) {
			this.sample_timestamp = sample_timestamp;
		}
		public Object getSample_data() {
			return sample_data;
		}
		public void setSample_data(Object sample_data) {
			this.sample_data = sample_data;
		}

}
