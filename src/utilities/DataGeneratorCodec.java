package utilities;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.rbnb.sapi.ChannelMap;

public class DataGeneratorCodec {
	/**
	 * Helper method to wrap sample point and timestamp (includes data type)
	 * 
	 * @param type - byte indicating the type of data to be wrapped as defined by ChannelMap
	 * @param timestamp - long variable represents time in ms since 1970
	 * @param data - Object containing datapoint to be encoded
	 * @return
	 * @see ChannelMap
	 */
	public static byte[] encodeValuePair(byte type,long timestamp,Object data){
		byte[] time_stamp = ByteBuffer.allocate(8).putLong(timestamp).array();
		byte[] data_point = null;
		
		//Switch over the type of the data to be encoded
		//Create a byte array of the appropriate length and insert the data as bytes
		switch(type){
		case ChannelMap.TYPE_FLOAT32:
			data_point = ByteBuffer.allocate(4).putFloat((Float)data).array();
			break;
		case ChannelMap.TYPE_FLOAT64:
			data_point = ByteBuffer.allocate(8).putDouble((Double)data).array();
			break;
		case ChannelMap.TYPE_INT16:
			data_point = ByteBuffer.allocate(2).putShort((Short)data).array();
			break;
		case ChannelMap.TYPE_INT32:
			data_point = ByteBuffer.allocate(4).putInt((Integer)data).array();
			break;
		case ChannelMap.TYPE_INT64:
			data_point = ByteBuffer.allocate(8).putLong((Long)data).array();
			break;
		case ChannelMap.TYPE_INT8:
			data_point = ByteBuffer.allocate(1).put((Byte)data).array();
			break;
		case ChannelMap.TYPE_STRING:
			String str = (String)data;
			data_point = str.getBytes();
			break;
		default: 
			//TODO: log unknown data type
			break;

		}
		
		//If data was a known type
		if(data_point != null){
			//Create a blob object to store type/timestamp/data
			byte[] blob = new byte[1 + time_stamp.length + data_point.length];
			//Copy each byte array into 
			System.arraycopy(new byte[]{type},0,blob,0,1);
			System.arraycopy(time_stamp,0,blob,1,time_stamp.length);
			System.arraycopy(data_point,0,blob,1 + time_stamp.length,data_point.length);
			return blob;
		}
		else{
			return null;
		}
	}
	/**
	 * 
	 * @param rbnb_timestamp
	 * @param blob
	 * @return
	 */
	public static SampleTimestampPackage decodeValuePair(double rbnb_timestamp,byte[] blob){	
		int data_type = blob[0];
		long final_rbnb_timestamp = (long)(rbnb_timestamp*1000);
		
		long sample_timestamp = ByteBuffer.wrap(blob, 1, 8).getLong();		
		
		switch(data_type){
		case ChannelMap.TYPE_FLOAT32:
			return new SampleTimestampPackage(final_rbnb_timestamp,sample_timestamp,ByteBuffer.wrap(blob, 9, 4).getFloat());			
		case ChannelMap.TYPE_FLOAT64:
			return new SampleTimestampPackage(final_rbnb_timestamp,sample_timestamp,ByteBuffer.wrap(blob, 9, 8).getDouble());
		case ChannelMap.TYPE_INT16:
			return new SampleTimestampPackage(final_rbnb_timestamp,sample_timestamp,ByteBuffer.wrap(blob, 9, 2).getShort());
		case ChannelMap.TYPE_INT32:
			return new SampleTimestampPackage(final_rbnb_timestamp,sample_timestamp,ByteBuffer.wrap(blob, 9, 4).getInt());
		case ChannelMap.TYPE_INT64:
			return new SampleTimestampPackage(final_rbnb_timestamp,sample_timestamp,ByteBuffer.wrap(blob, 9, 8).getLong());
		case ChannelMap.TYPE_INT8:
			return new SampleTimestampPackage(final_rbnb_timestamp,sample_timestamp,ByteBuffer.wrap(blob, 9, 1).get());
		case ChannelMap.TYPE_STRING:
			return new SampleTimestampPackage(final_rbnb_timestamp,sample_timestamp,new String(Arrays.copyOfRange(blob, 10, blob.length)));
		default: 
			//log unkown data type
			return null;

		}
	
		
		
		
		
	}

}
