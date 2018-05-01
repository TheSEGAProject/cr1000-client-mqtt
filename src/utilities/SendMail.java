package utilities;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;


//EXAMPLE USE:
/*
	public static void main(String[] args) {
		//Create the SendMail object
		SendMail sendmail = new SendMail("/media/mmc1/opt/RBNB","egr249a-02");		
		//...
		//Generate a dummy exception
		try{
			String value = null;
			value.charAt(0);
		}catch(Exception e){
        	if(!sendmail.sendAlert(AlertType.UNKNOWN,e)){
        		System.out.println("Error sending alert");
        	}        	
		}
	}
*/

/**
 * Simple mail interface for sending alerts
 * 
 * @author jdk85
 *
 */
public class SendMail {
	/** Date format that represents date as 'M/d/Y - HH:mm:ss:SSS' */
	private static final SimpleDateFormat sdf = new SimpleDateFormat("M/d/Y - HH:mm:ss:SSS");
	/** Header string used in generating email messages */
	private static final String header = " ==================== ";
	/** Session object used to send email */
	private Session session;
	/** Address used in the "FROM:" field */
	private String fromAddress;
	/** List of recipients for the email*/
	private String[] recipients;
	/** List of recipients for text messages*/
	private String[] text_recipients;
	/** The common server name from which the alert is generated */
	private String serverName;
	/** The path to the configuration file */
	private String config_path;
	/** The log object for the mail client */
	private SegaLogger log;
	/** Enum specifying type of alert */
	public enum AlertType {
		RBNB, WISARDCLIENT, CR1000CLIENT, RDF, UNKNOWN, RECONNECT
	}
	
	/**
	 * Constructor accepts an address that appears in the 'FROM:' field
	 * @param fromAddress
	 * @throws IOException 
	 */
	public SendMail(String mail_path, String common_name) throws IOException{
		//Set the configuration file path
		config_path = mail_path + "/config/mail.conf";
		//Create default properties list
		Properties props = new Properties();
		//Init session with default params and null authenticator 
		session = Session.getDefaultInstance(props, null);

		log = new SegaLogger(mail_path + "/logs/mail_" + common_name + "_Log.txt");
		readParametersFromFile(common_name);
	}
	
	/**
	 * Example root_directory/mail/common_name_mail.conf file:
	 * 
	 * from_address=noreply@romer.cefns.nau.edu
	 * recipients=jdk85@nau.edu,cp397@nau.edu
	 * server_name=ROMER
	 *
	 * @param root_directory
	 * @throws IOException 
	 */
	public void readParametersFromFile(String common_name) throws IOException{
		File file = new File(config_path);
		
		if(file.getParentFile().mkdirs()){
			System.out.println("Creating directory " + file.getParentFile().getAbsolutePath());
		}
		if(!file.exists()){
			BufferedWriter writer = new BufferedWriter(new FileWriter(config_path));
			System.out.println("\r\nWARNING: There is no recipient for send mail client!");
			System.out.println("To add a recipient, exit the RDF by pressing ctrl+c and ");
			System.out.println("edit " + file.getAbsolutePath() + " to contain the appropriate recipient\r\n");
			writer.append("from_address=noreply@romer.cefns.nau.edu");
			writer.newLine();
			writer.append("recipients=");
			writer.newLine();
			writer.append("text_recipients=");
			writer.newLine();
			writer.append("server_name=Romer");
			writer.newLine();
			writer.flush();
			writer.close();

		}
		BufferedReader reader = new BufferedReader(new FileReader(config_path));
		String currentLine;
		String[] tokens;
		while((currentLine = reader.readLine()) != null){
			tokens = currentLine.split("=");
			if(tokens[0].equalsIgnoreCase("from_address")){
				if(tokens.length == 2){
					fromAddress = tokens[1];
				}
			}
			else if(tokens[0].equalsIgnoreCase("recipients")){
				if(tokens.length == 2){
					recipients = tokens[1].split(",");
				}
			}
			else if(tokens[0].equalsIgnoreCase("text_recipients")){
				if(tokens.length == 2){
					text_recipients = tokens[1].split(",");
				}
			}
			else if(tokens[0].equalsIgnoreCase("server_name")){
				if(tokens.length == 2){
					serverName = tokens[1];
				}
			}
			else{
				log.write("Unrecognized token while reading parameter list");
			}
			
		}
	}
	
	
	
	
	
	
	/**
	 * This method accepts a list of recipients, a message subject, and message body
	 * and sends an email assuming Postfix is running on the host machine
	 * 
	 * @param recipients
	 * @param subject
	 * @param messageBody
	 * @throws AddressException
	 * @throws MessagingException
	 */
	public boolean sendMessage(AlertMessage alert){
		
		if(alert == null){
			return false;
		}
		
		try{
			//Create new MimeMessage using the previously created session
			Message msg = new MimeMessage(session);
			//Set the 'FROM:' field
			msg.setFrom(new InternetAddress(alert.getFromAddress()));
			//For each recipient in recipients, add the address to the 'TO:' field
			for(String s : alert.getRecipients()){
				msg.addRecipient(Message.RecipientType.TO,
						new InternetAddress(s));
			}
			//Set the subject of the message
			msg.setSubject(alert.getSubject());
			//Set the message body content
			msg.setText(alert.getMessage());
			//Send the email
			Transport.send(msg);
			return true;
		}catch(Exception e){
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			log.write(errors.toString());
			return false;
		}

	}
	
	
	/**
	 * Generates an AlertMessage object
	 * @param type
	 * @param recipients
	 * @param serverName
	 * @param errorMsg
	 * @return
	 */
	public AlertMessage generateMessage(String fromAddress, AlertType type, String[] recipients, String serverName, String clientName, String errorMsg){
		AlertMessage msg = new AlertMessage();
		msg.setFromAddress(fromAddress);
		msg.setRecipients(recipients);
		
		String messageBody = "An alert of type " + type.toString() + " has been generated by " + serverName
				+ "\r\n" + "If you believe you have received this message in error, please visit the " 
				+ "SEGA contact page <http://sega.nau.edu/contact> and include the contents of this message."
				+ "\r\n\r\n" + header + sdf.format(new Date()) + header + "\r\n";
		messageBody = messageBody.concat("\r\n" + errorMsg);		
		messageBody = messageBody.concat("\r\n\r\n\r\n" 
				+ "This is an automatically generated email – please do not reply to this email. For further information or assistance,"
				+ " please visit the SEGA contact page <http://sega.nau.edu/contact>");
		msg.setMessage(messageBody);
		
		switch(type){
		case RBNB:
			msg.setSubject("RBNB ALERT - " + clientName + " (" + serverName + ")");			
			break;
		case WISARDCLIENT:
			msg.setSubject("WiSARD Cleint ALERT - " + clientName + " (" + serverName + ")");	
			break;
		case CR1000CLIENT:
			msg.setSubject("CR1000 Client ALERT - " + clientName + " (" + serverName + ")");	
			break;
		case RDF:
			msg.setSubject("RDF ALERT - " + clientName + " (" + serverName + ")");	
			break;
		case UNKNOWN:
			msg.setSubject("UNKNOWN ALERT - " + clientName + " (" + serverName + ")");	
			break;
		case RECONNECT:
			msg.setSubject("RECONNECT SUCCESSFUL - " + clientName + " (" + serverName + ")");
			break;
		default:
			log.write("Unrecognized alert type while generating message");
			break;
		}
		
		return msg;
	}
	
	/**
	 * Generates an AlertMessage object
	 * @param type
	 * @param recipients
	 * @param serverName
	 * @param errorMsg
	 * @return
	 */
	public AlertMessage generateMessage(String clientName, AlertType type, Throwable e){
		StringWriter errors = new StringWriter();
		e.printStackTrace(new PrintWriter(errors));
		String errorMsg = errors.toString();
		
		return generateMessage(clientName, type, errorMsg);
	}
	/**
	 * Generates an AlertMessage object
	 * @param type
	 * @param recipients
	 * @param serverName
	 * @param errorMsg
	 * @return
	 */
	public AlertMessage generateMessage(String clientName, AlertType type, String errorMsg){
		
		if(fromAddress == null | recipients == null | serverName == null){
			//Need to call readParametersFromFile();
			return null;
		}
		else{
			AlertMessage msg = new AlertMessage();
			msg.setFromAddress(fromAddress);
			msg.setRecipients(recipients);
			
			String messageBody = "An alert of type " + type.toString() + " has been generated by " + serverName
					+ "\r\n" + "If you believe you have received this message in error, please visit the " 
					+ "SEGA contact page <http://sega.nau.edu/contact> and include the contents of this message."
					+ "\r\n\r\n" + header + sdf.format(new Date()) + header + "\r\n";
			messageBody = messageBody.concat("\r\n" + errorMsg);
			messageBody = messageBody.concat("\r\n\r\n\r\n" 
					+ "This is an automatically generated email – please do not reply to this email. For further information or assistance,"
					+ " please visit the SEGA contact page <http://sega.nau.edu/contact>");
			msg.setMessage(messageBody);
			
			switch(type){
			case RBNB:
				msg.setSubject("RBNB ALERT - " + clientName + " (" + serverName + ")");	
				break;
			case WISARDCLIENT:
				msg.setSubject("WiSARD Cleint ALERT - " + clientName + " (" + serverName + ")");	
				break;
			case CR1000CLIENT:
				msg.setSubject("CR1000 Client ALERT - " + clientName + " (" + serverName + ")");	
				break;
			case RDF:
				msg.setSubject("RDF ALERT - " + clientName + " (" + serverName + ")");	
				break;
			case UNKNOWN:
				msg.setSubject("UNKNOWN ALERT - " + clientName + " (" + serverName + ")");	
				break;
			case RECONNECT:
				msg.setSubject("RECONNECT SUCCESSFUL - " + clientName + " (" + serverName + ")");
				break;
			default:
				log.write("Unrecognized alert type while generating message");
				break;
			}
			
			return msg;
		}
	}
	
	/**
	 * 
	 * @param fromAddress
	 * @param type
	 * @param recipients
	 * @param serverName
	 * @param errorMsg
	 * @return
	 */
	public boolean sendAlert(String fromAddress, AlertType type, String[] recipients, String serverName, String clientName, String errorMsg){
		return sendMessage(generateMessage(fromAddress, type, recipients, serverName, clientName, errorMsg));
	}
	
	/**
	 * 
	 * @param type
	 * @param errorMsg
	 * @return
	 */
	public boolean sendAlert(String clientName, AlertType type,Throwable e){
		return sendMessage(generateMessage(clientName, type, e));
	}
	
	/**
	 * 
	 * @param type
	 * @param errorMsg
	 * @return
	 */
	public boolean sendAlert(String clientName, AlertType type,String errorMsg){
		return sendMessage(generateMessage(clientName, type, errorMsg));
	}
	
	/**
	 * 
	 * @param clientName
	 * @param type
	 * @param message
	 * @return
	 */
	public boolean sendText(String clientName, AlertType type, String message){
		return sendMessage(generateText(clientName,type,message));
	}
	
	/**
	 * 
	 * @param clientName
	 * @param type
	 * @param errorMsg
	 * @return
	 */
	public AlertMessage generateText(String clientName, AlertType type, String errorMsg){
		
		if(fromAddress == null | text_recipients == null | serverName == null){
			//Need to call readParametersFromFile();
			return null;
		}
		else{
			AlertMessage msg = new AlertMessage();
			msg.setFromAddress(fromAddress);
			msg.setRecipients(text_recipients);
			
			String messageBody = "An alert of type " + type.toString() + " has been generated by " + serverName;					
			messageBody = messageBody.concat("\r\n" + errorMsg);
			msg.setMessage(messageBody);
			
			switch(type){
			case RBNB:
				msg.setSubject("RBNB ALERT - " + clientName + " (" + serverName + ")");	
				break;
			case WISARDCLIENT:
				msg.setSubject("WiSARD Cleint ALERT - " + clientName + " (" + serverName + ")");	
				break;
			case CR1000CLIENT:
				msg.setSubject("CR1000 Client ALERT - " + clientName + " (" + serverName + ")");	
				break;
			case RDF:
				msg.setSubject("RDF ALERT - " + clientName + " (" + serverName + ")");	
				break;
			case UNKNOWN:
				msg.setSubject("UNKNOWN ALERT - " + clientName + " (" + serverName + ")");	
				break;
			case RECONNECT:
				msg.setSubject("RECONNECT SUCCESSFUL - " + clientName + " (" + serverName + ")");
				break;
			default:
				log.write("Unrecognized alert type while generating message");
				break;
			}
			
			return msg;
		}
	}
	/**
	 * Inner class holds all information about an alert
	 * @author jdk85
	 *
	 */
	public class AlertMessage{
		private String[] recipients;		
		private String fromAddress, subject, message;
		
		public AlertMessage(){
			//Default constructor - do nothing
		}
		/**
		 * 
		 * @return
		 */
		public String getSubject() {
			return subject;
		}
		/**
		 * 
		 * @param subject
		 */
		public void setSubject(String subject) {
			this.subject = subject;
		}
		/**
		 * 
		 * @return
		 */
		public String getMessage() {
			return message;
		}
		/**
		 * 
		 * @param message
		 */
		public void setMessage(String message) {
			this.message = message;
		}

		/**
		 * @return the recipients
		 */
		public String[] getRecipients() {
			return recipients;
		}

		/**
		 * @param recipients the recipients to set
		 */
		public void setRecipients(String[] recipients) {
			this.recipients = recipients;
		}

		/**
		 * @return the fromAddress
		 */
		public String getFromAddress() {
			return fromAddress;
		}

		/**
		 * @param fromAddress the fromAddress to set
		 */
		public void setFromAddress(String fromAddress) {
			this.fromAddress = fromAddress;
		}

	}
}
