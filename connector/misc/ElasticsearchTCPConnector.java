package fr.edf.dco.common.connector.misc;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import fr.edf.dco.common.connector.Connectable;
import fr.edf.dco.common.connector.base.Constants;

/**
 * ElasticSearch indexes TCP access connector
 * 
 * @author ahmed-a-externe.mabrouk@edf.fr
 */
public class ElasticsearchTCPConnector implements Connectable {

	//-------------------------------------------------------------------
	// CONSTRUCTOR
	//-------------------------------------------------------------------


	/**
	 * Creating the Elasticseach container given the cluster name, hosts, the port and the Xpack login 
	 * of an Elasticsearch running instance
	 * @param host
	 * @param port
	 * @user user 
	 * @password password
	 */
	public ElasticsearchTCPConnector(String clusterName, String hosts, int port, String user, String password) {
		this.clusterName = clusterName;
		this.hosts = hosts;
		this.port = port;
		this.user = user;
		this.password = password;
	}

	//-------------------------------------------------------------------
	// IMPLEMENTATION
	//-------------------------------------------------------------------

	/**
	 * Connecting to ES
	 */
	public void connect() {

		Settings settings = Settings.builder()
				.put("cluster.name", clusterName)
				.put("xpack.security.user", user+":"+password)
				.build(); 

		InetSocketTransportAddress[] adresses = null;

		int i = 0;
		for (String host : hosts.split(",", -1)) {
			try {
				adresses[i] = new InetSocketTransportAddress(InetAddress.getByName(host), port);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			i ++;
		}
		
		this.client = new PreBuiltXPackTransportClient(settings).addTransportAddresses(adresses);
	}  

	/**
	 * Closing ES connection
	 */
	public void disconnect() {
		this.client.close();
	}
	
	/**
	 * Get the TransportClient
	 */
	public TransportClient getClient() {
		return this.client;
	}

	//-------------------------------------------------------------------
	// DATA MEMBERS
	//-------------------------------------------------------------------

	private int                                   port = Constants.ELASTIC_DEFAULT_TCP_PORT;
	private String                                clusterName;
	private String                                hosts;
	private String 								  user;
	private String 								  password;
	private TransportClient                       client;
}
