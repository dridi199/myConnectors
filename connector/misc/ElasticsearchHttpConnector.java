package fr.edf.dco.common.connector.misc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fr.edf.dco.common.connector.Connectable;
import fr.edf.dco.common.connector.base.ConnectorException;
import fr.edf.dco.common.connector.base.Constants;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.config.HttpClientConfig.Builder;
import io.searchbox.cluster.Health;
import io.searchbox.core.*;
import io.searchbox.params.Parameters;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * ElasticSearch indexes HTTP access connector
 * 
 * @author ahmed-a-externe.mabrouk@edf.fr
 */
public class ElasticsearchHttpConnector implements Connectable {

	//-------------------------------------------------------------------
	// CONSTRUCTOR
	//-------------------------------------------------------------------

	/**
	 * Creating the Elasticseach container given the host and the port
	 * of an Elasticsearch running instance
	 * @param hosts
	 * @param port
	 */
	public ElasticsearchHttpConnector(String hosts, int port) {
		this.hosts = hosts;
		this.port = port;
	}

	/**
	 * Creating the Elasticseach container given the host, the port and the shield login 
	 * of an Elasticsearch running instance
	 * @param hosts
	 * @param port
	 * @user user 
	 * @password password
	 */
	public ElasticsearchHttpConnector(String hosts, int port, String user, String password) {
		this(hosts, port);
		this.user = user;
		this.password = password;
		this.useShieldsecurity = true;
	}

	//-------------------------------------------------------------------
	// IMPLEMENTATION
	//-------------------------------------------------------------------

	/**
	 * Connecting to ES
	 */
	public void connect() throws ConnectorException {
		JestClientFactory factory = new JestClientFactory();

		List<String> nodes = new ArrayList<String>();
		String[] hostArray = hosts.split(",", -1);

		for (String host : hostArray) {
			nodes.add("http://" + host + ":" + this.port);
		}

		Builder builder = new HttpClientConfig.Builder(nodes);
		if(useShieldsecurity) builder.defaultCredentials(this.user, this.password);
		factory.setHttpClientConfig(builder.multiThreaded(true).connTimeout(60000).readTimeout(60000).build());

		this.client = factory.getObject();	
	}  

	/**
	 * Closing ES connection
	 */
	public void disconnect() {
		this.client.shutdownClient();
	}

	/**
	 * Get document from elastic 
	 * @param index
	 * @param type
	 * @param id
	 * @return
	 */
	public String get(String index, String type, String id)  {
		String result = null;
		Get get = new Get.Builder(index, id).type(type).build();

		try {
			result = this.client.execute(get).getSourceAsString();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return result;
	}

	/**
	 * Insert data into ES Index and type
	 */
	public void put(String index, String type, JSONObject source) {
		Index indexObject = new Index.Builder(source).index(index).type(type).build();
		try {
			client.execute(indexObject);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    /**
     *
     * @param index
     * @param type
     * @param source
     */
	public void bulk(String index, String type, String source){
	    Bulk b = new Bulk.Builder().defaultIndex(index).defaultType(type).addAction(new Index.Builder(source).build()).build();

	    try {
            client.execute(b);
        } catch (Exception e){
	        e.printStackTrace();
        }
    }

	/**
	 * update data into ES Index and type
	 */
	public void update(String index, String type, String id, Map<String,String> source){

		source.remove("_id");
	    Update updateObject = new Update.Builder(source).index(index).type(type).id(id).build();
		try {
			client.execute(updateObject);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void delete(String index, String type, String id){
	    Delete deleteObject = new Delete.Builder(id).index(index).type(type).build();

	    try{
	        client.execute(deleteObject);
        } catch(IOException e){
	        e.printStackTrace();
        }
    }


	/**
	 * Delete by query from elastic 
	 * @param index
	 * @param type
	 * @param terms
	 * @param values
	 */
	public void deleteByQuery(String index, String type, String terms, List<String> values) {
	
		List<String> listToRemove = new ArrayList<String>();
		// Create query 
		for(int i = 0; i < values.size() ; i++) listToRemove.add("\""+values.get(i)+"\"");
		String query = "{\n" +
				"    \"query\": {\n" +
				"        \"terms\": { \""+terms+"\" : "+listToRemove+" }\n" +
				"    }\n" +
				"}";		
		DeleteByQuery deleteByQuery = new DeleteByQuery.Builder(query)
				.addIndex(index)
				.addType(type).build();
		
		JestResult result = null;
		try {
			// Delete by query
			result = this.client.execute(deleteByQuery);
		} catch (IOException e) {
			e.printStackTrace();
			if(result != null) result.getErrorMessage();
		}
	}

    public void deleteBySpecificQuery(String index, String type, String q) {
        JSONObject j = null;
	    try{
            j = new JSONObject(q);
        } catch(JSONException je){
            je.printStackTrace();
        }
        DeleteByQuery deleteByQuery = new DeleteByQuery.Builder(j.toString())
                .addIndex(index)
                .addType(type).build();

        try {
            // Delete by query
            client.execute(deleteByQuery);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


	/**
	 * Ssearch from index with query 
	 * @param index
	 * @param type
	 * @param query
	 * @return
	 */
	public String search(String index, String type, String query) {
		String result = null;
		Search search = new Search.Builder(query).addIndex(index).addType(type).build();
		try {
			result = this.client.execute(search).getSourceAsString();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return result;
	}
	/**
	 * Ssearch from index with query 
	 * @param index
	 * @param type
	 * @param query
	 * @return
	 */
	public List<String> searchList(String index, String type, String query) throws IOException {
		Search search = new Search.Builder(query).addIndex(index).addType(type).build();
		return this.client.execute(search).getSourceAsStringList();
	}

	public String checkHealthClusterStatus() {
		String health = "";
		try {
			JestResult result = this.client.execute(new Health.Builder().build());
			health = result.getJsonObject().get("status").getAsString();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return health;
	}

	public String checkHealthWithIndexClusterStatus(String index){
		String health = "";
		try {
			JestResult result = this.client.execute(new Health.Builder().addIndex(index).build());
			health = result.getJsonObject().get("status").getAsString();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return health;
	}

	public List<String> searchListWithMeta(String index, String type, String query){
		List<String> result = null;
		List<SearchResult.Hit<Map,Void>> hits;
		Search search = new Search.Builder(query).addIndex(index).addType(type).build();

		try {
			hits = this.client.execute(search).getHits(Map.class);
			if(hits!= null) {
				if (hits.size() > 0) {
					result = new ArrayList<>();

					for (int i = 0; i < hits.size(); i++) {
						SearchResult.Hit hit = hits.get(i);
						Map source = (Map) hit.source;
						String id = hit.id;
						source.put("_id", id);
						String s = (source.toString().substring(1)).replaceAll(",\\s", ",");
						result.add(s.substring(0, s.length() - 1));
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Return internal client instance
	 * @return	JestClient instance
	 */
	public JestClient getClient() {
		return this.client;
	}
	
	//-------------------------------------------------------------------
	// DATA MEMBERS
	//-------------------------------------------------------------------

	private int                                   port = Constants.ELASTIC_DEFAULT_HTTP_PORT;
	private String                                hosts;
	private String 								  user;
	private String 								  password;
	private JestClient                            client;
	private boolean 							  useShieldsecurity = false;					 
}
