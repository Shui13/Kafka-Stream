package com.aps.akamill.dao;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ElasticSearchClient {

	public static final String CLUSTER_NAME = "magicMike";
	public static final String INDEX_NAME = "account_information";
	public static final String TYPE_NAME = "details";
	public static final String HOST_NAME = "dev-anta-tools01.kendall.corp.akamai.com";
	public static final int HOST_PORT = 9300;

	public Map<Integer, String> getAccountMap() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		Map<Integer, String> cpcodeMap = new HashMap<Integer, String>();

		Settings settings = Settings.builder().put("cluster.name", ElasticSearchClient.CLUSTER_NAME).build();
		TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(
				new InetSocketTransportAddress(InetAddress.getByName(ElasticSearchClient.HOST_NAME),
						ElasticSearchClient.HOST_PORT));

		SearchResponse searchResponse = client.prepareSearch(ElasticSearchClient.INDEX_NAME)
				.setTypes(ElasticSearchClient.TYPE_NAME).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();

		SearchHit[] shs = searchResponse.getHits().getHits();
		for (SearchHit hit : shs) {
			Map<String, Object> rm = hit.getSource();
			cpcodeMap.put((Integer) rm.get("cpcode"), (String) rm.get("account_id"));
		}
		client.close();
		return cpcodeMap;
	}

}
