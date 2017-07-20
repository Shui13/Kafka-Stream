package com.aps.akamill;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.akamai.akamill.stream.Stream.FieldData;
import com.akamai.akamill.stream.Stream.StreamBundle;
import com.akamai.akamill.stream.Stream.StreamMsg;
import com.aps.akamill.dao.ElasticSearchClient;
import com.google.protobuf.InvalidProtocolBufferException;

public class StreamProcessor {

	public static final String R_LINE = "aps_r_msg";
	public static final String F_LINE = "aps_f_msg";
	public static String JSON_CONF_FILE_LOC = "default";
	final static Logger logger = Logger.getLogger(StreamProcessor.class);

	String applicationId;
	String commitIntervalMS;
	String inputTopic;
	String outputTopic;
	String bootstrapServerList;
	JSONArray rLineTemplate;
	JSONArray fLineTemplate;
	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

	StreamProcessor() throws Exception {
		JSONParser parser = new JSONParser();
		JSONObject jsonObject = null;
		try {
			jsonObject = (JSONObject) parser.parse(new FileReader(StreamProcessor.JSON_CONF_FILE_LOC));
		} catch (Exception e) {
			throw e;
		}
		Properties props = new Properties();
		props.load(new FileInputStream((String) jsonObject.get("log_file_loc")));
		PropertyConfigurator.configure(props);
		this.inputTopic = (String) jsonObject.get("input_topic");
		this.outputTopic = (String) jsonObject.get("output_topic");
		this.bootstrapServerList = (String) jsonObject.get("bootstrap_server_list");
		this.commitIntervalMS = (String) jsonObject.get("commit_interval_ms");
		this.applicationId = (String) jsonObject.get("application_id");
		this.rLineTemplate = (JSONArray) jsonObject.get("r_line");
		this.fLineTemplate = (JSONArray) jsonObject.get("f_line");
	}

	public static void main(String[] args) throws Exception {
		logger.info("Kafka stream started");
		StreamProcessor.JSON_CONF_FILE_LOC = args[0];
		StreamProcessor streamProcessor = new StreamProcessor();
		final Serde<String> stringSerde = Serdes.String();
		final Serde<byte[]> byteArraySerde = Serdes.ByteArray();
		final Properties streamsConfiguration = new Properties();
		Map<Integer, String> accInfo = (new ElasticSearchClient()).getAccountMap();
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, streamProcessor.commitIntervalMS);
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, streamProcessor.applicationId);
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, streamProcessor.bootstrapServerList);
		streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
		final KStreamBuilder builder = new KStreamBuilder();
		final KStream<String, byte[]> textLines = builder.stream(stringSerde, byteArraySerde,
				streamProcessor.inputTopic);
		KStream<String, String> outputStream = textLines.flatMapValues(f -> {
			int i = 0;
			logger.info("Pushin... : it");
			StreamBundle protoStream = null;
			byte[] binaryStream = null;
			List<JSONObject> resultJJ = new ArrayList<JSONObject>();
			try {
				binaryStream = streamProcessor.decompress(f);
				while (i < binaryStream.length) {
					byte[] b = Arrays.copyOfRange(binaryStream, i, i + 4);
					int msgLen = java.nio.ByteBuffer.wrap(b).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
					byte[] protoMsg = Arrays.copyOfRange(binaryStream, i + 4, i + msgLen);
					try {
						protoStream = StreamBundle.parseFrom(protoMsg);
					} catch (InvalidProtocolBufferException e1) {
						logger.error("Exception while parse from protobuf:");
						logger.error(e1, e1);
					}

					if (R_LINE.equalsIgnoreCase(protoStream.getName())) {
						for (StreamMsg strMsg : protoStream.getRowList()) {
							JSONObject resJObj = new JSONObject();
							for (Object obj : streamProcessor.rLineTemplate) {
								streamProcessor.makeJson((JSONObject) obj, resJObj, strMsg);
							}
							resultJJ.add(resJObj);
						}
					} else if (F_LINE.equalsIgnoreCase(protoStream.getName())) {
						for (StreamMsg strMsg : protoStream.getRowList()) {
							JSONObject resJObj = new JSONObject();
							for (Object obj : streamProcessor.fLineTemplate) {
								streamProcessor.makeJson((JSONObject) obj, resJObj, strMsg);
							}
							resultJJ.add(resJObj);
						}
					}
					i = i + msgLen;
				}
			} catch (Exception e) {
				logger.error(e, e);
				// throw new RuntimeException(e);
			}
			return resultJJ;
		}).map((key, val) -> {
			if (null != val.get("cpcode") && accInfo.containsKey((int) ((long) val.get("cpcode")))) {
				return new KeyValue<>(accInfo.get((int) ((long) val.get("cpcode"))), val.toJSONString());
			}
			logger.info("Cpcode Not found: " + val.get("cpcode"));
			return new KeyValue<>(null, null);
		});
		outputStream.to(stringSerde, stringSerde, streamProcessor.outputTopic);
		final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	public byte[] decompress(byte[] contentBytes) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		IOUtils.copy(new GZIPInputStream(new ByteArrayInputStream(contentBytes)), out);
		return out.toByteArray();
	}

	public void makeJson(JSONObject jObject, JSONObject resJObj, StreamMsg strMsg) {
		Long pos = (Long) jObject.get("position");
		String fieldName = (String) jObject.get("name");
		FieldData fd = strMsg.getKeyfields(pos.intValue());
		if ("Strf".equalsIgnoreCase((String) jObject.get("type"))) {
			resJObj.put(fieldName, fd.getStrf());
		} else if ("Int64F".equalsIgnoreCase((String) jObject.get("type"))) {
			resJObj.put(fieldName, (Long) fd.getInt64F());
		} else if ("Dblf".equalsIgnoreCase((String) jObject.get("type"))) {
			resJObj.put(fieldName, 1000 * fd.getDblf());
		} else if ("Int32".equalsIgnoreCase((String) jObject.get("type"))) {
			resJObj.put(fieldName, (int) fd.getIntf());
		}
	}

}
