package com.meituan.camus.etl;

import kafka.javaapi.TopicMetadata;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by chenshangan on 14-6-20.
 */
public class MeituanKafkaTopicFilter {

	private final static Logger log = Logger.getLogger(MeituanKafkaTopicFilter.class);

	public static List<TopicMetadata> filterWhiteList(
			List<TopicMetadata> topicMetadataList, Set<String> whiteListTopics) {

		List<TopicMetadata> retTopicMetadataList = new ArrayList<TopicMetadata>();
		if (whiteListTopics.size() != 1) {
			log.error("White list topic is more than one, white list topic: " + whiteListTopics);
			return retTopicMetadataList;
		}
		String topic = whiteListTopics.iterator().next();
		for (TopicMetadata crt : topicMetadataList) {
			if (crt.topic().equalsIgnoreCase(topic)) {
				retTopicMetadataList.add(crt);
				break;
			}
		}

		return retTopicMetadataList;
	}
}
