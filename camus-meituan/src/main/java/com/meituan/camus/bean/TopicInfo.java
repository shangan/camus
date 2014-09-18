package com.meituan.camus.bean;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by chenshangan on 14-6-17.
 */
public class TopicInfo {

  private String topicName;
  private Map<Integer, PartitionInfo> partitionInfoMap;

  public TopicInfo(String topicName) {
    this.topicName = topicName;
    this.partitionInfoMap = new HashMap<Integer, PartitionInfo>();
  }

  public TopicInfo(String topicName, Map<Integer, PartitionInfo> partitionInfoMap) {
    this.topicName = topicName;
    this.partitionInfoMap = partitionInfoMap;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public Map<Integer, PartitionInfo> getPartitionInfoMap() {
    return partitionInfoMap;
  }

  public void setPartitionInfoMap(Map<Integer, PartitionInfo> partitionInfoMap) {
    this.partitionInfoMap = partitionInfoMap;
  }

  public void setPartitionInfo(int partitionId, long earliestOffset, long latestOffset, long currentOffset) {
    PartitionInfo p = new PartitionInfo(partitionId, earliestOffset, latestOffset, currentOffset);
    partitionInfoMap.put(partitionId, p);

  }

  public void setPartitionEarliestOffset(int partitionId, long earliestOffset) {
    PartitionInfo partitionInfo = getPartitionInfo(partitionId);
    if (partitionInfo == null) {
      partitionInfo = new PartitionInfo(partitionId);
      partitionInfoMap.put(partitionId, partitionInfo);
    }
    partitionInfo.setEarliestOffset(earliestOffset);
  }

  public void setPartitionLatestOffset(int partitionId, long latestOffset) {
    PartitionInfo partitionInfo = getPartitionInfo(partitionId);
    if (partitionInfo == null) {
      partitionInfo = new PartitionInfo(partitionId);
      partitionInfoMap.put(partitionId, partitionInfo);
    }
    partitionInfo.setLatestOffset(latestOffset);
  }

  public void setPartitionCurrentOffset(int partitionId, long currentOffset) {
    PartitionInfo partitionInfo = getPartitionInfo(partitionId);
    if (partitionInfo == null) {
      partitionInfo = new PartitionInfo(partitionId);
      partitionInfoMap.put(partitionId, partitionInfo);
    }
    partitionInfo.setCurrentOffset(currentOffset);
  }


  public PartitionInfo getPartitionInfo(int partitionId) {
    return partitionInfoMap.get(partitionId);
  }

  @Override
  public String toString() {
    return "TopicInfo{" +
      "topicName='" + topicName + '\'' +
      ", partitionInfoMap=" + partitionInfoMap +
      '}';
  }
}
