package com.meituan.camus.bean;

/**
 * Created by chenshangan on 14-6-17.
 */
public class PartitionInfo {
  private int id;
  private long earliestOffset;
  private long latestOffset;
  private long currentOffset;
  private long count;

  public PartitionInfo(int id) {
    this.id = id;
  }

  public PartitionInfo(int id, long earliestOffset, long latestOffset, long currentOffset) {
    this.id = id;
    this.earliestOffset = earliestOffset;
    this.latestOffset = latestOffset;
    this.currentOffset = currentOffset;
    this.count = latestOffset - earliestOffset;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public long getEarliestOffset() {
    return earliestOffset;
  }

  public void setEarliestOffset(long earliestOffset) {
    this.earliestOffset = earliestOffset;
  }

  public long getLatestOffset() {
    return latestOffset;
  }

  public void setLatestOffset(long latestOffset) {
    this.latestOffset = latestOffset;
  }

  public long getCurrentOffset() {
    return currentOffset;
  }

  public void setCurrentOffset(long currentOffset) {
    this.currentOffset = currentOffset;
  }

  @Override
  public String toString() {
    return "PartitionInfo{" +
      "id=" + id +
      ", earliestOffset=" + earliestOffset +
      ", latestOffset=" + latestOffset +
      ", currentOffset=" + currentOffset +
      ", count=" + count +
      '}';
  }
}
