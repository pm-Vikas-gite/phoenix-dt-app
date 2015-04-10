package phoenix.datatorrent.model;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.annotate.JsonIgnore;

public class ImpressionBase implements Serializable {

  private static final long serialVersionUID = 7755330841255112038L;
  private int pubId;
  private int lineItemId;
  private int creativeId;
  private int targetId;
  private int adUnitId;
  private long timestamp;
  private String recordKey;


  /*
   * private int siteId; private int dealMetaId; private int dspId; private String advertiser;
   * private int atdId; private long timestamp; private int channelId; private int platformId;
   * 
   * @JsonIgnore private int timeZone;
   * 
   * @JsonIgnore private int dspTimeZone;
   * 
   * @JsonIgnore private long publisherTimestamp;
   * 
   * @JsonIgnore private long dspTimeStamp;
   * 
   * @JsonIgnore private long originalTimeStamp;
   */

  

  @JsonIgnore
  private int key;


  private TimeUnit timeUnit;


  public static final int DIMENSION_PUBLISHER_ID = 1 << 0;
  public static final int DIMENSION_LINE_ITEM_ID = 1 << 1;
  public static final int DIMENSION_CREATIVE_ID = 1 << 2;
  public static final int DIMENSION_TARGET_ID = 1 << 3;
  public static final int DIMENSION_AD_UNIT_ID = 1 << 4;

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 89 * hash + this.pubId;
    hash = 89 * hash + this.lineItemId;
    hash = 89 * hash + this.creativeId;
    hash = 89 * hash + this.targetId;
    hash = 89 * hash + this.adUnitId;
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }

    final ImpressionBase adEventChk = (ImpressionBase) obj;
    if (this.pubId != adEventChk.pubId) {
      return false;
    }
    if (this.lineItemId != adEventChk.lineItemId) {
      return false;
    }
    if (this.creativeId != adEventChk.creativeId) {
      return false;
    }
    if (this.targetId != adEventChk.targetId) {
      return false;
    }
    if (this.adUnitId != adEventChk.adUnitId) {
      return false;
    }
    if (this.timestamp != adEventChk.timestamp) {
      return false;
    }
    return true;
  }


  @Override
  public String toString() {
    return "ImpressionBase [pubId=" + pubId + ", lineItemId=" + lineItemId + ", creativeId="
        + creativeId + ", targetId=" + targetId + ", adUnitId=" + adUnitId + " timestamp="
        + timestamp + "]";
  }

  public String getRecordKey() {
    return recordKey;
  }

  public void setRecordKey(String recordKey) {
    this.recordKey = recordKey;
  }
  
  public int getKey() {
    return key;
  }

  public void setKey(int key) {
    this.key = key;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public void setTimeUnit(TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
  }

  public void setPubId(int pubId) {
    this.pubId = pubId;
  }

  public int getPubId() {
    return this.pubId;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public int getLineItemId() {
    return lineItemId;
  }

  public void setLineItemId(int lineItemId) {
    this.lineItemId = lineItemId;
  }

  public int getCreativeId() {
    return creativeId;
  }

  public void setCreativeId(int creativeId) {
    this.creativeId = creativeId;
  }

  public int getTargetId() {
    return targetId;
  }

  public void setTargetId(int targetId) {
    this.targetId = targetId;
  }

  public int getAdUnitId() {
    return adUnitId;
  }

  public void setAdUnitId(int adUnitId) {
    this.adUnitId = adUnitId;
  }
}
