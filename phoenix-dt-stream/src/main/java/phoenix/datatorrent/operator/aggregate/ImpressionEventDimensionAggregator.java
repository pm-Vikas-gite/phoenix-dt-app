package phoenix.datatorrent.operator.aggregate;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import phoenix.datatorrent.builder.DimensionEnum;
import phoenix.datatorrent.model.DimensionKey;
import phoenix.datatorrent.model.ImpressionEvent;

import com.datatorrent.lib.statistics.DimensionsComputation;

public class ImpressionEventDimensionAggregator implements
    DimensionsComputation.Aggregator<ImpressionEvent, Aggregate> {

  private TimeUnit timeUnit;
  private int key;
  private String dimensionSpec;

  private static final long serialVersionUID = -1158712404344006604L;

  public ImpressionEventDimensionAggregator() {
    this.dimensionSpec = null; // for Kryo
  }

  public ImpressionEventDimensionAggregator(DimensionKey dimensionSelector) {
    this.timeUnit = dimensionSelector.getTimeUnit();
    computeKey(dimensionSelector);
    this.dimensionSpec = dimensionSelector.toString();
  }

  // TODO : return null for invalid values for the given key.Tushar to update on platform support
  // first.
  public Aggregate getGroup(ImpressionEvent src, int aggregatorIndex) {
    Aggregate aggregate = new Aggregate();
    aggregate.aggregatorIndex = aggregatorIndex;
    if (timeUnit != null) {
      // TODO : use JODA time and data utils for PDT,PST - impacts default offsets.
      aggregate.setTimestamp(TimeUnit.MILLISECONDS.convert(
          timeUnit.convert(src.getTimestamp(), TimeUnit.MILLISECONDS), timeUnit));
    }

    if ((key & ImpressionEvent.DIMENSION_PUBLISHER_ID) != 0) {
      aggregate.setPubId(src.getPubId());
    }
    if ((key & ImpressionEvent.DIMENSION_LINE_ITEM_ID) != 0) {
      aggregate.setLineItemId(src.getLineItemId());
    }
    if ((key & ImpressionEvent.DIMENSION_CREATIVE_ID) != 0) {
      aggregate.setCreativeId(src.getCreativeId());
    }
    if ((key & ImpressionEvent.DIMENSION_TARGET_ID) != 0) {
      aggregate.setTargetId(src.getTargetId());
    }
    if ((key & ImpressionEvent.DIMENSION_AD_UNIT_ID) != 0) {
      aggregate.setAdUnitId(src.getAdUnitId());
    }

    aggregate.setTimeUnit(timeUnit);
    aggregate.setKey(key);
    return aggregate;
  }

  public void aggregate(Aggregate dest, ImpressionEvent src) {
    boolean rtbImpression = true;

    // check if the received impression is null
    // TODO : check for null destination & src,in-case getGroup returns null
    if (src == null) {
      return;
    }
    dest.setPaidImpressions(dest.getPaidImpressions() + 1);

  }

  public void aggregate(Aggregate dest, Aggregate src) {

    // TODO : check for null destination & src,in-case getGroup returns null
    dest.setPaidImpressions(dest.getPaidImpressions() + src.getPaidImpressions());

  }

  private void computeKey(DimensionKey dimensionSelector) {
    Collection<DimensionEnum> dimensions = dimensionSelector.getDimensions();
    for (DimensionEnum dimensionEnum : dimensions) {
      if (dimensionEnum != null) {
        switch (dimensionEnum) {
          case PUBLISHER_ID:
            key |= ImpressionEvent.DIMENSION_PUBLISHER_ID;
            break;
          case LINE_ITEM_ID:
            key |= ImpressionEvent.DIMENSION_LINE_ITEM_ID;
            break;
          case CREATIVE_ID:
            key |= ImpressionEvent.DIMENSION_CREATIVE_ID;
            break;
          case TARGET_ID:
            key |= ImpressionEvent.DIMENSION_TARGET_ID;
            break;
          case AD_UNIT_ID:
            key |= ImpressionEvent.DIMENSION_AD_UNIT_ID;
            break;
          default:
            break;
        }
      }
    }
  }

  /*
   * null & zero checks for data received from the ImpressionEvent. Change here suffices no need to
   * change getGroup
   */
  public int computeHashCode(ImpressionEvent adEvent) {
    int hash = 5;
    if ((key & ImpressionEvent.DIMENSION_PUBLISHER_ID) != 0) {
      hash = 89 * hash + adEvent.getPubId();
    }
    if ((key & ImpressionEvent.DIMENSION_LINE_ITEM_ID) != 0) {
      hash = 89 * hash + adEvent.getLineItemId();
    }
    if ((key & ImpressionEvent.DIMENSION_CREATIVE_ID) != 0) {
      hash = 89 * hash + adEvent.getCreativeId();
    }
    if ((key & ImpressionEvent.DIMENSION_TARGET_ID) != 0) {
      hash = 89 * hash + adEvent.getTargetId();
    }
    if ((key & ImpressionEvent.DIMENSION_AD_UNIT_ID) != 0) {
      hash = 89 * hash + adEvent.getAdUnitId();
    }
    if (timeUnit != null) {
      long millis = timeUnit.convert(adEvent.getTimestamp(), TimeUnit.MILLISECONDS);
      hash = hash + (int) (millis ^ (millis >>> 32));
    }
    return hash;
  }

  public boolean equals(ImpressionEvent adEvent1, ImpressionEvent adEvent2) {
    if (timeUnit != null) {
      if (timeUnit.convert(adEvent1.getTimestamp(), TimeUnit.MILLISECONDS) != timeUnit.convert(
          adEvent2.getTimestamp(), TimeUnit.MILLISECONDS)) {
        return false;
      }
    }
    if ((key & ImpressionEvent.DIMENSION_PUBLISHER_ID) != 0) {
      if (adEvent1.getPubId() != adEvent2.getPubId()) {
        return false;
      }
    }
    if ((key & ImpressionEvent.DIMENSION_LINE_ITEM_ID) != 0) {
      if (adEvent1.getLineItemId() != adEvent2.getLineItemId()) {
        return false;
      }
    }
    if ((key & ImpressionEvent.DIMENSION_CREATIVE_ID) != 0) {
      if (adEvent1.getLineItemId() != adEvent2.getLineItemId()) {
        return false;
      }
    }
    if ((key & ImpressionEvent.DIMENSION_TARGET_ID) != 0) {
      if (adEvent1.getTargetId() != adEvent2.getTargetId()) {
        return false;
      }
    }
    if ((key & ImpressionEvent.DIMENSION_AD_UNIT_ID) != 0) {
      if (adEvent1.getAdUnitId() != adEvent2.getAdUnitId()) {
        return false;
      }
    }
    return true;
  }

  protected TimeUnit getTimeUnit() {
    return timeUnit;
  }

  protected int getKey() {
    return key;
  }

  public String getDimensionSpec() {
    return dimensionSpec;
  }

  @Override
  public String toString() {
    return "LoggerImpressionEventDimensionAggregator [dimensionSelector=" + getDimensionSpec()
        + "]";
  }
}
