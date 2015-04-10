package phoenix.datatorrent.operator.aggregate;

import org.codehaus.jackson.annotate.JsonIgnore;

import phoenix.datatorrent.model.ImpressionBase;

import com.datatorrent.lib.statistics.DimensionsComputation;

public class Aggregate extends ImpressionBase implements DimensionsComputation.AggregateEvent {

  private static final long serialVersionUID = 5650208192673646936L;

  @JsonIgnore
  public int aggregatorIndex;
  private long paidImpressions;

  public long getPaidImpressions() {
    return paidImpressions;
  }

  public void setPaidImpressions(long paidImpressions) {
    this.paidImpressions = paidImpressions;
  }

  public int getAggregatorIndex() {
    return aggregatorIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Aggregate)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    Aggregate aggregate = (Aggregate) o;

    if (aggregatorIndex != aggregate.aggregatorIndex) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return "Aggregate [aggregatorIndex=" + aggregatorIndex + ", timeUnit=" + getTimeUnit()
        + ",  pubId=" + getPubId() + ", lineItemId=" + getLineItemId() + ", creativeId=" + getCreativeId()
        + ", targetId=" + getTargetId() + ", adUnitId=" + getAdUnitId() + ", timestamp="
        + getTimestamp() + ", paidImpressions="+getPaidImpressions()+"]";
  }

}
