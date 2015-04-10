package phoenix.datatorrent.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import phoenix.datatorrent.builder.DimensionEnum;
import phoenix.datatorrent.common.DimensionPartitionType;

/**
 * Class representing dimensions and their aggregation time span
 * 
 * @author Vikas
 * 
 */
public final class DimensionKey {

  private Collection<DimensionEnum> dimensions = new ArrayList<DimensionEnum>();
  private TimeUnit timeUnit;
  private DimensionPartitionType dimensionPartitionType;


  public DimensionKey(final TimeUnit timeUnit, final Collection<DimensionEnum> dimensions) {
    this.dimensions = dimensions;
    this.timeUnit = timeUnit;
  }


  public DimensionKey(final TimeUnit timeUnit, final Collection<DimensionEnum> dimensions,
      final DimensionPartitionType dimensionPartitionType) {
    this.dimensions = dimensions;
    this.timeUnit = timeUnit;
    this.dimensionPartitionType = dimensionPartitionType;
  }

  public Collection<DimensionEnum> getDimensions() {
    return dimensions;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public DimensionPartitionType getDimensionPartitionType() {
    return dimensionPartitionType;
  }

  @Override
  public String toString() {
    return "DimensionKey [dimensions=" + dimensions + ", timeUnit=" + timeUnit
        + ", dimensionPartitionType=" + dimensionPartitionType + "]";
  }
}
