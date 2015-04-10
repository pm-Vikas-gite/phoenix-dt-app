package phoenix.datatorrent.builder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import phoenix.datatorrent.common.DimensionPartitionType;
import phoenix.datatorrent.model.DimensionKey;

/**
 * Class responsible for building dimension specifications ( Dimension Key )
 * 
 * @author sandip
 * 
 */
public final class DimensionSpecificationBuilder {

  private Collection<DimensionEnum> dimensions = new ArrayList<DimensionEnum>();
  private Collection<TimeUnit> timeUnit = new ArrayList<TimeUnit>();
  private DimensionPartitionType partitionType;

  public DimensionSpecificationBuilder addDimension(final DimensionEnum dimension) {
    this.dimensions.add(dimension);
    return this;
  }

  public DimensionSpecificationBuilder addTimeUnit(final TimeUnit timeUnit) {
    this.timeUnit.add(timeUnit);
    return this;
  }

  public DimensionSpecificationBuilder setDimensionPartitionType(
      DimensionPartitionType partitionType) {
    this.partitionType = partitionType;
    return this;
  }

  /**
   * create key for each value of timeUnit and for all given dimension
   * 
   * @return Collection<DimensionKey>
   */
  public List<DimensionKey> build() {
    final List<DimensionKey> dimensionSpecs = new ArrayList<DimensionKey>();
    for (TimeUnit currentUnit : timeUnit) {
      dimensionSpecs.add(new DimensionKey(currentUnit, dimensions, partitionType));
    }
    return dimensionSpecs;
  }
}
