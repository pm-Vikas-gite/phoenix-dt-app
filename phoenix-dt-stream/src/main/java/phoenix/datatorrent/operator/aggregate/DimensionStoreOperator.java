/*
 * Copyright (c) 2012-2013 DataTorrent, Inc. All Rights Reserved.
 */
package phoenix.datatorrent.operator.aggregate;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import phoenix.datatorrent.common.DimensionPartitionType;
import phoenix.datatorrent.model.ImpressionEvent;
import phoenix.datatorrent.operator.output.mysql.JdbcTransactionableOutputOperator;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DimensionStoreOperator extends BaseOperator {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionStoreOperator.class);

  // Expiration duration.
  // The expiration duration can be in DAYS, HOURS or MINUTES.This is what is held in this map.

  private final Map<TimeUnit, Long> expirationDurations = new HashMap<TimeUnit, Long>();
  private Boolean checkDefaultExpirationDurations = false;

  @NotNull
  private DimensionsComputation.Aggregator<ImpressionEvent, Aggregate>[] aggregators;

  // Used for expiration.
  private long latestTimeStamp = 0;

  // window count after which old data is checked for expiration from memory.This is in terms of the
  // number of
  // streaming windows.
  // Default value is 5 minutes.
  private int expirationWindowCount = 600;
  private int currentWindow = expirationWindowCount;
  private transient ObjectMapper mapper;

  JdbcTransactionableOutputOperator JdbcOutput = new JdbcTransactionableOutputOperator();

  public final transient DefaultInputPort<Aggregate> data = new DefaultInputPort<Aggregate>() {
    @Override
    public void process(Aggregate tuple) {
      System.out.println("[DimensionStoreOperator] : " + tuple.toString());
      Aggregate destination = aggregates.get(tuple);
      if (destination == null) {

        aggregates.put(tuple, tuple);
        addAggregate(tuple);
        modifiedKeys.add(tuple);
        destination = tuple;
      } else {
        int aggregatorIndex = tuple.getAggregatorIndex();
        aggregators[aggregatorIndex].aggregate(destination, tuple);
        modifiedKeys.add(destination);
      }
    }

    @Override
    public Class<? extends StreamCodec<Aggregate>> getStreamCodec() {
      return getAggregateStreamCodec();
    }

  };


  public final transient DefaultOutputPort<Aggregate> output = new DefaultOutputPort<Aggregate>();
  // output port which emits tuples that are expired. This is done before these tuples are purged.
  public final transient DefaultOutputPort<Aggregate> expiredData =
      new DefaultOutputPort<Aggregate>();


  /**
   * Map of all keys needs to be persisted, everything else is derived.
   */
  final Map<Aggregate, Aggregate> aggregates = Maps.newHashMap();

  private final transient Set<Aggregate> modifiedKeys = Sets.newHashSet();


  /**
   * Organize keys into time buckets for time series retrieval and expiration.Can be multiple
   * timebuckets such as a minute based, hour based, 5 minutes, 10 minutes, etc.
   */
  final transient Map<TimeUnit, Map<Long, Set<Aggregate>>> timeBucketUnits = Maps.newHashMap();



  private void addToTimeBucket(Aggregate tuple) {
    Map<Long, Set<Aggregate>> timeBucketUnit = timeBucketUnits.get(tuple.getTimeUnit());

    if (timeBucketUnit == null) {
      timeBucketUnits.put(tuple.getTimeUnit(), timeBucketUnit = Maps.newTreeMap());
    }

    Set<Aggregate> timeBucket = timeBucketUnit.get(tuple.getTimestamp());

    if (timeBucket == null) {
      timeBucketUnit.put(tuple.getTimestamp(), timeBucket = Sets.newHashSet());
    }
    timeBucket.add(tuple);
  }


  private void addAggregate(Aggregate tuple) {
    addToTimeBucket(tuple);

  }

  /**
   * Sets the aggregators.
   * 
   * @param aggregators
   */
  public void setAggregators(
      @Nonnull DimensionsComputation.Aggregator<ImpressionEvent, Aggregate>[] aggregators) {
    this.aggregators = aggregators;
  }

  @Override
  public void setup(Context.OperatorContext context) {
    super.setup(context);
    /*
     * JdbcTransactionalStore transactionalStore = new JdbcTransactionalStore();
     * transactionalStore.setDbDriver("com.mysql.jdbc.Driver");
     * transactionalStore.setDbUrl("jdbc:mysql://localhost:3306/phoenix");
     * transactionalStore.setConnectionProperties("user:kdbuser,password:KdBuSeR12!");
     * 
     * 
     * JdbcOutput.setBatchSize(3); JdbcOutput.setStore(transactionalStore);
     * JdbcOutput.setup(context);
     */
    if (!aggregates.isEmpty()) {
      LOG.info("Rebuilding index from checkpointed state ({} keys)", aggregates.size());
      for (Aggregate key : aggregates.keySet()) {
        addAggregate(key);
      }
    }
  }

  @Override
  public void endWindow() {
   for (Aggregate value : modifiedKeys) {
      System.out.println("value sent to store : "+value.toString());
      
      if(value.getTimeUnit() == TimeUnit.HOURS){
      output.emit(value);
      }
      aggregates.remove(value);
    }
   // expireData();

    modifiedKeys.clear();

    /* Expire old data from memory periodically */
    currentWindow--;
    if (currentWindow <= 0) {
      expireData();
      currentWindow = expirationWindowCount;
    }

    System.out.println("Printing Dimension Store !");
    Iterator<Map.Entry<Aggregate, Aggregate>> iterator = aggregates.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Aggregate, Aggregate> aggregateEntry = iterator.next();
      System.out.println(aggregateEntry.getKey() + " :: " + aggregateEntry.getValue());
    }
  }

  private void writeToMysql(Aggregate e) {
    
    LOG.info("Written to JDBC : ", e.toString());
    output.emit(e);
  }

  public static class AggregateStreamCodec extends KryoSerializableStreamCodec<Aggregate> {
    @Override
    public int getPartition(Aggregate t) {
      // get current aggregator index type from aggIndexPartionTypeMap
      DimensionPartitionType dimensionPartitionType = getDimensionPartitionType(t.aggregatorIndex);

      // note that keys used for topN cannot be included
      int hash = 5;
      hash = 89 * hash + t.aggregatorIndex;

      switch (dimensionPartitionType) {
        case FEEDBACK_VIEW:
          // partition by index and publisher
          hash = 89 * hash + t.getPubId();
          break;

        default:
          // partition by index and publisher -- default
          hash = 89 * hash + t.getPubId();
          break;
      }
      return hash;
    }
  }

  private static boolean isFeedbackView(int e) {
    int aggregatorIndex = e;
    if ((aggregatorIndex >= 0 && aggregatorIndex <= 1)) {
      return true;
    } else {
      return false;
    }
  }

  public static DimensionPartitionType getDimensionPartitionType(int aggregatorIndex) {
    DimensionPartitionType dimensionPartitionType = null;
    // TODO call the function instead
    if (isFeedbackView(aggregatorIndex)) {
      // publisher view loggger
      dimensionPartitionType = DimensionPartitionType.FEEDBACK_VIEW;
    }

    return dimensionPartitionType;
  }


  protected Class<? extends StreamCodec<Aggregate>> getAggregateStreamCodec() {
    return AggregateStreamCodec.class;
  }

  // Will be called as - setExpirationDuration(120, MINUTES, MINUTES);
  // where
  // duration : Actual number of units for which the expiration is to be set
  // timeUnit : The time unit in which the duration is specified
  // expirationTimeUnit : the bucket out of "DAYS, HOURS, MINUTES" to which this expiration should
  // belong

  public void setExpirationDuration(int duration, TimeUnit timeUnit, TimeUnit expirationTimeUnit) {
    long expirationDuration = timeUnit.toMillis(duration);
    expirationDurations.put(expirationTimeUnit, expirationDuration);
  }

  public void setDefaultExpirationDurations() {

    // None of the expiration durations are set. So initialize them to default.
    if (null == expirationDurations || expirationDurations.isEmpty()) {
      setExpirationDuration(1, TimeUnit.DAYS, TimeUnit.DAYS);
      setExpirationDuration(1, TimeUnit.MINUTES, TimeUnit.MINUTES);
      setExpirationDuration(1, TimeUnit.HOURS, TimeUnit.HOURS);
    } else {
      // Expired duration set only partially. So set for the remaining time units.
      if (expirationDurations.get(TimeUnit.DAYS) == null) {
        setExpirationDuration(1, TimeUnit.DAYS, TimeUnit.DAYS);
      }

      if (expirationDurations.get(TimeUnit.HOURS) == null) {
        setExpirationDuration(1, TimeUnit.HOURS, TimeUnit.HOURS);
      }

      if (expirationDurations.get(TimeUnit.MINUTES) == null) {
        setExpirationDuration(1, TimeUnit.MINUTES, TimeUnit.MINUTES);
      }
    }
  }

  public void setExpirationWindowCount(int count) {
    expirationWindowCount = count;
    currentWindow = expirationWindowCount;
  }

  /**
   * Remove old data from memory,
   */
  protected void expireData() {

    if (!checkDefaultExpirationDurations) {
      setDefaultExpirationDurations();
      checkDefaultExpirationDurations = true;
    }

    
    for (Map.Entry<TimeUnit, Long> entry : expirationDurations.entrySet()) {

      long expirationDuration = entry.getValue();
      long expireTimeStamp = latestTimeStamp - expirationDuration;
      long expired = 0;
      Aggregate expiredAggregate = new Aggregate();

      Map<Long, Set<Aggregate>> timeBucketUnit = timeBucketUnits.get(entry.getKey());

      if (timeBucketUnit == null) {
        continue;
      }

      Iterator<Map.Entry<Long, Set<Aggregate>>> iter = timeBucketUnit.entrySet().iterator();

      while (iter.hasNext()) {
        Map.Entry<Long, Set<Aggregate>> curEntry = iter.next();

        if (curEntry.getKey() < expireTimeStamp) {
          Set<Aggregate> agr = curEntry.getValue();

          for (Aggregate e : agr) {
            expiredAggregate = e;
            writeToMysql(e);
            aggregates.remove(e);
            expiredData.emit(expiredAggregate);
            expired++;
          }

          // Remove time-bucket also, as it is not needed.
          iter.remove();
        }
      }
     // LOG.info("Number of expired keys: {} {}", entry.getKey(), expired);
    }
  }


}
