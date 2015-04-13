package phoenix.datatorrent.app;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;



/*
 * import matrix.datatorrent.common.Constants; import matrix.datatorrent.model.DimensionKey; import
 * matrix.datatorrent.model.ImpressionEvent; import
 * matrix.datatorrent.monitoring.MonitoringConstants; import
 * matrix.datatorrent.operator.aggregate.Aggregate; import
 * matrix.datatorrent.operator.aggregate.DimensionStoreOperatorWithMultiLoggerStreams; import
 * matrix.datatorrent.operator.aggregate.ImpressionEventDimensionAggregator; import
 * matrix.datatorrent.operator.input.AvroFSInputOperator; import
 * matrix.datatorrent.operator.input.CompressedLoggerEventConvertor; import
 * matrix.datatorrent.operator.input.EventFilter; import
 * matrix.datatorrent.operator.input.LoggerImpressionEventConvertor; import
 * matrix.datatorrent.operator.input.MatrixDirectoryScanner; import
 * matrix.datatorrent.operator.input.MatrixHalfHourlyDirectoryScanner; import
 * matrix.datatorrent.operator.input.PubFilter; import
 * matrix.datatorrent.operator.input.TrackerImpressionEventConvertor; import
 * matrix.datatorrent.operator.input.kafka.DecompressInputOperator; import
 * matrix.datatorrent.operator.input.kafka.PartitionableKafkaInputOperator; import
 * matrix.datatorrent.operator.output.DailyAggregateWriter; import
 * matrix.datatorrent.operator.output.KafkaAggregateWriter; import
 * matrix.datatorrent.operator.query.KafkaJsonEncoder;
 */
import org.apache.hadoop.conf.Configuration;

import phoenix.datatorrent.common.Constants;
import phoenix.datatorrent.model.DimensionKey;
import phoenix.datatorrent.model.ImpressionEvent;
import phoenix.datatorrent.operator.aggregate.Aggregate;
import phoenix.datatorrent.operator.aggregate.DimensionStoreOperator;
import phoenix.datatorrent.operator.aggregate.ImpressionEventDimensionAggregator;
import phoenix.datatorrent.operator.input.LoggerImpressionConverter;
import phoenix.datatorrent.operator.input.kafka.DecompressInputOperator;
import phoenix.datatorrent.operator.input.kafka.PartitionableKafkaInputOperator;
import phoenix.datatorrent.operator.output.mysql.JdbcTransactionableOutputOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.kafka.HighlevelKafkaConsumer;
import com.datatorrent.contrib.kafka.KafkaConsumer;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.datatorrent.lib.statistics.DimensionsComputation;

@ApplicationAnnotation(name = "PhoenixRealTimeFeedbackApp")
public class PhoenixDTApplication implements StreamingApplication {
  // private static final String CW_NAME_SPACE = "cw.phoenix.name.space";

  public void populateDAG(DAG dag, Configuration conf) {
    dag.setAttribute(DAG.APPLICATION_NAME, conf.get(Constants.APP_NAME, Constants.APP_DEFAULT_NAME));


    /******************************** Kafka Input *****************************/
    PartitionableKafkaInputOperator kafkaInput = new PartitionableKafkaInputOperator();
    kafkaInput.setConsumer(getKafkaConsumer(conf, "kafkaClient"));
    kafkaInput = dag.addOperator("kafkaData", kafkaInput);

    /**************************************************************************/

    /******************************* Decompress *******************************/
    DecompressInputOperator decompress =
        dag.addOperator("decompress", new DecompressInputOperator());
    dag.setOutputPortAttribute(decompress.output, PortContext.QUEUE_CAPACITY, 1000);

    /**************************************************************************/

    /**************************** Impression Converter ************************/
    LoggerImpressionConverter avroLoggerImpressionConverter =
        dag.addOperator("AvroLoggerImpressionConverter", new LoggerImpressionConverter());
    dag.setInputPortAttribute(avroLoggerImpressionConverter.data, PortContext.PARTITION_PARALLEL,
        true);

    /**************************************************************************/


    /************************* Dimension Computation **************************/
    List<DimensionKey> feedBackDimensionSpecifications = new LinkedList<DimensionKey>();
    ApplicationHelper.phoenixFeedbackLoopSpec(feedBackDimensionSpecifications);

    ImpressionEventDimensionAggregator[] loggerImpressionAggregator =
        new ImpressionEventDimensionAggregator[feedBackDimensionSpecifications.size()];
    for (int i = 0; i < feedBackDimensionSpecifications.size(); ++i) {
      loggerImpressionAggregator[i] =
          new ImpressionEventDimensionAggregator(feedBackDimensionSpecifications.get(i));
    }

    DimensionsComputation<ImpressionEvent, Aggregate> loggerDimensionsComputation =
        dag.addOperator("LoggerDimensionsComputation",
            new DimensionsComputation<ImpressionEvent, Aggregate>());
    dag.setInputPortAttribute(loggerDimensionsComputation.data, PortContext.QUEUE_CAPACITY, 10000);
    dag.setInputPortAttribute(loggerDimensionsComputation.data, PortContext.PARTITION_PARALLEL,
        true);

    loggerDimensionsComputation.setAggregators(loggerImpressionAggregator);
    dag.setAttribute(loggerDimensionsComputation, Context.OperatorContext.APPLICATION_WINDOW_COUNT,
        2);

    List<ImpressionEventDimensionAggregator> allDimensionSpec =
        new LinkedList<ImpressionEventDimensionAggregator>();
    for (int i = 0; i < feedBackDimensionSpecifications.size(); ++i) {
      allDimensionSpec.add(new ImpressionEventDimensionAggregator(feedBackDimensionSpecifications
          .get(i)));
    }

    /**************************************************************************/

    /******************************** Store ************************************/
    DimensionStoreOperator dimensionStore =
        dag.addOperator("DimensionStore", new DimensionStoreOperator());
    dag.setAttribute(dimensionStore, OperatorContext.APPLICATION_WINDOW_COUNT, 2);

    ImpressionEventDimensionAggregator[] aggregatorsArr =
        new ImpressionEventDimensionAggregator[allDimensionSpec.size()];
    aggregatorsArr = allDimensionSpec.toArray(aggregatorsArr);

    dimensionStore.setAggregators(aggregatorsArr);

    /**************************************************************************/
    // ConsoleOutputOperator console = dag.addOperator("output", new ConsoleOutputOperator());

    /************************** JDBC Out **************************************/
    JdbcTransactionableOutputOperator dbWriter =
        dag.addOperator("DatabaseWriter", new JdbcTransactionableOutputOperator());

  /*  dbWriter.getStore().setDbDriver("com.mysql.jdbc.Driver");
    dbWriter.getStore().setDbUrl("jdbc:mysql://localhost:3306/phoenix");
    dbWriter.getStore().setConnectionProperties("user:kdbuser,password:KdBuSeR12!");
    
    dbWriter.getStore().setMetaTable("dt_window_id_tracker");
    dbWriter.getStore().setMetaTableAppIdColumn("dt_application_id");
    dbWriter.getStore().setMetaTableOperatorIdColumn("dt_operator_id");
    dbWriter.getStore().setMetaTableWindowColumn("dt_window_id");
  */  
    JdbcTransactionalStore transactionalStore = new JdbcTransactionalStore();
    transactionalStore.setDbDriver("com.mysql.jdbc.Driver");
    transactionalStore.setDbUrl("jdbc:mysql://172.20.12.79:3306/phoenix");
    transactionalStore.setConnectionProperties("user:kdbuser,password:KdBuSeR12!");
    dbWriter.setBatchSize(1);
  /*  com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, 1);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributeMap);
*/
    dbWriter.setStore(transactionalStore);

    /**************************************************************************/

    /***************************** DAG ****************************************/
    dag.addStream("PhoenixLoggerInputStream", kafkaInput.oport, decompress.in);
    dag.addStream("Decompress", decompress.output, avroLoggerImpressionConverter.data);
    // dag.addStream("output", avroLoggerImpressionConverter.output, console.input);

    dag.addStream("LoggerImpressions", avroLoggerImpressionConverter.output,
        loggerDimensionsComputation.data);
    dag.addStream("LoggerAggregates", loggerDimensionsComputation.output, dimensionStore.data);
    dag.addStream("DimensionStore", dimensionStore.output, dbWriter.input);
    /******************************************************************************/
  }

  private KafkaConsumer getKafkaConsumer(Configuration conf, String clientName) {
    KafkaConsumer consumer = null;
    String type = conf.get("kafka.consumertype");
    type = "simple";
    if (type.equals("highlevel")) {
      // Create template high-level consumer
      Properties props = new Properties();
      props.put("zookeeper.connect", conf.get("kafka.zookeeper"));
      props.put("group.id", "amla3_group");
      props.put("auto.offset.reset", "smallest");
      consumer = new HighlevelKafkaConsumer(props);
    } else {
      // topic is set via property file
      // consumer = new SimpleKafkaConsumer("jlogger", 10000, 100000, clientName, new
      // HashSet<Integer>());
     /* consumer =
          new SimpleKafkaConsumer(null, "jlogger", 10000, 100000, clientName,
              new HashSet<Integer>());*/
      consumer = new SimpleKafkaConsumer(null, 10000, 100000, clientName, new HashSet<Integer>());
    }
    return consumer;
  }

  public static void main(String[] args) throws Exception {

    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("kafka.consumertype", "simple");
    lma.prepareDAG(new PhoenixDTApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    // lc.run(20000);
    lc.run();
  }
}
