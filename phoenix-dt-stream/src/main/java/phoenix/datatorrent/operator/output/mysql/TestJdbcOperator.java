package phoenix.datatorrent.operator.output.mysql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.Nonnull;

import com.datatorrent.lib.db.jdbc.AbstractJdbcTransactionableOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.google.common.collect.Lists;

public class TestJdbcOperator {

  private static class TestEvent {
    int id;

    TestEvent(int id) {
      this.id = id;
    }
  }

  public TestJdbcOperator() {
    JdbcTransactionalStore store = new JdbcTransactionalStore();
    store.setMetaTable("dt_window_id_tracker");
    store.setMetaTableAppIdColumn("dt_application_id");
    store.setMetaTableOperatorIdColumn("dt_operator_id");
    store.setMetaTableWindowColumn("dt_window_id");
  }



  private static class TestOutputOperator extends
      AbstractJdbcTransactionableOutputOperator<TestEvent> {
    public static final String INSERT_STATUS_STATEMENT = "INSERT INTO test_table values (?)";

    @Nonnull
    @Override
    protected String getUpdateCommand() {
      return INSERT_STATUS_STATEMENT;
    }

    @Override
    protected void setStatementParameters(PreparedStatement statement, TestEvent tuple)
        throws SQLException {
      statement.setInt(1, tuple.id);
      statement.addBatch();
    }
  }


  public static void main(String[] args) {

    /*JdbcTransactionalStore transactionalStore = new JdbcTransactionalStore();
    transactionalStore.setDbDriver("com.mysql.jdbc.Driver");
    transactionalStore.setDbUrl("jdbc:mysql://localhost:3306/phoenix");
    transactionalStore.setConnectionProperties("user:kdbuser,password:KdBuSeR12!");
*/
    TestOutputOperator outputOperator = new TestOutputOperator();
    outputOperator.getStore().setDbDriver("com.mysql.jdbc.Driver");
    outputOperator.getStore().setDbUrl("jdbc:mysql://localhost:3306/phoenix");
    outputOperator.getStore().setConnectionProperties("user:kdbuser,password:KdBuSeR12!");
    outputOperator.setBatchSize(1);
    List<TestEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new TestEvent(i));
    }
    outputOperator.beginWindow(0);
    for (TestEvent event : events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();
  }

}
