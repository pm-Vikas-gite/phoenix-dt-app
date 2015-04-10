/*package phoenix.datatorrent.operator.output.mysql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import twitter4j.Status;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.lib.db.jdbc.AbstractJdbcTransactionableOutputOperator;

@ApplicationAnnotation(name = "TwitterDumpDemo")
public class TwitterDumpApplication implements StreamingApplication {
  public static class Status2Database extends AbstractJdbcTransactionableOutputOperator<Status> {
    public static final String INSERT_STATUS_STATEMENT =
        "insert into tweets (window_id, creation_date, text, userid) values (?, ?, ?, ?)";

    public Status2Database() {
      store.setMetaTable("dt_window_id_tracker");
      store.setMetaTableAppIdColumn("dt_application_id");
      store.setMetaTableOperatorIdColumn("dt_operator_id");
      store.setMetaTableWindowColumn("dt_window_id");
    }

    @Nonnull
    @Override
    protected String getUpdateCommand() {
      return INSERT_STATUS_STATEMENT;
    }

    @Override
    protected void setStatementParameters(PreparedStatement statement, Status tuple)
        throws SQLException {
      statement.setLong(1, currentWindowId);
      statement.setDate(2, new java.sql.Date(tuple.getCreatedAt().getTime()));
      statement.setString(3, tuple.getText());
      statement.setString(4, tuple.getUser().getScreenName());
      statement.addBatch();
    }
  }

  public void populateDAG(DAG dag, Configuration conf) {
    // dag.setAttribute(DAGContext.APPLICATION_NAME, "TweetsDump");
    TwitterSampleInput twitterStream = dag.addOperator("TweetSampler", new TwitterSampleInput());
    // ConsoleOutputOperator dbWriter = dag.addOperator("DatabaseWriter", new
    // ConsoleOutputOperator());
    Status2Database dbWriter = dag.addOperator("DatabaseWriter", new Status2Database());
    dbWriter.getStore().setDbDriver("com.mysql.jdbc.Driver");
    dbWriter.getStore().setDbUrl("jdbc:mysql://node6.morado.com:3306/twitter");
    dbWriter.getStore().setConnectionProperties("user:twitter");
    dag.addStream("Statuses", twitterStream.status, dbWriter.input).setLocality(
        Locality.CONTAINER_LOCAL);
  }
}
*/