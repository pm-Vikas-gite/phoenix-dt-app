package phoenix.datatorrent.operator.output.mysql;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import phoenix.datatorrent.operator.aggregate.Aggregate;

import com.datatorrent.api.Context;
import com.datatorrent.lib.db.jdbc.AbstractJdbcTransactionableOutputOperator;

public class JdbcTransactionableOutputOperator extends AbstractJdbcTransactionableOutputOperator<Aggregate>
{
  public static final String INSERT_STATUS_STATEMENT =
      "insert into HourlyLineItemDelivery1 (date, line_item_id, creative_id, target_id, ad_unit_id, `key`, `value`) values (?, ?, ?, ?, ?, ?, ?) "
      + "ON DUPLICATE KEY UPDATE value=value+?";

  /*public static final String INSERT_STATUS_STATEMENT =
      "insert into HourlyLineItemDelivery (date, line_item_id, creative_id, target_id, ad_unit_id, `key`, `value`) values (?, ?, ?, ?, ?, ?, ?)";*/
  
  public void setup(Context.OperatorContext context){
    super.setup(context);
  }
    
  @Override
  protected String getUpdateCommand() {
    return INSERT_STATUS_STATEMENT;
  }

  @Override
  protected void setStatementParameters(PreparedStatement statement, Aggregate tuple) throws SQLException {

    try {
      statement.setString(1, getDate(tuple.getTimestamp()));
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    //statement.setDate(1, new java.sql.Date(tuple.getTimestamp()));
    statement.setInt(2, tuple.getLineItemId());
    statement.setInt(3, tuple.getCreativeId());
    statement.setInt(4, tuple.getTargetId());
    statement.setInt(5, tuple.getAdUnitId());
    statement.setString(6, "impressions");
    statement.setInt(7, (int) tuple.getPaidImpressions());
    statement.setInt(8, (int) tuple.getPaidImpressions());
//    statement.addBatch();
  }

  private String getDate(long timestamp) throws ParseException {
    Date date = new Date(timestamp);
    SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    //format.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
    format.setTimeZone(TimeZone.getTimeZone("GMT"));
    String formatted = format.format(date);
    java.util.Date utilDate = format.parse(formatted);
    java .sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
      
    return formatted;
  }

}
