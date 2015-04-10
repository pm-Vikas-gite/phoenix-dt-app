package phoenix.datatorrent.utils;

import java.util.HashMap;

public class Checker {

  private static HashMap<Object, Object> map = null;

  static {
    map = new HashMap<Object, Object>();
    map.put(Integer.class, new Integer(0));
    map.put(Long.class, new Long(0));
    map.put(Double.class, new Double(0));
    map.put(String.class, new String());
  }

  @SuppressWarnings("unchecked")
  public static <T> T check(Object param, Class<T> type) {
    return (null != param) ? (T) param : (T) map.get(type);
  }

}
