package phoenix.datatorrent.builder;

import java.util.HashMap;

/**
 * Enum defining list of dimensions
 * 
 * @author Vikas
 * 
 */
public enum DimensionEnum {

  PUBLISHER_ID("pubId"), LINE_ITEM_ID("lineItemId"), CREATIVE_ID("creativeId"), TARGET_ID(
      "targetId"), AD_UNIT_ID("adUnitId");

  private String dimensionName;

  private static final HashMap<String, DimensionEnum> enumMap =
      new HashMap<String, DimensionEnum>();

  private DimensionEnum(final String dimensionName) {
    this.dimensionName = dimensionName;
  }

  public String getDimensionName() {
    return this.dimensionName;
  }

  public static DimensionEnum fromString(final String key) {
    return enumMap.get(key);
  }

  static {
    for (DimensionEnum dimension : DimensionEnum.values()) {
      enumMap.put(dimension.getDimensionName(), dimension);
    }
  }

}
