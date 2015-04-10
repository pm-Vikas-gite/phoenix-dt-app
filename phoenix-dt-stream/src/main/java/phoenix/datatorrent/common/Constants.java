package phoenix.datatorrent.common;

public class Constants {
  // Logger fields
  public static final String PUBLISHER_ID = "pubID";
  public static final String SITE_ID = "sId";
  public static final String ADOM = "aDOM";
  public static final String WINNING_CAMPAIGN_ID = "wcId";
  public static final String DEAL_ID = "wdId";
  public static final String ECPM = "ecpm";
  public static final String CAMPAIGN_ECPM = "sp";
  public static final String PUBLISHER_ECPM = "pp";
  public static final String TIMESTAMP = "ts";
  public static final String NETWORK_ECPM = "ecpm";
  public static final String ADSERVER_ID = "wAdnId";
  public static final String DEFAULTS_AD_NETWORK_ID = "dAdnId";
  public static final String PASSBACK = "PB";
  public static final String CAMPAIGN_ARRAY = "cmpg";
  public static final String RTB_CAMPAIGN_ID = "id";
  public static final String BID_ARRAY = "bid";
  public static final String BID_ID = "bId";
  public static final String WINNING_BID_ID = "wbId";
  public static final String BID_STATUS = "status";
  public static final String BID_STATUS_ZB = "ZB";
  public static final String BID_STATUS_FLOOR_REJECT = "FF";
  public static final String BID_STATUS_SBF_REJECT = "SBFF";
  public static final String BID_STATUS_BLOCK_LIST = "DWLF";
  public static final String BID_STATUS_UNCAT = "UAF";
  public static final String BID_STATUS_OTHER = "AF";
  public static final String DSP_DEAL_RESPONSE = "redlId";
  public static final String SEAT_ID = "seat";
  public static final String PLTAFORM = "iT";
  public static final String LOGGER_PLTAFORM_DESKTOP = "ds";
  public static final String LOGGER_PLTAFORM_MOBILE_WEB = "mw";
  public static final String LOGGER_PLTAFORM_MOBILE_APP = "ma";
  public static final String FIRST_PRICE = "fp";
  public static final String LOGGER_MOB = "mob";
  public static final String LOGGER_MOB_OS = "os";
  public static final String LOGGER_MOB_APP_ID = "ai";
  public static final String LOGGER_DC_ID = "dcId";
  public static final String LOGGER_DC = "dc";
  public static final String AD_ID = "adId";
  public static final String LINE_ITEM_ID = "lineItemID";
  public static final String CREATIVE_ID = "creativeID";
  public static final String TARGET_ID = "targetID";
  public static final String AD_UNIT_ID = "adUnitID";
  public static final String IMPRESSIONS = "impressions";
  public static final String AD_REQUESTS = "adRequests";
  public static final String AD_IMPRESSION = "impression";
  
  // Tracker fields
  public static final String TRACKER_OPERATION_ID = "operId";
  public static final String TRACKER_PUBLISHER_ID = "pubId";
  public static final String TRACKER_SITE_ID = "siteId";
  public static final String TRACKER_WINNING_CAMPAIGN_ID = "campaignId";
  public static final String TRACKER_AD_ID = "adId";
  public static final String TRACKER_CREATIVE_ID = "creativeId";
  public static final String TRACKER_PIXEL_ID = "pixelId";
  public static final String TRACKER_INDIRECT_AD_ID = "indirectAdId";
  public static final String TRACKER_AD_NETWORK_ID = "adServerId";
  public static final String TRACKER_TIMESTAMP = "kltstamp";
  public static final String TRACKER_IS_MOBILE_APP = "isMobileApp";
  public static final String TRACKER_DEFAULT_SERVER_ID = "defaultedAdServerId";
  public static final String TRACKER_AD_SIZE_ID = "kadsizeid";
  public static final String TRACKER_MOB_FLAG = "mobflag";
  public static final String TRACKER_WINNING_DEAL_ID = "wDlId";
  public static final String TRACKER_WINNING_BUYER_ID = "wDSPByrId";
  public static final String TRACKER_PREDICTED_CTR = "pctr";
  public static final String TRACKER_DEFAULT_ADNETWORK_ID = "defaultedAdServerId";
  public static final String TRACKER_DEFAULT_CAMP_ID = "defcmpgId";
  public static final String TRACKER_DEFAULTED_ACTUAL_ECPM = "defactecpm";
  public static final String TRACKER_DEFAULTED_ECPM = "defecpm";
  public static final String TRACKER_ECPM = "kefact";
  public static final String TRACKER_ACTUAL_ECPM = "kpbmtpfact";
  public static final String TRACKER_DEFAULT_HANDLED = "defaultReq";
  public static final String TRACKER_MOBILE_OS_ID = "osid";
  public static final String TRACKER_COUNTRY = "country";
  public static final String TUPLE_TIMESTAMP = "tracker_t_stamp";
  public static final String TRACKER_PASSBACK = "passback";
  public static final String TRACKER_FIRST_PRICE = "fp";
  public static final String BID_DEAL_ID = "rdId";
  public static final String TRACKER_DC_ID = "dcId";
  public static final String TRACKER_DCID = "dcid";

  // Application configs
  public static final String APP_NAME = "app.name";
  public static final String APP_DEFAULT_NAME = "PhoenixRealTimeFeedbackApp";

    public static final String HDFS_OFFSET_FILE_PATH = "hdfs.offset.file.path";
  public static final String HDFS_OFFSET_DEFAULT_NYC_FILE_PATH = "/tmp/nyc-kafkaOffsetPath";
  public static final String HDFS_OFFSET_DEFAULT_NYC2_FILE_PATH = "/tmp/nyc-kafkaOffsetPath2";
  public static final String HDFS_OFFSET_DEFAULT_AMS_FILE_PATH = "/tmp/ams-kafkaOffsetPath";
  public static final String HDFS_OFFSET_DEFAULT_SFO_FILE_PATH = "/tmp/sfo-kafkaOffsetPath";
  public static final String HDFS_OFFSET_FILE_DELIMITER = ":";

  
}
