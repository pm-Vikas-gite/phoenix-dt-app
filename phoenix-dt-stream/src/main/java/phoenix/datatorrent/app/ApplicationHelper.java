package phoenix.datatorrent.app;

import java.util.List;
import java.util.concurrent.TimeUnit;
import phoenix.datatorrent.builder.DimensionEnum;
import phoenix.datatorrent.builder.DimensionSpecificationBuilder;
import phoenix.datatorrent.common.DimensionPartitionType;
import phoenix.datatorrent.model.DimensionKey;

final class ApplicationHelper {

  private ApplicationHelper() {}

  static void phoenixFeedbackLoopSpec(final List<DimensionKey> feedbackSpec) {

    feedbackSpec.addAll(new DimensionSpecificationBuilder()
        .addDimension(DimensionEnum.PUBLISHER_ID).addDimension(DimensionEnum.LINE_ITEM_ID)
        .addDimension(DimensionEnum.CREATIVE_ID).addDimension(DimensionEnum.TARGET_ID)
        .addDimension(DimensionEnum.AD_UNIT_ID).addTimeUnit(TimeUnit.DAYS)
        .setDimensionPartitionType(DimensionPartitionType.FEEDBACK_VIEW).build());

    feedbackSpec.addAll(new DimensionSpecificationBuilder()
        .addDimension(DimensionEnum.PUBLISHER_ID).addDimension(DimensionEnum.LINE_ITEM_ID)
        .addDimension(DimensionEnum.CREATIVE_ID).addDimension(DimensionEnum.TARGET_ID)
        .addDimension(DimensionEnum.AD_UNIT_ID).addTimeUnit(TimeUnit.HOURS)
        .setDimensionPartitionType(DimensionPartitionType.FEEDBACK_VIEW).build());
  }
}
