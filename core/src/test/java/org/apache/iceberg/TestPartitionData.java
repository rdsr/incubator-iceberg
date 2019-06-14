package org.apache.iceberg;

import org.junit.Test;

import static org.apache.iceberg.types.Types.*;

public class TestPartitionData {
  private static final Schema SCHEMA = new Schema(
      NestedField.required(1, "a_struct", StructType.of(
          NestedField.required(2, "a_primitive", LongType.get())))
  );

  @Test
  public void testNestedField() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("a_struct.a_primitive").build();
    new PartitionData(spec.partitionType());
  }
}
