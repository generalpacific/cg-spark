package org.apache.cgspark.core;

import java.security.InvalidParameterException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;

/**
 * Distribution Type of the data.
 */
public enum DistributionType {
  UNIFORM("uni"), GAUSSIAN("gaus"), CORRELATED("cor"), ANTI_CORRELATED("anti"), CIRCLE(
      "circle");

  private final String name;
  private final static Map<String, DistributionType> map;

  static {
    final ImmutableMap.Builder<String, DistributionType> builder =
        ImmutableMap.builder();
    for (DistributionType type : DistributionType.values()) {
      builder.put(type.name, type);
    }
    map = builder.build();
  }


  private DistributionType(final String name) {
    this.name = name;
  }

  public static DistributionType fromName(final String name) {
    if (StringUtils.isBlank(name)) {
      throw new InvalidParameterException(
          "Name cannot be blank for DistributionType");
    }
    final DistributionType type = map.get(name);
    if (type == null) {
      throw new InvalidParameterException("Invalid name for DistributionType: "
          + name);
    }
    return type;
  }
}
