package org.apache.cgspark.input.generator;

import java.security.InvalidParameterException;
import java.util.Map;
import java.util.Random;

import org.apache.cgspark.core.DistributionType;
import org.apache.cgspark.core.Rectangle;

import com.google.common.collect.ImmutableMap;

/**
 * Factory that returns the the Point based on the {@link DistributionType}
 */
public final class PointGeneratorFactory {

    private final Rectangle mbr;
    private final Random random;
    private final double rho;
    private final Map<DistributionType, PointGenerator> map;

    public PointGeneratorFactory(final Rectangle mbr, final Random random,
                                 final double rho) {
        this.mbr = mbr;
        this.random = random;
        this.rho = rho;

        final ImmutableMap.Builder<DistributionType, PointGenerator> builder
                = ImmutableMap.builder();
        builder.put(DistributionType.UNIFORM, new UniformDistPointGenerator
                (this.mbr, this.random));
        builder.put(DistributionType.ANTI_CORRELATED, new
                AntiCorrelatedDistPointGenerator(this.mbr, this.random, this
                .rho));
        builder.put(DistributionType.CIRCLE, new CircleDistPointGenerator
                (this.mbr, this.random));
        builder.put(DistributionType.CORRELATED, new
                CorrelatedDistPointGenerator(this.mbr, this.random, this.rho));
        builder.put(DistributionType.GAUSSIAN, new GaussianDistPointGenerator
                (this.mbr, this.random));
        map = builder.build();
    }

    public PointGenerator getPointGenerator(final DistributionType type) {
        final PointGenerator pointGenerator = map.get(type);
        if (pointGenerator == null) {
            throw new InvalidParameterException("No PointGenerator present " +
                    "for: " + type);
        }
        return pointGenerator;
    }
}
