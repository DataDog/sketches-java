package com.datadoghq.sketch.ddsketch.mapping;

/**
 * A mapping between {@code double} values and {@code int} values that imposes relative guarantees on the composition
 * of {@link #value} and {@link #index}. Specifically, for any value {@code v} between {@link #minIndexableValue()}
 * and {@link #maxIndexableValue()}, implementations of {@link IndexMapping} must be such that {@code value(index(v))}
 * is close to {@code v} with a relative error that is less than {@link #relativeAccuracy()}.
 *
 * <table border="1" style="width:100%">
 * <tr>
 * <td> Mapping </td>
 * <td> Index usage overhead given actual guarantee </td>
 * <td> Max index usage overhead given requested guarantee </td>
 * <td> Computational cost </td>
 * </tr>
 * <tr>
 * <td> {@link LogarithmicMapping} </td>
 * <td> 0% (optimal) </td>
 * <td> 0% (optimal) </td>
 * <td> 100% (reference) </td>
 * </tr>
 * <tr>
 * <td> {@link QuadraticallyInterpolatedMapping} </td>
 * <td> ~8% ({@code 3/(4*log(2))-1}) </td>
 * <td> ~8% ({@code 3/(4*log(2))-1}) </td>
 * <td> ~25% </td>
 * </tr>
 * <tr>
 * <td> {@link LinearlyInterpolatedMapping} </td>
 * <td> ~44% ({@code 1/log(2)-1}) </td>
 * <td> ~44% ({@code 1/log(2)-1}) </td>
 * <td> ~20% </td>
 * </tr>
 * <tr>
 * <td> {@link BitwiseLinearlyInterpolatedMapping} </td>
 * <td> ~44% ({@code 1/log(2)-1}) </td>
 * <td> ~189% ({@code 2/log(2)-1}) </td>
 * <td> ~15% </td>
 * </tr>
 * <caption>caption</caption>
 * </table>
 */
public interface IndexMapping {

    int index(double value);

    double value(int index);

    double relativeAccuracy();

    double minIndexableValue();

    double maxIndexableValue();
}
