//
// Copyright (c) 2024 Dynatrace LLC. All rights reserved.
//
// This software and associated documentation files (the "Software")
// are being made available by Dynatrace LLC for the sole purpose of
// illustrating the implementation of certain algorithms which have
// been published by Dynatrace LLC. Permission is hereby granted,
// free of charge, to any person obtaining a copy of the Software,
// to view and use the Software for internal, non-production,
// non-commercial purposes only – the Software may not be used to
// process live data or distributed, sublicensed, modified and/or
// sold either alone or as part of or in combination with any other
// software.
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
//
package com.dynatrace.exaloglogpaper;

import static com.dynatrace.hash4j.util.Preconditions.checkArgument;

import com.dynatrace.hash4j.distinctcount.HyperLogLog;
import com.dynatrace.hash4j.distinctcount.StateChangeObserver;
import com.dynatrace.hash4j.distinctcount.UltraLogLog;

/**
 * A martingale estimator, that can be used in conjunction with a distinct counter such as {@link
 * HyperLogLog} or {@link UltraLogLog} to obtain slightly more accurate estimates for
 * non-distributed data streams than the corresponding standard estimators. For distributed data
 * streams, which require merging of partial results into a final result, the use of this martingale
 * estimator is not very useful and therefore not recommended.
 *
 * <p>In order to get correct estimates, this estimator must be updated with every single add
 * operation of the corresponding distinct count data structure using {@link HyperLogLog#add(long,
 * StateChangeObserver)} and {@link UltraLogLog#add(long, StateChangeObserver)}, respectively.
 *
 * <p>The estimator will become invalid, if the associated data structure is modified using
 * non-element addition operations such as {@link HyperLogLog#add(HyperLogLog)} or {@link
 * UltraLogLog#add(UltraLogLog)}. It is possible to initiate a new martingale estimator using the
 * values returned by {@code getStateChangeProbability()} and {@code getDistinctCountEstimate()} of
 * the corresponding distinct counter. At that point, the martingale estimator returns the same
 * estimate as the standard estimator. However, if many further elements are added, the martingale
 * estimator may again produce better estimates.
 *
 * <p>The estimator remains valid if the associated data structure is downsized with {@link
 * HyperLogLog#downsize(int)} or {@link UltraLogLog#downsize(int)}.
 *
 * <p>References:
 *
 * <ul>
 *   <li>Ting, Daniel. "Streamed approximate counting of distinct elements: Beating optimal batch
 *       methods." Proceedings of the 20th ACM SIGKDD international conference on Knowledge
 *       discovery and data mining. 2014.
 *   <li>Cohen, Edith. "All-distances sketches, revisited: HIP estimators for massive graphs
 *       analysis." Proceedings of the 33rd ACM SIGMOD-SIGACT-SIGART symposium on Principles of
 *       database systems. 2014.
 *   <li>Pettie, Seth, Dingyu Wang, and Longhui Yin. "Non-mergeable sketching for cardinality
 *       estimation." arXiv preprint arXiv:2008.08739 (2020).
 * </ul>
 */
public final class MartingaleEstimator implements StateChangeObserver {

  private double distinctCountEstimate;
  private double stateChangeProbability;

  /** Constructor. */
  public MartingaleEstimator() {
    reset();
  }

  /** Resets the martingale estimator to its initial state. */
  public void reset() {
    this.distinctCountEstimate = 0;
    this.stateChangeProbability = 1;
  }

  /**
   * Constructor.
   *
   * <p>Can be used to continue estimation if an estimate of the current distinct count is known and
   * if the current state change probability of the accompanying sketch is known.
   *
   * @param distinctCountEstimate the current distinct count estimate
   * @param stateChangeProbability the current state change probability of the accompanying sketch
   */
  public MartingaleEstimator(double distinctCountEstimate, double stateChangeProbability) {
    set(distinctCountEstimate, stateChangeProbability);
  }

  /**
   * Sets the martingale estimator to a given distinct count estimate and a given state change
   * probability.
   *
   * <p>Can be used to continue estimation if an estimate of the current distinct count is known and
   * if the current state change probability of the accompanying sketch is known.
   *
   * @param distinctCountEstimate the current distinct count estimate
   * @param stateChangeProbability the current state change probability of the accompanying sketch
   */
  public void set(double distinctCountEstimate, double stateChangeProbability) {
    checkArgument(distinctCountEstimate >= 0, "Distinct count estimate must be non-negative!");
    checkArgument(
        stateChangeProbability >= 0 && stateChangeProbability <= 1,
        "State change probability must be in the range [0,1]!");
    this.distinctCountEstimate = distinctCountEstimate;
    if (stateChangeProbability
        <= 0) { // if state change probability == -0.0 set it to +0.0, to avoid negative infinite
      // estimates
      stateChangeProbability = 0.;
    }
    this.stateChangeProbability = stateChangeProbability;
  }

  /**
   * Returns the distinct count estimate.
   *
   * @return the distinct count estimate
   */
  public double getDistinctCountEstimate() {
    return distinctCountEstimate;
  }

  // visible for testing
  double getStateChangeProbability() {
    return stateChangeProbability;
  }

  @Override
  public void stateChanged(double probabilityDecrement) {
    distinctCountEstimate += 1. / stateChangeProbability;
    stateChangeProbability -= probabilityDecrement;
    if (stateChangeProbability <= 0) { // numerical errors could lead to negative probability
      stateChangeProbability =
          0; // set to zero in this case => next state change will set estimate = infinite
    }
  }

  @Override
  public String toString() {
    return MartingaleEstimator.class.getSimpleName()
        + "{"
        + "distinctCountEstimate="
        + distinctCountEstimate
        + ", stateChangeProbability="
        + stateChangeProbability
        + '}';
  }
}
