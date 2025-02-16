//
// Copyright (c) 2023-2025 Dynatrace LLC. All rights reserved.
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
#include <fstream>
#include <random>
#include <string>
#include <vector>
#include <algorithm>
#include <memory>

#include "SpikeSketchConfig.hpp"
#include "HyperLogLogLogConfig.hpp"

using namespace std;

vector<uint64_t> getDistinctCounts(uint64_t max, double relativeStep) {
	vector < uint64_t > result;
	while (max > 0) {
		result.push_back(max);
		max = min(max - 1,
				static_cast<uint64_t>(ceil(max / (1 + relativeStep))));
	}
	reverse(result.begin(), result.end());
	return result;
}

class StatisticsBuilder {
private:
	const uint64_t numCycles;
	const uint64_t trueDistinctCount;
	vector<uint64_t> inMemorySizeInBytesValues;
	vector<uint64_t> serializationSizeInBytesValues;
	vector<double> distinctCountEstimateValues;
public:
	StatisticsBuilder(uint64_t numCycles, uint64_t trueDistinctCount) : numCycles(
			numCycles), trueDistinctCount(trueDistinctCount), inMemorySizeInBytesValues(
			numCycles), serializationSizeInBytesValues(numCycles), distinctCountEstimateValues(
			numCycles) {
	}

	void add(uint64_t cycleIndex, uint64_t inMemorySizeInBytes,
			uint64_t serializationSizeInBytes, double distinctCountEstimate) {
		inMemorySizeInBytesValues[cycleIndex] = inMemorySizeInBytes;
		serializationSizeInBytesValues[cycleIndex] = serializationSizeInBytes;
		distinctCountEstimateValues[cycleIndex] = distinctCountEstimate;
	}

	friend class Statistics;

};

class Statistics {

private:
	const uint64_t trueDistinctCount;
	const uint64_t count = 0;
	uint64_t sumInMemorySizeInBytes = 0;
	uint64_t sumInMemorySizeInBytesSquared = 0;
	uint64_t minimumInMemorySizeInBytes = numeric_limits < uint64_t > ::max();
	uint64_t maximumInMemorySizeInBytes = numeric_limits < uint64_t > ::min();
	uint64_t sumSerializationSizeInBytes = 0;
	uint64_t sumSerializationSizeInBytesSquared = 0;
	uint64_t minimumSerializationSizeInBytes = numeric_limits < uint64_t
			> ::max();
	uint64_t maximumSerializationSizeInBytes = numeric_limits < uint64_t
			> ::min();

	double sumDistinctCountEstimationError = 0;

	double sumDistinctCountEstimationErrorSquared = 0;

public:
	Statistics(const StatisticsBuilder &statisticsBuilder) : trueDistinctCount(
			statisticsBuilder.trueDistinctCount), count(
			statisticsBuilder.numCycles) {

		for (uint64_t i = 0; i < statisticsBuilder.numCycles; ++i) {
			uint64_t inMemorySizeInBytes =
					statisticsBuilder.inMemorySizeInBytesValues[i];
			uint64_t serializationSizeInBytes =
					statisticsBuilder.serializationSizeInBytesValues[i];
			double distinctCountEstimate =
					statisticsBuilder.distinctCountEstimateValues[i];
			minimumInMemorySizeInBytes = std::min(minimumInMemorySizeInBytes,
					inMemorySizeInBytes);
			maximumInMemorySizeInBytes = std::max(maximumInMemorySizeInBytes,
					inMemorySizeInBytes);
			sumInMemorySizeInBytes += inMemorySizeInBytes;
			sumInMemorySizeInBytesSquared += inMemorySizeInBytes
					* inMemorySizeInBytes;
			minimumSerializationSizeInBytes = std::min(
					minimumSerializationSizeInBytes, serializationSizeInBytes);
			maximumSerializationSizeInBytes = std::max(
					maximumSerializationSizeInBytes, serializationSizeInBytes);
			sumSerializationSizeInBytes += serializationSizeInBytes;
			sumSerializationSizeInBytesSquared += serializationSizeInBytes
					* serializationSizeInBytes;
			double distinctCountEstimationError = distinctCountEstimate
					- trueDistinctCount;
			sumDistinctCountEstimationError += distinctCountEstimationError;
			sumDistinctCountEstimationErrorSquared +=
					distinctCountEstimationError * distinctCountEstimationError;
		}
	}

	double getAverageSerializationSizeInBytes() const {
		return sumSerializationSizeInBytes / (double) count;
	}

	double getAverageInMemorySizeInBytes() const {
		return sumInMemorySizeInBytes / (double) count;
	}

	double getRelativeEstimationBias() const {
		return (sumDistinctCountEstimationError / count) / trueDistinctCount;
	}

	double getRelativeEstimationRmse() const {
		return sqrt(sumDistinctCountEstimationErrorSquared / count)
				/ trueDistinctCount;
	}

	uint64_t getTrueDistinctCount() const {
		return trueDistinctCount;
	}

	uint64_t getMinimumInMemorySizeInBytes() const {
		return minimumInMemorySizeInBytes;
	}

	uint64_t getMaximumInMemorySizeInBytes() const {
		return maximumInMemorySizeInBytes;
	}

	uint64_t getMinimumSerializationSizeInBytes() const {
		return minimumSerializationSizeInBytes;
	}

	uint64_t getMaximumSerializationSizeInBytes() const {
		return maximumSerializationSizeInBytes;
	}

	double getEstimatedInMemoryMVP() const {
		return getAverageInMemorySizeInBytes() * 8.
				* sumDistinctCountEstimationErrorSquared
				/ (static_cast<double>(count) * trueDistinctCount
						* trueDistinctCount);
	}

	double getEstimatedSerializationMVP() const {
		return getAverageSerializationSizeInBytes() * 8.
				* sumDistinctCountEstimationErrorSquared
				/ (static_cast<double>(count) * trueDistinctCount
						* trueDistinctCount);
	}

	double getStandardDeviationInMemorySizeInBytes() const {
		return sqrt(
				count * sumInMemorySizeInBytesSquared
						- sumInMemorySizeInBytes * sumInMemorySizeInBytes)
				/ static_cast<double>(count);
	}

	double getStandardDeviationSerializationSizeInBytes() const {
		return sqrt(
				count * sumSerializationSizeInBytesSquared
						- sumSerializationSizeInBytes
								* sumSerializationSizeInBytes)
				/ static_cast<double>(count);
	}
};

template<typename T> void test(const T &config = T()) {

	uint64_t num_cycles = 1000000;

	vector < uint64_t > distinct_counts;
	distinct_counts.push_back(1);
	distinct_counts.push_back(2);
	distinct_counts.push_back(5);
	distinct_counts.push_back(10);
	distinct_counts.push_back(20);
	distinct_counts.push_back(50);
	distinct_counts.push_back(100);
	distinct_counts.push_back(200);
	distinct_counts.push_back(500);
	distinct_counts.push_back(1000);
	distinct_counts.push_back(2000);
	distinct_counts.push_back(5000);
	distinct_counts.push_back(10000);
	distinct_counts.push_back(20000);
	distinct_counts.push_back(50000);
	distinct_counts.push_back(100000);
	distinct_counts.push_back(200000);
	distinct_counts.push_back(500000);
	distinct_counts.push_back(1000000);

	vector<StatisticsBuilder> statisticsBuilders;
	for (uint64_t distinct_count : distinct_counts) {
		statisticsBuilders.emplace_back(num_cycles, distinct_count);
	}

	mt19937_64 seed_rng(0);
	vector < uint64_t > seeds;
	for (uint64_t cycle_idx = 0; cycle_idx < num_cycles; ++cycle_idx) {
		seeds.push_back(seed_rng());
	}

	// construct all sketches in advance, which is necessary,
	// because SpikeSketch's constructor is not thread-safe and initializes global variables
	vector < unique_ptr<typename T::sketch_type> > sketches;
	for (uint64_t cycle_idx = 0; cycle_idx < num_cycles; ++cycle_idx) {
		sketches.emplace_back(config.createNew());
	}

#pragma omp parallel for default(none) shared(distinct_counts, num_cycles, config, seeds, sketches, statisticsBuilders)
	for (uint64_t cycle_idx = 0; cycle_idx < num_cycles; ++cycle_idx) {

		mt19937_64 rng(seeds[cycle_idx]);

		auto &sketch = *(sketches[cycle_idx]);

		uint64_t distinct_counts_idx = 0;
		uint64_t distinct_count = 0;
		while (true) {
			if (distinct_count == distinct_counts[distinct_counts_idx]) {
				statisticsBuilders[distinct_counts_idx].add(cycle_idx,
						config.getInMemorySizeInBytes(sketch),
						config.getSerializedSizeInBytes(sketch),
						config.estimate(sketch));
				distinct_counts_idx += 1;
				if (distinct_counts_idx == distinct_counts.size())
					break;
			}
			config.add(sketch, rng());
			distinct_count += 1;
		}
	}

	vector<Statistics> statistics;
	for (StatisticsBuilder statisticsBuilder : statisticsBuilders)
		statistics.emplace_back(statisticsBuilder);

	ofstream o(
			"../results/comparison-empirical-mvp/" + config.getLabel()
					+ ".csv");

	o << "number of cycles = " << num_cycles << "; data structure = "
			<< config.getLabel() << endl;
	o << "true distinct count";
	o << "; minimum memory size";
	o << "; average memory size";
	o << "; maximum memory size";
	o << "; standard deviation memory size";
	o << "; minimum serialization size";
	o << "; average serialization size";
	o << "; maximum serialization size";
	o << "; standard deviation serialization size";
	o << "; relative distinct count estimation bias";
	o << "; relative distinct count estimation rmse";
	o << "; estimated memory MVP";
	o << "; estimated serialization MVP";
	o << endl;

	for (Statistics s : statistics) {
		o << s.getTrueDistinctCount();
		o << "; " << s.getMinimumInMemorySizeInBytes();
		o << "; " << s.getAverageInMemorySizeInBytes();
		o << "; " << s.getMaximumInMemorySizeInBytes();
		o << "; " << s.getStandardDeviationInMemorySizeInBytes();
		o << "; " << s.getMinimumSerializationSizeInBytes();
		o << "; " << s.getAverageSerializationSizeInBytes();
		o << "; " << s.getMaximumSerializationSizeInBytes();
		o << "; " << s.getStandardDeviationSerializationSizeInBytes();
		o << "; " << s.getRelativeEstimationBias();
		o << "; " << s.getRelativeEstimationRmse();
		o << "; " << s.getEstimatedInMemoryMVP();
		o << "; " << s.getEstimatedSerializationMVP();
		o << endl;
	}
}

int main() {
	test(HyperLogLogLogConfig(11));
	test(SpikeSketchConfig(128));
}

