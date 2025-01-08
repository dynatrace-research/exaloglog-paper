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

class Statistics {

private:
	const uint64_t trueDistinctCount;
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
	uint64_t count = 0;

	double sumDistinctCountEstimationError = 0;

	double sumDistinctCountEstimationErrorSquared = 0;

public:
	Statistics(uint64_t trueDistinctCount) : trueDistinctCount(
			trueDistinctCount) {
	}

	void add(uint64_t inMemorySizeInBytes, uint64_t serializedSizeInBytes,
			double distinctCountEstimate) {
#pragma omp critical 
		{
			count += 1;
			minimumInMemorySizeInBytes = std::min(minimumInMemorySizeInBytes,
					inMemorySizeInBytes);
			maximumInMemorySizeInBytes = std::max(maximumInMemorySizeInBytes,
					inMemorySizeInBytes);
			sumInMemorySizeInBytes += inMemorySizeInBytes;
			sumInMemorySizeInBytesSquared += inMemorySizeInBytes
					* inMemorySizeInBytes;
			minimumSerializationSizeInBytes = std::min(
					minimumSerializationSizeInBytes, serializedSizeInBytes);
			maximumSerializationSizeInBytes = std::max(
					maximumSerializationSizeInBytes, serializedSizeInBytes);
			sumSerializationSizeInBytes += serializedSizeInBytes;
			sumSerializationSizeInBytesSquared += serializedSizeInBytes
					* serializedSizeInBytes;
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
	vector<Statistics> data;
	for (uint64_t distinct_count : distinct_counts) {
		data.emplace_back(distinct_count);
	}

	mt19937_64 seed_rng(0);
	vector < uint64_t > seeds;
	for (uint64_t i = 0; i < num_cycles; ++i) {
		seeds.push_back(seed_rng());
	}

#pragma omp parallel for
	for (uint64_t i = 0; i < num_cycles; ++i) {

		mt19937_64 rng(seeds[i]);

		auto sketch = config.create();

		uint64_t distinct_counts_idx = 0;
		uint64_t distinct_count = 0;
		while (true) {
			if (distinct_count == distinct_counts[distinct_counts_idx]) {
				data[distinct_counts_idx].add(
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

	for (Statistics s : data) {
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

