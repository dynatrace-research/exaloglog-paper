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
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "hyperlogloglog/HyperLogLogLog.hpp"

class HyperLogLogLogConfig {

	uint8_t p;

public:
	HyperLogLogLogConfig(uint8_t p) : p(p) {
	}

	hyperlogloglog::HyperLogLogLog<uint64_t> create() const {
		return hyperlogloglog::HyperLogLogLog < uint64_t
				> (1 << p, 3, hyperlogloglog::HyperLogLogLog < uint64_t
						> ::HYPERLOGLOGLOG_COMPRESS_WHEN_APPEND
						| hyperlogloglog::HyperLogLogLog < uint64_t
								> ::HYPERLOGLOGLOG_COMPRESS_TYPE_INCREASE);
	}

	void add(hyperlogloglog::HyperLogLogLog<uint64_t> &sketch,
			uint64_t hash) const {
		sketch.add(hash);
	}

	double estimate(
			const hyperlogloglog::HyperLogLogLog<uint64_t> &sketch) const {
		double estimate = sketch.estimate();
		assert(estimate == estimate);
		return estimate;
	}

	size_t getInMemorySizeInBytes(
			const hyperlogloglog::HyperLogLogLog<uint64_t> &sketch) const {
		return sketch.in_memory_size_in_bytes();
	}

	size_t getSerializedSizeInBytes(
			const hyperlogloglog::HyperLogLogLog<uint64_t> &sketch) const {
		return (sketch.bitSize() + 7) / 8;
	}

	std::string getLabel() const {
		return "HyperLogLogLog (p = " + std::to_string(p) + ")";
	}
};

std::vector<uint64_t> getDistinctCounts(uint64_t max, double relativeStep) {
	std::vector < uint64_t > result;
	while (max > 0) {
		result.push_back(max);
		max = std::min(max - 1,
				static_cast<uint64_t>(std::ceil(max / (1 + relativeStep))));
	}
	std::reverse(result.begin(), result.end());
	return result;
}

class Statistics {

private:
	const uint64_t trueDistinctCount;
	uint64_t sumInMemorySizeInBytes = 0;
	uint64_t minimumInMemorySizeInBytes = std::numeric_limits < uint64_t
			> ::max();
	uint64_t maximumInMemorySizeInBytes = std::numeric_limits < uint64_t
			> ::min();
	uint64_t sumSerializationSizeInBytes = 0;
	uint64_t minimumSerializationSizeInBytes = std::numeric_limits < uint64_t
			> ::max();
	uint64_t maximumSerializationSizeInBytes = std::numeric_limits < uint64_t
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
		count += 1;
		minimumInMemorySizeInBytes = std::min(minimumInMemorySizeInBytes,
				inMemorySizeInBytes);
		maximumInMemorySizeInBytes = std::max(maximumInMemorySizeInBytes,
				inMemorySizeInBytes);
		sumInMemorySizeInBytes += inMemorySizeInBytes;
		minimumSerializationSizeInBytes = std::min(
				minimumSerializationSizeInBytes, serializedSizeInBytes);
		maximumSerializationSizeInBytes = std::max(
				maximumSerializationSizeInBytes, serializedSizeInBytes);
		sumSerializationSizeInBytes += serializedSizeInBytes;
		double distinctCountEstimationError = distinctCountEstimate
				- trueDistinctCount;
		sumDistinctCountEstimationError += distinctCountEstimationError;
		sumDistinctCountEstimationErrorSquared += distinctCountEstimationError
				* distinctCountEstimationError;
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
		return std::sqrt(sumDistinctCountEstimationErrorSquared / count)
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
};

template<typename T> void test(const T &config = T()) {

	std::mt19937_64 rng(0);

	uint64_t num_cycles = 100000;

	std::vector < uint64_t > distinct_counts;
	distinct_counts.push_back(1000000);
	std::vector<Statistics> data;
	for (uint64_t distinct_count : distinct_counts) {
		data.emplace_back(distinct_count);
	}

	for (uint64_t i = 0; i < num_cycles; ++i) {
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

	std::ofstream o(
			"results/comparison-empirical-mvp/" + config.getLabel() + ".csv");

	o << "number of cycles = " << num_cycles << "; data structure = "
			<< config.getLabel() << std::endl;
	o << "true distinct count";
	o << "; minimum memory size";
	o << "; average memory size";
	o << "; maximum memory size";
	o << "; minimum serialization size";
	o << "; average serialization size";
	o << "; maximum serialization size";
	o << "; relative distinct count estimation bias";
	o << "; relative distinct count estimation rmse";
	o << "; estimated memory MVP";
	o << "; estimated serialization MVP";
	o << std::endl;

	for (Statistics s : data) {
		o << s.getTrueDistinctCount();
		o << "; " << s.getMinimumInMemorySizeInBytes();
		o << "; " << s.getAverageInMemorySizeInBytes();
		o << "; " << s.getMaximumInMemorySizeInBytes();
		o << "; " << s.getMinimumSerializationSizeInBytes();
		o << "; " << s.getAverageSerializationSizeInBytes();
		o << "; " << s.getMaximumSerializationSizeInBytes();
		o << "; " << s.getRelativeEstimationBias();
		o << "; " << s.getRelativeEstimationRmse();
		o << "; " << s.getEstimatedInMemoryMVP();
		o << "; " << s.getEstimatedSerializationMVP();
		o << std::endl;
	}
}

int main() {
#pragma omp parallel
#pragma omp single
	{
#pragma omp task
		test(HyperLogLogLogConfig(13));

	}
}

