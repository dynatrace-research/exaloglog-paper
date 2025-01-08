//
// Copyright (c) 2023-2024 Dynatrace LLC. All rights reserved.
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
#include <iostream>
#include <chrono>

#include "SpikeSketchConfig.hpp"
#include "HyperLogLogLogConfig.hpp"

using namespace std;

template<typename T>
void measureMergingAndEstimation(ofstream &f, string label, uint64_t numElements,
		uint64_t dataSize, uint64_t numRepetitions, const T &config = T()) {

	mt19937_64 rng(0);

	vector<typename T::sketch_type> sketches;
	for (uint64_t i = 0; i < numRepetitions*2; ++i) {
		typename T::sketch_type sketch = config.create();
		for (uint64_t c = 0; c < numElements; ++c) {
			config.add(sketch, rng());
		}
		sketches.push_back(sketch);
	}

	auto beginMeasurement = chrono::high_resolution_clock::now();
    for(uint64_t i = 0; i < numRepetitions*2; i+=2) {
       config.estimate(config.merge(sketches[i+0], sketches[i+1]));
    }
	auto endMeasurement = chrono::high_resolution_clock::now();
	double mergeAndEstimationTimeInMicroSeconds = chrono::duration<double>(
			endMeasurement - beginMeasurement).count()
			/ static_cast<double>(numRepetitions) * 1e6;
	f << label << "; " << numElements << "; " << mergeAndEstimationTimeInMicroSeconds
			<< endl;
}

int main() {
	uint64_t numRepetitions = 10000;
	uint64_t dataSize = 16;

	ofstream f("../results/benchmarks/benchmark-results-merge-and-estimation-cpp.csv");

	f << "dataSize=" << dataSize << "; numRepetitions=" << numRepetitions
			<< endl;
	f << "data structure; distinct count; merge time (us)" << endl;

	vector < uint64_t > distinctCounts = { 1,2,5,10,20,50, 100, 200, 500, 1000, 2000, 5000,
			10000, 20000, 50000, 100000, 200000, 500000, 1000000 };

    // merge operation of SpikeSketch is flawed -> no merge benchmark for spike sketch
    // see https://github.com/duyang92/SpikeSketch/issues/1
	/*for (uint64_t distinctCount : distinctCounts) {
		measureMergingAndEstimation(f, "SpikeSketch (128 buckets)", distinctCount,
				dataSize, numRepetitions, SpikeSketchConfig(128));
	}*/
	for (uint64_t distinctCount : distinctCounts) {
		measureMergingAndEstimation(f, "HyperLogLogLog (p=11)", distinctCount, dataSize,
				numRepetitions, HyperLogLogLogConfig(11));
	}

}

