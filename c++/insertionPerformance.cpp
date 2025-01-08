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
#include <iostream>
#include <chrono>

#include "SpikeSketchConfig.hpp"
#include "HyperLogLogLogConfig.hpp"

using namespace std;

template<typename T>
void measureInsertion(ofstream &f, string label, uint64_t numElements,
		uint64_t dataSize, uint64_t numRepetitions, const T &config = T()) {

	using bytes_randomizer = independent_bits_engine<mt19937_64, CHAR_BIT, uint8_t>;
	mt19937_64 rng(0);
	bytes_randomizer bytes(rng);

	vector < vector < uint8_t >> data;
	for (uint64_t i = 0; i < numElements; ++i) {
		vector < uint8_t > d(dataSize);
		generate(d.begin(), d.end(), ref(bytes));
		data.push_back(d);
	}

	auto beginMeasurement = chrono::high_resolution_clock::now();

	for (uint64_t a = 0; a < numRepetitions; ++a) {
		auto sketch = config.create();
		for (const auto &d : data) {
			config.add(sketch, d);
		}
	}
	auto endMeasurement = chrono::high_resolution_clock::now();
	double insertionTimeInNanoSeconds = chrono::duration<double>(
			endMeasurement - beginMeasurement).count()
			/ static_cast<double>(numRepetitions) / numElements * 1e9;
	f << label << "; " << numElements << "; " << insertionTimeInNanoSeconds
			<< endl;
}

int main() {
	uint64_t numRepetitions = 1000;
	uint64_t dataSize = 16;

	ofstream f("../results/benchmarks/benchmark-results-insertion-cpp.csv");

	f << "dataSize=" << dataSize << "; numRepetitions=" << numRepetitions
			<< endl;
	f << "data structure; distinct count; insertion time (ns)" << endl;

	vector < uint64_t > distinctCounts = { 1, 2, 5, 10, 20, 50, 100, 200, 500,
			1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000,
			1000000 };

	for (uint64_t distinctCount : distinctCounts) {
		measureInsertion(f, "SpikeSketch (128 buckets)", distinctCount,
				dataSize, numRepetitions, SpikeSketchConfig(128));
	}
	for (uint64_t distinctCount : distinctCounts) {
		measureInsertion(f, "HyperLogLogLog (p=11)", distinctCount, dataSize,
				numRepetitions, HyperLogLogLogConfig(11));
	}

}

