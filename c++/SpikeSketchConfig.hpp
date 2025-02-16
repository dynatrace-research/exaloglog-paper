//
// Copyright (c) 2024-2025 Dynatrace LLC. All rights reserved.
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
#ifndef SPIKE_SKETCH_CONFIG
#define SPIKE_SKETCH_CONFIG

#include <vector>
#include <cassert>

#include "SpikeSketch/SpikeSketch/spike_sketch_extend.h"

void merge(vector<vector<spike_sketch>> spikeSketchArray,
		vector<spike_sketch> &merged_ss);

double mutiBktQuery(vector<spike_sketch> &spikeSketchArray, double alpha0,
		double alpha1, double beta0, double beta1, double coe);

class SpikeSketchConfig {

	static constexpr int n = 20; //Number of cells in a single bucket
	static constexpr int ncode = 4; //Number of bits in a single cell
	static constexpr int p = 12;

	const uint32_t numOfBuckets;

	static constexpr uint32_t seed = 0x529b9601;

public:

	typedef vector<spike_sketch> sketch_type;

	SpikeSketchConfig(uint32_t numOfBuckets) : numOfBuckets(numOfBuckets) {

	}

	vector<spike_sketch>* createNew() const {
		vector < spike_sketch > *spikeSketchArray = new vector<spike_sketch>();
		spikeSketchArray->reserve(numOfBuckets);
		for (uint32_t bktIdx = 0; bktIdx < numOfBuckets; bktIdx++) {
			spikeSketchArray->emplace_back(n, p, ncode, seed);
		}
		return spikeSketchArray;
	}

	vector<spike_sketch> create() const {
		vector < spike_sketch > spikeSketchArray;
		spikeSketchArray.reserve(numOfBuckets);
		for (uint32_t bktIdx = 0; bktIdx < numOfBuckets; bktIdx++) {
			spikeSketchArray.emplace_back(n, p, ncode, seed);
		}
		return spikeSketchArray;
	}

	void add(vector<spike_sketch> &sketch, uint64_t hash) const {
		uint32_t tempInt32 = 0;
		MurmurHash3_x86_32(&hash, 8, seed + 231321, &tempInt32);
		spike_sketch &ss = sketch[tempInt32 % numOfBuckets];
		ss.update(hash);
	}

	void add(vector<spike_sketch> &sketch, const vector<uint8_t> &data) const {
		uint64_t hash[2];
		MurmurHash3_x86_128(&data[0], data.size(), UINT32_C(0x6e0a09bd), &hash);
		spike_sketch &ss = sketch[hash[1] % numOfBuckets];
		ss.update(hash[0]);
	}

	double estimate(const vector<spike_sketch> &sketch) const {

		double alpha0 = 0.1;
		double alpha1 = 0.88;
		double beta0 = 1.12;
		double beta1 = 1.46;
		double myCoe = 0.573; //Coefficient of correction

		double estimate = mutiBktQuery(
				const_cast<vector<spike_sketch>&>(sketch), alpha0, alpha1,
				beta0, beta1, myCoe);
		return estimate;
	}

	size_t getInMemorySizeInBytes(const vector<spike_sketch> &sketch) const {
		return static_cast<size_t>(numOfBuckets) * 8;
	}

	size_t getSerializedSizeInBytes(const vector<spike_sketch> &sketch) const {
		return static_cast<size_t>(numOfBuckets) * 8;
	}

	std::string getLabel() const {
		return "SpikeSketch (numOfBuckets = " + std::to_string(numOfBuckets)
				+ ")";
	}

	vector<spike_sketch> merge(const vector<spike_sketch> &sketch1,
			const vector<spike_sketch> &sketch2) const {
		vector < spike_sketch > merged = create();
		vector < vector < spike_sketch >> sketches = { sketch1, sketch2 };
		::merge(sketches, merged);
		return merged;
	}
};

#endif // SPIKE_SKETCH_CONFIG
