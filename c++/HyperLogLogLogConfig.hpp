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
#ifndef HYPER_LOG_LOG_LOG_CONFIG
#define HYPER_LOG_LOG_LOG_CONFIG

#include "hyperlogloglog/HyperLogLogLog.hpp"

class HyperLogLogLogConfig {

	uint8_t p;

public:

	typedef typename hyperlogloglog::HyperLogLogLog<uint64_t> sketch_type;

	HyperLogLogLogConfig(uint8_t p) : p(p) {
	}

	hyperlogloglog::HyperLogLogLog<uint64_t>* createNew() const {
		return new hyperlogloglog::HyperLogLogLog<uint64_t>(1 << p, 3,
				hyperlogloglog::HyperLogLogLog < uint64_t
						> ::HYPERLOGLOGLOG_COMPRESS_WHEN_APPEND
						| hyperlogloglog::HyperLogLogLog < uint64_t
								> ::HYPERLOGLOGLOG_COMPRESS_TYPE_INCREASE);
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

	void add(hyperlogloglog::HyperLogLogLog<uint64_t> &sketch,
			const vector<uint8_t> &data) const {
		uint64_t hash[2];
		MurmurHash3_x86_128(&data[0], data.size(), UINT32_C(0x6e0a09bd), &hash);
		sketch.add(hash[0]);
	}

	double estimate(
			const hyperlogloglog::HyperLogLogLog<uint64_t> &sketch) const {
		return sketch.estimate();
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

	hyperlogloglog::HyperLogLogLog<uint64_t> merge(
			const hyperlogloglog::HyperLogLogLog<uint64_t> &sketch1,
			const hyperlogloglog::HyperLogLogLog<uint64_t> &sketch2) const {
		return sketch1.merge(sketch2);
	}
};

#endif // HYPER_LOG_LOG_LOG_CONFIG
