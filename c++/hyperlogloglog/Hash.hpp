// This file was taken and derived from 
// https://github.com/mkarppa/hyperlogloglog/tree/50670ae4d70b4f1164c94a6b7cc4b32e2bf70982.
//
// This file was originally published under the following license:
//
//
// MIT License
//
// Copyright (c) 2022 Matti Karppa
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#ifndef HYPERLOGLOGLOG_HASH
#define HYPERLOGLOGLOG_HASH

#include <climits>
#include <cstdint>
#include <string>

namespace hyperlogloglog {
  template<typename T, typename Word = uint64_t>
  Word fibonacciHash(const T& x, int b = CHAR_BIT*sizeof(Word));

  template<>
  inline uint64_t fibonacciHash(const uint64_t& x, int b) {
    static_assert(CHAR_BIT*sizeof(uint64_t) == 64);
    return 0x9e3779b97f4a7c15*x >> (64-b);
  }

  template<typename T, typename Word = uint64_t>
  Word farmhash(const T& x);

  template<>
  inline uint64_t farmhash(const std::string& x) {
    assert(false); // not used in our tests
    return 0;
  }

  template<>
  inline uint64_t farmhash(const uint64_t& x) {
    return x; // do not hash as the input is already random in our tests
  }
}

#endif // HYPERLOGLOGLOG_HASH
