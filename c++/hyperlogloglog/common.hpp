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

#ifndef HYPERLOGLOGLOG_COMMON
#define HYPERLOGLOGLOG_COMMON

#include <arpa/inet.h>
#include <cstdint>

namespace hyperlogloglog {
  template<typename T>
  inline int clz(T x);

  template<>
  inline int clz(unsigned int x) {
    return __builtin_clz(x);
  }

  template<>
  inline int clz(unsigned long x) {
    return __builtin_clzl(x);
  }

  template<>
  inline int clz(unsigned long long x) {
    return __builtin_clzll(x);
  }

  template<typename T>
  int rho(T x) {
    return clz(x) + 1;
  }

  template<typename T>
  constexpr T log2i(T x) {
    return x < 2 ? 0 : 1 + log2i(x >> 1);
  }



#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#ifndef htonll // MacOS X defines this as a macro
    inline uint64_t htonll(uint64_t x) {
    return (static_cast<uint64_t>(htonl(x & 0xffffffff)) << 32) |
      (htonl(x >> 32));
  }

  inline uint64_t ntohll(uint64_t x) {
    return (static_cast<uint64_t>(ntohl(x & 0xffffffff)) << 32) |
      (htonl(x >> 32));
  }
#endif // htonll
#endif // __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
}

#endif // HYPERLOGLOGLOG_COMMON
