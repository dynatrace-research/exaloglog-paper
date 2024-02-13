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

#ifndef HYPERLOGLOGLOG_PACKED_MAP
#define HYPERLOGLOGLOG_PACKED_MAP

#include "PackedVector.hpp"

namespace hyperlogloglog {
  /**
   * This class represents a ``packed map'', that is, a dictionary
   * type that maps keys to values such that they are stored
   * internally in a Packed Vector with minimal bit usage (in terms of
   * multiples of word length).
   *
   * The internal representation is a sorted array.
   * There can be no multiples of keys.
   */
  template<typename Word = uint64_t>
  class PackedMap {
  public:
    /**
     * keySize : Number of bits per key
     * valueSize : Number of bits per value
     */
    PackedMap(size_t keySize, size_t valueSize) :
      keySize(keySize), valueSize(valueSize), elemSize(keySize + valueSize),
      keyMask(~(~((Word)0)<<keySize)), valueMask(~(~((Word)0)<<valueSize)),
      arr(elemSize) { }


    /**
     * Returns the number of key-value pairs stored
     */
    inline size_t size() const {
      return arr.size();
    }

    /**
     * Returns the ith value
     */
    Word at(size_t i) const {
      return arr.get(i) & valueMask;
    }

    /**
     * Returns the ith key
     */
    Word keyAt(size_t i) const {
      return arr.get(i) >> valueSize;
    }


    
    /**
     * Returns the index of the value associated with the key,
     * or a negative value if the key is not found.
     */
    int find(Word key) const {
      int l = 0;
      int r = size() - 1;
      while (l <= r) {
        int m = (l+r)/2;
        Word k = keyAt(m);
        if (k < key) 
          l = m+1;
        else if (k > key)
          r = m-1;
        else
          return m;
      }
      return -1;
    }



    /**
     * Adds a new key-value pair. If the key is already in the data
     * structure, its value will be replaced. Otherwise, the data
     * structure the pair will be added as a new element to the data
     * structure using insertion sort like addition.
     */
    void add(Word key, Word value) {
      int i = find(key);
      Word kv;
      packElement(kv, key, value);
      if (i >= 0)
        arr.set(i, kv);
      else {
        arr.append(kv);
        i = size()-1;
        while (i > 0 && keyAt(i-1) > key) {
          arr.set(i,arr.get(i-1));
          --i;
        }
        arr.set(i, kv);
      }
    }



    /**
     * Erases the given key from the array. If the key does not exist,
     * does not do anything.
     */
    inline void erase(Word key) {
      int i = find(key);
      if (i >= 0)
        eraseAt(i);
    }



    /**
     * Erases the element at the given position.
     */
    inline void eraseAt(size_t i) {
      arr.erase(i);
    }



    /**
     * Returns the number of bits inhabitet by the actual key/value
     * pairs (capacity might be larger).
     */
    inline size_t bitSize() const {
      return arr.bitSize();
    }
    
    
    inline size_t getAllocatedMemorySizeInBytes() const {
      return arr.getAllocatedMemorySizeInBytes();
    }
    
  private:
    inline void packElement(Word& kv, const Word& key, const Word& value) {
      kv = ((key & keyMask) << valueSize) | (value&valueMask);
    }

    
          
    size_t keySize;
    size_t valueSize;
    size_t elemSize;
    size_t keyMask; // keySize ones
    size_t valueMask; // valueSize ones
    PackedVector<Word> arr; // internal array
  };
}

#endif // HYPERLOGLOGLOG_PACKED_MAP
