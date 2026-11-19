// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "../../src/runtime/vendor/xxhash/xxhash.h"
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <vector>
int main() {
 constexpr std::uint64_t c=0x9e3779b97f4a7c15ULL;
 for(auto dims: {std::array<int,2>{100,10000},{1000,1000},{10,100000}}) {
  for(int mode=0;mode<2;++mode) {
   std::vector<std::uint64_t> hashes;
   std::array<std::size_t,8> parts{};
   for(int a=0;a<dims[0];++a) for(int b=0;b<dims[1];++b) {
    std::array<std::uint64_t,3> words{static_cast<std::uint64_t>(a),static_cast<std::uint64_t>(b),0};
    std::uint64_t h=0;
    if(mode) h=XXH3_64bits(words.data(),sizeof(words));
    else for(auto v:words) h^=v+c+(h<<6)+(h>>2);
    hashes.push_back(h); ++parts[h&7];
   }
   std::sort(hashes.begin(),hashes.end());
   const auto unique=std::unique(hashes.begin(),hashes.end())-hashes.begin();
   std::printf("%d,%d,%s,%zu,%td,%zu,%zu\n",dims[0],dims[1],mode?"xxh3":"boxed-old",hashes.size(),unique,*std::min_element(parts.begin(),parts.end()),*std::max_element(parts.begin(),parts.end()));
  }
 }
}
