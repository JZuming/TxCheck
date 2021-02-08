/// @file
/// @brief randomness

#ifndef RANDOM_HH
#define RANDOM_HH

#define __FILE_RANDOM__
#define __RAND_FILE_NAME__ "rand_file.txt"

#include <random>
#include <utility>
#include <stdexcept>
#include <iterator>
#include <fstream>
#include <map>
#include <string>
#include <memory>
#include <iostream>

namespace smith {
  extern std::mt19937_64 rng;
}

using std::string;
using std::ifstream;
using std::map;
using std::pair;
using std::make_pair;
using std::shared_ptr;

int d6(), d9(), d12(), d20(), d42(), d100();
std::string random_identifier_generate();
int dx(int x);

struct file_random_machine {
    string filename;
    ifstream fin;
    int end_pos;
    static map<string, struct file_random_machine*> stream_map;
    static struct file_random_machine *get(string filename);
    file_random_machine(string s);
    ~file_random_machine();
    int get_random_num(int min, int max, int byte_num);
};

template<typename T> T& random_pick(std::vector<T>& container) {
    if (!container.size())
        throw std::runtime_error("No candidates available");

#ifndef __FILE_RANDOM__
    std::uniform_int_distribution<int> pick(0, container.size()-1);
    return container[pick(smith::rng)];
#else
    return container[dx(container.size()) - 1];
#endif
}

template<typename I>
I random_pick(I beg, I end) {
    if (beg == end)
      throw std::runtime_error("No candidates available");

#ifndef __FILE_RANDOM__
    std::uniform_int_distribution<> pick(0, std::distance(beg, end) - 1);
    std::advance(beg, pick(smith::rng));
    return beg;
#else
    std::advance(beg, dx(std::distance(beg, end)) - 1);
    return beg;
#endif
}

template<typename I>
I random_pick(std::pair<I,I> iters) {
  return random_pick(iters.first, iters.second);
}

#endif
