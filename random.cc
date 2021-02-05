#include "random.hh"

namespace smith {
  std::mt19937_64 rng;
}

int d6() {
  static std::uniform_int_distribution<> pick(1, 6);
  return pick(smith::rng);
}

int d9() {
  static std::uniform_int_distribution<> pick(1, 9);
  return pick(smith::rng);
}

int d12() {
  static std::uniform_int_distribution<> pick(1, 12);
  return pick(smith::rng);
}

int d20() {
  static std::uniform_int_distribution<> pick(1, 20);
  return pick(smith::rng);
}

int d42() {
  static std::uniform_int_distribution<> pick(1, 42);
  return pick(smith::rng);
}

int d100() {
  static std::uniform_int_distribution<> pick(1, 100);
  return pick(smith::rng);
}

int dx(int x) {
    std::uniform_int_distribution<> pick(1, x);
    return pick(smith::rng);
}

std::string abnormal_random_identifier_generate() {
#define MAX_NAME_LENTH 64
#define MAX_NAME_SCOPE 63
    
    int name_length;
    if (dx(10) < 10) // 9/10
        name_length = dx(MAX_NAME_LENTH / 8) + 1;
    else // 1/10
        name_length = dx(MAX_NAME_LENTH + 8) + 1; // illegal name length

    std::string name;
    for (int i = 0; i < name_length; i++) {
        int choice;
        if (i == 0)
            choice = dx(MAX_NAME_SCOPE - 10);
        else 
            choice = dx(MAX_NAME_SCOPE + 3);

        if (choice <= 26) 
            name.push_back('a' + choice);
        else if (choice <= 52)
            name.push_back('A' + choice - 26);
        else if (choice <= 53) 
            name.push_back('_');
        else if (choice <= 63)
            name.push_back('0' + choice - 53);
        else 
            name.push_back(dx(256) - 1); // illegal name
    }
    return name;
}

std::string random_identifier_generate() {
#define MAX_NAME_SCOPE 63
    int name_length = dx(10);
    std::string name;
    for (int i = 0; i < name_length; i++) {
        int choice;
        if (i == 0)
            choice = dx(MAX_NAME_SCOPE - 10);
        else 
            choice = dx(MAX_NAME_SCOPE);

        // 1-63
        if (choice <= 26) // 1-26
            name.push_back('a' - 1 + choice);
        else if (choice <= 52)
            name.push_back('A' - 1 + choice - 26);
        else if (choice <= 53) 
            name.push_back('_');
        else if (choice <= 63)
            name.push_back('0' - 1 + choice - 53);
        else 
            name.push_back(dx(256) - 1); // illegal name
    }
    return name;
}