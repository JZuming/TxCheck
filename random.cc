#include "random.hh"

namespace smith {
  std::mt19937_64 rng;
}

int d6() {
#ifndef __FILE_RANDOM__
    static std::uniform_int_distribution<> pick(1, 6);
    return pick(smith::rng);
#else
    static struct file_random_machine* frand = file_random_machine::get(__RAND_FILE_NAME__);
    return frand->get_random_num(1, 6, 1);
#endif
}

int d9() {
#ifndef __FILE_RANDOM__
    static std::uniform_int_distribution<> pick(1, 9);
    return pick(smith::rng);
#else
    static struct file_random_machine* frand = file_random_machine::get(__RAND_FILE_NAME__);
    return frand->get_random_num(1, 9, 1);
#endif
}

int d12() {
#ifndef __FILE_RANDOM__
    static std::uniform_int_distribution<> pick(1, 12);
    return pick(smith::rng);
#else
    static struct file_random_machine* frand = file_random_machine::get(__RAND_FILE_NAME__);
    return frand->get_random_num(1, 12, 1);
#endif
}

int d20() {
#ifndef __FILE_RANDOM__
    static std::uniform_int_distribution<> pick(1, 20);
    return pick(smith::rng);
#else
    static struct file_random_machine* frand = file_random_machine::get(__RAND_FILE_NAME__);
    return frand->get_random_num(1, 20, 1);
#endif
}

int d42() {
#ifndef __FILE_RANDOM__
    static std::uniform_int_distribution<> pick(1, 42);
    return pick(smith::rng);
#else
    static struct file_random_machine* frand = file_random_machine::get(__RAND_FILE_NAME__);
    return frand->get_random_num(1, 42, 2);
#endif
}

int d100() {
#ifndef __FILE_RANDOM__
    static std::uniform_int_distribution<> pick(1, 100);
    return pick(smith::rng);
#else
    static struct file_random_machine* frand = file_random_machine::get(__RAND_FILE_NAME__);
    return frand->get_random_num(1, 100, 2);
#endif
}

// 1 - x
int dx(int x) {
#ifndef __FILE_RANDOM__
    std::uniform_int_distribution<> pick(1, x);
    return pick(smith::rng);
#else
    static struct file_random_machine* frand = file_random_machine::get(__RAND_FILE_NAME__);
    if (x == 1)
        return 1;
    
    int bytenum;
    if (x <= 0xff / 10) 
        bytenum = 1;
    else if (x <= 0xffff / 10)
        bytenum = 2;
    else if (x <= 0xffffff / 10)
        bytenum = 3;
    else
        bytenum = 4;
    return frand->get_random_num(1, x, bytenum);
#endif
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

file_random_machine::file_random_machine(string s)
: filename(s)
{
    ifstream fin(filename, std::ios::binary);
    fin.seekg(0, std::ios::end);
    end_pos = fin.tellg();
    fin.seekg(0, std::ios::beg);

    if (end_pos == 0) {
        buffer = NULL;
        return;
    }
    
    if (end_pos < 100) {
        std::cerr << "Exit: rand file is too small (should larger than 100 byte)" << endl;
        exit(0);
    }
    buffer = new char[end_pos + 5];
    cur_pos = 0;
    read_byte = 0;

    fin.read(buffer, end_pos);
    fin.close();
}

file_random_machine::~file_random_machine()
{
    if (buffer != NULL)
        delete[] buffer;
    
    std::ofstream fout(__READ_BYTE_NUM__);
    fout << read_byte << endl;
    fout.close();
}

map<string, struct file_random_machine*> file_random_machine::stream_map;

struct file_random_machine *file_random_machine::get(string filename)
{
    if (stream_map.count(filename))
        return stream_map[filename];
    else
        return stream_map[filename] = new file_random_machine(filename);
}

int file_random_machine::get_random_num(int min, int max, int byte_num)
{
    if (buffer == NULL)
        return 0;
    
    int scope = max - min;
    if (scope <= 0) 
        return min;
    
    // default: small endian
    auto readable = end_pos - cur_pos;
    auto read_num = readable < byte_num ? readable : byte_num;
    int rand_num = 0;
    memcpy(&rand_num, buffer + cur_pos, read_num);
    cur_pos += read_num;

    if (cur_pos >= end_pos)
        cur_pos = 0;

    read_byte += read_num;
    
    return min + rand_num % scope;
}