/*
*
*/
#ifndef PAGEFILE_H
#define PAGEFILE_H

#include "util/parameters.hpp"
#include "io/directfile.hpp"

namespace lsm {
namespace io {

typedef size_t pid;

struct PageHeader {
    pid next_page;
    pid prev_page;
};

class PageFile {
public:
    static std::unique_ptr<PageFile> create(std::unique_ptr<DirectFile> file);
    static std::unique_ptr<PageFile> create(const std::string fname, bool new_file=true);


private:

};

}
}

#endif
