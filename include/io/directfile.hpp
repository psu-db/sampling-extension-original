#ifndef HEAPFILE_H
#define HEAPFILE_H

#include <memory>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string>

#include "util/base.hpp"
#include "util/types.hpp"

namespace lsm { namespace io {

struct DirectFileHeaderData {
    uint64_t reserved;
};

class DirectFile {
public:
    /*
     * Create a new HeapFile object by opening the file with name fname. If new_file is
     * true, the file will be empty on return--either created or truncated depending on
     * if the file is open already or not.
     *
     * If the creation of the HeapFile fails, this function will return nullptr.
     */
    static std::unique_ptr<DirectFile> create(const std::string fname, bool new_file=true);

    /*
     * Destroy the HeapFile object, automatically closing the associated file if necessary.
     */
    ~DirectFile();

    /*
     * Create a new Directfile object. You should typically not call this
     * constructor directly, use the static create method instead.
     */
    DirectFile(int fd, std::string fname, off_t size, mode_t mode);

    /*
    * reads amount bytes starting at offset into buffer. Returns 1 on success,
    * and 0 on failure. The memory pointed to by buffer is not guaranteed to be
    * left unchanged by this function if it fails to perform the entire read.
    *
    * It is an error if the offset is not sector aligned, or if amount is not
    * an even multiple of the sector size. This is because this object opens
    * files with the O_DIRECT flag. It is also an error if offset + amount
    * falls outside of the already allocated range of the file.
    *
    * It is the caller's responsibility to allocate memory for the buffer.
    */
    int read(byte *buffer, off_t amount, off_t offset);

    /*
    * writes amount bytes from the buffer to the file, starting at offset.
    * Returns 1 on success, and 0 on failure. The file is not guaranteed to
    * remain unchanged should this method exit in error.
    *
    * It is an error if the offset is not sector aligned, or if amount is not
    * an even multiple of the sector size. This is because this object opens
    * files with the O_DIRECT flag. It is also an error if offset + amount
    * falls outside of the already allocated range of the file.
    *
    * It is the caller's responsibility to deallocate the buffer.
    */
    int write(const byte *buffer, off_t amount, off_t offset);

    /*
    * Allocates amount bytes at the end of the file. Returns 1 on success and 0
    * on failure. It is an error for amount to not be an even multiple of the
    * sector size.
    */
    int allocate(size_t amount);


    /*
    * Closes the file if it is presently open, and returns 1 on success. If the
    * file is currently closed, returns 2 and does nothing. If the file is open
    * and the closing is a failure, returns 0.
    */
    int close_file();

    /*
    * Open the file if it is presently closed. Returns 1 on success, 2 if the
    * file is already open, and 0 if the reopening of the file fails.
    */
    int reopen();


    /*
     * Delete the file referenced by this object. Returns 1 on success and zero
     * on failure.
     */
    int remove();


    /*
     * Returns true if the underlying file is open, and false if it is not.
     */
    bool is_open();

    /*
     * Returns the allocated size of the file, in bytes.
     */
    off_t get_size();


    std::string get_fname();

    // Testing accessors
    #ifdef UNIT_TESTING
    int get_fd();
    #endif

private:
    int fd;
    bool file_open;
    off_t size;
    mode_t mode;
    std::string fname;
    int flags;

    /* Verify that the specified amount and offset are valid. Namely, ensures that
     * they are properly aligned, and that they are within the allocated size
     * of the file. Also checks if the file is open.
     */
    bool verify_io_parms(off_t amount, off_t offset);
};

}}
#endif
