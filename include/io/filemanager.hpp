/*
 * filemanager.hpp
 * Douglas Rumbaugh
 *
 * A management class to ease the creation, deletion, etc., of files within the
 * code base. It owns all files created by itself, and will automatically handle
 * cleaning them up on destruction.
 */
#ifndef H_FILEMANAGER
#define H_FILEMANAGER

#include <unordered_map>
#include <vector>

#include "util/base.hpp"
#include "util/types.hpp"
#include "io/directfile.hpp"
#include "io/pagedfile.hpp"
#include "io/indexpagedfile.hpp"
#include "cstdlib"

namespace lsm { namespace io {

class FileManager { public:
    /*
     * Create a new FileManager object. root_directory specifies the directory
     * in which the manager will store its files. If a metadata file is
     * provided (need not be within the root directory), it will be processed
     * to obtain information about pre-existing files and IDs known to the
     * manager. If one is not specified, a new one will be generated. Managed
     * files, their IDs, etc., will be persisted to this metadata file on the
     * destruction of the FileManager object.
     */
    FileManager(const std::string root_directory, const std::string
                metadata_file="");

    /*
     * Open a file and create a DirectFile object from it. If the specified
     * file cannot be opened, nullptr will be returned. If new_file is set to
     * true, then the file will be created if it doesn't exist, and truncated
     * if it does. It is an error to attempt to open a non-existent file with
     * new_file set to false.
     */
    DirectFile *create_dfile(const std::string fname="", FileId *flid=nullptr);

    /*
     * Open a file and create an IndexPagedFile object from it. If the
     * specified file cannot be opened, nullptr will be returned. If new_file
     * is set to true, then the file will be created if it doesn't exist, and
     * truncated if it does. It is an error to attempt to open a non-existent
     * file with new_file set to false. If initialize_virtualization and
     * new_file are both true, will initialize a virtual file header within the
     * file prior to returning it, to support creating virtual files. This
     * argument will be ignored if new_file is false.
     */
    IndexPagedFile *create_indexed_pfile(const std::string fname="",
                                         bool initialize_virtualization=false);

    /*
     * Open a temporary/scratch file and return it as an IndexPagedFile. When
     * this file is closed, it will automatically be deleted. It can be made
     * permanent if desired by calling its make_permanent() method.
     */
    IndexPagedFile *create_temp_indexed_pfile();


    /*
     * 
     *
     */
    DirectFile *get_dfile(const std::string fname);
    DirectFile *get_dfile(FileId flid);

    PagedFile *get_pfile(const std::string fname);
    PagedFile *get_pfile(FileId flid);

    FileId get_flid(const std::string fname, bool full_pathname=true);
    std::string get_name(FileId flid);

    /*
     * Close an open file by FileId. This will call the destructors on all
     * managed files based upon the specified FileId    --including PagedFiles,
     * VirtualFiles, etc. No references to this file should be accessed
     * following this call.
     */
    void close_file(FileId flid);

    /*  
     * Close an open DirectFile. This will call the destructors on all managed
     * files based upon the specified DirectFile--including PagedFiles,
     * VirtualFiles, etc. No references to this file should be accessed
     * following this call.
     */
    void close_file(DirectFile *dfile);

    /*
     * Close an open PagedFile. This will call the destructors on all managed
     * files based upon the DirectFile underlying the specified
     * PagedFile--including other PagedFiles, VirtualFiles, etc. No references
     * to this file should be accessed following this call.
     */
    void close_file(PagedFile *pfile);


    /*
     * Returns the path to the metadata file used by this manager. It will
     * match the one passed into the constructor, if one were provided.
     * Otherwise, it will be generated automatically.
     */
    std::string get_metafile_name();

private:
    FileId generate_flid();
    void flush_metadata();
    void process_metafile();
    std::string generate_filename();
    bool check_existance(std::string fname);

    FileId file_counter;    

    std::unordered_map<FileId, DirectFile*> dfile_map;
    std::unordered_map<FileId, PagedFile*> pfile_map;
    std::unordered_map<std::string, FileId> fname_map;

    // the order of these two are important. The PagedFiles must be deleted
    // before the DirectFiles, and it appears to go in reverse order.
    std::vector<std::unique_ptr<DirectFile>> dfiles;
    std::vector<std::unique_ptr<PagedFile>> pfiles;

    std::string root_dir;
    std::string metafile;
};


}}
#endif
