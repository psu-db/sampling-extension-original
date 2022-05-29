/*
 *
 */

#include "io/filemanager.hpp"

namespace lsm { namespace io {

FileManager::FileManager(const std::string root_directory, const std::string metadata_file)
: root_dir(root_directory), metafile(metadata_file)
{
    if (this->metafile == "") {
        this->metafile = root_dir + this->generate_filename();
        this->file_counter = 0;
    } else {
        this->process_metafile();
    }
}


FileId FileManager::generate_flid()
{
    return ++this->file_counter;
}


DirectFile *FileManager::create_dfile(const std::string fname, FileId *flid)
{
    if (check_existance(fname)) {
        return nullptr; // file already exists in the manager
    }

    std::string file = this->root_dir + ((fname == "") ? this->generate_filename() : fname);

    auto flags = O_RDWR | O_DIRECT | O_CREAT | O_TRUNC;
    mode_t mode = 0644;
    off_t size = 0;

    int fd = open(file.c_str(), flags, mode);
    if (fd == -1) {
        return nullptr;
    }

    auto dfile = std::make_unique<DirectFile>(fd, file, 0, mode);
    auto dfile_ptr = dfile.get();
    auto new_flid = this->generate_flid();

    this->dfile_map.insert({new_flid, dfile_ptr});
    this->fname_map.insert({file, new_flid});
    this->dfiles.emplace_back(std::move(dfile));

    if (flid) {
        *flid = new_flid;
    }

    return dfile_ptr;
}


IndexPagedFile *FileManager::create_indexed_pfile(const std::string fname, bool initialize_virtualization)
{
    FileId new_flid;
    auto dfile_ptr = this->create_dfile(fname, &new_flid);

    // file already exists or could not be created.
    if (dfile_ptr == nullptr) {
        return nullptr;
    }

    IndexPagedFile::initialize(dfile_ptr, new_flid);
    auto pfile = std::make_unique<IndexPagedFile>(dfile_ptr, false);
    auto pfile_ptr = pfile.get();

    if (initialize_virtualization) {
        pfile_ptr->initialize_for_virtualization();
    }

    this->pfile_map.insert({new_flid, pfile_ptr});
    this->pfiles.emplace_back(std::move(pfile));

    return pfile_ptr;
}


IndexPagedFile *FileManager::create_temp_indexed_pfile()
{
    FileId new_flid;
    auto dfile_ptr = this->create_dfile("", &new_flid);

    // file already exists or could not be created.
    if (dfile_ptr == nullptr) {
        return nullptr;
    }

    IndexPagedFile::initialize(dfile_ptr, new_flid);
    auto pfile = std::make_unique<IndexPagedFile>(dfile_ptr, true);
    auto pfile_ptr = pfile.get();
    this->pfile_map.insert({new_flid, pfile_ptr});
    this->pfiles.emplace_back(std::move(pfile));

    return pfile_ptr;
}


DirectFile *FileManager::get_dfile(const std::string fname)
{
    auto res = this->fname_map.find(fname);
    if (res == this->fname_map.end()) {
        return nullptr;
    }

    FileId flid = res->second;
    return this->get_dfile(flid);
}


DirectFile *FileManager::get_dfile(FileId flid)
{
    auto res = this->dfile_map.find(flid);
    if (res == this->dfile_map.end()) {
        return nullptr;
    }

    return res->second;
}


PagedFile *FileManager::get_pfile(const std::string fname) 
{
    auto res = this->fname_map.find(fname);
    if (res == this->fname_map.end()) {
        return nullptr;
    }

    FileId flid = res->second;
    return this->get_pfile(flid);
}


PagedFile *FileManager::get_pfile(FileId flid)
{
    auto res = this->pfile_map.find(flid);
    if (res == this->pfile_map.end()) {
        return nullptr;
    }

    return res->second;
}


std::string FileManager::generate_filename()
{
    return std::to_string(this->file_counter) + "_" + std::to_string(rand()) + ".dat";
    
}


bool FileManager::check_existance(std::string fname)
{
    if (fname == "") {
        return false;
    }

    std::string file = this->root_dir + fname;
    auto res = this->fname_map.find(file);

    return res != this->fname_map.end();
}


FileId FileManager::get_flid(const std::string fname, bool full_pathname)
{
    std::string file;
    if (full_pathname) {
        file = fname;
    } else {
        file = this->root_dir + fname;
    }

    auto res = this->fname_map.find(file);
    return (res == this->fname_map.end()) ? INVALID_FLID : res->second;
}


std::string FileManager::get_name(FileId flid)
{
    auto res = this->dfile_map.find(flid);
    return (res == this->dfile_map.end()) ? "" : res->second->get_fname();
}


std::string FileManager::get_metafile_name()
{
    return this->metafile;
}


void FileManager::process_metafile()
{

}

}}
