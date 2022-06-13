/*
 *
 */

#include "sampling/level.hpp"

namespace lsm { namespace sampling {

BTreeLevel::BTreeLevel(size_t run_capacity, size_t record_capacity, 
                       std::vector<io::IndexPagedFile *> files, 
                       global::g_state *state,
                       double max_deletion_proportion)
{
    this->run_capacity = run_capacity;
    this->record_capacity = record_capacity;
    this->max_deleted_prop = max_deletion_proportion;

    this->run_count = 0;
    this->record_count = 0;

    this->runs = std::vector<std::unique_ptr<ds::StaticBTree>>(run_capacity);
    this->state = state;

    this->record_cmp = state->record_schema->get_record_cmp();
    this->key_cmp = state->record_schema->get_key_cmp();

    for (size_t i=0; i<files.size(); i++) {
        if (i < this->run_capacity) {
            this->runs[i] = std::make_unique<ds::StaticBTree>(files[i], state);
            this->run_count++;
            this->record_count += this->runs[i]->get_record_count();
        }
    }
}


ds::StaticBTree *BTreeLevel::get_run(size_t idx) 
{
    if (idx > this->run_capacity) {
        return nullptr;
    }

    return this->runs[idx].get();
}


int BTreeLevel::emplace_run(std::unique_ptr<ds::StaticBTree> run)
{
    if (this->run_count == this->run_capacity) {
        return 0; // no room
    }

    for (size_t i=0; i<this->run_capacity; i++) {
        if (this->runs[i].get() == nullptr) {
            this->runs[i] = std::move(run);
            this->run_count++;
            this->record_count += this->runs[i]->get_record_count();
            return 1;
        }
    }

    return 0;
}


bool BTreeLevel::can_emplace_run() 
{
    return this->run_count < this->run_capacity;
}


bool BTreeLevel::can_merge_with(size_t incoming_record_count)
{
    if (this->run_count < this->run_capacity) {
        return true;
    } else if (this->run_capacity == 1 && this->record_count + incoming_record_count <= this->record_capacity) {
        return true;
    }

    return false;
}


bool BTreeLevel::can_merge_with(BTreeLevel *level) 
{
    return this->can_merge_with(level->get_record_count());
}


io::Record BTreeLevel::get_by_key(const byte *key, FrameId *frid, Timestamp time) 
{
    for (int32_t i=this->run_capacity - 1; i >= 0; i--) {
        auto rec = this->runs[i]->get(key, frid, time);
        if (rec.is_valid()) {
            return rec;
        }
    }

    return io::Record();
}


int BTreeLevel::remove(byte* /*key*/, byte* /*value*/, Timestamp /*time*/)
{
    return 0;
}


void BTreeLevel::truncate()
{
    // Because files are std::moved from level to level, this should be safe to
    // do. Any file that has been relocated will no longer be here, so there
    // isn't any risk of closing a file that some other level is currently
    // using. This will, of course, need to be revisited for concurrency.
    for (size_t i=0; i<this->run_capacity; i++) {
        if (this->runs[i]) {
            auto pfile = this->runs[i]->get_pfile();
            this->runs[i] = nullptr;
            this->state->file_manager->close_file(pfile->get_flid());
        }
    }

    this->run_count = 0;
    this->record_count = 0;
}


std::unique_ptr<ds::StaticBTree> BTreeLevel::merge_runs()
{
    std::vector<std::unique_ptr<iter::GenericIterator<io::Record>>> iters;
    PageNum page_cnt = 0;
    for (size_t i=0; i<this->run_capacity; i++) {
        if (this->runs[i]) {
            iters.push_back(this->runs[i]->start_scan());
            page_cnt += this->runs[i]->get_leaf_page_count();
        }
    }

    auto merge = std::make_unique<iter::MergeIterator>(iters, this->record_cmp);

    return ds::StaticBTree::create(std::move(merge), page_cnt, this->state);
}


int BTreeLevel::merge_with(std::unique_ptr<ds::StaticBTree> new_run)
{
    // For tiering, we emplace the run if we can. This also catches the 
    // case of merging into an empty level under leveling.
    if (this->run_count < this->run_capacity) {
        this->emplace_run(std::move(new_run));
        return 1;
    }

    // For leveling, we merge the new run with the run on this level (there
    // will only ever be one)
    if (this->can_merge_with(new_run->get_record_count())) {
        std::vector<std::unique_ptr<iter::GenericIterator<io::Record>>> iters;
        if (this->runs[0]) {
            iters.push_back(this->runs[0]->start_scan());
        }
        
        iters.push_back(new_run->start_scan());
        
        auto merge_itr = std::make_unique<iter::MergeIterator>(iters, this->record_cmp);
        PageNum page_cnt = new_run->get_leaf_page_count() + this->runs[0]->get_leaf_page_count();

        auto new_btree = ds::StaticBTree::create(std::move(merge_itr), page_cnt, this->state);

        // abort if the creation of the new, merged, level failed for some reason.
        if (!new_btree) {
            return 0;
        }

        // FIXME: will need a different approach when concurrency comes into place
        this->truncate();
        this->emplace_run(std::move(new_btree));
        return 1;
    }

    // This level could not support the merge due to being at run and/or record capacity.
    return 0;
}


int BTreeLevel::merge_with(BTreeLevel *level) 
{
    auto temp = level->merge_runs();
    return this->merge_with(std::move(temp));
}


int BTreeLevel::merge_with(std::unique_ptr<iter::GenericIterator<io::Record>> sorted_itr)
{
    // iterator must support element count to merge it.
    if (!sorted_itr->supports_element_count()) {
        return 0;
    }

    if (this->can_merge_with(sorted_itr->element_count())) {
        std::vector<std::unique_ptr<iter::GenericIterator<io::Record>>> iters;
        PageNum existing_page_cnt = 0;
        if (this->runs[0]) {
            iters.push_back(this->runs[0]->start_scan());
            existing_page_cnt = this->runs[0]->get_leaf_page_count();
        }

        size_t new_element_cnt = sorted_itr->element_count();
        iters.push_back(std::move(sorted_itr));
        
        auto merge_itr = std::make_unique<iter::MergeIterator>(iters, this->record_cmp);
        size_t records_per_page = (parm::PAGE_SIZE - io::PageHeaderSize) / this->state->record_schema->record_length();
        PageNum page_cnt = ceil((double) new_element_cnt / (double) records_per_page) + existing_page_cnt;

        auto new_btree = ds::StaticBTree::create(std::move(merge_itr), page_cnt, this->state);

        // abort if the creation of the new, merged, level failed for some reason.
        if (!new_btree) {
            return 0;
        }

        // FIXME: will need a different approach when concurrency comes into place
        this->truncate();
        this->emplace_run(std::move(new_btree));
        return 1;
    }

    return 0;
}


std::vector<std::unique_ptr<SampleRange>> BTreeLevel::get_sample_ranges(byte *lower_key, byte *upper_key)
{
    std::vector<std::unique_ptr<SampleRange>> ranges;
    for (size_t i=0; i<this->run_capacity; i++) {
        if (this->runs[i]) {
            auto range = BTreeSampleRange::create(this->runs[i].get(), lower_key, upper_key, this->state);

            if (range) {
                ranges.emplace_back(std::move(range));
            }
        }
    }

    return ranges;
}


size_t BTreeLevel::get_record_capacity()
{
    return this->record_capacity;
}


size_t BTreeLevel::get_record_count()
{
    return this->record_count;
}


size_t BTreeLevel::get_run_capacity()
{
    return this->run_capacity;
}


size_t BTreeLevel::get_run_count()
{
    return this->run_count;
}


size_t BTreeLevel::get_memory_utilization()
{
    return 0;
}


void BTreeLevel::print_level()
{
    return;
}

std::unique_ptr<iter::GenericIterator<Record>> BTreeLevel::start_scan()
{
    if (this->runs.size() == 0) {
        return nullptr;
    }

    if (this->runs[0]) {
        return this->runs[0]->start_scan();
    }

    return nullptr;
}
}}
