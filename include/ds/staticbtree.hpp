/*
 *
 */

#ifndef H_STATICBTREE
#define H_STATICBTREE

#include "util/base.hpp"
#include "io/pagedfile.hpp"
#include "util/mergeiter.hpp"
#include "util/mem.hpp"
#include "catalog/schema.hpp"
#include "catalog/field.hpp"
#include "io/fixedlendatapage.hpp"
#include "io/readcache.hpp"

namespace lsm { namespace ds {

struct StaticBTreeMetaHeader {
    PageNum root_node;
    PageNum first_data_page;
    PageNum last_data_page;
};


struct StaticBTreeInternalNodeHeader {
    PageNum next_sibling;
    PageNum prev_sibling;
};

constexpr PageOffset StaticBTreeInternalNodeHeaderSize = MAXALIGN(sizeof(StaticBTreeInternalNodeHeader));

const PageNum BTREE_META_PNUM = 1;

class StaticBTree {
public:
    /*
     * Initialize a StaticBTree structure within the file specified by pfile.
     * The file is assumed to be empty and the resulting file contents are
     * undefined if this is not the case. It copies up to data_page_cnt number
     * of pages of records from record_itr into a contiguous page range,
     * assuming that the iterator returns the records in a proper sorted order,
     * and constructs an BTree index structure atop this, initializing the
     * header page appropriately.
     *
     * record_schema and key_cmp are used for parsing and comparing the
     * records.
     *
     * TODO: A bulk-write interface for pages would make this a lot more
     * efficient, rather than doing IOs one page at a time. I could punch
     * through to directfile, but it might be better to add that functionality
     * to the PagedFile interface directly. For now, we'll just do page-by-page
     * and make it more efficient later.
     */
    static std::unique_ptr<StaticBTree> initialize(io::PagedFile *pfile,
                                                   std::unique_ptr<iter::MergeIterator>
                                                   record_iter, PageNum data_page_cnt, 
                                                   catalog::FixedKVSchema *record_schema,
                                                   iter::CompareFunc key_cmp,
                                                   io::ReadCache *cache);

    /*
     * Return a Static BTree object created from pfile. prfile is assumed to
     * have already been properly initialized bia a call to
     * StaticBTree::initialize at some point in its existence. If this is not
     * the case, all method calls to the returned object are undefined.
     */
    StaticBTree(io::PagedFile *pfile, catalog::FixedKVSchema *record_schema,
                iter::CompareFunc key_cmp, io::ReadCache *cache);

    /*
     * Returns the first leaf page pid within the tree that contains a key greater than
     * or equal to the specified boundary key. Returns INVALID_PID if no pages
     * satisfy this constraint.
     */
    PageId get_lower_bound(const byte *key);


    /*
     * Returns the last leaf page pid within the tree that contains a key
     * less than or equal to the specified boundary key. Returns INVALID_PID
     * if no pages satisfy this constraint.
     */
    PageId get_upper_bound(const byte *key);


    /*
     * Returns true if this tree contains a tombstone for a record with a matching
     * key that has a timestamp less-than-or-equal-to the provided one.
     */
    bool tombstone_exists(const byte *key, Timestamp time=0);


private:
    StaticBTreeMetaHeader *get_metapage();

    io::PagedFile *pfile;
    catalog::FixedKVSchema *record_schema;
    std::unique_ptr<catalog::FixedKVSchema> internal_index_schema; // schema for internal nodes
    iter::CompareFunc key_cmp;
    PageNum root_page;
    PageNum first_data_page;
    PageNum last_data_page;
    io::ReadCache *cache;


    PageNum search_internal_node_lower(PageNum pnum, const byte *key);
    PageNum search_internal_node_upper(PageNum pnum, const byte *key);
    SlotId search_leaf_page(byte *page_buf, const byte *key);


    static int initial_page_allocation(io::PagedFile *pfile, PageNum page_cnt, PageId *first_leaf, PageId *first_internal, PageId *meta);
    static std::unique_ptr<catalog::FixedKVSchema> generate_internal_schema(catalog::FixedKVSchema *record_schema);
    static PageNum generate_internal_levels(io::PagedFile *pfile, PageNum first_page, catalog::FixedKVSchema *schema);
};

}}

#endif
