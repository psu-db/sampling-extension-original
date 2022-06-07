/*
 *
 */

#ifndef H_BTREESAMPLERANGE
#define H_BTREESAMPLERANGE

#include "sampling/samplerange.hpp"
#include "ds/staticbtree.hpp"

namespace lsm { namespace sampling {

class BTreeSampleRange : public SampleRange {
public:
    BTreeSampleRange(ds::StaticBTree *btree, global::g_state *state, PageNum first_page, PageNum last_page,
                     byte *lower_key, byte *upper_key);

    io::Record get(FrameId *frid) override;
    size_t length() override;

private:
    global::g_state *state;

    ds::StaticBTree *btree;
    PageNum first_page;
    PageNum last_page;
    byte *lower_key;
    byte *upper_key;
};

}}

#endif
