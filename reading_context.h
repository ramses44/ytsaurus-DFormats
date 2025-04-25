#pragma once

#include <util/generic/maybe.h>

struct TReadingContext {
    int64_t TableIndex;
    bool AfterKeySwitch;
    int64_t RowIndex;
    int64_t RangeIndex;
};