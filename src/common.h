#pragma once

#include "pg_allocator.h"

#include <string>
#include <vector>
#include <optional>
#include <unordered_map>

extern "C" {
#include <varatt.h>
}


template<typename CharT, typename Traits = std::char_traits<CharT>>
using PgBasicString = std::basic_string<CharT, Traits, PgAllocator<CharT>>;
using PgString = PgBasicString<char>;

template<typename T>
using PgVector = std::vector<T, PgAllocator<T>>;

template<typename Key, typename T, typename Hash = std::hash<Key>, typename Comp = std::equal_to<Key>>
using PgUnorderedMap = std::unordered_map<Key, T, Hash, Comp, PgAllocator<std::pair<const Key, T>>>;

// Treat all amounts below AMOUNT_EPSILON as zeros
static constexpr const double AMOUNT_EPSILON = 1e-12;

// Verify that incoming transfer amount is equal to outgoing transfer amount with the following abs precision
static constexpr const double TRANSFER_AMOUNT_EPSILON = 1e-8;

template<typename AccountEntry>
struct CbTransfer
{

    // Some transfers can provide unique transfer id, in this case it's a preferred way to match outgoing and incoming records
    std::optional<PgString> mTransferId;

    // For the rest, we rely on triplet (source, dest, amount)
    PgString mSourceAccount;
    PgString mDestinationAccount;
    double mAmount;

    PgVector<AccountEntry> mEntries;

    bool operator==(const CbTransfer& o) const
    {
        if (mTransferId.has_value() && o.mTransferId.has_value())
            return mTransferId == o.mTransferId;

        // if mTransferId is defined for either in or out record then it should be defined for both records
        if (mTransferId.has_value() != o.mTransferId.has_value())
            return false;

        return mSourceAccount == o.mSourceAccount && mDestinationAccount == o.mDestinationAccount && std::abs(mAmount - o.mAmount) < TRANSFER_AMOUNT_EPSILON;
    }
};

template<>
struct std::hash<PgString> : private std::hash<std::string_view>
{
    [[nodiscard]] std::size_t operator()(const PgString& s) const
    {
        return std::hash<std::string_view>::operator()(std::string_view{s});
    }
};

template<typename StringType>
[[nodiscard]] inline StringType textToString(text* t)
{
    return StringType{VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t)};
}
