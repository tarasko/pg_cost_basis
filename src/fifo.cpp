#include "pg_allocator.h"

#include <string>
#include <string_view>
#include <optional>
#include <vector>
#include <deque>
#include <unordered_map>
#include <numeric>
#include <cmath>

extern "C"
{
#include <postgres.h>
#include <catalog/pg_type_d.h>
#include <utils/memutils.h>
#include <utils/array.h>
#include <utils/jsonb.h>
#include <fmgr.h>
#include <varatt.h>
}

namespace {

template<typename CharT, typename Traits = std::char_traits<CharT>>
using PgBasicString = std::basic_string<CharT, Traits, PgAllocator<CharT>>;

using PgString = PgBasicString<char>;

template<typename T>
using PgDeque = std::deque<T, PgAllocator<T>>;

template<typename T>
using PgVector = std::vector<T, PgAllocator<T>>;

template<typename Key, typename T, typename Hash = std::hash<Key>, typename Comp = std::equal_to<Key>>
using PgUnorderedMap = std::unordered_map<Key, T, Hash, Comp, PgAllocator<std::pair<const Key, T>>>;

} // namespace {

template<>
struct std::hash<PgString> : private std::hash<std::string_view>
{
    [[nodiscard]] std::size_t operator()(const PgString& s) const
    {
        return std::hash<std::string_view>::operator()(std::string_view{s});
    }
};

namespace {

char jsTagKey[] = "t";
char jsAmountKey[] = "a";
char jsPlKey[] = "pl";
char jsCostBasisKey[] = "cb";

// Treat all amounts below AMOUNT_EPSILON as zeros
const double AMOUNT_EPSILON = 1e-12;

// Verify that incoming transfer amount is equal to outgoing transfer amount with the following abs precision
const double TRANSFER_AMOUNT_EPSILON = 1e-8;

template<typename StringType>
[[nodiscard]] inline StringType textToString(text* t)
{
    return StringType{VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t)};
}

struct CbAccountEntry
{
    PgString mOriginatingAccount;
    int64_t mOriginatingTag;
    double mCostBasis;
    double mAmount;
};

struct CbTransfer
{
    // Some transfers can provide unique transfer id, in this case it's much more reliable to match outgoing and incoming records
    std::optional<PgString> mTransferId;

    // For the rest, we rely on triplet (source, dest, amount)
    PgString mSourceAccount;
    PgString mDestinationAccount;
    double mAmount;

    PgVector<CbAccountEntry> mRecords;

    bool operator==(const CbTransfer& o) const
    {
        if (mTransferId.has_value() && o.mTransferId.has_value())
            return mTransferId == o.mTransferId;

        // if transfer_id is defined for either in or out records then it should be defined for both records
        if (mTransferId.has_value() != o.mTransferId.has_value())
            return false;

        return mSourceAccount == o.mSourceAccount && mDestinationAccount == o.mDestinationAccount && std::abs(mAmount - o.mAmount) < TRANSFER_AMOUNT_EPSILON;
    }
};

struct CbFifoState
{
    using Fifo = PgDeque<CbAccountEntry>;
    using RealizedList = PgVector<CbAccountEntry>;

    struct SharedState
    {
        PgUnorderedMap<PgString, Fifo> mAccountEntries;
        PgVector<CbTransfer> mTransfers;
    };

    [[nodiscard]] static double totalFifoBalance(const Fifo& fifo) noexcept
    {
        return std::accumulate(fifo.cbegin(), fifo.cend(), 0.0, [](double total, const auto& entry) { return total + entry.mAmount; });
    }

    [[nodiscard]] static CbFifoState* newState(CbFifoState* oldState = nullptr, double price = 1.0)
    {
        SharedState* sharedState = oldState == nullptr ?
            new (pallocHook<CbFifoState::SharedState>()) CbFifoState::SharedState{} :
            oldState->mSharedState;

        return new (pallocHook<CbFifoState>()) CbFifoState{sharedState, price};
    }

    [[nodiscard]] CbFifoState* initiateTransfer(const PgString& account, const PgString& destinationAccount, const std::optional<PgString>& txId, double amount, int64_t tag)
    {
        CbFifoState::Fifo& accountFifo = mSharedState->mAccountEntries[account];

        CbTransfer transfer{txId, account, destinationAccount, -amount, {}};

        // Amount is always negative because initiating records always withdraw funds
        double remainingAmountToTransfer = -amount;

        while (!accountFifo.empty() && remainingAmountToTransfer >= AMOUNT_EPSILON)
        {
            auto& entry = accountFifo.front();
            if (entry.mAmount < AMOUNT_EPSILON) [[unlikely]]
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("%ld: attempt to transfer from account \"%s\" that has negative balance records",
                                tag, account.c_str())));
                return newState(this);
            }

            if (remainingAmountToTransfer > entry.mAmount)
            {
                remainingAmountToTransfer -= entry.mAmount;
                transfer.mRecords.push_back(CbAccountEntry{entry.mOriginatingAccount, entry.mOriginatingTag, entry.mCostBasis, entry.mAmount});
                accountFifo.pop_front();
            }
            else
            {
                transfer.mRecords.push_back(CbAccountEntry{entry.mOriginatingAccount, entry.mOriginatingTag, entry.mCostBasis, remainingAmountToTransfer});
                entry.mAmount -= remainingAmountToTransfer;
                remainingAmountToTransfer = 0.0;
                if (entry.mAmount < AMOUNT_EPSILON)
                    accountFifo.pop_front();
            }
        }

        if (remainingAmountToTransfer >= AMOUNT_EPSILON) [[unlikely]]
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("%ld: not enough balance on \"%s\", %g left untransfered",
                            tag, account.c_str(), remainingAmountToTransfer)));
        }

        mSharedState->mTransfers.push_back(std::move(transfer));

        return CbFifoState::newState(this);
    }

    [[nodiscard]] CbFifoState* finalizeTransfer(const PgString& account, const PgString& sourceAccount, const std::optional<PgString>& txId, double amount, int64_t tag)
    {
        CbFifoState::Fifo& accountFifo = mSharedState->mAccountEntries[account];

        CbTransfer transferKey{txId, sourceAccount, account, amount, {}};
        auto transferIter = std::find(mSharedState->mTransfers.begin(), mSharedState->mTransfers.end(), transferKey);
        if (transferIter == mSharedState->mTransfers.end()) [[unlikely]]
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("%ld: can't finalize transfer %s -> %s %g, unable to match with initiating record",
                            tag, sourceAccount.c_str(), account.c_str(), amount)));
            return newState(this);
        }

        if (std::abs(transferIter->mAmount - amount) > TRANSFER_AMOUNT_EPSILON) [[unlikely]]
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("%ld: can't finalize transfer, in/out amounts mismatch: %g, %g",
                            tag, transferIter->mAmount, amount)));
            return newState(this);
        }

        accountFifo.insert(accountFifo.end(), transferIter->mRecords.begin(), transferIter->mRecords.end());
        mSharedState->mTransfers.erase(transferIter);

        return CbFifoState::newState(this);
    }

    [[nodiscard]] CbFifoState* realize(const PgString& account, double price, double amount, int64_t tag)
    {
        CbFifoState::Fifo& accountFifo = mSharedState->mAccountEntries[account];
        CbFifoState* newState = CbFifoState::newState(this, price);

        if (std::abs(amount) < AMOUNT_EPSILON)
            return newState;

        double remainingAmount = amount;

        while (std::abs(remainingAmount) >= AMOUNT_EPSILON && !accountFifo.empty() && std::signbit(accountFifo.front().mAmount) != std::signbit(remainingAmount))
        {
            auto& entry = accountFifo.front();
            if (std::signbit(entry.mAmount) == std::signbit(entry.mAmount + remainingAmount))
            {
                // don't cross 0
                newState->mLastRealized.push_back(CbAccountEntry{entry.mOriginatingAccount, entry.mOriginatingTag, entry.mCostBasis, -remainingAmount});
                entry.mAmount += remainingAmount;
                remainingAmount = 0.0;
                if (std::abs(entry.mAmount) < AMOUNT_EPSILON)
                    accountFifo.pop_front();
            }
            else
            {
                // cross 0
                newState->mLastRealized.push_back(CbAccountEntry{entry.mOriginatingAccount, entry.mOriginatingTag, entry.mCostBasis, entry.mAmount});
                remainingAmount += entry.mAmount;
                accountFifo.pop_front();
            }
        }

        if (std::abs(remainingAmount) >= AMOUNT_EPSILON)
            accountFifo.push_back(CbAccountEntry{account, tag, price, remainingAmount});

        return newState;
    }

    [[nodiscard]] size_t numAccounts() const noexcept
    {
        return mSharedState->mAccountEntries.size();
    }

    [[nodiscard]] size_t totalEntries() const noexcept
    {
        return std::accumulate(mSharedState->mAccountEntries.cbegin(), mSharedState->mAccountEntries.cend(), size_t(0),
                               [](size_t total, auto& entry) { return total + entry.second.size(); });
    }

    [[nodiscard]] double totalBalance() const noexcept
    {
        return std::accumulate(mSharedState->mAccountEntries.cbegin(), mSharedState->mAccountEntries.cend(), 0.0,
                               [](double total, auto& entry) { return total + totalFifoBalance(entry.second); });
    }

    [[nodiscard]] double capitalGain() const noexcept
    {
        return std::accumulate(mLastRealized.cbegin(), mLastRealized.cend(), 0.0, [this](double total, auto& entry) {
            return total + entry.mAmount * (mLastPrice - entry.mCostBasis);
        });
    }

    [[nodiscard]] char* toPstring() const
    {
        return psprintf("(g:%lu,c:%lu,b:%g,rlen:%zu)", numAccounts(), totalEntries(), totalBalance(), mLastRealized.size());
    }

    [[nodiscard]] ArrayType* lastRealizedTags() const
    {
        if (mLastRealized.empty())
        {
            return construct_empty_array(INT8OID);
        }
        else
        {
            Datum elems[mLastRealized.size()];
            std::transform(mLastRealized.begin(), mLastRealized.end(), elems, [](auto& e) { return Int64GetDatum(e.mOriginatingTag); });
            return construct_array_builtin(elems, sizeof(elems)/sizeof(Datum), INT8OID);
        }
    }

    [[nodiscard]] JsonbValue* lastRealizedToJsonb() const
    {
        JsonbParseState* parseState = nullptr;

        pushJsonbValue(&parseState, WJB_BEGIN_ARRAY, NULL);

        for (auto& entry : mLastRealized)
        {
            pushJsonbValue(&parseState, WJB_BEGIN_OBJECT, NULL);
            {
                JsonbValue key;
                key.type = jbvString;
                key.val.string.len = sizeof(jsTagKey) - 1;
                key.val.string.val = jsTagKey;
                pushJsonbValue(&parseState, WJB_KEY, &key);

                JsonbValue val;
                val.type = jbvNumeric;
                val.val.numeric = int64_to_numeric(entry.mOriginatingTag);
                pushJsonbValue(&parseState, WJB_VALUE, &val);
            }

            {
                JsonbValue key;
                key.type = jbvString;
                key.val.string.len = sizeof(jsAmountKey) - 1;
                key.val.string.val = jsAmountKey;
                pushJsonbValue(&parseState, WJB_KEY, &key);

                JsonbValue val;
                val.type = jbvNumeric;
                val.val.numeric = int64_div_fast_to_numeric(static_cast<int64_t>(entry.mAmount*1e8), 8);
                pushJsonbValue(&parseState, WJB_VALUE, &val);
            }

            {
                JsonbValue key;
                key.type = jbvString;
                key.val.string.len = sizeof(jsPlKey) - 1;
                key.val.string.val = jsPlKey;
                pushJsonbValue(&parseState, WJB_KEY, &key);

                double pl = entry.mAmount * (mLastPrice - entry.mCostBasis);
                JsonbValue val;
                val.type = jbvNumeric;
                val.val.numeric = int64_div_fast_to_numeric(static_cast<int64_t>(pl*1e8), 8);
                pushJsonbValue(&parseState, WJB_VALUE, &val);
            }

            {
                JsonbValue key;
                key.type = jbvString;
                key.val.string.len = sizeof(jsCostBasisKey) - 1;
                key.val.string.val = jsCostBasisKey;
                pushJsonbValue(&parseState, WJB_KEY, &key);

                JsonbValue val;
                val.type = jbvNumeric;
                val.val.numeric = int64_div_fast_to_numeric(static_cast<int64_t>(entry.mCostBasis*1e8), 8);
                pushJsonbValue(&parseState, WJB_VALUE, &val);
            }

            pushJsonbValue(&parseState, WJB_END_OBJECT, NULL);
        }

        return pushJsonbValue(&parseState, WJB_END_ARRAY, NULL);
    }

    void validateAtEnd() const
    {
        for (auto& transfer : mSharedState->mTransfers)
        {
            ereport(WARNING,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("unfinished transfer detected %s -> %s: %g, withdrawal without deposit",
                            transfer.mSourceAccount.c_str(), transfer.mDestinationAccount.c_str(), transfer.mAmount)));
        }

        for (auto& [account, accountEntries] : mSharedState->mAccountEntries)
        {
            for (auto& entry : accountEntries)
            {
                if (std::abs(entry.mAmount) >= AMOUNT_EPSILON)
                {
                    ereport(INFO,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("remaining amount detected %s %g, not all amount was realized at end",
                                    entry.mOriginatingAccount.c_str(), entry.mAmount)));
                }
            }
        }
    }

private:
    CbFifoState(SharedState* sharedState, double price) : mSharedState{sharedState}, mLastPrice{price} {}

private:
    // Allocated in CurTransactionContext, shared between calls, never freed explicitly
    SharedState* mSharedState;
    // Contains the list of last realized amounts with PnL
    RealizedList mLastRealized;
    // The last realized price
    double mLastPrice = 1.0;
};

// IMPORTANT: If this fails, change the expected size and adjust(!!!) pg_cost_basis--*.sql
static_assert(sizeof(CbFifoState) == 40);

} // namespace {

extern "C"
{

PG_FUNCTION_INFO_V1(CbFifoState_in);
Datum CbFifoState_in(PG_FUNCTION_ARGS)
{
    CbFifoState* state = CbFifoState::newState();
    PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(CbFifoState_out);
Datum CbFifoState_out(PG_FUNCTION_ARGS)
{
    CbFifoState* state = reinterpret_cast<CbFifoState*>(PG_GETARG_POINTER(0));
    PG_RETURN_CSTRING(state->toPstring());
}

PG_FUNCTION_INFO_V1(CbFifo_capital_gain);
Datum CbFifo_capital_gain(PG_FUNCTION_ARGS)
{
    CbFifoState* state = reinterpret_cast<CbFifoState*>(PG_GETARG_POINTER(0));
    PG_RETURN_FLOAT8(state->capitalGain());
}

PG_FUNCTION_INFO_V1(CbFifo_realized_tags);
Datum CbFifo_realized_tags(PG_FUNCTION_ARGS)
{
    CbFifoState* state = reinterpret_cast<CbFifoState*>(PG_GETARG_POINTER(0));
    ArrayType* result = state->lastRealizedTags();
    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(CbFifo_realized_entries);
Datum CbFifo_realized_entries(PG_FUNCTION_ARGS)
{
    CbFifoState* state = reinterpret_cast<CbFifoState*>(PG_GETARG_POINTER(0));
    JsonbValue* res = state->lastRealizedToJsonb();

    PG_RETURN_POINTER(JsonbValueToJsonb(res));
}

PG_FUNCTION_INFO_V1(CbFifo_sfunc);
Datum CbFifo_sfunc(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(5)) [[unlikely]]
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("tag is null")));
    }
    int64_t tag = PG_GETARG_INT64(5);

    if (PG_ARGISNULL(0)) [[unlikely]]
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%lu: fifo state can't be null", tag)));
    }
    CbFifoState* state = reinterpret_cast<CbFifoState*>(PG_GETARG_POINTER(0));

    if (PG_ARGISNULL(1)) [[unlikely]]
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%lu: account can't be null", tag)));
    }
    PgString account = textToString<PgString>(PG_GETARG_TEXT_PP(1));

    if (PG_ARGISNULL(4)) [[unlikely]]
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%lu: amount can't be null null", tag)));
    }
    double amount = PG_GETARG_FLOAT8(4);

    // We should reset the state, if there is no previous tag in the group
    if (PG_ARGISNULL(6)) [[unlikely]]
    {
        state->validateAtEnd();
        state = CbFifoState::newState();
    }

    CbFifoState* newState;

    if (PG_ARGISNULL(2)) [[likely]]
    {
        if (PG_ARGISNULL(3)) [[unlikely]]
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("%lu: price can't be null", tag)));
        }

        // regular entry
        double price = PG_GETARG_FLOAT8(3);

        newState = state->realize(account, price, amount, tag);
    }
    else
    {
        bool ignoreTransfer = false;
        if (!PG_ARGISNULL(7))
            ignoreTransfer = PG_GETARG_BOOL(7);

        if (ignoreTransfer)
            newState = CbFifoState::newState(state);
        else
        {
            std::optional<PgString> transferId;
            if (!PG_ARGISNULL(8))
                transferId = textToString<PgString>(PG_GETARG_TEXT_PP(8));

            if (amount < 0)
            {
                PgString destinationAccount = textToString<PgString>(PG_GETARG_TEXT_PP(2));
                newState = state->initiateTransfer(account, destinationAccount, transferId, amount, tag);
            }
            else
            {
                PgString sourceAccount = textToString<PgString>(PG_GETARG_TEXT_PP(2));
                newState = state->finalizeTransfer(account, sourceAccount, transferId, amount, tag);
            }
        }
    }

    PG_RETURN_POINTER(newState);
}

}

