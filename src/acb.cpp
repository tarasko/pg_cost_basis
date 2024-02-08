#include "common.h"
#include "sfunc.h"

#include <cmath>

extern "C"
{
#include <postgres.h>
#include <fmgr.h>
}

namespace {

struct CbAcbAccountEntry
{
    double mCostBasis = 1.0;
    double mAmount = 0;
};

class CbAcbState
{
    struct SharedState
    {
        PgUnorderedMap<PgString, CbAcbAccountEntry> mAccountEntries;
        PgVector<CbTransfer<CbAcbAccountEntry>> mTransfers;
    };

    // Allocated in CurTransactionContext, shared between calls, never freed explicitly
    // Contains (cost basis, amount) for each account and pending asset transfers
    SharedState* mSharedState;

    explicit CbAcbState(SharedState* sharedState)
        : mSharedState(sharedState)
    {}

 public:
    double mCostBasisBefore = 1.0;
    double mCostBasisAfter = 1.0;
    double mBalanceBefore = 0.0;
    double mBalanceAfter = 0.0;
    double mCapitalGain = 0.0;

    [[nodiscard]] static CbAcbState* newState(CbAcbState* oldState = nullptr)
    {
        SharedState* sharedState = oldState == nullptr ?
            new (pallocHook<CbAcbState::SharedState>()) CbAcbState::SharedState{} :
            oldState->mSharedState;

        return new (pallocHook<CbAcbState>()) CbAcbState{sharedState};
    }

    [[nodiscard]] CbAcbState* realize(const PgString& account, double price, double amount, int64_t tag)
    {
        CbAcbAccountEntry& accountEntry = mSharedState->mAccountEntries[account];
        CbAcbState* newState = CbAcbState::newState(this);

        newState->realizeImpl(accountEntry, price, amount);
        return newState;
    }

    [[nodiscard]] CbAcbState* initiateTransfer(
            const PgString& account, const PgString& destinationAccount, const std::optional<PgString>& txId,
            double amount, std::optional<double> price,
            int64_t tag)
    {
        CbAcbAccountEntry& accountEntry = mSharedState->mAccountEntries[account];
        CbAcbState* newState = CbAcbState::newState(this);

        newState->mCostBasisBefore = accountEntry.mCostBasis;
        newState->mBalanceBefore = accountEntry.mAmount;
        newState->mBalanceAfter = accountEntry.mAmount + amount;
        newState->mCapitalGain = 0.0;

        if (std::abs(newState->mBalanceAfter) < AMOUNT_EPSILON)
            newState->mBalanceAfter = 0.0;

        CbTransfer<CbAcbAccountEntry> transfer{txId, account, destinationAccount, -amount, {}};

        // Depending on the case we should evaluate
        // * newState->mCostBasisAfter
        // * transferred entries
        // * accountEntry (cost basis and resulting amount)
        if (newState->mBalanceBefore < 0.0)
        {
            // We are already negative on the balance. Transfer here is akin to asset acquisition
            if (!price.has_value())
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("tag %ld: not enough balance on \"%s\", %g left untransfered, price must be specifiied in order to go negative on transfers",
                                tag, account.c_str(), std::abs(mBalanceAfter))));
                return newState;
            }
            newState->mCostBasisAfter = newState->mBalanceAfter == 0.0 ?
                        newState->mCostBasisBefore :
                        (accountEntry.mCostBasis * accountEntry.mAmount + *price * amount) / newState->mBalanceAfter;

            transfer.mEntries.push_back({*price, -amount});

            accountEntry.mAmount = newState->mBalanceAfter;
            accountEntry.mCostBasis = newState->mCostBasisAfter;
        }
        else if (newState->mBalanceAfter < 0.0)
        {
            // Not enough balance to transfer, we're allowed to go negative if price is specified
            // Price becomes cost basis for negative position
            if (!price.has_value())
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("tag %ld: not enough balance on \"%s\", %g left untransfered",
                                tag, account.c_str(), std::abs(mBalanceAfter))));
                return newState;
            }
            newState->mCostBasisAfter = *price;

            transfer.mEntries.push_back({newState->mCostBasisBefore, newState->mBalanceBefore});
            transfer.mEntries.push_back({newState->mCostBasisAfter, -newState->mBalanceAfter});

            accountEntry.mAmount = newState->mBalanceAfter;
            accountEntry.mCostBasis = newState->mCostBasisAfter;
        }
        else
        {
            // Enough balance to transfer
            newState->mCostBasisAfter = newState->mCostBasisBefore;

            transfer.mEntries.push_back({accountEntry.mCostBasis, -amount});

            accountEntry.mAmount = newState->mBalanceAfter;
        }

        mSharedState->mTransfers.push_back(std::move(transfer));
        return newState;
    }

    [[nodiscard]] CbAcbState* finalizeTransfer(const PgString& account, const PgString& sourceAccount, const std::optional<PgString>& transferId, double amount, int64_t tag)
    {
        CbAcbAccountEntry& accountEntry = mSharedState->mAccountEntries[account];
        CbAcbState* newState = CbAcbState::newState(this);

        CbTransfer<CbAcbAccountEntry> transferKey{transferId, sourceAccount, account, amount, {}};
        auto transferIter = std::find(mSharedState->mTransfers.begin(), mSharedState->mTransfers.end(), transferKey);
        if (transferIter == mSharedState->mTransfers.end()) [[unlikely]]
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("tag %ld: can't finalize transfer %s -> %s %g, unable to match with initiating record",
                            tag, sourceAccount.c_str(), account.c_str(), amount)));
            return newState;
        }

        if (std::abs(transferIter->mAmount - amount) > TRANSFER_AMOUNT_EPSILON) [[unlikely]]
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("tag %ld: can't finalize transfer, in/out amounts mismatch: %g, %g",
                            tag, transferIter->mAmount, amount)));
            return newState;
        }

        for (auto& e : transferIter->mEntries)
            newState->realizeImpl(accountEntry, e.mCostBasis, e.mAmount);

        mSharedState->mTransfers.erase(transferIter);

        return newState;
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

        for (auto& [account, accountEntry] : mSharedState->mAccountEntries)
        {
            if (std::abs(accountEntry.mAmount) >= AMOUNT_EPSILON)
            {
                ereport(INFO,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("remaining amount detected %s %g, not all amount was realized at end",
                                account.c_str(), accountEntry.mAmount)));
            }
        }
    }

private:
    void realizeImpl(CbAcbAccountEntry& accountEntry, double price, double amount)
    {
        mCostBasisBefore = accountEntry.mCostBasis;
        mBalanceBefore = accountEntry.mAmount;
        mBalanceAfter = accountEntry.mAmount + amount;

        if (std::abs(mBalanceAfter) < AMOUNT_EPSILON)
            mBalanceAfter = 0.0;

        // -- open position, increase position
        if (std::signbit(mBalanceBefore) == std::signbit(amount))
        {
            mCostBasisAfter = mBalanceAfter == 0.0 ?
                        mCostBasisBefore :
                        (accountEntry.mCostBasis * accountEntry.mAmount + price * amount) / mBalanceAfter;
        }
        // close position and do NOT cross 0 volume
        // cost basis doesn't change as a result
        else if (std::signbit(mBalanceBefore) == std::signbit(mBalanceAfter))
        {
            mCostBasisAfter = mCostBasisBefore;
            mCapitalGain += amount * (mCostBasisBefore - price);
        }
        // close position and cross 0
        // cost basis becomes equal to price
        else
        {
            mCostBasisAfter = price;
            mCapitalGain += mBalanceBefore * (price - mCostBasisBefore);
        }

        accountEntry.mCostBasis = mCostBasisAfter;
        accountEntry.mAmount = mBalanceAfter;
    }
};

// IMPORTANT: If this fails, change the expected size and adjust(!!!) pg_cost_basis--1.0.sql
static_assert(sizeof(CbAcbState) == 48);

}

extern "C" {

PG_FUNCTION_INFO_V1(CbAcbState_in);
Datum CbAcbState_in(PG_FUNCTION_ARGS)
{
    CbAcbState* state = CbAcbState::newState();
    PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(CbAcbState_out);
Datum CbAcbState_out(PG_FUNCTION_ARGS)
{
    CbAcbState* state = reinterpret_cast<CbAcbState*>(PG_GETARG_POINTER(0));
    char *result = psprintf("(%g,%g,%g,%g,%g)", state->mCostBasisBefore, state->mCostBasisAfter, state->mBalanceBefore, state->mBalanceAfter, state->mCapitalGain);
    PG_RETURN_CSTRING(result);
}

PG_FUNCTION_INFO_V1(CbAcbState_cost_basis_before);
Datum CbAcbState_cost_basis_before(PG_FUNCTION_ARGS)
{
    CbAcbState* state = reinterpret_cast<CbAcbState*>(PG_GETARG_POINTER(0));
    PG_RETURN_FLOAT8(state->mCostBasisBefore);
}

PG_FUNCTION_INFO_V1(CbAcbState_cost_basis_after);
Datum CbAcbState_cost_basis_after(PG_FUNCTION_ARGS)
{
    CbAcbState* state = reinterpret_cast<CbAcbState*>(PG_GETARG_POINTER(0));
    PG_RETURN_FLOAT8(state->mCostBasisAfter);
}

PG_FUNCTION_INFO_V1(CbAcbState_balance_before);
Datum CbAcbState_balance_before(PG_FUNCTION_ARGS)
{
    CbAcbState* state = reinterpret_cast<CbAcbState*>(PG_GETARG_POINTER(0));
    PG_RETURN_FLOAT8(state->mBalanceBefore);
}

PG_FUNCTION_INFO_V1(CbAcbState_balance_after);
Datum CbAcbState_balance_after(PG_FUNCTION_ARGS)
{
    CbAcbState* state = reinterpret_cast<CbAcbState*>(PG_GETARG_POINTER(0));
    PG_RETURN_FLOAT8(state->mBalanceAfter);
}

PG_FUNCTION_INFO_V1(CbAcbState_capital_gain);
Datum CbAcbState_capital_gain(PG_FUNCTION_ARGS)
{
    CbAcbState* state = reinterpret_cast<CbAcbState*>(PG_GETARG_POINTER(0));
    PG_RETURN_FLOAT8(state->mCapitalGain);
}

PG_FUNCTION_INFO_V1(CbAcb_sfunc);
Datum CbAcb_sfunc(PG_FUNCTION_ARGS)
{
    return commonSFunc<CbAcbState>(fcinfo);
}

} // extern "C"
