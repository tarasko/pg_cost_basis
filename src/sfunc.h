#pragma once

#include "common.h"

extern "C"
{
#include <postgres.h>
#include <fmgr.h>
}

template<typename CostBasisState>
Datum commonSFunc(PG_FUNCTION_ARGS)
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
                 errmsg("tag %lu: state can't be null", tag)));
    }
    CostBasisState* state = reinterpret_cast<CostBasisState*>(PG_GETARG_POINTER(0));

    if (PG_ARGISNULL(1)) [[unlikely]]
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("tag %lu: account can't be null", tag)));
    }
    PgString account = textToString<PgString>(PG_GETARG_TEXT_PP(1));

    if (PG_ARGISNULL(4)) [[unlikely]]
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("tag %lu: amount can't be null null", tag)));
    }
    double amount = PG_GETARG_FLOAT8(4);

    // Reset the state when there is no previous tag in the group
    // I was very surprised to learn that
    // cb_fifo(...) over (partition by ... order by tag)
    // doesn't create a new aggregate for each partition, but just tries to re-use state from previous partition
    // This helps to detect that we begin with a new partition.
    if (PG_ARGISNULL(6)) [[unlikely]]
    {
        state->validateAtEnd();
        state = CostBasisState::newState();
    }

    CostBasisState* newState;

    // if source_or_destination defined then it is a transfer

    if (PG_ARGISNULL(2)) [[likely]]
    {
        if (PG_ARGISNULL(3)) [[unlikely]]
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("tag %lu: price can't be null", tag)));
        }
        double price = PG_GETARG_FLOAT8(3);

        newState = state->realize(account, price, amount, tag);
    }
    else
    {
        bool ignoreTransfer = false;
        if (!PG_ARGISNULL(7))
            ignoreTransfer = PG_GETARG_BOOL(7);

        std::optional<double> price;
        if (!PG_ARGISNULL(3))
            price = PG_GETARG_FLOAT8(3);

        if (ignoreTransfer)
            newState = CostBasisState::newState(state);
        else
        {
            std::optional<PgString> transferId;
            if (!PG_ARGISNULL(8))
                transferId = textToString<PgString>(PG_GETARG_TEXT_PP(8));

            if (amount < 0)
            {
                PgString destinationAccount = textToString<PgString>(PG_GETARG_TEXT_PP(2));
                newState = state->initiateTransfer(account, destinationAccount, transferId, amount, price, tag);
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
