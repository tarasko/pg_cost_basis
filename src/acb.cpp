#include <cmath>

extern "C"
{
#include <postgres.h>
#include <fmgr.h>

struct CbAcbState
{
    double mCostBasisBefore;
    double mCostBasisAfter;
    double mBalanceBefore;
    double mBalanceAfter;
    double mCapitalGain;
};

// IMPORTANT: If this fails, change the expected size and adjust(!!!) pg_cost_basis--1.0.sql
static_assert(sizeof(CbAcbState) == 40);

PG_FUNCTION_INFO_V1(CbAcbState_in);
Datum CbAcbState_in(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);

    double a;
    double b;
    double c;
    double d;
    double e;

    if (sscanf(str, " ( %lf , %lf , %lf , %lf , %lf )", &a, &b, &c, &d, &e) != 5)
    {
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
               errmsg("invalid input syntax for AcbState: \"%s\"", str)));
    }

    CbAcbState* state = new (palloc(sizeof(CbAcbState))) CbAcbState{a, b, c, d, e};
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
    if (PG_ARGISNULL(0)) [[unlikely]]
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("acb state can't be null")));
    }
    CbAcbState* state = reinterpret_cast<CbAcbState*>(PG_GETARG_POINTER(0));

    if (PG_ARGISNULL(2)) [[unlikely]]
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("amount can't be null null")));
    }
    double amount = PG_GETARG_FLOAT8(2);

    void* buffer = palloc(sizeof(CbAcbState));
    CbAcbState* newState;

    // CbAcbState (cost_basis_before, cost_basis_after, balance_before, balance_after, capital_gains)
    // std::signbit - returns false for positives, true for negatives

    if (amount == 0) [[unlikely]]
    {
        newState = new (buffer) CbAcbState{
                state->mCostBasisAfter, state->mCostBasisAfter,
                state->mBalanceAfter, state->mBalanceAfter,
                0.0
        };
    }
    else
    {
        if (PG_ARGISNULL(1)) [[unlikely]]
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("price can't be null")));
        }
        double price = PG_GETARG_FLOAT8(1);

        // -- open position, increase position
        if (std::signbit(state->mBalanceAfter) == std::signbit(amount))
        {
            newState = new (buffer) CbAcbState{
                    state->mCostBasisAfter, (state->mCostBasisAfter * state->mBalanceAfter + price * amount) / (state->mBalanceAfter + amount),
                    state->mBalanceAfter, state->mBalanceAfter + amount,
                    0.0
            };
        }
        // close position and do NOT cross 0 volume
        // cost basis doesn't change as a result
        else if (std::signbit(state->mBalanceAfter) == std::signbit(state->mBalanceAfter + amount))
        {
            newState = new (buffer) CbAcbState{
                    state->mCostBasisAfter, state->mCostBasisAfter,
                    state->mBalanceAfter, state->mBalanceAfter + amount,
                    amount * (state->mCostBasisAfter - price)
            };
        }
        // close position and cross 0
        // cost basis becomes equal to price
        else
        {
            newState = new (buffer) CbAcbState{
                    state->mCostBasisAfter, price,
                    state->mBalanceAfter, state->mBalanceAfter + amount,
                    state->mBalanceAfter * (price - state->mCostBasisAfter)
            };
        }
    }

    PG_RETURN_POINTER(newState);
}

} // extern "C"

