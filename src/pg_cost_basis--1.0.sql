CREATE TYPE cb_acb_state;

CREATE FUNCTION cb_acb_state_in(cstring)
    RETURNS cb_acb_state
    AS 'MODULE_PATHNAME', 'CbAcbState_in'
    LANGUAGE C IMMUTABLE STRICT
    PARALLEL SAFE;
	
CREATE FUNCTION cb_acb_state_out(cb_acb_state)
    RETURNS cstring
    AS 'MODULE_PATHNAME', 'CbAcbState_out'
    LANGUAGE C IMMUTABLE STRICT
    PARALLEL SAFE;

CREATE FUNCTION cb_acb_cost_basis_before(cb_acb_state)
    RETURNS float
    AS 'MODULE_PATHNAME', 'CbAcbState_cost_basis_before'
    LANGUAGE C IMMUTABLE STRICT
    PARALLEL SAFE;

CREATE FUNCTION cb_acb_cost_basis_after(cb_acb_state)
    RETURNS float
    AS 'MODULE_PATHNAME', 'CbAcbState_cost_basis_after'
    LANGUAGE C IMMUTABLE STRICT
    PARALLEL SAFE;

CREATE FUNCTION cb_acb_balance_before(cb_acb_state)
    RETURNS float
    AS 'MODULE_PATHNAME', 'CbAcbState_balance_before'
    LANGUAGE C IMMUTABLE STRICT
    PARALLEL SAFE;

CREATE FUNCTION cb_acb_balance_after(cb_acb_state)
    RETURNS float
    AS 'MODULE_PATHNAME', 'CbAcbState_balance_after'
    LANGUAGE C IMMUTABLE STRICT
    PARALLEL SAFE;

CREATE FUNCTION cb_acb_capital_gain(cb_acb_state)
    RETURNS float
    AS 'MODULE_PATHNAME', 'CbAcbState_capital_gain'
    LANGUAGE C IMMUTABLE STRICT
    PARALLEL SAFE;

CREATE TYPE cb_acb_state (
   internallength = 40,
   input = cb_acb_state_in,
   output = cb_acb_state_out,
   alignment = double
);

CREATE FUNCTION cb_acb_sfunc(cb_acb_state, float, float)
    RETURNS cb_acb_state
    AS 'MODULE_PATHNAME', 'CbAcb_sfunc'
    LANGUAGE C IMMUTABLE
    PARALLEL SAFE;

CREATE OR REPLACE AGGREGATE cb_acb(price float, amount float)
(
    sfunc = cb_acb_sfunc,
    stype = cb_acb_state,
    -- cost_basis_before, cost_basis_after, balance_before, balance_after, capital_gain
    initcond = '(1,1,0,0,0)',
    parallel = safe
);

CREATE TYPE cb_fifo_state;

CREATE FUNCTION cb_fifo_state_in(cstring)
    RETURNS cb_fifo_state
    AS 'MODULE_PATHNAME', 'CbFifoState_in'
    LANGUAGE C IMMUTABLE STRICT
    PARALLEL SAFE;

CREATE FUNCTION cb_fifo_state_out(cb_fifo_state)
    RETURNS cstring
    AS 'MODULE_PATHNAME', 'CbFifoState_out'
    LANGUAGE C IMMUTABLE STRICT
    PARALLEL SAFE;

CREATE TYPE cb_fifo_state (
   internallength = 40,
   input = cb_fifo_state_in,
   output = cb_fifo_state_out,
   alignment = double
);

CREATE FUNCTION cb_fifo_capital_gain(cb_fifo_state)
    RETURNS float
    AS 'MODULE_PATHNAME', 'CbFifo_capital_gain'
    LANGUAGE C IMMUTABLE STRICT
    PARALLEL SAFE;

CREATE FUNCTION cb_fifo_realized_tags(cb_fifo_state)
    RETURNS bigint[]
    AS 'MODULE_PATHNAME', 'CbFifo_realized_tags'
    LANGUAGE C IMMUTABLE STRICT
    PARALLEL SAFE;

CREATE FUNCTION cb_fifo_realized_entries(cb_fifo_state)
    RETURNS jsonb
    AS 'MODULE_PATHNAME', 'CbFifo_realized_entries'
    LANGUAGE C IMMUTABLE STRICT
    PARALLEL SAFE;

CREATE FUNCTION cb_fifo_sfunc(cb_fifo_state, account text, source_or_destination_account text, price float, amount float, tag bigint, prev_tag bigint, ignore_transfer bool, transfer_id text)
    RETURNS cb_fifo_state
    AS 'MODULE_PATHNAME', 'CbFifo_sfunc'
    LANGUAGE C IMMUTABLE
    PARALLEL SAFE;

CREATE OR REPLACE AGGREGATE cb_fifo(account text, source_or_destination_account text, price float, amount float, tag bigint, prev_tag bigint, ignore_transfer bool, transfer_id text)
(
    sfunc = cb_fifo_sfunc,
    stype = cb_fifo_state,
    initcond = '',
    parallel = safe
);
