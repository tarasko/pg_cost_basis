# Overview
This Postgres extension introduces aggregates that help to track assets cost basis and calculate realized capital gains.
Implemented in C++, much faster than pure SQL or PLPGSQL alternative. 
Currently, only ACB and FIFO methods are supported. Feel free to contribute other methods (LIFO, HIFO etc). 
I'm also open to changing the existing interface. It's not set in stone.
## Build and install
```
# Make sure that you have postgresql-server-dev installed. 
sudo apt install postgresql-server-dev-16

# Clone the extension, make build directory inside, run cmake and then make install
cd pg_cost_basis
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
sudo make VERBOSE=1 install
```
Now, connect to the database and register the extension
```
create extension pg_cost_basis;
```

## Example
### Create test_data view.
```
create or replace view test_data as 
select *, lag(tag, 1) over (order by tag) prev_tag 
from (
        values
        -- acquire some assets on 2 exchanges
        ('exch_2', null, 1000, 3, 1, null::bool, null),
        ('exch_1', null, 1100, 1, 2, null::bool, null),

        -- transfer assets from exch_2 to exch_1
        -- match out/in transfer records by transfer_id
        ('exch_2', 'exch_1', null, -2, 3, null::bool, 'transfer1'), 
        ('exch_1', 'exch_2', null, 2, 4, null::bool, 'transfer1'),

        ('exch_1', null, 1400, -1, 5, null::bool, null),	
        ('exch_1', null, 1500, -2, 6, null::bool, null),	
        ('exch_1', null, 2000, 1, 7, null::bool, null),
        ('exch_1', null, 2100, -2, 8, null::bool, null),
        ('exch_1', null, 2000, 2, 9, null::bool, null),

        -- transfer without transfer_id, match out/in records by triplet (source, destination, amount)
        ('exch_1', 'exch_2', null, -1, 10, null::bool, null),
        ('exch_2', 'exch_1', null, 1, 11, null::bool, null)		
        )
as t(account, dest_account, price, amount, tag, ignore_transfer, transfer_id);
```
### Calculate realized gains using average cost basis without transfers (ACB0) method. 
All transfers are ignored and cost basis is calculate for entier balance across of all accounts.
This is the fastest and least memory consuming cost basis method.
```
select account, dest_account, price, amount, tag, transfer_id,
        cb_acb0_capital_gain(s) capital_gain,
        cb_acb0_balance_before(s) balance_before,
        cb_acb0_balance_after(s) balance_after,
        cb_acb0_cost_basis_before(s) cost_basis_before,
        cb_acb0_cost_basis_after(s) cost_basis_after
from (
        select *, cb_acb0(price, case when dest_account is null then amount else 0.0 end) over (order by tag) s
        from test_data
)
```
|account|dest_account|price|amount|tag|transfer_id|capital_gain|balance_before|balance_after|cost_basis_before|cost_basis_after|
|-------|------------|-----|------|---|-----------|------------|--------------|-------------|-----------------|----------------|
|exch_2|[NULL]|1,000|3|1|[NULL]|0|0|3|1|1,000|
|exch_1|[NULL]|1,100|1|2|[NULL]|0|3|4|1,000|1,025|
|exch_2|exch_1|[NULL]|-2|3|transfer1|0|4|4|1,025|1,025|
|exch_1|exch_2|[NULL]|2|4|transfer1|0|4|4|1,025|1,025|
|exch_1|[NULL]|1,400|-1|5|[NULL]|375|4|3|1,025|1,025|
|exch_1|[NULL]|1,500|-2|6|[NULL]|950|3|1|1,025|1,025|
|exch_1|[NULL]|2,000|1|7|[NULL]|0|1|2|1,025|1,512.5|
|exch_1|[NULL]|2,100|-2|8|[NULL]|1,175|2|0|1,512.5|1,512.5|
|exch_1|[NULL]|2,000|2|9|[NULL]|0|0|2|1,512.5|2,000|
|exch_1|exch_2|[NULL]|-1|10|[NULL]|0|2|2|2,000|2,000|
|exch_2|exch_1|[NULL]|1|11|[NULL]|0|2|2|2,000|2,000|

### Calculate realized gains using average-cost-basis (ACB) method with transfers support.
```
select account, dest_account, price, amount, tag, transfer_id,
        cb_acb_capital_gain(state) capital_gain,
        cb_acb_balance_before(state) balance_before,
        cb_acb_balance_after(state) balance_after,
        cb_acb_cost_basis_before(state) cost_basis_before,
        cb_acb_cost_basis_after(state) cost_basis_after
from (
        select *, 
                cb_acb(account, dest_account, price, amount, tag, prev_tag, ignore_transfer, transfer_id) over (order by tag) state
        from test_data
)
```

|account|dest_account|price|amount|tag|transfer_id|capital_gain|balance_before|balance_after|cost_basis_before|cost_basis_after|
|-------|------------|-----|------|---|-----------|------------|-------------|----------------|
|exch_2|[NULL]|1,000|3|1|[NULL]|0|0|3|1|1,000|
|exch_1|[NULL]|1,100|1|2|[NULL]|0|0|1|1|1,100|
|exch_2|exch_1|[NULL]|-2|3|transfer1|0|3|1|1,000|1,000|
|exch_1|exch_2|[NULL]|2|4|transfer1|0|1|3|1,100|1,033.3333333333|
|exch_1|[NULL]|1,400|-1|5|[NULL]|366.6666666667|3|2|1,033.3333333333|1,033.3333333333|
|exch_1|[NULL]|1,500|-2|6|[NULL]|933.3333333333|2|0|1,033.3333333333|1,033.3333333333|
|exch_1|[NULL]|2,000|1|7|[NULL]|0|0|1|1,033.3333333333|2,000|
|exch_1|[NULL]|2,100|-2|8|[NULL]|100|1|-1|2,000|2,100|
|exch_1|[NULL]|2,000|2|9|[NULL]|100|-1|1|2,100|2,000|
|exch_1|exch_2|[NULL]|-1|10|[NULL]|0|1|0|2,000|2,000|
|exch_2|exch_1|[NULL]|1|11|[NULL]|0|1|2|1,000|1,500|

### Calculate realized gains using first-in-first-out (FIFO) method with transfers support.
```
select account, dest_account, price, amount, tag, transfer_id,
	cb_fifo_capital_gain(fifo) capital_gain,
	cb_fifo_realized_tags(fifo) realized_tags,
	cb_fifo_realized_entries(fifo) realized_entries
from (
	select *, 
		cb_fifo(account, dest_account, price, amount, tag, prev_tag, ignore_transfer, transfer_id) over (order by tag) fifo
	from test_data
)
```

|account|dest_account|price|amount|tag|transfer_id|capital_gain|realized_tags|realized_entries|
|-------|------------|-----|------|---|-----------|------------|-------------|----------------|
|exch_2|[NULL]|1,000|3|1|[NULL]|0|{}|[]|
|exch_1|[NULL]|1,100|1|2|[NULL]|0|{}|[]|
|exch_2|exch_1|[NULL]|-2|3|transfer1|0|{}|[]|
|exch_1|exch_2|[NULL]|2|4|transfer1|0|{}|[]|
|exch_1|[NULL]|1,400|-1|5|[NULL]|300|{2}|[{"a": 1.00000000, "t": 2, "cb": 1100.00000000, "pl": 300.00000000}]|
|exch_1|[NULL]|1,500|-2|6|[NULL]|1,000|{1}|[{"a": 2.00000000, "t": 1, "cb": 1000.00000000, "pl": 1000.00000000}]|
|exch_1|[NULL]|2,000|1|7|[NULL]|0|{}|[]|
|exch_1|[NULL]|2,100|-2|8|[NULL]|100|{7}|[{"a": 1.00000000, "t": 7, "cb": 2000.00000000, "pl": 100.00000000}]|
|exch_1|[NULL]|2,000|2|9|[NULL]|100|{8}|[{"a": -1.00000000, "t": 8, "cb": 2100.00000000, "pl": 100.00000000}]|
|exch_1|exch_2|[NULL]|-1|10|[NULL]|0|{}|[]|
|exch_2|exch_1|[NULL]|1|11|[NULL]|0|{}|[]|
