# pg_cost_basis
Postgres extension with aggregates that help to track assets cost basis and calculate realized capital gains.
Implemented in C++, much faster than pure SQL or PLPGSQL alternative. 
Currently, only ACB and FIFO methods are supported. Feel free to contribute other methods (LIFO, HIFO etc). 
I'm also open to changing the existing interface. It's not set in stone.

### Example
Create test_data view.
```
create or replace view test_data as 
select *, lag(tag, 1) over (order by tag) prev_tag 
from (
	values
	-- acquire some assets on 2 different exchanges
	('exch_2', null, 1000, 2, 1, null::bool, null),
	('exch_1', null, 1100, 1, 2, null::bool, null),
	
	-- transfer assets from exch_2 to exch_1
	-- out-record should always go first and has negative amount
	-- in-record goes later, not necessary immediately. In-record finalize transfer.
	-- match out/in transfer records by transfer_id
	('exch_2', 'exch_1', null, -2, 3, null::bool, 'transfer1'), 
	('exch_1', 'exch_2', null, 2, 4, null::bool, 'transfer1'),
  
	('exch_1', null, 1500, -3, 5, null::bool, null),	  -- dispose
	('exch_1', null, 2000, 1, 6, null::bool, null),     -- buy more
	('exch_1', null, 2100, -2, 7, null::bool, null),    -- dispose and go negative 
	('exch_1', null, 2000, 2, 8, null::bool, null),     -- close negative position, acquire some
	-- transfer from exch_1 to exch_2
	-- transfer_id is null (not available), so match out/in records by (source, destination, amount)
	('exch_1', 'exch_2', null, -1, 9, null::bool, null),
	('exch_2', 'exch_1', null, 1, 10, null::bool, null)		
	)
as t(account, dest_account, price, amount, tag, ignore_transfer, transfer_id);
```
Calculate realized gains using average cost basis (ACB) method
```
select *,
	cb_acb_capital_gain(s),
	cb_acb_cost_basis_before(s),
	cb_acb_balance_before(s),
	cb_acb_cost_basis_after(s),
	cb_acb_balance_after(s)
from (
	select *, cb_acb(price, case when dest_account is null then amount else 0.0 end) over (order by tag) s
	from test_data
)
```
Calculate realized gains using first-in-first-out (FIFO) method
```
select *,
	cb_fifo_capital_gain(fifo),
	cb_fifo_realized_tags(fifo),
	cb_fifo_realized_entries(fifo)
from (
	select *, 
		cb_fifo(account, dest_account, price, amount, tag, prev_tag, ignore_transfer, transfer_id) over (order by tag) fifo
	from test_data
)
```
