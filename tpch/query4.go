package main

var query4 = `
select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= '1994-08-01'
	and o_orderdate < date_add('1994-08-01', interval '3' month)
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority;
`
