package main

var query6 = `
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= '1997-01-01'
	and l_shipdate < date_add('1997-01-01', interval '1' year)
	and l_discount between 0.07 - 0.01 and 0.07 + 0.01
	and l_quantity < 24;
`
