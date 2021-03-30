-- SORT_QUERY_RESULTS

select x,col1 from (select x,array(1,2,3) as arr from foo) f lateral view explode(arr) tbl1 as col1;

SELECT key, value FROM (SELECT key FROM src group by key) a lateral view explode(array(1, 2)) value as value;

SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol SORT BY key ASC, myCol ASC LIMIT 1;

select col1 from foo lateral view explode(array(x,y)) tbl1 as col1;

SELECT col1, col2 FROM nested LATERAL VIEW explode(s2.f8.f10) tbl1 AS col1 LATERAL VIEW explode(s3.f12) tbl2 AS col2;

select col1,d from baz lateral view hiveudtf(ai) tbl1 as col1;

select col1,col2,d from baz lateral view hiveudtf(ai) tbl1 as col1 lateral view hiveudtf(ai) tbl2 as col2;

select col1 from foo lateral view myudtf(x,y) tbl1 as col1;
