
from (
 from ${hiveconf:SRC}
 select
  ${hiveconf:ID} as id_expr
  ,${hiveconf:TS} as ts_expr
  ,${hiveconf:EXPR} as event_expr
) as sub
select id_expr, regexpr_replace(concat_ws(' ',sort_array(collect_set(concat_ws('#',ts_expr, event_expr)))), '\d+#','') as seq
group by id_expr
