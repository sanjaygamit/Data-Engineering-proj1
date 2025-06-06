
#-------------------------------------------------#
1.  Three consecutive days where cnt exceeds 100
with ds as (
select  
id, dt, cnt , rownum , id - rownum as r, 
count(*) over(partition by id-rownum) g_cnt
from t where cnt >=100)
select id, dt, cnt, g_cnt from ds where g_cnt >=3; 
; 
#-------------------------------------------------#


#-------------------------------------------------#
2.  All possible combination of numbers 


with ds1 as (select 4 n from dual ), 
ds2 (select rownum r 
from dual connect by level <= (select n from ds1))
select 
listagg('[]'||t1.r||','|| t2.r||']',',') within group (order by t1.r,t2.r)
from ds2 t1 cross join ds2 t2
where t2.r>t1.r;
#-------------------------------------------------#
 

#-------------------------------------------------#
3. Oracle SQL to Compute values based on previous rows
with ds as (select id, dt,val,to_date(dt,'MON-YY') val_dt, 
            row_number() over(partiion by id order b y to_date(dt,'MON-YY')) rn, 
            sum(sal)over(partition by id order by do_date(dt,'MON-YY')) sum_Val 
            from t )
select id, dt, val, sum_val/rn as avg_val from ds;
#-------------------------------------------------#

#-------------------------------------------------#
4.Oracle SQL to Find missing number in given range of values

select name, s_start+l - 1 
from a, lateral(select level l from dual connect by level <= a.s_end - a.s_start+1); 
#-------------------------------------------------#


#-------------------------------------------------#
5. Oracle SQL to concatenate previous row value continuously

with ds as (select col1, col2, 
        listagg(col2,',') within group (order by col2) over (partition by col1) agg, 
        row_number()over(partition by col1 order by col2) rn
from t)
select col1, col2, agg, rn 
instr(agg||',',',',1,rn),
substr(agg,1,instr(agg||',',',',1,rn)-1)
from ds
;         

#-------------------------------------------------#
6. compute count of group of repeating values

with ds as (select distinct cd, 
row_number() over(order by dt),
row_number() over(partition by cd order by dt) rn
from t ) 

select cd, count(*) from ds groi by cd order by cd;
#-------------------------------------------------# 


#-------------------------------------------------# 
7. 