
Team_Name
1 India
2 Pakistan
3 Australia
4 Afghanistan

Team Opponent_Team
Australia | Afghanistan
India     | Afghanistan
Pakistan  | Afghanistan
India     | Australia
Pakistan  | Australia
Pakistan  | India

select t1.team team, t2.team Opponent_team 
from team t1 cross join team t2
where t1.team < t2.team

or 

with ds as (select team_name, 
row_number() over(order by team_name) as teamid 
from team)

select t1.team_name team, t2.team_name Opponent_team
from ds t1 cross join ds t2
where t1.teamid < t2.teamid; 

#-------------------------------------------------#
1. Find the ranking of student in each department such that   
    a) Marks should be in descending order 
    b) Gap in ranks. 

   Logic for 6th column 
    a) Marks should be in descending order 
    b) Find the ranking of student in each department such that there should be no gap between 2 ranks. 

    select 
        rank() over(partition by dept order by marks asc) as Rnk, 
        dense_rank() over(partition by dept order by marks desc) d_rank   
    from class; 

  https://www.youtube.com/watch?v=FiuvGT_ACSA&list=PLP1_hACWUYHxV0r2alS0i_H7oLOH5DXh3&index=2 

#-------------------------------------------------#

#-------------------------------------------------#
 https://www.youtube.com/watch?v=_pc04NUzsg4&list=PLP1_hACWUYHxV0r2alS0i_H7oLOH5DXh3&index=3
Find the change in sales qty w.r.t prev month sales for each product.

 with ds as (select 
    product_id,
    Month_number,
 lag(sales_qty,1,sales_qty) over(partition by product_id order by Month_number) pre_month_qty, 
    sales_qty
 from sales)
 select 
product_id, Month_number, Sales_qty, pre_month_qty, Sales_qty  pre_month_qty as diff_qty
 from ds ; 
 
#-------------------------------------------------#

#-------------------------------------------------#
1. Find the active transaction for each user with transaction start and end time for each individual transaction. 

with ds as (select 
Transaction_date as Transaction_start_time,
coalesce(dateadd(second,-1,lead(transaction_date,over(partition by user_id order by transaction_date))),'9999-12-12 23:59:59') as Transaction_end_time 
from 
transaction)

select 
case when Transaction_end_time = '9999-12-12 23:59:59' then 'Active' 
else 'Inactive' end as IsActiveTransaction; 

#-------------------------------------------------#

#-------------------------------------------------#
Find the running sum of order price for every customer. 

"running sum"  is the cumulative total of a sequence of numbers, where each new number is added to the total of all previous numbers in the sequence.



#-------------------------------------------------#


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
7.Oracle SQL to compute group number for repeating number 

with ds as (select  
            --nvl(lag(c2,1) over(order by c1),0) r, 
            case when c2 <> nvl(lag(c2,1) over(order by c1),0)
            then 1 else 0 end r 
            from t)
           select c1,c2,r,sum 
           c1,c2,sum(r) over(order by c1) from ds ; 

#-------------------------------------------------#

#-------------------------------------------------#
8. Write a query to complete the start and end of group in the given missing sequence of numbers. 
select c, rownum, c-rownum as r 
from t; 

select min(c), max(c), c-rownum 
from t 
group by c-rownum 
order by 1; 


(select start_range, end_range from t1) t1, 
(select rownum r from dual connect by level <= (select max(end_range) from t1)t2)
where  t2.r >= t1.start_range and t2.r <= t1.end_range
-----  t2.r between t1.start_range and  t1.end_range
; 

select start_range, end_range, 
    r-1, 
    start_range + r-1 
from t, 
lateral(select rownum r from dual connect by level <= end_range - start_range + 1) ;     


#-------------------------------------------------#
9. Write a query to split the data into multiple groups. 

with ds as (
select sno,sname,NTILE(3) over(order by sno) grp from t

)
select 
    grp,
    null sno, 
    null sname, 
    MIN(sno) min_sno, 
    MAX(sno) max_sno,
from ds
group by grp 
UNION ALL 
select 
    grp, 
    sno, 
    sname, 
    null, 
    null
from ds
order by grp, sno, nulls last;     
#-------------------------------------------------#


 #-------------------------------------------------#
 10. Write a SQL to check whether two strings are "anagram" of each other. 
 
 "ANAGRAM" An anagram is a word or phrase formed by rearranging the letters of a different word or phrase, typically using all the original letters exactly once. 
 Ex : HEART ==> EARTH
 
 select sno, s1, s2, greatest(LENGTH(S1), LENGTH(S2)), l
 substr(s1,l,1) c1, 
 substr(s2,l,1) c2, 
 listagg(substr(s1,l,1)) within group (order by substr(s1,l,1)) str1,
  listagg(substr(s1,l,1)) within group (order by substr(s1,l,1)) str2, 
  case when listagg(substr(s1,l,1)) within group (order by substr(s1,l,1)) = 
         listagg(substr(s2,l,1)) within group (order by substr(s2,l,1)) 
      then 'ANAGRAM' else 'NOT ANAGRAM' end anagram
 from t
 lateral(select level l from dual connect by level <= greatest(length(s1), length(s2))); 


select 
-- greatest(length(s1),length(s2)), 
-- substr(s1,l,1) c1, 
-- substr(s2,l,1) c2,
listagg(substr(s1,l,1)) within group (order by substr(s1,l,1)) =  listagg(substr(s2,l,1)) within group (order by substr(s2,l,1)) then 'ANAGRAM' else 'NOT ANAGRAM' end anagram
from t 
lateral(select level l from dual connect by level <= greatest(length(s1),length(s2))); 
#-------------------------------------------------#


#-------------------------------------------------#
11. Write a SQL to transform data as given below. 
    -  Tax rate is considered  "added" if no rate existed a day before the current activation data. 
    - Tax rate is considered "Modified" if the rate has changed as compared to previous date. 
    - Tax rate is considered "Removed" if no rate on the immediate next date. 
