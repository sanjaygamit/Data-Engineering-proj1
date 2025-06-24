1. Can you describe your experience with Oracle PLSQL and how it relates to this role ?
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

 

#-------------------------------------------------#

#-------------------------------------------------#
 
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
-- 1. Find the active transaction for each user with transaction start and end time for each individual transaction. 

with ds as (select 
Transaction_date as Transaction_start_time,
coalesce(dateadd(second,-1,lead(transaction_date) over(partition by user_id order by transaction_date)),'9999-12-12 23:59:59') as Transaction_end_time 
from 
transaction)

select 
case when Transaction_end_time = '9999-12-12 23:59:59' then 'Active' 
else 'Inactive' end as IsActiveTransaction; 

#-------------------------------------------------#

#-------------------------------------------------#
Find the running sum of order price for every customer. 


-- "running sum"  is the cumulative total of a sequence of numbers, where each new number is added to the total of all previous numbers in the sequence.

with ds as (select (qty*price) as TotalPrice
from customer_order)
select 
sum(TotalPrice) over(Partition by customer_id oder by ItemId) as RunningPrice,

sum(TotalPrice) over(Partition by customer_id oder by ItemId ROWS UNBOUNDED PRECEDING) as RunningPrice,  
-- DEFAULT  "ROWS UNBOUNDED PRECEDING"

sum(TotalPrice) over(Partition by customer_id oder by ItemId ROWS 1 PRECEDING) as RunningPrice1ROWPRECEDING,

sum(TotalPrice) over(Partition by customer_id oder by ItemId ROWS BETWEEN  CURRENT ROW AND 1 FOLLOWING ) as RunningPricFollowing


from ds; 

#-------------------------------------------------#

#-------------------------------------------------#
-- From given item price table find the first month and last month price for each item to analyse the varification of price. 

select 
FIRST_VALUE(PRICE) OVER(PARTITION BY ITEMID ORDER BY MONTHNO) AS FirstMonthPrice,
LAST_VALUE(PRICE) OVER(PARTITION BY ITEMID ORDER BY MONTHNO
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LastMonthPrice
from 
price_table;


#-------------------------------------------------#

#-------------------------------------------------#
From the given attendanceLog input table, write a sql query to get the output as shown below. 
Data
EmpId | Date_Value | Attendance 
1    | 2023-01-01 | Present
1    | 2023-01-02 | Present
1    | 2023-01-03 | Absent
2    | 2023-01-01 | Present     
2    | 2023-01-02 | Absent
2    | 2023-01-03 | Present

output 
EmpId | Start_date | End_date | Attendance
1     | 2023-01-01 | 2023-01-03 | Present
1     | 2023-01-03 | 2023-01-03 | Absent
1     | 2023-01-03 | 2023-01-03 | Present
2     | 2023-01-01 | 2023-01-02 | Present
2     | 2023-01-02 | 2023-01-02 | Absent

Query : 

with ds as (
            select empid, 
            row_number() over(partition by empid, attendance order by date_value) as rn, 

            dateadd(day,-1*(row_number()over(partition by empid, attendance order by date_Value)),date_value) base_date
            from 
            attendancelog
            )

select empid,min(date_value) as start_date , max(date_value) as end_date, attendance 
from ds 
group by empid, base_date,attendance
order by empid,base_date
; 
#-------------------------------------------------#

#-------------------------------------------------#
-- Forward filling in  SQL is a technique used to fill in missing values in a dataset by carrying forward the last known value. This is particularly useful in time series data or datasets where values may be missing for certain periods.


collageid | studentid | Deptid 
1         | 1         | 101
1         | 2         | null
1         | 3         | null
1         | 4         | null
1         | 11        | 201
1         | 12        | null
1         | 13        | null
1         | 14        | null

output 
collageid | studentid | Deptid 
1         | 1         | 101
1         | 2         | 101
1         | 3         | 101
1         | 4         | 101
1         | 11        | 201
1         | 12        | 201
1         | 13        | 201
1         | 14        | 201



ds as  (select collageid, studentid, deptid,
        count(deptid) over(order by studentid) as cnt_deptid 
        
        from 
        studentDetails)
select FIRST_VALUE(deptid) over(partition by cnt_deptid order by studentid) as new_deptid
from ds;

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
  listagg(substr(s2,l,1)) within group (order by substr(s2,l,1)) str2, 
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

Input table 
ACTIVATION_DT | DEACTIVATION_DT | rate
---------------------------------------
01-JAN-18     | 31-JAN-18       | 10
01-FEB-18     | 27-FEB-18       | 15
01-MAR-18     | 30-APR-18       | 12 

_________________________________________
DT            |    RATE         | CHANGE
-----------------------------------------
01-JAN-18     | 10              | ADDED 
01-FEB-18     | 15              | MODIFIED 
27-FEB-18     | 15              | REMOVED 
01-MAR-18     | 12              | ADDED 
30-APR-18     | 12              | REMOVED

ACTIVATION_DT | DEACTIVATION_DT | rate     | P_A_DT     | P_D_DT    | P_RATE   | N_A_DT    | N_D_DT    | N_RATE
-----------------------------------------------------------------------------------------------------------------
01-JAN-18     | 31-JAN-18       | 10       |            |           |          | 01-FEB-18 | 27-FEB-18 | 15   
01-FEB-18     | 27-FEB-18       | 15       | 01-JAN-18  | 31-JAN-18 | 10       | 01-MAR-18 | 30-APR-18 | 12             
01-MAR-18     | 30-APR-18       | 12       | 01-FEB-18  | 27-FEB-18 | 15       |           |           |          


with ds as ()    select ACTIVATION_DT, DEACTIVATION_DT, rate,
            lag(ACTIVATION_DT) OVER (ORDER BY ACTIVATION_DT) AS P_A_DT,
            lag(DEACTIVATION_DT) OVER (ORDER BY ACTIVATION_DT) AS P_D_DT,
            lag(rate) OVER (ORDER BY ACTIVATION_DT) AS P_RATE,
            lead(ACTIVATION_DT) OVER (ORDER BY ACTIVATION_DT) AS N_A_DT,
            lead(DEACTIVATION_DT) OVER (ORDER BY ACTIVATION_DT) AS N_D_DT,
            lead(rate) OVER (ORDER BY ACTIVATION_DT) AS N_RATE    
            from tax_rates ),

DS2 AS    (  select  ACTIVATION_DT, DEACTIVATION_DT, rate, P_A_DT, P_D_DT, P_RATE, N_A_DT, N_D_DT, N_RATE,
                case 
                    when P_A_DT is null OR ACTIVATION_DT <> P_D_DT + 1 then 'ADDED' 
                    WHEN ACTIVATION_DT = P_D_DT +1 AND RATE <> P_RATE THEN 'MODIFIED' 
                END ADD_OR_MOD,
                CASE 
                    WHEN N_A_DT IS NULL THEN OR DEACTIVATION_DT + 1 <> N_A_DT THEN 'REMOVED' 
                END REMOVE 
        FROM DS1 )  

        SELECT ACTIVATION_DT, DEACTIVATION_DT, rate, P_A_DT, P_D_DT, P_RATE, N_A_DT, N_D_DT, N_RATE, ADD_OR_MOD, REMOVE,
            DECODE(1,1,ACTIVATION_DT,DEACTIVATION_DT) DT, 
            RATE, 
            DECODE(1,1,ADD_OR_MOD,REMOVE) CHANGE_LOG
            FROM DS2, (SELECT LEVEL L FROM DUAL CONNECT BY LEVEL <=2)
        ORDER BY ACTIVATION_DT, 1 ;            
                    

41. NTILE function 

with ds as (
    select sno, sname, NTILE(3)over(order by sno) grp
    from student
)
select 
    grp, 
    null sno, 
    null sname, 
    min(sno) min_sno, 
    max(sno) max_sno
from ds
group by grp 
union all 
select 
    grp, 
    sno, 
    sname, 
    null, 
    null
from ds
order by grp, sno nulls last;     

1. Write a query to select the rows that have "A" in any of the columns(col1,col2,col3,col4,col5) without using "OR" keyword. 

select * from table_a where 'A' in (col1,col2,col3,col4,col5); 


2. Write "SQL" statement to select employees getting salary greater than average salary of the department that are working in. 

select deptno, trunc(avg(sal)) av_sal 
from emp 
group by deptno; 

    1.
select * from emp a,(select deptno, trunc(avg(sal)) av_sal 
from emp 
group by deptno) b
where a.deptno = b.deptno and a.sal > b.av_sal; 

    2. 
select * from emp a where a.sal > (select avg(b.sal) from emp b where a.deptno = b.deptno);


    3. 
select * from 
(select empno, ename, job, mgr, hiredate, sal, comm, deptno , 
        avg(sal) over(partition by deptno) as avg_sal 
from emp)        
where sal > avg_sal; 

    4. 
with function avg_saql(p_deptno number) return number as 
    v_avg_sal number; 
    begin 
        sleect avg(sal) into v_avg_sal from emp where deptno = p_deptno; 
    return v_avg_sal; 
    end avg_sal; 

select empno, ename, job, mgr, sal,deptno from emp where sal > avg_sal(deptno);)    


    5. 
    Write "SQL" statement to select data from "TAB1" that are not exists in "TAB2" without using "NOT" keyword. 

    select * from tab1 where not exists (select 1 from tab2 where tab2.c1 = tab1.c1);

    select * from tab1
    minus 
    select * from tab2; 

    select * from tab1
    where 1 > (select count(*) from tab2 where tab1.c1 = tab2.c1); 

    select * from 
    tab1 left outer join tab2 on tab1.c1 = tab2.c1
    where tab2.c1 is null; 

    select tab1.c1, tab2.c1
    from tab1 full outer join tab2 on (tab1.c1 = tab2.c1)
    where tab2.c1 is null; 


    select c1 
    from tab1
    where (select count(1) from tab2 where tab2.c1 = tab1.c1) = 0; 

    4. For the given table "CRICKET"

    MATCH NO | TEAM A    | TEAM B    | WINNER    |  
    ---------|-----------|-----------|-----------|
    1.       | WESTINDIES| SRILANKA  | WESTINDIES|
    2.       | INDIA     | SRILANKA  | INDIA     | 
    3.       | AUSTRALIA | SRILANKA  | AUSTRALIA |
    4.       | WESTINDIES| SRILANKA  | SRILANKA  |
    5.       | AUSTRALIA | INDIA     | AUSTRALIA |
    6.       | WESTINDIES| SRILANKA  | WESTINDIES| 
    7.       | INDIA     | WESTINDIES| WESTINDIES|
    8.       | WESTINDIES| AUSTRALIA | AUSTRALIA |
    9.       | WESTINDIES| INDIA     | INDIA     |
   10.       | AUSTRALIA | WESTINDIES| WESTINDIES|
   11.       | WESTINDIES| SRILANKA  | WESTINDIES|
   12.       | INDIA     | AUSTRALIA | INDIA     |
   13.       | SRILANKA  | NEWZEALAND| SRILANKA  |
   14.       | NEWZEALAND| INDIA     | INDIA     |
   ----------|-----------|-----------|-----------|

   1. Number of matches "played" by each team. 
   2. Number of matches "won" by each team. 
   3. Number of matches "lost" by each team. 


   with matches_played as ( select team_name, count(*) cnt from 
                           (select team_a team_name from cricket 
                           union all 
                           selectc team_b from cricket)
                           group by team_name ), 
        matches_won as ( select WINNER, count(*) cnt from cricket group by winner)                   
    select team_name, matches_played.cnt, nvl(matches_won.cnt,0) from matches_played full outer join matches_won
                        on matches_played.team_name = matches_won.winner; 


    15. Write a query to print number from 1 to "n" numbers. 



print("Minimum height of the binary tree is", min_height_binary(root))

	select 
        substr(s,rownum,1) output1,
        substr(s,rownum*-1,1) output2,
        substr(s,1,rownum) output3,
        substr(s,rownum) output4, 
        rpad(' ',rownum,' ')||substr(s,rownum) output5,
        rpad(' ',length(s)+1-rownum,' ')||substr(s,rownum) output6,
        rpad(' ',rownum,' ')||substr(s,rownum) output5,
from dual, (select 'WELCOME' S from dual) 
connect by level <= length('S');
	
    
  extract name from email id; 

with d as (select 'sanju.gamit@gmai.com' m from dual),
  ds as (    select * from d;   
select substr(m,1,instr(m,'@')-1) n,  
substr(m,instr(m,'@')+1) d 
from d ) 
select 
select substr(n,1,instr(n,'.',1,1))
from ds;


select empno, ename, mgr, sal, sys_connect_by_path(ename, '---->')
from emp_t 
start with mgr is null 
connect by prior empno = mgr; 

select empno, ename, mgr, sal, 
(select sum(sal)
from emp_t 
start with mgr is null 
connect by prior empno = a.empno) group_sal
from emp_t a 
; 

# Write a SQl TO list "no of employees" & "name of employees" reporting to each person. 



