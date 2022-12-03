--1/tính tổng thời gian mỗi nhân viên dành cho mỗi ngày tại văn phòng
--Làm tròn thời gian tính bằng giờ đến số nguyên gần nhất.

employee table:
+-------------+---------------------+--------+
| emp_id      | time                | in_out |
+-------------+---------------------+--------+
| 1           | 2019-01-01 09:00:00 | in     |       
| 2           | 2019-01-01 12:00:00 | in     |       
| 1           | 2019-01-01 13:00:00 | out    |
| 2           | 2019-01-01 17:00:00 | out    |
| 2           | 2019-01-02 07:00:00 | in     |
| 2           | 2019-01-02 12:00:00 | out    |
| 2           | 2019-01-02 17:00:00 | in     |
| 2           | 2019-01-02 23:00:00 | out    |
| 3           | 2019-01-03 07:00:00 | in     |
| 3           | 2019-01-03 17:30:00 | out    |
+-------------+---------------------+--------+
--Solution:
--đầu tiên dùng windown funtion xử lý thêm cột thời gian in out cho từng nhân viên theo từng ngày phân vùng theo id
-- thêm cột vào bảng mới
with  time_hours as
(
select *, 
lag(time,1,null) over (partition by emp_id order by time) as prev_time
from employee
WHERE Employees.in_out = 'out'
)
-- sau đó trừ ra để lấy số giờ ở office

, t1 as
(SELECT emp_id,
          to_char("time"::date, 'MM/DD/YYYY') AS date_at_office,
          EXTRACT(day FROM "time" - PrevInOutTimeStamp) / 24 AS time_in_hours
from time_hours)
-- làm tròn
SELECT emp_id,
       date_at_office,
       round(SUM(time_in_hours),1) AS time_in_hours
FROM t1
GROUP BY emp_id,
         date_at_office ;
---2/ Tìm các công ty đã đăng ký
Table: accounts
+------------+---------+
|Column Name |Type     |
+------------+---------+
|id          |var      |      
|company_name|var      |  
|signup_date |timestamp|
|current_mrr |int      |
+------------+---------+
Table: sending
+--------------+---------+
|Column Name   |Type     |
+--------------+---------+
|msg_id        |var      |      
|id            |var      |  
|delivered_date|timestamp|
|msg_type      |var      |
|receive_count |int      |
|open_count    |int      |
|click_count   |int      |
+--------------+---------+
/* Viết một truy vấn để tìm những công ty đã đăng ký vào tháng 9 năm 2019,
xác định 3 công ty có số lượng email nhận được cao nhất vào tháng 2 năm 2020,
sau đó liệt kê 3 message_id luồng hàng đầu của họ từ tháng đó dựa trên tổng số lần mở.*/
		Table: accounts
+---+-------------+--------------------+------------+
|id |company_name |signup_date         |current_mrr |
+---+-------------+--------------------+------------+
|1  |facebook     |2019-09-11 10:00:20 |100         |
|2  |twitter      |2019-09-21 11:23:00 |120         |
|3  |tik tok      |2019-09-12 18:03:00 |154         |
|4  |amazon       |2019-09-24 17:35:00 |0           |
|5  |youtube      |2019-09-02 8:30:30  |135         |
|6  |google       |2019-08-02 8:30:30  |146         |
|7  |etsy         |2020-03-17 9:15:20  |122         |
+---+-------------+--------------------+------------+
Table: sending
+-----------+--------------------+---------+-------------+----------+-----------+
|msg_id |id |delivered_date      |msg_type |receive_count|open_count|click_count|
+-----------+--------------------+---------+-------------+----------+-----------+
|101    |1  |2020-02-21 19:02:00 |flow     |13           |6         |6          |
|102    |1  |2020-02-14 17:51:00 |sms      |12           |11        |7          |
|103    |1  |2020-02-11 10:01:20 |campaign |15           |8         |4          |
|104    |1  |2020-02-05 14:20:10 |flow     |14           |12        |10         |
|114    |1  |2020-02-18 14:20:10 |sms      |11           |6         |3          |
|105    |1  |2020-02-09 10:00:00 |flow     |22           |12        |11         |
|106    |1  |2020-02-17 20:03:00 |flow     |13           |12        |5          |
|126    |1  |2020-02-16 20:03:00 |campaign |17           |15        |9          |
|116    |1  |2020-03-15 20:03:00 |flow     |20           |13        |4          |
|201    |2  |2020-02-08 19:02:00 |flow     |13           |5         |2          |
|202    |2  |2020-02-13 17:51:00 |sms      |26           |20        |17         |
 -- tạo bảng xếp hạng công ty với các điều kiện ngày tháng, 
--giới hạn 3 và lấy các trường id cũng như tổng nhận
WITH rank_company AS
  (SELECT acc.id, sum(received_count) received_sum
   FROM sending s
   LEFT JOIN accounts acc ON s.id = acc.id
   WHERE date_part('year', acc.signup_date) = 2019
     AND date_part('month', acc.signup_date) = 9
     AND date_part('year', s.delivered_date) = 2020
     AND date_part('month', s.delivered_date) = 2
   GROUP BY acc.id
   ORDER BY received_sum DESC
   LIMIT 3),
--sau đó dùng windown funtions xử lý phân nhóm rồi xếp hạng 
--đồng thời id phải nằm trong bảng rank trên cùng type = flow

   t1 as
   (SELECT s.msg_id,
          acc.company_name,
          s.msg_type,
          s.opened_count,
          row_number() over(PARTITION BY acc.company_name
                            ORDER BY s.opened_count DESC) rn
   FROM sending s
   LEFT JOIN accounts acc ON s.id = acc.id
   WHERE s.message_type = 'flow'
     AND s.id IN (SELECT id
                          FROM rank_company) 
  )
  --lấy top 3
  SELECT company_name, msg_id, msg_type, opened_count
FROM t1
 WHERE rn IN (1,2,3)
ORDER BY opened_count DESC
--3/ Viết truy vấn để tìm số trang hiện có với latest_event
able: pages_info
+-------+--------------------------------------+----------+
|page_id|event_time                            |page_flag |
+-------+--------------------------------------+----------+
|1      |current_timestamp - interval '6 hours'|ON        |
|1      |current_timestamp - interval '3 hours'|OFF       |
|1      |current_timestamp - interval '1 hours'|ON        |
|2      |current_timestamp - interval '3 hours'|ON        |
|2      |current_timestamp - interval '1 hours'|OFF       |
|3      |current_timestamp                     |ON        |
+-------+--------------------------------------+----------+

-- xếp hạng theo page_id và cho time giảm dần để biết để số 1 là sự kiện gần nhất
with seq as (
select
page_flag
,row_number() over (
partition by page_id
order by event_time desc
) sequence
from pages_info
)

select count(*)  as result
from seq
where
page_flag='ON'
and sequence=1
--4/ Viết truy vấn để tìm (những) khách hàng đã thực hiện 
--2 đơn đặt hàng khác nhau trong khoảng thời gian hai ngày
Table: orders
+----+--------+-------------------+
|id  |user_id |order_date         |
+----+--------+-------------------+
|1   |11      |2022-01-18 13:10:21|
|2   |12      |2022-01-11 16:12:21|
|3   |11      |2022-01-19 9:54:21 |
|4   |12      |2022-01-21 21:23:21|
|5   |15      |2022-01-02 22:13:21|
|6   |15      |2022-01-03 11:21:21|
|7   |16      |2022-01-01 12:45:21|
|8   |12      |2022-01-22 8:34:21 |
+----+--------+-------------------+
--như cũ ta sử dụng windownfuntions để phân vùng theo user id và sắp xếp theo ngày
with t1 as
(
select user_id, order_date,
lag(order_date) over(partition by user_id order by order_date asc) as prev_day
from orders
)
-- lấy các id duy nhất mà ngày của hiệu 2 ngày <=2 -> theo yêu cầu đề bài
select distinct user_id
from t1
where date_part(day,order_date-prev_day) <=2

---5/ tìm ra user tương tác trên 5 lần trên ngày khi có 2 cột user a và b như sau:
Table: interactions
+--------+------+----------------+
|user_a  |user_b|interaction_date|
+--------+------+----------------+
|1       |2     |current_date - 1|
|5       |2     |current_date - 1|
|5       |1     |current_date - 1|
|3       |5     |current_date - 1|
|2       |3     |current_date - 1|
|4       |2     |current_date - 1|
|4       |5     |current_date - 1|
|3       |4     |current_date - 1|
 with t1 as
 (
 select user_a as user, 
 interaction_date
 from interactions

  union all 

 select user_b, 
 interaction_date
 from interactions
 )
 select count(*)
 from
 (select user, count(*) as count
 from t1
 group by user 
 having count(*) >=5)
 --6/Tìm người nhận email đủ điều kiện
 /*Viết truy vấn để tìm người dùng đang hoạt động trên gói Miễn phí để liên hệ với họ 1 ngày trước khi hết hạn dùng thử 
 và thông báo cho họ về số lượng máy chủ đang hoạt động mà họ đang sử dụng. 
 Trả lại email, số lượng máy chủ đang hoạt động và ngày họ sẽ được liên hệ. 
 (Gợi ý: Bản dùng thử miễn phí kéo dài 14 ngày.)*/
 Table: users
+-----------------------------+-------+
|Column Name                  |Type   |
+-----------------------------+-------+    
|user_id                      |int    |
|email                        |var    |
|signup                       |date   |
|active                       |boolean|
|plan_id                      |int    |
+-----------------------------+-------+
Table: servers
+--------------+-------+
|Column Name   |Type   |
+--------------+-------+
|user_id       |int    |
|server        |int    |
|active        |int    |
+--------------+-------+
Table: plans
+--------------------+---------+
|Column Name         |Type     |
+--------------------+---------+
|plan_id             |int      |
|plan_type           |int      |
|supported_servers   |int      |
+--------------------+---------+

SELECT
u.email
,COUNT(s.server) servers
    ,signup::DATE + interval '13 days' as noti_day
FROM users u
INNER JOIN "plans" p
ON p.plan_id = u.plan_id
AND p.plan_type = 'free'
LEFT JOIN servers s
ON s.user_id = u.user_id
AND s.active = true
WHERE u.active = true
GROUP BY 1,3;
--LEFT JOIN của các máy chủ giúp đưa vào thông tin máy chủ cho hàm COUNT() 
--với điều kiện là nó chỉ bao gồm các bản ghi nơi máy chủ đang hoạt động.

---7/ tìm hiệu quả khuyến mãi thông qua các bảng sau
Table: orders
+----------+--------+------------+-----------+----------+------------------+
|product_id|store_id|customer_id |promotion_id|units_sold|transaction_date |
+----------+--------+------------+-----------+----------+------------------+
|1         |10      |100         |1000       |1         |2019-01-01 9:00:00| 
|1         |10      |200         |null       |2         |2019-01-01 9:00:00| 
|1         |10      |300         |1001       |3         |2019-01-01 9:00:00| 
|1         |10      |400         |1002       |3         |2019-01-01 9:00:00| 
Table: promotions
+------------+
|promotion_id|
+------------+
|1001        |
|1002        |
|1003        | 
--Viết một truy vấn để tìm bao nhiêu phần trăm đơn đặt hàng đã áp dụng khuyến mãi? 
 --đơn đặt hàng là khuyến mãi hợp lệ nếu có id_khuyến mãi trong bảng khuyến mãi
 select (
count(p.promotion_id)::float
/
count(o.product_id)::float
)::decimal(10,2) pct_with_promo
from orders o
left join promotions p
on p.promotion_id=o.promotion_id


--8 Viết câu truy vấn để biết khách hàng nào đã mua sản phẩm của cả nhãn hiệu 1 và nhãn hiệu 2. 
--Sắp xếp kết quả theo họ và tên ASC.
Table: orders
+----------+--------+------------+-----------+----------+------------------+
|product_id|store_id|customer_id |promotion_id|units_sold|transaction_date |
+----------+--------+------------+-----------+----------+------------------+
|1         |10      |100         |1000       |1         |2019-01-01 9:00:00| 
|1         |10      |200         |null       |2         |2019-01-01 9:00:00| 
|1         |10      |300         |1001       |3         |2019-01-01 9:00:00| 
|1         |10      |400         |1002       |3         |2019-01-01 9:00:00|
Table: products
+----------+----------------+----------+------------+-----+
|product_id|product_class_id|brand_name|product_name|price|
+----------+----------------+----------+------------+-----+
|1         |1               |brand_1   |product_1   |4    |
|2         |1               |brand_2   |product_2   |2    |
|3         |2               |brand_2   |product_3   |9    |
Table: customers
+------------+----------+---------+
| customer_id|first_name|last_name|
+------------+----------+---------+
|100         |A         |J        |
|200         |B         |K        |
|300         |C         |L        |
|400         |D         |M        |
|500         |E         |N        |

select
o.customer_id,
concat(' ',c.first_name,c.last_name) as fullname
from orders o
inner join products p
on p.product_id=o.product_id
and p.brand_name in ('brand_1','brand_2')
left join customers c
on c.customer_id=o.customer_id
group by
o.customer_id,
fullname
having
count(case when p.brand_name='brand_1' then 1 else null end)>0
and count(case when p.brand_name='brand_2' then 1 else null end)>0
order by
fullname asc
--9/ Viết truy vấn để thiết lập nhóm hàng tháng dựa trên đơn đặt hàng đầu tiên được đặt.
Table: user_user  
+----------+         
|user_id   |
+----------+
|1         |
|2         |
|3         |
|4         |
Table: user_delivery
+----+---------------------+---------+
|id  |actual_delivery_time |eatery_id|
+----+---------------------+---------+
|11  |2021-12-01 13:00:21  |21       |
|12  |2021-11-01 13:00:21  |22       |
|13  |2021-10-21 13:00:21  |23       |
|14  |2021-09-01 13:00:21  |21       |
	Table: user_ordercart
+----+-----------+-----------+------------------+
|id  |user_id    |delivery_id|is_first_ordercart|
+----+-----------+-----------+------------------+
|34  |1          |24         |true              |
|35  |2          |24         |true              |
|36  |1          |23         |false             |
|37  |3          |23         |true              |
Table: eatery_eatery
+----+------------+---------+
|id  |eatery_name |market_id|
+----+------------+---------+
|21  |A           |5        |
|22  |B           |8        |
|23  |C           |6        |
|24  |D           |9        |
select
    to_char(ud.actual_delivery_time,’YYYY-MM’) monthly_cohorts
    ,count(uo.is_first_ordercart) number_users_first_ordercart
from user_delivery ud
inner join user_ordercart uo
    on uo.delivery_id=ud.id
    and uo.is_first_ordercart=’true’
group by 1
order by 1 asc;








