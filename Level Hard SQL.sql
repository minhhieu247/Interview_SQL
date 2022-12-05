--1/truy vấn tính phần trăm xác nhận
table:-- confirmation_attempt
+--------+---------------------------+-----------------+
|conf_id |conf_date                  |conf_phone_number|
+--------+---------------------------+-----------------+
|1       |current_timestamp:: DATE -2|080332157        |
|2       |current_timestamp:: DATE -3|080362158        |
|3       |current_timestamp:: DATE -7|080389242        |
|4       |current_timestamp:: DATE -1|080256055        |
|5       |current_timestamp:: DATE -1|080389234        |
+--------+---------------------------+-----------------+

table: --confirmation
+--------+---------------------------+-----------------+
|conf_id |conf_date                  |conf_phone_number|
+--------+---------------------------+-----------------+
|1       |current_timestamp:: DATE -2|080332157        |
|2       |current_timestamp:: DATE -3|080362158        |
|4       |current_timestamp:: DATE -1|080256055        |
+--------+---------------------------+-----------------+

select 
round(100 * count(c.conf_phone_number) / count(ca.conf_phone_number) , 2) as percent_
from confirmation_attempt as ca
left join confirmation as c
on ca.conf_phone_number=c.conf_phone_number



--2/ Tìm số lần thử trung bình trước khi mỗi đấu trường giành chiến thắng. 
-- thắng 6-> win, thua 2-> lose
Table: arena
+--------+------+-----+
|attempt |match |won  |
+--------+------+-----+
|1       |1     |false|
|1       |2     |true |
|1       |3     |true |
|1       |4     |false|
|2       |1     |true |
|2       |2     |true |
|2       |3     |false|

--chúng ta cần làm là xác định xem mỗi lần thử dẫn đến thắng hay thua:
WITH win_loss AS (
  SELECT
      attempt
      ,CASE
          WHEN COUNT(NULLIF(won,true))=2 THEN 'loss'
          WHEN COUNT(NULLIF(won,false))=6 THEN 'won'
          ELSE 'unknown'
      END  as attempt_result
  FROM arena
  GROUP BY attempt
)
--null if trả về nếu 2 giá trị bằng nhau còn không trả về gt đầu

SELECT
    ROUND(
      1.0*COUNT(NULLIF(attempt_result,'won'))
      /
      COUNT(NULLIF(attempt_result,'loss'))
    ,2) avg_attempts_to_win
FROM win_loss;
--3/ Viết truy vấn để tìm tỷ lệ đăng bài cho ngày hôm
--qua đối với tất cả người dùng đang hoạt động theo quốc gia.
Table: actions
+-------+-------+-------------------------+
|user_id|action |date                     |
+-------+-------+-------------------------+
|1      |post   |current_timestamp::DATE-3|
|2      |edit   |current_timestamp::DATE-2|
|3      |post   |current_timestamp::DATE-1|
|4      |post   |current_timestamp::DATE-1|
table: active_users
+-------+---------+-------+-------------------------+
|user_id|country  |active |date                     |
+-------+---------+-------+-------------------------+
|8      |USA      |true   |current_timestamp::DATE-1|
|9      |Spain    |true   |current_timestamp::DATE-1|
|3      |Colombia |true   |current_timestamp::DATE-1|

select
u.country
,(
sum(
case when action='post' then 1 else 0 end
)::float
/
count(1)::float --1 is (u.country)
) as  yesterday_rate
from actions as  a
join active_users  as u
on u.user_id=a.user_id and u.active=true
where a.date= current_timestamp::date-1
group by 1--(u.country)
-- trên tử chỉ tính các hoạt động đăng bài action = post

--3/ viết một truy vấn để hiển thị tỷ lệ đăng bài hiện tại cho ngày hôm qua
able: events
+-------+--------------------------+--------+-----+-----------+
|user_id|date                      |post_id |type |application|
+-------+--------------------------+--------+-----+-----------+
|1      |current_timestamp::DATE-5 |101     |post |twitter    |
|2      |current_timestamp::DATE-1 |102     |edit |facebook   |
|3      |current_timestamp::DATE-4 |105     |post |twitter    |
|4      |current_timestamp::DATE-1 |109     |like |facebook   |
|5      |current_timestamp::DATE-1 |102     |post |facebook   |
select
application
,(
sum(case when type='post' then 1 else 0 end)::float
/
count(1)::float
) as rd
from events
where date = current_timestamp::date-1
group by application
order by application asc
--4/ viết một truy vấn để hiển thị tỷ lệ đăng bài hiện tại cho mọi thời điểm.
Table: actions
+-------+-------+-------------------------+
|user_id|action |date                     |
+-------+-------+-------------------------+
|1      |post   |current_timestamp::DATE-3|
|2      |edit   |current_timestamp::DATE-2|
|3      |post   |current_timestamp::DATE-1|

select round(1.0*
sum(case when action='post' then 1 else 0 end)
/
count(*)
,2) post_rate
from actions;
--5/Viết truy vấn để trả về 3 chuỗi truy cập trang liên tục dài nhất 
--được xếp hạng hàng đầu
Table: visits
+--------+---------------+------------+
|user_id |date                        | 
|1|      |current_timestamp::DATE - 0 |
|1|      |current_timestamp::DATE - 1 |
|1|      |current_timestamp::DATE - 2 |
|1|      |current_timestamp::DATE - 3 |
|1|      |current_timestamp::DATE - 4 |
|2|      |current_timestamp::DATE - 1 |
|2|      |current_timestamp::DATE - 3 |
|2|      |current_timestamp::DATE - 4 |
|2|      |current_timestamp::DATE - 5 |
|2|      |current_timestamp::DATE - 6 |
with groups as (
select
user_id
,date
,date::date - (
row_number() over (
partition by user_id
order by date asc
)
)::int+1 as grp
from visits
)
/* sử dụng ROW_NUMBER() để tạo chuỗi theo ngày.
Sau đó, chúng tôi trừ số thứ tự từ ngày để tạo một nhóm.
tính toán grp sẽ không phải luôn là ngày đầu tiên chuỗi bắt đầu, 
nhưng nó sẽ giống nhau cho tất cả các ngày xảy ra một ngày sau giá trị ngày hiện tại bởi vì mỗi khi số thứ tự tăng lên, 
nếu ngày cũng tăng lên như vậy số tiền, 
phép trừ sẽ dẫn đến cùng một ngày bắt đầu. Một lần nữa,
đó sẽ không phải là ngày chuỗi bắt đầu, 
nhưng nó sẽ giống nhau đối với từng nhóm ngày cách nhau một ngày theo trình tự.*/
--Nếu ngày tiếp theo tăng hơn 1, phép trừ ROWNUMBER() sẽ dẫn đến một ngày grp khác với ngày trước đó vì nó tăng khác với ROWNUMBER().
,ranked as (
select
user_id
,grp
,count(*) streak_size
,rank() over (
order by count(*) desc
) streak_rank
from groups
group by
user_id
,grp
)

select
user_id
,streak_size
,streak_rank
from ranked
where streak_rank <= 3
order by streak_rank asc;
--6/ Viết một truy vấn để tìm tất cả các khách hàng trả tiền đã gửi 4 hoặc 5 tin nhắn trong tuần của ngày 10 tháng 2 năm 2020; 
--cũng bao gồm số lượng tin nhắn đã gửi là chiến dịch, luồng và/hoặc SMS.
Table: accounts
+---+-------------+--------------------+------------+
|id |company_name |signup_date         |current_mrr |
+---+-------------+--------------------+------------+
|1  |facebook     |2019-09-11 10:00:20 |100         |
|2  |twitter      |2019-09-21 11:23:00 |120         |
|3  |tik tok      |2019-09-12 18:03:00 |154         |
|4  |amazon       |2019-09-24 17:35:00 |0           |
|5  |youtube      |2019-09-02 8:30:30  |135         |
Table: sending
+-----------+--------------------+---------+-------------+----------+-----------+
|msg_id |id |delivered_date      |msg_type |receive_count|open_count|click_count|
+-----------+--------------------+---------+-------------+----------+-----------+
|101    |1  |2020-02-21 19:02:00 |flow     |13           |6         |6          |
|102    |1  |2020-02-14 17:51:00 |sms      |12           |11        |7          |
|103    |1  |2020-02-11 10:01:20 |campaign |15           |8         |4          |
|104    |1  |2020-02-05 14:20:10 |flow     |14           |12        |10         |

SELECT company_name,
       num_mess,
       flow,
       campaign,
       sms
FROM
  (SELECT acc.company_name,
          count(s.msg_id) AS num_mess,
          sum(CASE
                  WHEN s.msg_type = 'flow' THEN 1
                  ELSE 0
              END) AS flow,
          sum(CASE
                  WHEN s.msg_type = 'campaign' THEN 1
                  ELSE 0
              END) AS campaign,
          sum(CASE
                  WHEN s.msg_type = 'sms' THEN 1
                  ELSE 0
              END) AS sms
   FROM sending s
   LEFT JOIN accounts acc ON s.id = acc.id
   WHERE s.delivered_date::DATE BETWEEN '2020-02-10' AND '2020-02-16' 
   AND current_mrr <> 0
   GROUP BY acc.company_name) t
WHERE num_mess IN (4,5);
Output

+-------------+---------+-----+---------+----+
|company_name |num_mess |flow |campaign |sms |
+-------------+---------+-----+---------+----+
|twitter      |4        |2    |1        |1   |
+-------------+---------+-----+---------+----+
--7/ Viết truy vấn để tìm giá thầu sớm thứ hai cho mỗi khách hàng vào ngày họ đặt 2 giá thầu trở lên trong cùng một ngày. 
--Vui lòng trả lại id khách hàng, ngày đặt hàng và id giá thầu thứ hai.
with ranked as (
select
customer_id
,order_datetime::date
,bid_id
,rank() over (
partition by customer_id, order_datetime::date
order by order_datetime asc
) order_seq
from bids
)

select
customer_id
,order_datetime
,bid_id second_bid
from ranked
where order_seq=2
--8/  Viết truy vấn để tìm ra khách hàng nào đã mua sản phẩm từ cả nhãn hiệu 1 và nhãn hiệu 2.
Table: orders
+------------------+-----------+
| Column Name      | Type      |
+------------------+-----------+    
|product_id        |int        |  
|store_id          |int        |
|customer_id       |int        |
|promotion_id      |int        |
|units_sold        |dec        |
+------------------+-----------+
Table: products
+-----------------+---------+
| Column Name     | Type    |
+-----------------+---------+
|product_id       |int      |      
|product_class_id |int      |  
|brand_name       |var      |  
|product_name     |var      |  
|price            |dec      |  
+-----------------+---------+
Table: customers
+-----------------+---------+
| Column Name     | Type    |
+-----------------+---------+
|customer_id      |int      |
|first_name       |var      |
|last_name        |var      |  
+-----------------+---------+

select
o.customer_id
,c.first_name
,c.last_name
from orders o
inner join products p
on p.product_id=o.product_id
and p.brand_name in ('brand_1','brand_2')
left join customers c
on c.customer_id=o.customer_id
group by
o.customer_id
,c.first_name
,c.last_name
having
count(case when p.brand_name='brand_1' then 1 else null end)>0
and count(case when p.brand_name='brand_2' then 1 else null end)>0
order by
c.first_name
,c.last_name;
/*Bắt đầu bằng cách chọn customer_id từ đơn đặt hàng, vì đó là thứ nguyên chính.

INNER JOIN sản phẩm trên trường được chia sẻ productid và brandname trong ('brand1','brand2') để buộc cả hai bảng chỉ sử dụng kết quả cho các dải bắt buộc này. Điều này có nghĩa là  không phải sử dụng mệnh đề WHERE!

Left join bảng khách hàng để nhận các thứ nguyên khác mà chúng tôi cần trong tập kết quả của mình.

NHÓM THEO tất cả các kích thước.

Chúng tôi sử dụng mệnh đề HAVING để buộc các điều kiện trên các giá trị tổng hợp, tương tự như mệnh đề WHERE. Trong trường hợp này, chúng tôi yêu cầu các tên thương hiệu phải khớp với các thương hiệu được yêu cầu của chúng tôi và chỉ tính một giá trị nếu điều đó đúng trong câu lệnh CASE, vì giá trị NULL không được tổng hợp bởi hàm COUNT().

Cuối cùng, ĐẶT HÀNG THEO các trường được yêu cầu*/