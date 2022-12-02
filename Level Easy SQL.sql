
--- 1/ Top 3 topics nhiều video nhất

--Table: users 
+------------+---------+
|Column Name |Type     |
+------------+---------+    
|id          |int      |  
|post_id     |int      |
|date_posted |timestamp|
+------------+---------+
--Table: topics 
+------------+---------+
|Column Name |Type     |
+------------+---------+    
|id          |int      |  
|topic       |var      |
|post_id     |int      |
+------------+---------+
Viết truy vấn tìm top 3 chủ đề trên tiktok nhận được nhận nhiều video nhất.

SELECT
t.topic, 
COUNT(post_id)
FROM topics AS t
LEFT JOIN users AS u
ON t.post.id=u.post.id
WHERE 
DATEPART(YEAR,date_posted) = 2022 AND DATEPART(MONTH, date_posted)= 6 -- review funtions time

GROUP BY t.topic
ORDER BY COUNT(post_id) DESC
limit 3; 
 

 -- 2/ Viết truy vấn tìm thời gian mới nhất người dùng đã đi ít nhất 1 chuyến
Table: trips
+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
|trip_id       |int      |
|rider_id      |int      |
|driver_id     |int      |
|trip_timestamp|timestamp|
+--------------+---------+

 
  SELECT
 rider_id, 
 MAX(trip_timestamp) AS latest_trip_timestamp 
 FROM trips
 GROUP BY reder_id
 HAVING COUNT(trip_id) >=1
 ORDER BY rider_id ASC;
  

  -- 3/  Truy xuất ngọn núi cao thứ 3 ở từng địa điểm
   Table: mountains 
+------------------+-----------+
| Column Name      | Type      |
+------------------+-----------+    
|name              |var        |  
|height            |dec        |  
|country           |var        |
+------------------+-----------+
 WITH third_mountains AS 
 (
 SELECT name, country,
 RANK() OVER( PARTITION BY country ORDER BY height )  AS rank
 FROM mountains 
 )
 SELECT *
 FROM third_mountains
 WHERE height = 3
 ORDER BY country ASC --- thay thế sub query

 --4/	top 5 phim được đánh giá cao, trả về 
 
 Table: users                 
+------------+---------+
|Column Name |Type     |
+------------+---------+    
|id          |int      |
|user_name   |var      |
|occupation  |var      |
+------------+---------+
Table: movies
+-----------+---------+
|Column Name|Type     |
+-----------+---------+    
|id         |int      |
|name       |var      |
+-----------+---------+
Table: reviews
+-----------+---------+
|Column Name|Type     |
+-----------+---------+    
|critic     |int      |
|movie      |int      |
|rating     |int      |
|date       |date     |
+-----------+---------+
 
 with t1 as
 (
 SELECT m.name , AVG(r.rating) AS "rating" , COUNT(r.critic) as "num_reviews"
  FROM reviews r
  INNER JOIN movies m ON m.id = r.movie
  INNER JOIN users u ON u.id = r.critic
  WHERE LOWER(u.occupation) LIKE 'engineer'
  GROUP BY m.id, m.name
  )

  select name, rating
  from t1
  where number_reveiws >= 20
  order by rating desc
  limit 5

  --5/ tìm người nhận ít nhất với một khoản tiền gửi,

  Table: user
+-----------------------------+-------+
| Column Name                 | Type  |
+-----------------------------+-------+    
|id                           |int    |
|name                         |var    |
|email                        |var    |
|deposit_count                |dec    |
|dfs_date                     |date   |
|sportsbook_date              |date   |
|casino_date                  |date   |
+-----------------------------+-------+
Table: entry
+--------------+-------+
| Column Name  | Type  |
+--------------+-------+
|entry_id      |int    |
|game_id       |int    |
|user_id       |int    |
|entry_date    |date   |
|entry_fee     |dec    |
|winnings      |dec    |
|mobile_entry  |var    |
+--------------+-------+
Table: games
+--------------------+---------+
| Column Name        | Type    |
+--------------------+---------+
|game_id             |int      |      
|sport               |var      |
|size                |int      |
+--------------------+---------+
SELECT COUNT(u.id) AS number_users
     FROM "user" u
     JOIN entry e ON e.user_id = u.id
     JOIN games g ON g.game_id = e.game_id
     AND sport = 'NFL' WHERE deposit_count >= 1
     AND dfs_date IS NOT NULL
     AND DATE_PART('year', dfs_date::DATE) = 2019
     AND u.id IN
       (SELECT DISTINCT entry.user_id
        FROM entry
        WHERE DATE_PART('year', entry_date::DATE) = 2019 )
--tìm số lượng người dùng đã thực hiện ít nhất một khoản tiền gửi 
--và đăng ký trên ít nhất một sản phẩm trong năm trước (2019)
SELECT COUNT(*) AS Number_users
  FROM "user" WHERE deposit_count >= 1
  AND ((dfs_date IS NOT NULL
        AND DATE_PART('YEAR', dfs_date::DATE) = 2019)
       OR (sportsbook_date IS NOT NULL
           AND DATE_PART('YEAR', sportsbook_date::DATE) = 2019)
       OR (casino_date IS NOT NULL
           AND DATE_PART('YEAR', casino_date::DATE) = 2019))
--6/ tìm người dùng đủ điều kiện cho chiến dịch
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
--Viết truy vấn để nhắm mục tiêu người dùng đang hoạt động trên gói miễn phí cho chiến dịch email.
--Bao gồm email của khách hàng 
--và mức kế hoạch được đề xuất dựa trên số lượng máy chủ đang hoạt động mà họ đang sử dụng.
\\\\\
 
 -- step 1: 
 --xác định có bao nhiêu máy chủ mà mỗi người dùng miễn phí đang tích cực sử dụng:
 
 with user_usage as
 (
 select u.email, count(s.server) as servers
 from users as u 
 join plan as p on u.plan_id=p.plan_id
 where p.plan_type  ='free'
 left join servers as s on u.user_id=s.user_id and u.active = 'true'
 where active like 'true'
 group by u.email
 ), 
 --explain: where đảm bảo là người dùng đang hoạt động
  --step 2: xác định quy mô của gói 
  --mà mỗi người dùng sẽ cần dựa trên mức sử dụng hoạt động của họ
  plan_size as
  (
  select u.email,
  u.servers, min(p.supported_servers) as size_needed
  from user_usage as u
  left join  plans as p
  on p.supported_servers >= u.servers
  group by  1,2
  )
SELECT
    a.email
    ,p.plan_type plan_suggestion
FROM plan_size a
LEFT JOIN "plans" p
    ON p.supported_servers = a.size_needed
WHERE p.plan_type <> 'free';
--7/ Đấu trường sẽ thắng nếu thắng 6 trận. 
--Đấu trường bị hủy nếu thua 2 trận. 
--Tìm số trận thắng trung bình trên mỗi lần tham gia đấu trường 
--Làm tròn đến 2 chữ số thập phân
Table: arena
+------------+--------+
|Column Name |Type    |
+------------+--------+
|attempt     |int     |      
|match       |int     |  
|won         |boolean |  
+------------+--------+

WITH wins AS (
  SELECT
      attempt
      ,count(won) wins
  FROM arena
  WHERE won = 'true'
  GROUP BY attempt
)

SELECT ROUND(AVG(wins),2) avg_wins
FROM wins;
