/*Table: interactions
+------------------+-----------+
| Column Name      | Type      |
+------------------+-----------+    
|user_a            |int        |  
|user_b            |int        |
|interaction_date  |date       | 
+------------------+-----------+*/
--Viết truy vấn để tìm số lượng người dùng đã tương tác từ 5 lần trở lên trong ngày qua

WITH t1 AS
(
SELECT user_a AS user_1
FROM  interactions
WHERE interaction_date=current_date-1

UNION ALL 
SELECT user_b
FROM  interactions
WHERE interaction_date=current_date-1
),  
t2 as
	(
	SELECT user_1, COUNT(*) AS count_than5
	FROM t1
	GROUP BY user_1 
	HAVING COUNT(*) > 5
	)
SELECT COUNT(*)  AS Over_Five_Interactions
FROM t2