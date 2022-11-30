/*Table: products
+-----------------+---------+
| Column Name     | Type    |
+-----------------+---------+
|product_id       |int      |      
|product_class_id |int      |  
|brand_name       |var      |  
|product_name     |var      |  
|price            |dec      |  
+-----------------+---------+*/
--	Viết một truy vấn trả về tên thương hiệu có ít nhất hai sản phẩm khác nhau 
--và giá trung bình lớn hơn $3
--trả về thứ tự kết quả theo tên thương hiệu
 SELECT brand_name 
 FROM  products
 GROUP BY brand_name 
 HAVING COUNT(DISTINCT product_id) >2 AND AVG(price) >3
 ORDER BY 1