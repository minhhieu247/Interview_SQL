
WITH a1 AS 
(
SELECT customer_id,order_datetime, bid_id ,
RANK() OVER(PARTITION BY customer_id, order_datetime ORDER BY order_datetime) AS rank
FROM dbo.bids
)

SELECT a1.customer_id,a1.order_datetime,a1.bid_id
FROM a1
WHERE a1.rank = 1