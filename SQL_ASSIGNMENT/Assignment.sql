SELECT t1.name,
    t1.age,
    t1.city,
    t1.zipcode,
    t1.phone,
    t1.state_code,
    t1.order_id,
    t2.County,
    t2.order_date,
    t2.phone AS phone2,
    t2.order_id AS order_id2 INTO table3 FROM  table1 t1
FULL OUTER JOIN table2 t2
ON  t1.state_code = t2.state_code AND t1.name = t2.name;

select * from table3;
select top 5 * from table3;

select * into table5 from table3 where  state_code IN ('TX', 'CA', 'AZ', 'NY', 'FL');

select * from table5;

Select TOP 1 
state_code, count(state_code) as value_count 
from table3 group by state_code order by value_count desc ;


SELECT TOP 1
state_code, 
YEAR(order_date) AS year,
COUNT(order_id) AS order_count
FROM table3 
GROUP BY state_code, YEAR(order_date)
ORDER BY order_count DESC;

SELECT TOP 1 
    YEAR(order_date) AS year, 
    state_code, 
    COUNT(order_id) AS order_count
FROM TABLE3
GROUP BY YEAR(order_date), state_code
ORDER BY order_count DESC;

SELECT top 5 state_code, 
    city,
    YEAR(order_date) AS order_year,
    COUNT(order_id) AS order_count
FROM  table3
GROUP BY state_code, city, YEAR(order_date)
ORDER BY order_count DESC;

SELECT TOP 5
    YEAR(order_date) AS year, 
    state_code, 
    city, 
    COUNT(order_id) AS order_count
FROM TABLE3
GROUP BY YEAR(order_date), state_code, city
ORDER BY order_count DESC;


SELECT TOP 1 name, age, COUNT(order_id) AS order_count
FROM TABLE3
GROUP BY name, age
HAVING COUNT(order_id) > 10
ORDER BY age DESC;

SELECT TOP 1 name, age, COUNT(order_id) AS order_count
FROM TABLE3
GROUP BY name, age
HAVING COUNT(order_id) > 3
ORDER BY age DESC;

SELECT order_id,
    LEN(CONVERT(VARCHAR(36), order_id)) AS total_length,
    SUBSTRING(CONVERT(VARCHAR(36), order_id), 1, 8) AS part1, LEN(SUBSTRING(CONVERT(VARCHAR(36), order_id), 1, 8)) AS part1_length,
    SUBSTRING(CONVERT(VARCHAR(36), order_id), 10, 4) AS part2, LEN(SUBSTRING(CONVERT(VARCHAR(36), order_id), 10, 4)) AS part2_length,
    SUBSTRING(CONVERT(VARCHAR(36), order_id), 15, 4) AS part3, LEN(SUBSTRING(CONVERT(VARCHAR(36), order_id), 15, 4)) AS part3_length,
    SUBSTRING(CONVERT(VARCHAR(36), order_id), 20, 4) AS part4, LEN(SUBSTRING(CONVERT(VARCHAR(36), order_id), 20, 4)) AS part4_length,
    SUBSTRING(CONVERT(VARCHAR(36), order_id), 25, 12) AS part5, LEN(SUBSTRING(CONVERT(VARCHAR(36), order_id), 25, 12)) AS part5_length
FROM table3;


SELECT * FROM information_schema.tables;
