--users dimension table 
CREATE TABLE DWD_users (
  user_id VARCHAR(60) NOT NULL,
  email VARCHAR(100) NOT NULL,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL
);

-- courses dimension table 
CREATE TABLE DWD_courses (
  course_id VARCHAR(60) NOT NULL,
  title VARCHAR(100) NOT NULL,
  description VARCHAR(150) NOT NULL,
  published_date TIMESTAMP NOT NULL
);

--certificates fact table 
CREATE TABLE DWF_certificates (
  certificate_id SERIAL PRIMARY KEY,
  course_id VARCHAR(60) NOT NULL,
  user_id VARCHAR(60) NOT NULL,
  completed_date TIMESTAMP NOT NULL,
  start_date TIMESTAMP NOT NULL
);


---Business dependent queries ---

--average complete time
CREATE VIEW avg_complete_time AS
SELECT AVG(EXTRACT(EPOCH FROM(c.completed_date - c.start_date)))/(60*60*24) avg_complete_time
FROM dwf_certificates c;

--average spent time by course
CREATE VIEW avg_spent_time AS
SELECT co.title course_name,AVG(EXTRACT(EPOCH FROM(ce.completed_date - ce.start_date))/(60*60)) avg_spent_time
FROM dwf_certificates ce
INNER JOIN dwd_courses co
ON ce.course_id = co.course_id
GROUP BY co.title;

--report of fastest vs. slowest users completing a course
CREATE VIEW fastest_slowest_user AS
WITH cte AS (
    SELECT ce.user_id,ce.course_id, EXTRACT(EPOCH FROM(ce.completed_date - ce.start_date))/(60*60) as spent_time,
        RANK() OVER (ORDER BY EXTRACT(EPOCH FROM(ce.completed_date - ce.start_date))/(60*60)) fastest,
        RANK() OVER (ORDER BY EXTRACT(EPOCH FROM(ce.completed_date - ce.start_date))/(60*60) DESC) slowest
    FROM dwf_certificates ce
)
SELECT CONCAT(us.first_name,' ',us.last_name)as user_name, cte.spent_time
FROM cte
INNER JOIN dwd_users us
ON cte.user_id= us.user_id
WHERE cte.fastest = 1 OR cte.slowest = 1
ORDER BY spent_time;

--amount of certificates per customer
CREATE VIEW amount_cert_per_cust AS
WITH cte AS (
SELECT ce.user_id,COUNT(ce.certificate_id) as amount
FROM dwf_certificates ce
GROUP BY ce.user_id) 
SELECT CONCAT(us.first_name,' ',us.last_name)as user_name,cte.amount as amount_per_customer
FROM cte
INNER JOIN dwd_users us
ON cte.user_id= us.user_id;
