-- DATA PREPARATION

-- REMOVE DUPLICATE ROWS FROM RAW DATA TABLE
SELECT
	*
FROM gcash.transactions_staging;

WITH duplicate_cte AS ( -- to see if there's any duplicate row
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY user_id, transaction_id, transaction_date, transaction_amount, transaction_type, merchant_id
           ORDER BY transaction_id
         ) AS row_num
  FROM gcash.transactions_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Data standardization
UPDATE gcash.transactions_staging 
-- to convert all transaction_type to lowercase and remove extra spaces. Also, for data standardization
SET
  transaction_type = LOWER(TRIM(transaction_type));
  
SELECT -- to check for any null values for each columns in raw data table
  SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
  SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS null_transaction_id,
  SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) AS null_transaction_date,
  SUM(CASE WHEN transaction_amount IS NULL THEN 1 ELSE 0 END) AS null_transaction_amount,
  SUM(CASE WHEN transaction_type IS NULL OR transaction_type = '' THEN 1 ELSE 0 END) AS null_transaction_type,
  SUM(CASE WHEN merchant_id IS NULL THEN 1 ELSE 0 END) AS null_merchant_id
FROM gcash.transactions_staging;

-- convert the data type of registration_date under users table from text to date type
SELECT -- to ensure that all dates in the column have the same format
	u.registration_date,
    STR_TO_DATE(u.registration_date, '%m/%d/%Y')
FROM
	gcash.users u;
    
UPDATE -- actual change the format from its raw form
	gcash.users
SET 
	users.registration_date = STR_TO_DATE(registration_date, '%m/%d/%Y')
WHERE
	users.registration_date IS NOT NULL
    AND users.registration_date <> '';
    
ALTER TABLE gcash.users
MODIFY COLUMN registration_date DATE; -- to change data type from text to date

-- now let's join raw data table to users table for our data analysis and data visualization
SELECT -- to combine the raw data table to users table which will be used for data visualization
	t.user_id,
    t.transaction_id,
    t.transaction_date,
    t.transaction_amount,
    t.transaction_type,
    t.merchant_id,
    u.registration_date,
    u.age,
    CASE -- to change from integers to the actual location name using the reference table
		WHEN u.location = 1 THEN 'VisMin'
		WHEN u.location = 2 THEN 'SL'
		WHEN u.location = 3 THEN 'NL'
		WHEN u.location = 4 THEN 'GMA+'
		WHEN u.location = 5 THEN 'GMA'
		ELSE 'Unknown'
    END AS location
FROM gcash.transactions_staging t
	JOIN gcash.users1 u
    ON t.user_id = u.user_id;
    
DESCRIBE gcash.users;
DESCRIBE gcash.transactions_staging;








SELECT -- to combine the raw_data table to users table which will be used for data visualization
	t.user_id,
    t.transaction_id,
    t.transaction_date,
    t.transaction_amount,
    t.transaction_type,
    t.merchant_id,
    u.registration_date,
    u.age,
    u.location
FROM gcash.transactions_staging t
	JOIN gcash.users u
    ON t.user_id = u.user_id;
    

    

-- DATA ANALYSIS PART (EXPLORATORY DATA ANALYSIS)

SELECT -- number of transactions per user_id
	t.user_id,
    COUNT(t.user_id) AS total_transaction
FROM 
	gcash.transactions_staging t
GROUP BY
	1;
    
SELECT -- total transactions per region
	u.location,
    COUNT(t.user_id) AS total_transactions_per_location
FROM gcash.transactions_staging t
	JOIN gcash.users u
    ON t.user_id = u.user_id
GROUP BY
	1;

SELECT -- top transaction type
	t.transaction_type,
    COUNT(t.transaction_type) AS most_used_transactions
FROM
	gcash.transactions_staging t
GROUP BY
	1
ORDER BY
	2 DESC;	
    
SELECT -- leading merchants
	t.merchant_id,
    COUNT(t.merchant_id) AS most_used_merchants
FROM
	gcash.transactions_staging t
GROUP BY
	1
ORDER BY
	2 DESC;

SELECT -- monthly transaction volume
	DATE_FORMAT(t.transaction_date, '%Y-%m') AS month,
    AVG(t.transaction_amount) AS avg_transaction_amount,
    SUM(t.transaction_amount) AS total_transaction_amount
FROM 
	gcash.transactions_staging t
GROUP BY
	1
ORDER BY
	1;

SELECT -- 
  t.user_id,
  COUNT(*) AS transaction_count,
  SUM(t.transaction_amount) AS total_transaction_value,
  MIN(t.transaction_date) AS first_transaction_date,
  MAX(t.transaction_date) AS last_transaction_date
FROM 
	gcash.transactions_staging t
GROUP BY 
	t.user_id;

SELECT -- cohort analysis for user retention
  DATE_FORMAT(u.registration_date, '%Y-%m') AS cohort_month,
  TIMESTAMPDIFF(MONTH, u.registration_date, t.transaction_date) AS months_since_signup,
  COUNT(DISTINCT t.user_id) AS active_users
FROM 
	gcash.transactions_staging t
	JOIN gcash.users u 
    ON t.user_id = u.user_id
WHERE 
	t.transaction_date >= u.registration_date
GROUP BY 
	cohort_month, months_since_signup
ORDER BY 
	cohort_month, months_since_signup;

SELECT
	AVG(t.transaction_amount) AS avg_transaction_amount,
    MIN(t.transaction_amount) AS min_transaction_amount,
    MAX(t.transaction_amount) AS max_transaction_amount
FROM
	gcash.transactions_staging t;

SELECT
	AVG(user_transaction_count) AS avg_transaction_count,
    MAX(user_transaction_count) AS max_transaction_count,
    MIN(user_transaction_count) AS min_transaction_count
FROM
	(SELECT
		t.user_id,
        COUNT(user_id) AS user_transaction_count
	FROM
		gcash.transactions_staging t
	GROUP BY
		1
    ) as txn_per_user;
    
-- Now that we already computed for the average, maximum, 
-- and minimum amount of both transaction count and transaction 
-- amount, let's move on to user segmentation through engagement score
WITH user_stats AS ( 
  SELECT
    t.user_id,
    COUNT(*) AS txn_count,
    SUM(t.transaction_amount) AS total_txn_value,
    AVG(t.transaction_amount) AS avg_txn_value,
    MAX(t.transaction_date) AS last_txn_date,

    -- Frequency score
    CASE 
      WHEN COUNT(*) >= 5 THEN 3
      WHEN COUNT(*) >= 2 THEN 2
      ELSE 1
    END AS freq_score,

    -- Value score
    CASE
      WHEN AVG(t.transaction_amount) >= 700 THEN 3
      WHEN AVG(t.transaction_amount) >= 400 THEN 2
      ELSE 1
    END AS value_score,

    -- Recency score
    CASE
      WHEN MAX(t.transaction_date) >= DATE_SUB('2024-05-21', INTERVAL 30 DAY) THEN 1
      -- this is to see if there's a transaction made within the last 30 days of the available data
      ELSE 0
    END AS recency_score

  FROM gcash.transactions_staging t
  GROUP BY t.user_id
)

SELECT
  user_id,
  txn_count,
  total_txn_value,
  avg_txn_value,
  last_txn_date,
  freq_score,
  value_score,
  recency_score,
  (freq_score + value_score + recency_score) AS total_score,

  -- Segment based on score
  CASE
    WHEN (freq_score + value_score + recency_score) = 7 THEN 'Highly Engaged'
    WHEN (freq_score + value_score + recency_score) BETWEEN 4 AND 6 THEN 'At Risk'
    ELSE 'Low Engagement'
  END AS engagement_segment

FROM 
	user_stats
ORDER BY
	total_score;

SELECT 
  MAX(t.transaction_date) AS latest_txn,
  CURDATE() AS today,
  DATE_SUB(CURDATE(), INTERVAL 30 DAY) AS cutoff_date
FROM gcash.transactions_staging t;

SELECT
	COUNT(DISTINCT u.user_id) AS users_count
FROM
	gcash.users u
WHERE
	u.user_id IS NOT NULL;
    
SELECT
	t.user_id,
    COUNT(t.user_id) AS txn_count
FROM
	gcash.transactions_staging t
GROUP BY
	1
HAVING
	COUNT(t.user_id) = 0;