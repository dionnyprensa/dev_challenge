-- How many users were active on a given day (they made a deposit or withdrawal)
SELECT
	COUNT(users.user_sk)
FROM public."DimUsers" AS users
INNER JOIN public."FactWithdrawals" AS withdrawls
	ON users.user_sk = withdrawls.user_sk
INNER JOIN public."FactDeposits" AS deposits
	ON users.user_sk = deposits.user_sk
WHERE
	withdrawls.amount > 0
AND deposits.amount > 0
AND withdrawls.tx_status = 'complete'
AND deposits.tx_status = 'complete'
AND (
	DATE(deposits.event_timestamp) = TO_DATE('2022-05-23', 'YYYY-MM-DD')
	OR DATE(withdrawls.event_timestamp) = TO_DATE('2022-05-23', 'YYYY-MM-DD')
)
;


-- Identify users haven't made a deposit
WITH UsersWhoNeverCompleteDepositsCTE AS (
SELECT DISTINCT user_sk
FROM public."FactDeposits" AS deposits
WHERE
	tx_status = 'complete'
GROUP BY user_sk, tx_status
HAVING COUNT(user_sk) = 0

), depositsCTE AS (
SELECT DISTINCT deposits.user_sk
FROM public."FactDeposits" AS deposits
LEFT JOIN UsersWhoNeverCompleteDepositsCTE AS WHNCD
	ON deposits.user_sk = WHNCD.user_sk
WHERE WHNCD.user_sk IS NULL
)

SELECT
	users.user_sk
FROM public."DimUsers" AS users
LEFT JOIN depositsCTE
	ON users.user_sk = depositsCTE.user_sk
WHERE
	depositsCTE.user_sk IS NULL
;


-- Identify on a given day which users have made more than 5 deposits historically
SELECT
	users.user_sk
	, COUNT(deposits.deposit_sk) AS Historical_Deposits
FROM public."DimUsers" AS users
INNER JOIN public."FactDeposits" AS deposits
	ON users.user_sk = deposits.user_sk
WHERE
	DATE(deposits.event_timestamp) <= TO_DATE('2022-05-23', 'YYYY-MM-DD')
AND deposits.amount > 0
AND deposits.tx_status = 'complete'
GROUP BY users.user_sk
HAVING COUNT(deposits.deposit_sk) >= 5
ORDER BY Historical_Deposits
;


-- When was the last time a user a user made a login
SELECT
	users.user_sk
	, users.user_id
	, MAX(DATE(events.event_timestamp)) AS Last_Login
FROM public."DimUsers" AS users
INNER JOIN public."FactEvents" AS events
	ON users.user_sk = events.user_sk
INNER JOIN public."DimEvents" dimEvents
	ON events.event_dim_sk = dimEvents.event_sk
WHERE
	dimEvents.event_name IN ('login', 'login_api')
AND users.user_id = '0007cda84fafdcf42f96c4f4adb7f8ce'
GROUP BY users.user_sk
;


-- How many times a user has made a login between two dates
SELECT
	COUNT(users.user_sk) AS Last_Login
FROM public."DimUsers" AS users
INNER JOIN public."FactEvents" AS events
	ON users.user_sk = events.user_sk
INNER JOIN public."DimEvents" dimEvents
	ON events.event_dim_sk = dimEvents.event_sk
WHERE
	dimEvents.event_name IN ('login', 'login_api')
AND users.user_id = '085dfc68338d1ed37766086a1aee1934' --'00b546d495d29ea025af220831ceee42', '7eb5ac36014a76629c40069e46136a61'
AND DATE(events.event_timestamp) BETWEEN TO_DATE('2022-04-23', 'YYYY-MM-DD') AND TO_DATE('2022-05-23', 'YYYY-MM-DD')
GROUP BY users.user_sk
;


-- Number of unique currencies deposited on a given day
SELECT
	COUNT(DISTINCT currencies.currency_sk) AS Total_Unique_Currencies_Deposited
FROM public."DimCurrencies" AS currencies
INNER JOIN public."FactDeposits" AS deposits
	ON currencies.currency_sk = deposits.currency_sk
WHERE
	deposits.tx_status = 'complete'
AND DATE(deposits.event_timestamp) = TO_DATE('2022-05-02', 'YYYY-MM-DD')
;


-- Number of unique currencies withdrew on a given day
SELECT
	COUNT(DISTINCT currencies.currency_sk) AS Total_Unique_Currencies_Withdrew
FROM public."DimCurrencies" AS currencies
INNER JOIN public."FactWithdrawals" AS withdrawals
	ON currencies.currency_sk = withdrawals.currency_sk
WHERE
	withdrawals.tx_status = 'complete'
AND DATE(withdrawals.event_timestamp) = TO_DATE('2021-04-21', 'YYYY-MM-DD') --'2021-04-21', '2020-09-22', '2021-03-31'
;


-- Total amount deposited of a given currency on a given day
SELECT SUM(amount) AS Total_Amount_Deposited
FROM public."FactDeposits" AS deposits
INNER JOIN public."DimCurrencies" AS currencies
	ON deposits.currency_sk = currencies.currency_sk
WHERE
	deposits.tx_status = 'complete'
AND currencies.currency_name = 'mxn'
AND DATE(deposits.event_timestamp) = TO_DATE('2021-04-21', 'YYYY-MM-DD') --'2021-04-21', '2020-09-22', '2021-03-31'

/*
select count(1) from stage."DimInterfaces"
union all 
select count(1) from stage."DimStatuses"
union all 
select count(1) from stage."DimEvents"
union all 
select count(1) from stage."DimCurrencies"
union all 
select count(1) from stage."DimUsers"
union all 
select count(1) from stage."FactWithdrawals"
union all 
select count(1) from stage."FactEvents"
union all 
select count(1) from stage."FactDeposits"

*/