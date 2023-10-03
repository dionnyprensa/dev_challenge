-- CREATE USER challenge WITH PASSWORD 'challenge' CREATEDB;

-- CREATE DATABASE challenge
--   WITH
--   OWNER = challenge
--   ENCODING = 'UTF8'
--   LC_COLLATE = 'en_US.utf8'
--   LC_CTYPE = 'en_US.utf8'
--   TABLESPACE = pg_default
--   CONNECTION LIMIT = -1;
-- ;

\connect challenge;

CREATE SCHEMA stage;

DROP TABLE IF EXISTS public."DimInterfaces";
DROP TABLE IF EXISTS public."DimStatuses";
DROP TABLE IF EXISTS public."DimEvents";
DROP TABLE IF EXISTS public."DimCurrencies";
DROP TABLE IF EXISTS public."DimUsers";
DROP TABLE IF EXISTS public."FactWithdrawals";
DROP TABLE IF EXISTS public."FactEvents";
DROP TABLE IF EXISTS public."FactDeposits";
DROP TABLE IF EXISTS stage."DimInterfaces";
DROP TABLE IF EXISTS stage."DimStatuses";
DROP TABLE IF EXISTS stage."DimEvents";
DROP TABLE IF EXISTS stage."DimCurrencies";
DROP TABLE IF EXISTS stage."DimUsers";
DROP TABLE IF EXISTS stage."FactWithdrawals";
DROP TABLE IF EXISTS stage."FactEvents";
DROP TABLE IF EXISTS stage."FactDeposits";

/* public tables */
CREATE TABLE public."DimInterfaces"(
  interface_sk BIGSERIAL PRIMARY KEY,
  interface_name VARCHAR(16) NOT NULL,
  create_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE public."DimStatuses"(
  status_sk BIGSERIAL PRIMARY KEY,
  status_name VARCHAR(16) NOT NULL,
  create_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE public."DimEvents"(
  event_sk BIGSERIAL PRIMARY KEY,
  event_name VARCHAR(50) NOT NULL,
  create_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE public."DimCurrencies"(
  currency_sk BIGSERIAL PRIMARY KEY,
  currency_name VARCHAR(16) NOT NULL,
  create_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE public."DimUsers"(
  user_sk BIGSERIAL PRIMARY KEY,
  user_id VARCHAR(50) NOT NULL,
  create_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE public."FactWithdrawals"(
  withdrawal_sk BIGSERIAL PRIMARY KEY,
  id BIGINT NOT NULL,
  event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
  user_id VARCHAR(50) NOT NULL,
  user_sk BIGINT NOT NULL,
  amount DECIMAL(36,18) NOT NULL,
  interface VARCHAR(50) NOT NULL,
  interface_sk BIGINT NOT NULL,
  currency VARCHAR(8) NOT NULL,
  currency_sk BIGINT NOT NULL,
  tx_status VARCHAR(16) NOT NULL,
  status_sk BIGINT NOT NULL,
  create_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE public."FactEvents"(
  event_sk BIGSERIAL PRIMARY KEY,
  id BIGINT NOT NULL,
  event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
  user_id VARCHAR(40) NOT NULL,
  user_sk BIGINT NOT NULL,
  event_name VARCHAR(50) NOT NULL,
  event_dim_sk BIGINT NOT NULL,
  create_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE public."FactDeposits"(
  deposit_sk BIGSERIAL PRIMARY KEY,
  id BIGINT NOT NULL,
  event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
  user_id VARCHAR(50) NOT NULL,
  user_sk BIGINT NOT NULL,
  amount DECIMAL(36,18) NOT NULL,
  currency CHAR(5) NOT NULL,
  currency_sk BIGINT NOT NULL,
  tx_status VARCHAR(16) NOT NULL,
  status_sk BIGINT NOT NULL,
  create_at TIMESTAMP DEFAULT NOW()
);


/* stage tables */
CREATE TABLE stage."DimInterfaces"(
  index BIGINT,
  interface VARCHAR(16) NULL
);

CREATE TABLE stage."DimStatuses"(
  index BIGINT,
  tx_status VARCHAR(16) NOT NULL
);

CREATE TABLE stage."DimEvents"(
  index BIGINT,
  event_name VARCHAR(50) NULL
);

CREATE TABLE stage."DimCurrencies"(
  index BIGINT,
  currency VARCHAR(16) NOT NULL
);

CREATE TABLE stage."DimUsers"(
  index BIGINT,
  user_id VARCHAR(50) NULL
);

CREATE TABLE stage."FactWithdrawals"(
  index BIGINT,
  id BIGINT NULL,
  event_timestamp TIMESTAMP NULL,
  user_id VARCHAR(50) NULL,
  amount DECIMAL(36,18) NULL,
  interface VARCHAR(16) NULL,
  currency CHAR(5) NULL,
  tx_status VARCHAR(16) NULL
);

CREATE TABLE stage."FactEvents"(
  index BIGINT,
  id BIGINT NULL,
  event_timestamp TIMESTAMP NOT NULL,
  user_id VARCHAR(40) NOT NULL,
  event_name VARCHAR(50)
);

CREATE TABLE stage."FactDeposits"(
  index BIGINT,
  id BIGINT NULL,
  event_timestamp TIMESTAMP NULL,
  user_id VARCHAR(50) NULL,
  amount DECIMAL(36,18) NULL,
  currency CHAR(5) NULL,
  tx_status VARCHAR(16) NULL
);


/* Stored Procedures */
DROP PROCEDURE IF EXISTS stage.build_scd_interfaces();
DROP PROCEDURE IF EXISTS stage.build_scd_statuses();
DROP PROCEDURE IF EXISTS stage.build_scd_events();
DROP PROCEDURE IF EXISTS stage.build_scd_currencies();
DROP PROCEDURE IF EXISTS stage.build_scd_users();
DROP PROCEDURE IF EXISTS stage.load_fact_withdrawals();
DROP PROCEDURE IF EXISTS stage.load_fact_events();
DROP PROCEDURE IF EXISTS stage.load_fact_deposits();
DROP PROCEDURE IF EXISTS stage.clean_stage_tables();


CREATE OR REPLACE PROCEDURE stage.build_scd_interfaces()
LANGUAGE 'sql' AS $$
;WITH CTE AS (
	SELECT DISTINCT interface FROM stage."DimInterfaces" AS stage_
	WHERE NOT EXISTS(
		SELECT 1 FROM public."DimInterfaces" AS public_
		WHERE public_.interface_name != stage_.interface
	)
)

INSERT INTO public."DimInterfaces"(interface_name)
SELECT interface FROM CTE;
$$;

CREATE OR REPLACE PROCEDURE stage.build_scd_statuses()
LANGUAGE 'sql' AS $$
;WITH CTE AS (
	SELECT DISTINCT tx_status FROM stage."DimStatuses" AS stage_
	WHERE NOT EXISTS(
		SELECT 1 FROM public."DimStatuses" AS public_
		WHERE public_.status_name != stage_.tx_status
	)
)

INSERT INTO public."DimStatuses"(status_name)
SELECT tx_status FROM CTE;
$$;

CREATE OR REPLACE PROCEDURE stage.build_scd_events()
LANGUAGE 'sql' AS $$
;WITH CTE AS (
	SELECT DISTINCT event_name FROM stage."DimEvents" AS stage_
	WHERE NOT EXISTS(
		SELECT 1 FROM public."DimEvents" AS public_
		WHERE public_.event_name != stage_.event_name
	)
)

INSERT INTO public."DimEvents"(event_name)
SELECT event_name FROM CTE;
$$;


CREATE OR REPLACE PROCEDURE stage.build_scd_currencies()
LANGUAGE 'sql' AS $$
;WITH CTE AS (
	SELECT DISTINCT currency FROM stage."DimCurrencies" AS stage_
	WHERE NOT EXISTS(
		SELECT 1 FROM public."DimCurrencies" AS public_
		WHERE public_.currency_name != stage_.currency
	)
)

INSERT INTO public."DimCurrencies"(currency_name)
SELECT currency FROM CTE;
$$;

CREATE OR REPLACE PROCEDURE stage.build_scd_users()
LANGUAGE 'sql' AS $$
;WITH CTE AS (
	SELECT DISTINCT user_id FROM stage."DimUsers" AS stage_
	WHERE NOT EXISTS(
		SELECT 1 FROM public."DimUsers" AS public_
		WHERE public_.user_id != stage_.user_id
	)
)

INSERT INTO public."DimUsers"(user_id)
SELECT user_id FROM CTE;
$$;

CREATE OR REPLACE PROCEDURE stage.load_fact_withdrawals()
LANGUAGE 'sql' AS $$
INSERT INTO public."FactWithdrawals"(
	id,
	event_timestamp,
	user_id,
	user_sk,
	amount,
	interface,
	interface_sk,
	currency,
	currency_sk,
	tx_status,
	status_sk
)
SELECT
	id,
	CAST(withdrawals.event_timestamp AS TIMESTAMP WITH TIME ZONE) AS event_timestamp,
	withdrawals.user_id,
	users.user_sk,
 	withdrawals.amount,
	withdrawals.interface,
	interfaces.interface_sk,
	withdrawals.currency,
	currencies.currency_sk,
	withdrawals.tx_status,
	statuses.status_sk
FROM stage."FactWithdrawals" AS withdrawals
LEFT JOIN public."DimUsers" AS users
	ON withdrawals.user_id = users.user_id

LEFT JOIN public."DimInterfaces" AS interfaces
	ON withdrawals.interface = interfaces.interface_name

LEFT JOIN public."DimCurrencies" AS currencies
	ON withdrawals.currency = currencies.currency_name

LEFT JOIN public."DimStatuses" AS statuses
	ON withdrawals.tx_status = statuses.status_name
$$;

CREATE OR REPLACE PROCEDURE stage.load_fact_events()
LANGUAGE 'sql' AS $$
INSERT INTO public."FactEvents"(
	id,
	event_timestamp,
	user_id,
	user_sk,
	event_name,
	event_dim_sk
)
SELECT
	events.id,
	CAST(events.event_timestamp AS TIMESTAMP WITH TIME ZONE) AS event_timestamp,
	events.user_id,
	users.user_sk,
	events.event_name,
	events_dim.event_sk AS event_dim_sk
 	
FROM stage."FactEvents" AS events
LEFT JOIN public."DimUsers" AS users
	ON events.user_id = users.user_id
	
LEFT JOIN public."DimEvents" AS events_dim
	ON events.event_name = events_dim.event_name
$$;

CREATE OR REPLACE PROCEDURE stage.load_fact_deposits()
LANGUAGE 'sql' AS $$
INSERT INTO public."FactDeposits"(
	id,
	event_timestamp,
	user_id,
	user_sk,
	amount,
	currency,
	currency_sk,
	tx_status,
	status_sk
)
SELECT
	id,
	CAST(deposits.event_timestamp AS TIMESTAMP WITH TIME ZONE) AS event_timestamp,
	deposits.user_id,
	users.user_sk,
 	COALESCE(deposits.amount, 0) AS amount,
	deposits.currency,
	currencies.currency_sk,
	deposits.tx_status,
	statuses.status_sk
FROM stage."FactDeposits" AS deposits
LEFT JOIN public."DimUsers" AS users
	ON deposits.user_id = users.user_id

LEFT JOIN public."DimCurrencies" AS currencies
	ON deposits.currency = currencies.currency_name

LEFT JOIN public."DimStatuses" AS statuses
	ON deposits.tx_status = statuses.status_name
$$;

CREATE OR REPLACE PROCEDURE stage.clean_stage_tables()
LANGUAGE 'sql' AS $$
TRUNCATE TABLE stage."DimInterfaces";
TRUNCATE TABLE stage."DimStatuses";
TRUNCATE TABLE stage."DimEvents";
TRUNCATE TABLE stage."DimCurrencies";
TRUNCATE TABLE stage."DimUsers";
TRUNCATE TABLE stage."FactWithdrawals";
TRUNCATE TABLE stage."FactEvents";
TRUNCATE TABLE stage."FactDeposits";
$$;


ALTER PROCEDURE stage.build_scd_interfaces() OWNER TO challenge;
ALTER PROCEDURE stage.build_scd_statuses() OWNER TO challenge;
ALTER PROCEDURE stage.build_scd_events() OWNER TO challenge;
ALTER PROCEDURE stage.build_scd_currencies() OWNER TO challenge;
ALTER PROCEDURE stage.build_scd_users() OWNER TO challenge;
ALTER PROCEDURE stage.load_fact_withdrawals() OWNER TO challenge;
ALTER PROCEDURE stage.load_fact_events() OWNER TO challenge;
ALTER PROCEDURE stage.load_fact_deposits() OWNER TO challenge;
ALTER PROCEDURE stage.clean_stage_tables() OWNER TO challenge;