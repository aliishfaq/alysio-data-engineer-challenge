DROP DATABASE IF EXISTS alysio_stg;
DROP DATABASE IF EXISTS alysio;
CREATE DATABASE IF NOT EXISTS alysio_stg;
CREATE DATABASE IF NOT EXISTS alysio;
use alysio_stg;

DROP TABLE IF EXISTS batch;
CREATE TABLE batch (
    id INT AUTO_INCREMENT PRIMARY KEY,
    start_time DATETIME,
    end_time DATETIME,
    status VARCHAR(20),
    exceptions VARCHAR(500)
);

DROP TABLE IF EXISTS stg_activities;
CREATE TABLE `stg_activities` (
  `id` varchar(255) DEFAULT NULL,
  `contact_id` varchar(255) DEFAULT NULL,
  `opportunity_id` varchar(255) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  `subject` varchar(255) DEFAULT NULL,
  `timestamp` varchar(255) DEFAULT NULL,
  `duration_minutes` varchar(255) DEFAULT NULL,
  `outcome` varchar(255) DEFAULT NULL,
  `notes` varchar(255) DEFAULT NULL,
  `is_error` INT DEFAULT 0,
  `error_description`varchar(255) DEFAULT NULL 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS stg_contacts;
CREATE TABLE `stg_contacts` (
  `id` varchar(255) DEFAULT NULL,
  `email` varchar(255) DEFAULT NULL,
  `first_name` varchar(255) DEFAULT NULL,
  `last_name` varchar(255) DEFAULT NULL,
  `title` varchar(255) DEFAULT NULL,
  `company_id` varchar(255) DEFAULT NULL,
  `phone` varchar(255) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `created_date` varchar(255) DEFAULT NULL,
  `last_modified` varchar(255) DEFAULT NULL,
  `is_error` INT DEFAULT 0,
  `error_description`varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS stg_companies;
CREATE TABLE `stg_companies` (
  `id` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `domain` varchar(255) DEFAULT NULL,
  `industry` varchar(255) DEFAULT NULL,
  `size` varchar(255) DEFAULT NULL,
  `country` varchar(255) DEFAULT NULL,
  `created_date` varchar(255) DEFAULT NULL,
  `is_customer` varchar(255) DEFAULT NULL,
  `annual_revenue` varchar(255) DEFAULT NULL,
  `is_error` INT DEFAULT 0,
  `error_description`varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS stg_opportunities;
CREATE TABLE `stg_opportunities` (
  `id` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `contact_id` varchar(255) DEFAULT NULL,
  `company_id` varchar(255) DEFAULT NULL,
  `amount` varchar(255) DEFAULT NULL,
  `stage` varchar(255) DEFAULT NULL,
  `product` varchar(255) DEFAULT NULL,
  `probability` varchar(255) DEFAULT NULL,
  `created_date` varchar(255) DEFAULT NULL,
  `close_date` varchar(255) DEFAULT NULL,
  `is_closed` varchar(255) DEFAULT NULL,
  `forecast_category` varchar(255) DEFAULT NULL,
  `is_error` INT DEFAULT 0,
  `error_description`varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

use alysio;

DROP TABLE IF EXISTS companies;
CREATE TABLE `companies` (
  `company_id` INT AUTO_INCREMENT PRIMARY KEY,
  `source_id` VARCHAR(10) NOT NULL,
  `name` VARCHAR(255) DEFAULT NULL,
  `domain` VARCHAR(255) DEFAULT NULL,
  `industry` VARCHAR(255) DEFAULT NULL,
  `size` VARCHAR(255) DEFAULT NULL,
  `country` VARCHAR(2) DEFAULT NULL,
  `created_date` DATETIME DEFAULT NULL,
  `is_customer` BOOL DEFAULT NULL,
  `annual_revenue` VARCHAR(255) DEFAULT NULL,
  `batch_id` INT DEFAULT NULL, -- Batch date to store the current timestamp
  INDEX (`source_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE UNIQUE INDEX idx_source_id ON alysio.companies (source_id);

DROP TABLE IF EXISTS contacts;
CREATE TABLE `contacts` (
  `contact_id` INT AUTO_INCREMENT PRIMARY KEY,
  `source_id` varchar(255) DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  `first_name` varchar(120) DEFAULT NULL,
  `last_name` varchar(120) DEFAULT NULL,
  `title` varchar(100) DEFAULT NULL,
  `company_id` INT DEFAULT NULL,
  `phone` varchar(20) DEFAULT NULL,
  `status` varchar(50) DEFAULT NULL,
  `created_date` DATETIME DEFAULT NULL,
  `last_modified` DATETIME DEFAULT NULL,
  `batch_id` INT DEFAULT NULL,
  INDEX (`company_id`),
	CONSTRAINT `fk_company_id_contacts` FOREIGN KEY (`company_id`) REFERENCES `companies` (`company_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE UNIQUE INDEX idx_source_id ON alysio.contacts (source_id);

DROP TABLE IF EXISTS opportunities;
CREATE TABLE `opportunities` (
  `opportunity_id` INT AUTO_INCREMENT PRIMARY KEY,
  `source_id` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `contact_id` INT DEFAULT NULL,
  `company_id` INT DEFAULT NULL,
  `amount` varchar(255) DEFAULT NULL,
  `stage` varchar(255) DEFAULT NULL,
  `product` varchar(255) DEFAULT NULL,
  `probability` varchar(255) DEFAULT NULL,
  `created_date` DATETIME DEFAULT NULL,
  `close_date` varchar(255) DEFAULT NULL,
  `is_closed` varchar(255) DEFAULT NULL,
  `forecast_category` varchar(255) DEFAULT NULL,
  `batch_id` INT DEFAULT NULL,
  INDEX (`opportunity_id`),
	CONSTRAINT `fk_company_id_opp` FOREIGN KEY (`company_id`) REFERENCES `companies` (`company_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
	CONSTRAINT `fk_contact_id_opp` FOREIGN KEY (`contact_id`) REFERENCES `contacts` (`contact_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS activities;
CREATE TABLE `activities` (
  `source_id` varchar(255) DEFAULT NULL,
  `contact_id` INT DEFAULT NULL,
  `opportunity_id` INT DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  `subject` varchar(255) DEFAULT NULL,
  `timestamp` varchar(255) DEFAULT NULL,
  `duration_minutes` varchar(255) DEFAULT NULL,
  `outcome` varchar(255) DEFAULT NULL,
  `notes` varchar(255) DEFAULT NULL,
  `batch_id` INT DEFAULT NULL,
  INDEX (`opportunity_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

use alysio_stg;

DELIMITER $$
DROP PROCEDURE IF EXISTS TruncateStagingTables$$
CREATE PROCEDURE TruncateStagingTables()
BEGIN
    -- Truncate the tables
    TRUNCATE TABLE stg_activities;
    TRUNCATE TABLE stg_contacts;
    TRUNCATE TABLE stg_companies;
    TRUNCATE TABLE stg_opportunities;
END $$

DELIMITER ;


DELIMITER $$
DROP PROCEDURE IF EXISTS UpsertCompanies$$

CREATE PROCEDURE UpsertCompanies(IN batch_id INT)
BEGIN

    INSERT INTO alysio.companies (source_id, name, domain, industry, size, country, created_date, is_customer, annual_revenue, batch_id)
    SELECT 
        id, 
        name, 
        domain, 
        industry, 
        size, 
        country, 
        created_date, 
        is_customer, 
        annual_revenue, 
        batch_id AS batch_id
    FROM alysio_stg.stg_companies
    where is_error != 1
    ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        domain = VALUES(domain),
        industry = VALUES(industry),
        size = VALUES(size),
        country = VALUES(country),
        created_date = VALUES(created_date),
        is_customer = VALUES(is_customer),
        annual_revenue = VALUES(annual_revenue),
        batch_id = VALUES(batch_id);

END $$

DELIMITER ;

DELIMITER $$

DROP PROCEDURE IF EXISTS UpsertContacts$$

CREATE PROCEDURE UpsertContacts(IN batch_id INT)
BEGIN
    -- Insert new records
    INSERT INTO alysio.contacts(
        source_id, email, first_name, last_name, title, company_id, 
        phone, status, created_date, last_modified, batch_id
    )
    SELECT 
        CO.id,
        CO.email,
        CO.first_name,
        CO.last_name,
        CO.title,
        C.company_id,
        CO.phone,
        CO.status,
        CO.created_date,
        CO.last_modified,
        batch_id -- Use the parameter batch_id
    FROM alysio_stg.stg_contacts CO
	JOIN alysio.companies C ON C.source_id = CO.company_id
    LEFT JOIN alysio.contacts DC ON DC.source_id = CO.id
    WHERE DC.source_id IS NULL
    AND   CO.is_error != 1;

    -- Update existing records
    UPDATE alysio.contacts DC
    JOIN alysio_stg.stg_contacts CO ON CO.id = DC.source_id
	JOIN alysio.companies C ON C.source_id = CO.company_id
    SET 
        DC.email = CO.email,
        DC.first_name = CO.first_name,
        DC.last_name = CO.last_name,
        DC.title = CO.title,
        DC.company_id = C.company_id,
        DC.phone = CO.phone,
        DC.status = CO.status,
        DC.created_date = CO.created_date,
        DC.last_modified = CO.last_modified,
        DC.batch_id = batch_id  -- Use the parameter batch_id
    WHERE 
		CO.is_error != 1 AND(
        COALESCE(DC.email, '') != COALESCE(CO.email, '') OR
        COALESCE(DC.first_name, '') != COALESCE(CO.first_name, '') OR
        COALESCE(DC.last_name, '') != COALESCE(CO.last_name, '') OR
        COALESCE(DC.title, '') != COALESCE(CO.title, '') OR
        COALESCE(DC.company_id, 0) != COALESCE(C.company_id, 0) OR
        COALESCE(DC.phone, '') != COALESCE(CO.phone, '') OR
        COALESCE(DC.status, '') != COALESCE(CO.status, '') OR
        COALESCE(DC.created_date, '1970-01-01 00:00:00') != COALESCE(CO.created_date, '1970-01-01 00:00:00') OR
        COALESCE(DC.last_modified, '1970-01-01 00:00:00') != COALESCE(CO.last_modified, '1970-01-01 00:00:00'));
END $$

DELIMITER ;

DELIMITER $$

DROP PROCEDURE IF EXISTS UpsertOpportunities$$

CREATE PROCEDURE UpsertOpportunities(IN batch_id INT)
BEGIN
    -- Insert new records
    INSERT INTO alysio.opportunities(
			source_id,name,contact_id,company_id,amount,stage,product,probability,
			created_date,close_date,is_closed,forecast_category,batch_id
    )
    SELECT id,
		O.name,
		DC.contact_id,
		C.company_id,
		O.amount,
		O.stage,
		O.product,
		O.probability,
		O.created_date,
		O.close_date,
		O.is_closed,
		O.forecast_category,
        batch_id  -- Use the parameter batch_id
    FROM alysio_stg.stg_opportunities O
	JOIN alysio.companies C ON C.source_id = O.company_id
    JOIN alysio.contacts DC ON DC.source_id = O.contact_id
    LEFT JOIN alysio.opportunities DO ON DO.source_id = O.id
    WHERE DO.source_id IS NULL
    AND O.is_error != 1;

    -- Update existing records
    UPDATE alysio.opportunities DO
    JOIN alysio_stg.stg_opportunities O ON O.id = DO.source_id
	JOIN alysio.companies C ON C.source_id = O.company_id
    JOIN alysio.contacts DC ON DC.source_id = O.contact_id
    SET DO.name = O.name
		,DO.contact_id = DC.contact_id
		,DO.company_id = C.company_id
		,DO.amount = O.amount
		,DO.stage = O.stage
		,DO.product = O.product
		,DO.probability = O.probability
		,DO.created_date = O.created_date
		,DO.close_date = O.close_date
		,DO.is_closed = O.is_closed
		,DO.forecast_category = O.forecast_category
        ,DO.batch_id = batch_id  -- Use the parameter batch_id
    WHERE 
		O.is_error != 1 AND(
    COALESCE(DO.name, '') != COALESCE(O.name, '') OR
    COALESCE(DO.contact_id, 0) != COALESCE(DC.contact_id, 0) OR
    COALESCE(DO.company_id, 0) != COALESCE(C.company_id, 0) OR
    COALESCE(DO.amount, '') != COALESCE(O.amount, '') OR
    COALESCE(DO.stage, '') != COALESCE(O.stage, '') OR
    COALESCE(DO.product, '') != COALESCE(O.product, '') OR
    COALESCE(DO.probability, '') != COALESCE(O.probability, '') OR
    COALESCE(DO.created_date, '1970-01-01 00:00:00') != COALESCE(O.created_date, '1970-01-01 00:00:00') OR
    COALESCE(DO.close_date, '') != COALESCE(O.close_date, '') OR
    COALESCE(DO.is_closed, '') != COALESCE(O.is_closed, '') OR
    COALESCE(DO.forecast_category, '') != COALESCE(O.forecast_category, ''));
END $$

DELIMITER ;

DELIMITER $$

DROP PROCEDURE IF EXISTS UpsertActivities $$

CREATE PROCEDURE UpsertActivities(IN batch_id INT)
BEGIN
    -- Insert new records
    INSERT INTO alysio.activities(source_id,contact_id,opportunity_id,type,subject,timestamp,duration_minutes,outcome,notes,batch_id)
    SELECT A.id,
		DC.contact_id,
		O.opportunity_id,
		A.type,
		A.subject,
		A.timestamp,
		A.duration_minutes,
		A.outcome,
        A.notes,
        batch_id  -- Use the parameter batch_id
    FROM alysio_stg.stg_activities A
	LEFT JOIN alysio.opportunities O ON O.source_id = A.opportunity_id
    LEFT JOIN alysio.contacts DC ON DC.source_id = A.contact_id
    LEFT JOIN alysio.activities DA ON DA.source_id = A.id
    WHERE DA.source_id IS NULL
    AND A.is_error != 1;

    -- Update existing records
    UPDATE alysio.activities DA
    JOIN alysio_stg.stg_activities A ON A.id = DA.source_id
	JOIN alysio.opportunities O ON O.source_id = A.opportunity_id
    JOIN alysio.contacts DC ON DC.source_id = A.contact_id
    SET DA.contact_id		= DC.contact_id
		,DA.opportunity_id   = O.opportunity_id
		,DA.type             = A.type
		,DA.subject          = A.subject
		,DA.timestamp        = A.timestamp
		,DA.duration_minutes = A.duration_minutes
		,DA.outcome          = A.outcome
        ,DA.notes			 = A.notes
        ,DA.batch_id = batch_id  -- Use the parameter batch_id
    WHERE 
		A.is_error != 1 AND(
    COALESCE(DA.contact_id, 0) != COALESCE(DC.contact_id, 0) OR
    COALESCE(DA.opportunity_id, 0) != COALESCE(O.opportunity_id, 0) OR
    COALESCE(DA.type, '') != COALESCE(A.type, '') OR
    COALESCE(DA.subject, '') != COALESCE(A.subject, '') OR
    COALESCE(DA.timestamp, '1970-01-01 00:00:00') != COALESCE(A.timestamp, '1970-01-01 00:00:00') OR
    COALESCE(DA.duration_minutes, 0) != COALESCE(A.duration_minutes, 0) OR
    COALESCE(DA.outcome, '') != COALESCE(A.outcome, ''));

END $$

DELIMITER ;


DELIMITER $$

DROP PROCEDURE IF EXISTS run_validations$$

CREATE PROCEDURE run_validations()
BEGIN
/*
1: marked as Error and wont get loaded
2: marked as Warning and will get loaded
*/


    -- Remove Duplicates based on created date
    UPDATE alysio_stg.stg_contacts AS sc
	JOIN (
		SELECT id, MAX(created_date) AS latest_created_date
		FROM alysio_stg.stg_contacts
		GROUP BY id
	) AS latest
	ON sc.id = latest.id
	SET sc.is_error = 1,
		sc.error_description = 'Duplicate record'
	WHERE sc.created_date < latest.latest_created_date;
    
    -- Capture Invalid Phone Numbers
	UPDATE alysio_stg.stg_contacts C
    SET C.is_error = 2,
		C.error_description = 'Invalid Phone Number'
    WHERE LENGTH(TRIM(C.phone)) != 15;
    
    -- Capture Invalid Emails
    UPDATE alysio_stg.stg_contacts C
    SET C.is_error = 2,
		C.error_description = 'Invalid Email'
    WHERE INSTR(email, '@') = 0;
    
    -- Invalid Date range
    UPDATE alysio_stg.stg_contacts C
    SET C.is_error = 2,
		C.error_description = 'Invalid Dates'
    WHERE STR_TO_DATE(created_date, '%Y-%m-%d %H:%i:%s') > NOW()
    OR STR_TO_DATE(last_modified, '%Y-%m-%d %H:%i:%s') > NOW();
    
    -- Invalid Country Abbr
    UPDATE alysio_stg.stg_companies C
    SET C.is_error = 1,
		C.error_description = 'Invalid Country'
    WHERE LENGTH(TRIM(C.country)) > 2;
    
    

END $$

DELIMITER ;