desc table gmail_formatted
select * from gmail_automated
select * from gmail_formatted

CREATE OR REPLACE TABLE gmail_formatted (
    id STRING,
    name STRING,
    email_address STRING,
    username STRING,
    domain_name STRING,
    email_received_on STRING,
    urls STRING,
    url_domains STRING,
    base_domain STRING,
    source_mail STRING,
    list_urls VARIANT,
    list_url_domains VARIANT,
    list_base_domains VARIANT
);

alter task task_gmail_formatted resume;

CREATE OR REPLACE TASK task_gmail_formatted
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
AS
    BEGIN
        MERGE INTO gmail_formatted target
            USING (
                SELECT DISTINCT 
                    record_content:id::string AS id,
                    record_content:name::string AS name,
                    record_content:email_address::string AS email_address,
                    record_content:username::string AS username,
                    record_content:domain_name::string AS domain_name,
                    record_content:email_recieved_on::string AS email_received_on,
                    record_content:urls::string AS urls,
                    record_content:url_domains::string AS url_domains,
                    record_content:base_domains::string AS base_domain,
                    record_content:source_mail::string AS source_mail,
                    record_content:list_urls AS list_urls,
                    record_content:list_url_domains AS list_url_domains,
                    record_content:list_base_domains AS list_base_domains
                FROM gmail_automated
            ) source ON target.id = source.id
            WHEN MATCHED THEN
                UPDATE SET
                    target.name = source.name,
                    target.email_address = source.email_address,
                    target.username = source.username,
                    target.domain_name = source.domain_name,
                    target.email_received_on = source.email_received_on,
                    target.urls = source.urls,
                    target.url_domains = source.url_domains,
                    target.base_domain = source.base_domain,
                    target.source_mail = source.source_mail,
                    target.list_urls = source.list_urls,
                    target.list_url_domains = source.list_url_domains,
                    target.list_base_domains = source.list_base_domains
            WHEN NOT MATCHED THEN
                INSERT (id, name, email_address, username, domain_name, email_received_on, urls, url_domains, base_domain, source_mail, list_urls, list_url_domains, list_base_domains)
                VALUES (source.id, source.name, source.email_address, source.username, source.domain_name, source.email_received_on, source.urls, source.url_domains, source.base_domain, source.source_mail, source.list_urls, source.list_url_domains, source.list_base_domains);
    END;


CREATE OR REPLACE TABLE GMAIL_FLATTENED AS
    SELECT DISTINCT
        ID, NAME, EMAIL_ADDRESS, USERNAME, DOMAIN_NAME, SOURCE_MAIL, EMAIL_RECEIVED_ON, LIST_URLS.value::STRING AS URLS, LIST_URL_DOMAINS.value::STRING AS URL_DOMAINS, LIST_BASE_DOMAINS.value::STRING AS BASE_DOMAIN
    FROM GMAIL_FORMATTED,
        LATERAL FLATTEN(input => LIST_URLS) LIST_URLS,
        LATERAL FLATTEN(input => LIST_URL_DOMAINS) LIST_URL_DOMAINS,
        LATERAL FLATTEN(input => LIST_BASE_DOMAINS) LIST_BASE_DOMAINS;

        -- OR USE THE BELOW ONE AS IT WONT LOAD DIRECTLY

CREATE OR REPLACE TABLE GMAIL_FLATTENED (ID varchar, NAME varchar, EMAIL_ADDRESS varchar, USERNAME varchar, DOMAIN_NAME varchar, SOURCE_MAIL varchar, EMAIL_RECEIVED_ON varchar, URLS varchar, URL_DOMAINS varchar, BASE_DOMAIN varchar);

desc table GMAIL_FLATTENED
select * from GMAIL_FLATTENED


CREATE OR REPLACE TASK TASK_GMAIL_FLATTENED
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
AS
BEGIN
    MERGE INTO GMAIL_FLATTENED target
    USING (
        SELECT DISTINCT
             ID, NAME, USERNAME, EMAIL_ADDRESS, DOMAIN_NAME, SOURCE_MAIL, EMAIL_RECEIVED_ON, LIST_URLS.value::STRING AS URLS, LIST_URL_DOMAINS.value::STRING AS URL_DOMAINS, LIST_BASE_DOMAINS.value::STRING AS BASE_DOMAIN
    FROM GMAIL_FORMATTED,
        LATERAL FLATTEN(input => LIST_URLS) LIST_URLS,
        LATERAL FLATTEN(input => LIST_URL_DOMAINS) LIST_URL_DOMAINS,
        LATERAL FLATTEN(input => LIST_BASE_DOMAINS) LIST_BASE_DOMAINS
    ) source ON target.id = source.id
    WHEN MATCHED THEN
        UPDATE SET
            target.name = source.name,
            target.username = source.username,
            target.email_address = source.email_address,
            target.domain_name = source.domain_name,
            target.email_received_on = source.email_received_on,
            target.urls = source.urls,
            target.url_domains = source.url_domains,
            target.base_domain = source.base_domain,
            target.source_mail = source.source_mail
    WHEN NOT MATCHED THEN
        INSERT (id, name, username, email_address, domain_name, email_received_on, SOURCE_MAIL, urls, base_domain, url_domains)
        VALUES (source.id, source.name, source.username, source.email_address,  source.domain_name, source.email_received_on, source.source_mail, source.urls, source.base_domain, source.url_domains);
END;