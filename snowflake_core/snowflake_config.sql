-- create warehouse if not exists FRAUD_WH
-- with warehouse_size = 'XSMALL'
-- auto_suspend = 60
-- auto_resume = true
-- initially_suspended = true;

-- use warehouse FRAUD_WH;

-- create database if not exists FRAUD_DB;

--use database FRAUD_DB;

-- create schema if not exists RAW;
-- create schema if not exists KITCHEN;
-- create schema if not exists MARTS;

show warehouses;
show databases;
show schemas in database FRAUD_DB;