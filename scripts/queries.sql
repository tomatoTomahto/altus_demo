create database telco;
use telco;

create external table customers (
    id                      int,
    state                   string,
    area_code               string,
    phone_number            string,
    intl_plan               boolean,
    voice_mail_plan         boolean,
    number_vmail_messages   double,
    total_day_minutes       double,
    total_day_calls         double,
    total_day_charge        double,
    total_eve_minutes       double,
    total_eve_calls         double,
    total_eve_charge        double,
    total_night_minutes     double,
    total_night_calls       double,
    total_night_charge      double,
    total_intl_minutes      double,
    total_intl_calls        double,
    total_intl_charge       double,
    number_customer_service_calls   double,
    account_length          double,
    churned                 boolean
) stored as parquet
location 's3a://sgupta-s3/DStelco/data/customers';

select * from customers;