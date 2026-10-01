# mongo2ora
MongoDB to Oracle data migration tool

## Setup

### Create admin user in Oracle database
create user admin identified by "This_Is_Th3_Passw0rd" DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP;
alter user admin quota unlimited on users;
grant dba to admin;

### Create moviestream user in Oracle database
create user moviestream identified by "This_Is_Th3_Passw0rd" DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP;
alter user moviestream quota unlimited on users;
grant create session, soda_app, alter session, select_catalog_role to moviestream;

grant create table to moviestream;
grant create procedure to moviestream;
grant create sequence to moviestream;

grant execute on ctxsys.CTX_DDL to moviestream;
grant select any dictionary to moviestream;

