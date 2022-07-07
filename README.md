# mongo2ora
MongoDB to Oracle data migration tool

## Setup

### Install ojdl.jar inside local Maven repository
\apache-maven-3.8.1\bin>mvn install:install-file -Dfile=<fqpn>\lib\ojdl.jar -DgroupId=com.oracle -DartifactId=ojdl -Dversion=21.6.0.0.1 -Dpackaging=jar

### Create admin user in Oracle database
create user admin identified by "My_Strong_Pa55word" DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP;
alter user admin quota unlimited on users;
grant dba to admin;

### Create moviestream user in Oracle database
create user moviestream identified by "My_Strong_Pa55word" DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP;
alter user moviestream quota unlimited on users;
grant create session, soda_app, alter session, select_catalog_role to moviestream;

grant create table to moviestream;
grant create procedure to moviestream;
grant create sequence to moviestream;

grant execute on ctxsys.CTX_DDL to moviestream;
grant select any dictionary to moviestream;

