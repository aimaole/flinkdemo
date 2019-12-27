pg安装
./configure --without-readline
make
make install


timescaledb安装
./bootstrap -DUSE_OPENSSL=0
cd ./build && make
make install


adduser postgres
mkdir -p /usr/local/pgsql/data
chmod -R 0700 /usr/local/pgsql/data
chown -R postgres:postgres /usr/local/pgsql
su - postgres
/usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data
exit
cp /root/noah2/Noah2ETL-1.0-SNAPSHOT/postgressql/postgresql.conf /usr/local/pgsql/data/
su - postgres
/usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data -l /usr/local/pgsql/data/logfile start
/usr/local/pgsql/bin/psql
create user noah with password 'noah';
create database noah owner noah;
grant all privileges on database noah to noah;
\c noah;
create extension if not exists timescaledb cascade;
create schema noah2;
alter schema noah2 owner to noah;
\q
exit;
cp /root/noah2/Noah2ETL-1.0-SNAPSHOT/postgressql/pg_hba.conf /usr/local/pgsql/data/
su - postgres
/usr/local/pgsql/bin/pg_ctl stop -D /usr/local/pgsql/data
/usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data -l /usr/local/pgsql/data/logfile start
exit;



pg卸载
su - postgres
/usr/local/pgsql/bin/pg_ctl stop -D /usr/local/pgsql/data
exit;
rm -rf /usr/local/pgsql