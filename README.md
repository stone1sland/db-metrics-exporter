
run db
docker run --name pgsql -p 5432:5432 -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=dbname -d postgres:13.3
run script 
./bin --user user --password password --db dbname --ssl disable