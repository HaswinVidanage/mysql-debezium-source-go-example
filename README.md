## Simple MySQL Debezium source with Go consumer JSON format

```
docker-compose up -d

# Run this command to register mysql source for kafka connect after docker setup is done
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-mysql-source.json

go mod tidy
go run main.go
```

