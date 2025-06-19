# Acceleration Insertion for PostgreSQL from custom methods to multithreading and atomicity 🚀

## **Description**

This application is designed to test various methods for inserting data into a PostgreSQL database. It's built with:
- A Kotlin-based backend.
- A preloaded PostgreSQL database with over 100 million rows in the "payment_document" table.
- Multiple indexes in the "payment_document" table to influence the insertion process.

This branch maaintains atomicity of multithreaded inserts by storing the atomicity indicator in a separate table. For testing other approaches, please use the following branches:
- [main](https://github.com/FatovDI/pg-custom-to-multithreaded-inserts/tree/main): Experimenting with various data insertion approaches, ranging from custom PostgreSQL methods to multithreading, without guaranteeing atomicity of the entire operation.
- [ready_to_read](https://github.com/FatovDI/pg-custom-to-multithreaded-inserts/tree/ready_to_read): Maintains atomicity of multithreaded inserts using a ready_to_read flag within the table itself.
- [ready_to_read_transaction_id](https://github.com/FatovDI/pg-custom-to-multithreaded-inserts/tree/ready_to_read_transaction_id): Maintains atomicity of multithreaded inserts using a ready_to_read flag and a shared transaction_id field within the table itself.
- [prepared_transaction_patch](https://github.com/FatovDI/pg-custom-to-multithreaded-inserts/tree/prepared_transaction_patch): Maintains atomicity of multithreaded inserts using the prepared transaction method.

## **Prerequisites**

- **Storage:** Ensure you have at least 32GB of free space on your machine to work with the preloaded database.

## **Working with the Preloaded Database**

1. **Download the Image:** Grab the PostgreSQL image with preloaded data [here](https://disk.yandex.ru/d/gSJozEqP5-QTbQ) named `prep_db.tar.gz`.
2. **Load Image:** Import the image to your local Docker registry using:
   ```bash
   sudo docker load < prep_db.tar.gz
   ```
3. **Start Application:** Using git bash:
   ```bash
   sh start.sh
   ```
4. **Stop Application:** Using git bash:
   ```bash
   sh stop.sh
   ```

## **Working with an Empty Database**

1. **Configure Image:** Update `docker-compose.yaml` and change the line:
   ```yaml
   image: prep_db:1
   ```
   to:
   ```yaml
   image: postgres:latest
   ```
2. **Start Application:**
   ```bash
   sudo docker-compose up -d --build
   ```
3. **Stop Application:** Using git bash:
   ```bash
   sudo docker-compose down -v
   ```

## **API Endpoints**

Use the following endpoints to test different insertion methods. You can specify the number of rows to generate in the database and retrieve results with performance metrics. For convenience, utilize `curl -X POST` to interact with these endpoints. The number of rows to create is specified by the `count` path parameter.

```bash
# Create data using the INSERT method. The data will be saved in batches of 5,000 rows.
http://localhost:8080/test-insertion/insert/{count}

# Create data using the INSERT method with prepared statement. The data will be saved in batches of 5,000 rows.
http://localhost:8080/test-insertion/insert-prepared-statement/{count}

# Create data using the INSERT method with multi-row prepared statement. The data will be saved in batches of 5,000 rows.
http://localhost:8080/test-insertion/insert-prepared-statement-multi-row/{count}

# Create data using the INSERT method with a SELECT statement that utilizes unnest and array parameters. The data will be saved in batches of 5,000 rows.
http://localhost:8080/test-insertion/insert-prepared-statement-unnest/{count}

# Create data using the INSERT method with KProperty map.
http://localhost:8080/test-insertion/insert-by-property/{count}

# Create data using the INSERT method with prepared statement and dropping index before transaction and recreating it after that. The data will be saved in batches of 5,000 rows.
http://localhost:8080/test-insertion/insert-with-drop-index/{count}

# Create data using the COPY method without saving file to disk. The data will be saved in batches of 5,000 rows.
http://localhost:8080/test-insertion/copy/{count}

# Create data using the COPY method with concurrent approach. The data will be saved in batches of 5,000 rows.
http://localhost:8080/test-insertion/copy-concurrent/{count}

# Create data using the COPY method with concurrent and atomic approach. The data will be saved in batches of 5,000 rows.
http://localhost:8080/test-insertion/copy-concurrent-atomic-two-table/{count}

# Create data with KProperty map using the COPY method without saving file to disk.
http://localhost:8080/test-insertion/copy-by-property/{count}

# Create data using the COPY method with binary transformation. All data will be saved in one transaction.
http://localhost:8080/test-insertion/copy-by-binary/{count}

# Create data using the COPY method with binary transformation and concurrent approach. All data will be saved in one transaction.
http://localhost:8080/test-insertion/copy-binary-concurrent/{count}

# Create data using the COPY method with binary transformation concurrent and atomic approach. All data will be saved in one transaction.
http://localhost:8080/test-insertion/copy-binary-concurrent-atomic-two-table/{count}

# Create data with KProperty map using the COPY method with binary transformation.
http://localhost:8080/test-insertion/copy-by-binary-and-property/{count}

# Create data using the COPY method with saving file to disk.
http://localhost:8080/test-insertion/copy-by-file/{count}

# Create data using the COPY method with saving binary file to disk.
http://localhost:8080/test-insertion/copy-by-binary-file/{count}

# Create data using the COPY method and KProperty map with saving file to disk.
http://localhost:8080/test-insertion/copy-by-file-and-property/{count}

# Create data using the COPY method and KProperty map with saving binary file to disk.
http://localhost:8080/test-insertion/copy-by-binary-file-and-property/{count}

# Update data using SQL script.
http://localhost:8080/test-insertion/update/{count}

# Update data using KProperty map and SQL script. 
http://localhost:8080/test-insertion/update-by-property/{count}
```
