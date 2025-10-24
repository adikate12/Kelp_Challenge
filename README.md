# CSV to JSON Converter API

A Node.js application that converts CSV files to JSON and stores the data in PostgreSQL with automatic age distribution analysis.

## Features

- **Custom CSV Parser**: Built from scratch without external CSV parsing libraries
- **Streaming Architecture**: Memory-efficient processing using Node.js streams and readline
- **Batch Processing**: Processes records in configurable batches (default 1000 records)
- **Large File Support**: Available to handle 50,000+ records without memory issues
- **Nested Property Support**: Handles complex properties with dot notation (e.g., `name.firstName`, `address.line1`)
- **PostgreSQL Integration**: Stores data with proper schema mapping and connection pooling
- **Age Distribution Analysis**: Automatically calculates and displays age group statistics
- **RESTful API**: Express-based API with pagination support
- **Production Ready**: Error handling, logging, and graceful shutdown


## Prerequisites

- Node.js (v14 or higher)
- PostgreSQL (v12 or higher)
- npm or yarn


## Usage

### Start the application

```bash
npm start
```

For development with auto-reload:
```bash
npm run dev
```

The application will:
1. Initialize the database schema
2. Parse the CSV file
3. Insert records into PostgreSQL
4. Display age distribution report
5. Start the API server

### API Endpoints

#### Get all users (with pagination)
```http
GET http://localhost:3000/api/users
GET http://localhost:3000/api/users?limit=100&offset=0
```

#### Get user count
```http
GET http://localhost:3000/api/users/count
```

#### Process CSV file
```http
POST http://localhost:3000/api/process
```

#### Get age distribution
```http
GET http://localhost:3000/api/distribution
```

## CSV File Format

The first line must contain headers using dot notation for nested properties:

```csv
name.firstName,name.lastName,age,address.line1,address.line2,address.city,address.state,gender
Rohit,Prasad,35,A-563 Rakshak Society,New Pune Road,Pune,Maharashtra,male
```

### Mandatory Fields
- `name.firstName`
- `name.lastName`
- `age`

### Database Mapping

| CSV Field | Database Column | Type |
|-----------|----------------|------|
| name.firstName + name.lastName | name | VARCHAR |
| age | age | INT |
| address.* | address | JSONB |
| Other fields | additional_info | JSONB |

## Age Distribution Report

The application automatically generates a report like:

```
==================================================
AGE DISTRIBUTION REPORT
==================================================
Total Users: 20
--------------------------------------------------
Age-Group           % Distribution
--------------------------------------------------
< 20                10.00
20 to 40            45.00
40 to 60            25.00
> 60                20.00
==================================================
```

## Technical Implementation

### Custom CSV Parser with Streaming
- **Memory Efficient**: Uses Node.js readline and streams to process files line-by-line
- **Batch Processing**: Configurable batch size (default 1000 records) for optimal performance
- **Handles Large Files**: Can process 50,000+ records without loading entire file into memory
- **Pause/Resume**: Pauses stream while processing batches to prevent memory overflow
- Handles quoted values and commas within fields
- Supports nested properties with unlimited depth (a.b.c.d...z)
- Converts numeric strings to numbers automatically
- Groups related properties together

### Performance Optimization
```
Small files (<10,000 records):  parse()        - Load entire file
Large files (50,000+ records):  parseStream()  - Stream with batching
```

### Database Schema
```sql
CREATE TABLE public.users (
  id SERIAL PRIMARY KEY,
  name VARCHAR NOT NULL,
  age INT NOT NULL,
  address JSONB NULL,
  additional_info JSONB NULL
);
```

### Performance Considerations
- Uses PostgreSQL connection pooling for concurrent requests
- Batch processing (1000 records per batch by default)
- Streaming for memory-efficient large file handling
- Transaction management for data consistency
- Pagination support for API responses
- Available for files with 50,000+ records

## Error Handling

The application includes comprehensive error handling:
- CSV parsing errors (malformed data, missing columns)
- Database connection errors
- Transaction rollback on failures
- Detailed error logging

## Development

### Running in Development Mode
```bash
npm run dev
```

### Testing with Different CSV Files
1. Update `CSV_FILE_PATH` in `.env`
2. Restart the application or call `/api/process`

## Assumptions

1. The first line of the CSV file always contains column headers
2. Mandatory fields (name.firstName, name.lastName, age) are always present
3. All sub-properties of a complex property are placed consecutively
4. Numeric values in the age column are valid integers
5. The database connection details are configured correctly
6. The CSV file is accessible at the specified path

## Troubleshooting

### Database Connection Issues
- Verify PostgreSQL is running
- Check database credentials in `.env`
- Ensure database exists

### CSV Parsing Errors
- Verify CSV format matches the expected structure
- Check for proper comma separation
- Ensure mandatory fields are present

### Port Already in Use
- Change `PORT` in `.env` file
- Kill the process using the port: `lsof -ti:3000 | xargs kill`

## License

ISC

## Author

Created as part of a coding challenge for Kelp Global
