const express = require('express');
const path = require('path');
require('dotenv').config();

const { initializeDatabase, clearUsersTable } = require('./config/database');
const CSVParser = require('./utils/csvParser');
const userService = require('./services/userService');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

/**
 * Process CSV file and load data into database
 * Uses streaming for handling large files (50000+ records)
 */
async function processCsvFile() {
  try {
    const csvFilePath = process.env.CSV_FILE_PATH || './data/users.csv';
    const batchSize = parseInt(process.env.BATCH_SIZE || '1000');
    
    console.log(`\nStarting CSV processing from: ${csvFilePath}`);
    console.log(`Batch size: ${batchSize} records`);
    console.log('-'.repeat(50));
    
    // Clear existing data
    await clearUsersTable();
    
    // Parse the CSV file using streaming for large files
    const parser = new CSVParser(csvFilePath);
    
    let totalInserted = 0;
    
    // Process records in batches
    await parser.parseStream(batchSize, async (batch) => {
      const insertedCount = await userService.insertUserBatch(batch);
      totalInserted += insertedCount;
    });
    
    console.log(`\nData upload completed: ${totalInserted} total records inserted`);
    
    // Calculate and display age distribution
    await userService.calculateAgeDistribution();
    
  } catch (error) {
    console.error('Error processing CSV file:', error.message);
    throw error;
  }
}

// Routes
app.get('/', (req, res) => {
  res.json({
    message: 'CSV to JSON Converter API',
    version: '1.0.0',
    endpoints: {
      '/api/users': 'GET - Get all users (supports pagination with ?limit=100&offset=0)',
      '/api/users/count': 'GET - Get total user count',
      '/api/process': 'POST - Process CSV file and load into database',
      '/api/distribution': 'GET - Get age distribution report'
    },
    note: 'Available for large CSV files (50000+ records) using streaming'
  });
});

/**
 * Get all users with optional pagination
 */
app.get('/api/users', async (req, res) => {
  try {
    const limit = req.query.limit ? parseInt(req.query.limit) : null;
    const offset = req.query.offset ? parseInt(req.query.offset) : 0;
    
    const users = await userService.getAllUsers(limit, offset);
    const totalCount = await userService.getUserCount();
    
    res.json({
      success: true,
      count: users.length,
      total: totalCount,
      data: users,
      pagination: limit ? {
        limit,
        offset,
        hasMore: (offset + users.length) < totalCount
      } : null
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error fetching users',
      error: error.message
    });
  }
});

/**
 * Get total user count
 */
app.get('/api/users/count', async (req, res) => {
  try {
    const count = await userService.getUserCount();
    
    res.json({
      success: true,
      count: count
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error counting users',
      error: error.message
    });
  }
});

/**
 * Process CSV file endpoint
 */
app.post('/api/process', async (req, res) => {
  try {
    // Start processing in background and send immediate response
    processCsvFile().catch(error => {
      console.error('Background CSV processing failed:', error);
    });
    
    res.json({
      success: true,
      message: 'CSV file processing started. Check console for progress.'
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error starting CSV processing',
      error: error.message
    });
  }
});

/**
 * Get age distribution
 */
app.get('/api/distribution', async (req, res) => {
  try {
    const distribution = await userService.calculateAgeDistribution();
    
    res.json({
      success: true,
      data: distribution
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error calculating distribution',
      error: error.message
    });
  }
});

/**
 * Health check endpoint
 */
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString()
  });
});

/**
 * Initialize and start the application
 */
async function startApplication() {
  try {
    console.log('Initializing application...');
    
    // Initialize database
    await initializeDatabase();
    
    // Check if CSV file exists
    const fs = require('fs');
    const csvFilePath = process.env.CSV_FILE_PATH || './data/users.csv';
    
    if (fs.existsSync(csvFilePath)) {
      console.log(`CSV file found at: ${csvFilePath}`);
      
      // Process CSV file on startup
      await processCsvFile();
    } else {
      console.warn(`Warning: CSV file not found at ${csvFilePath}`);
      console.log('You can trigger processing later using POST /api/process');
    }
    
    // Start the server
    app.listen(PORT, () => {
      console.log(`\n${'='.repeat(50)}`);
      console.log(`Server is running on port ${PORT}`);
      console.log(`API available at http://localhost:${PORT}`);
      console.log(`${'='.repeat(50)}\n`);
    });
    
  } catch (error) {
    console.error('Failed to start application:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nShutting down gracefully...');
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\nShutting down gracefully...');
  process.exit(0);
});

// Start the application
startApplication();