const fs = require('fs');
const readline = require('readline');

/**
 * Custom CSV parser that converts CSV data to JSON objects
 * Handles nested properties using dot notation (e.g., name.firstName)
 * Optimized for large files using streaming
 */
class CSVParser {
  constructor(filePath) {
    this.filePath = filePath;
  }

  /**
   * Parse a CSV line considering quoted values and commas
   */
  parseLine(line) {
    const values = [];
    let current = '';
    let insideQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      
      if (char === '"') {
        insideQuotes = !insideQuotes;
      } else if (char === ',' && !insideQuotes) {
        values.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    
    // Don't forget the last value
    values.push(current.trim());
    return values;
  }

  /**
   * Convert dot notation to nested object
   * e.g., "name.firstName" with value "John" becomes { name: { firstName: "John" } }
   */
  createNestedObject(key, value) {
    const keys = key.split('.');
    const result = {};
    
    let current = result;
    for (let i = 0; i < keys.length - 1; i++) {
      const k = keys[i].trim();
      current[k] = current[k] || {};
      current = current[k];
    }
    
    const lastKey = keys[keys.length - 1].trim();
    current[lastKey] = value;
    
    return result;
  }

  /**
   * Deep merge two objects
   */
  mergeDeep(target, source) {
    for (const key in source) {
      if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
        if (!target[key]) {
          target[key] = {};
        }
        this.mergeDeep(target[key], source[key]);
      } else {
        target[key] = source[key];
      }
    }
    return target;
  }

  /**
   * Stream-based parsing for handling large files (50000+ records)
   * Processes records in batches and calls callback function
   */
  async parseStream(batchSize = 1000, onBatch) {
    return new Promise((resolve, reject) => {
      const fileStream = fs.createReadStream(this.filePath);
      const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      let headers = null;
      let batch = [];
      let lineNumber = 0;
      let totalRecords = 0;

      rl.on('line', async (line) => {
        lineNumber++;
        
        // Skip empty lines
        if (!line.trim()) {
          return;
        }

        // First line is headers
        if (lineNumber === 1) {
          headers = this.parseLine(line);
          return;
        }

        try {
          const values = this.parseLine(line);
          
          // Skip if line doesn't have enough values
          if (values.length !== headers.length) {
            console.warn(`Warning: Skipping line ${lineNumber} - column count mismatch`);
            return;
          }

          let record = {};
          
          // Map each header to its corresponding value
          for (let j = 0; j < headers.length; j++) {
            const header = headers[j].trim();
            let value = values[j].trim();
            
            // Try to convert numeric strings to numbers
            if (!isNaN(value) && value !== '') {
              value = Number(value);
            }
            
            const nestedObj = this.createNestedObject(header, value);
            record = this.mergeDeep(record, nestedObj);
          }
          
          batch.push(record);
          totalRecords++;

          // Process batch when it reaches the specified size
          if (batch.length >= batchSize) {
            rl.pause(); // Pause reading while processing
            
            try {
              await onBatch(batch);
              console.log(`Processed batch: ${totalRecords} records so far...`);
              batch = []; // Clear the batch
            } catch (error) {
              rl.close();
              reject(error);
              return;
            }
            
            rl.resume(); // Resume reading
          }
        } catch (error) {
          console.error(`Error parsing line ${lineNumber}:`, error.message);
        }
      });

      rl.on('error', (error) => {
        reject(error);
      });

      rl.on('close', async () => {
        // Process remaining records in the last batch
        if (batch.length > 0) {
          try {
            await onBatch(batch);
            console.log(`Processed final batch: ${totalRecords} total records`);
          } catch (error) {
            reject(error);
            return;
          }
        }
        
        console.log(`Successfully parsed ${totalRecords} records from CSV`);
        resolve(totalRecords);
      });
    });
  }

  /**
   * Main parsing function (loads all records in memory - use for small files)
   * For large files, use parseStream() instead
   */
  async parse() {
    try {
      const fileContent = fs.readFileSync(this.filePath, 'utf-8');
      const lines = fileContent.split('\n').filter(line => line.trim() !== '');
      
      if (lines.length === 0) {
        throw new Error('CSV file is empty');
      }

      // First line contains headers
      const headers = this.parseLine(lines[0]);
      const records = [];

      // Process each data row
      for (let i = 1; i < lines.length; i++) {
        const values = this.parseLine(lines[i]);
        
        // Skip if line doesn't have enough values
        if (values.length !== headers.length) {
          console.warn(`Warning: Skipping line ${i + 1} - column count mismatch`);
          continue;
        }

        let record = {};
        
        // Map each header to its corresponding value
        for (let j = 0; j < headers.length; j++) {
          const header = headers[j].trim();
          let value = values[j].trim();
          
          // Try to convert numeric strings to numbers
          if (!isNaN(value) && value !== '') {
            value = Number(value);
          }
          
          const nestedObj = this.createNestedObject(header, value);
          record = this.mergeDeep(record, nestedObj);
        }
        
        records.push(record);
      }

      console.log(`Successfully parsed ${records.length} records from CSV`);
      return records;
      
    } catch (error) {
      console.error('Error parsing CSV file:', error.message);
      throw error;
    }
  }
}

module.exports = CSVParser;