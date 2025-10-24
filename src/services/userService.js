const { pool } = require('../config/database');

/**
 * Service for handling user data operations
 */
class UserService {
  /**
   * Insert a batch of parsed CSV records into the database
   * Optimized for handling large datasets with batch processing
   */
  async insertUserBatch(records) {
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      // Prepare batch insert using parameterized queries
      for (const record of records) {
        // Extract mandatory fields
        const firstName = record.name?.firstName || '';
        const lastName = record.name?.lastName || '';
        const fullName = `${firstName} ${lastName}`.trim();
        const age = record.age || 0;
        
        // Extract address if it exists
        const address = record.address || null;
        
        // Collect additional info (everything except name, age, and address)
        const additionalInfo = {};
        for (const key in record) {
          if (key !== 'name' && key !== 'age' && key !== 'address') {
            additionalInfo[key] = record[key];
          }
        }
        
        // Insert into database
        await client.query(
          `INSERT INTO public.users (name, age, address, additional_info) 
           VALUES ($1, $2, $3, $4)`,
          [
            fullName,
            age,
            address ? JSON.stringify(address) : null,
            Object.keys(additionalInfo).length > 0 ? JSON.stringify(additionalInfo) : null
          ]
        );
      }
      
      await client.query('COMMIT');
      
      return records.length;
      
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('Error inserting user batch:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Insert parsed CSV records into the database (legacy method for small files)
   * For large files, use insertUserBatch with streaming
   */
  async insertUsers(records) {
    return this.insertUserBatch(records);
  }

  /**
   * Calculate and display age distribution report
   */
  async calculateAgeDistribution() {
    const client = await pool.connect();
    
    try {
      // Get all users
      const result = await client.query('SELECT age FROM public.users');
      const users = result.rows;
      const totalUsers = users.length;
      
      if (totalUsers === 0) {
        console.log('No users found in database');
        return;
      }
      
      // Count users in each age group
      let lessThan20 = 0;
      let between20And40 = 0;
      let between40And60 = 0;
      let above60 = 0;
      
      users.forEach(user => {
        const age = user.age;
        
        if (age < 20) {
          lessThan20++;
        } else if (age >= 20 && age < 40) {
          between20And40++;
        } else if (age >= 40 && age < 60) {
          between40And60++;
        } else {
          above60++;
        }
      });
      
      // Calculate percentages
      const percentLessThan20 = ((lessThan20 / totalUsers) * 100).toFixed(2);
      const percentBetween20And40 = ((between20And40 / totalUsers) * 100).toFixed(2);
      const percentBetween40And60 = ((between40And60 / totalUsers) * 100).toFixed(2);
      const percentAbove60 = ((above60 / totalUsers) * 100).toFixed(2);
      
      // Print the report
      console.log('\n' + '='.repeat(50));
      console.log('AGE DISTRIBUTION REPORT');
      console.log('='.repeat(50));
      console.log(`Total Users: ${totalUsers}`);
      console.log('-'.repeat(50));
      console.log('Age-Group'.padEnd(20) + '% Distribution');
      console.log('-'.repeat(50));
      console.log(`< 20`.padEnd(20) + percentLessThan20);
      console.log(`20 to 40`.padEnd(20) + percentBetween20And40);
      console.log(`40 to 60`.padEnd(20) + percentBetween40And60);
      console.log(`> 60`.padEnd(20) + percentAbove60);
      console.log('='.repeat(50) + '\n');
      
      return {
        total: totalUsers,
        distribution: {
          lessThan20: percentLessThan20,
          between20And40: percentBetween20And40,
          between40And60: percentBetween40And60,
          above60: percentAbove60
        }
      };
      
    } catch (error) {
      console.error('Error calculating age distribution:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Get all users from database
   * For large datasets, consider adding pagination
   */
  async getAllUsers(limit = null, offset = 0) {
    const client = await pool.connect();
    
    try {
      let query = 'SELECT * FROM public.users ORDER BY id';
      const params = [];
      
      // Add pagination if limit is specified
      if (limit) {
        query += ' LIMIT $1 OFFSET $2';
        params.push(limit, offset);
      }
      
      const result = await client.query(query, params);
      return result.rows;
    } catch (error) {
      console.error('Error fetching users:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Get total count of users
   */
  async getUserCount() {
    const client = await pool.connect();
    
    try {
      const result = await client.query('SELECT COUNT(*) as count FROM public.users');
      return parseInt(result.rows[0].count);
    } catch (error) {
      console.error('Error counting users:', error);
      throw error;
    } finally {
      client.release();
    }
  }
}

module.exports = new UserService();