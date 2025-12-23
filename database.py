# database.py
import sqlite3
import os
from datetime import datetime

DB_NAME = 'facebook_scraper.db'

def init_database():
    """Initialize the database with required tables"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Create scraping_jobs table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS scraping_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_name TEXT,
        status TEXT DEFAULT 'pending',
        total_urls INTEGER DEFAULT 0,
        processed_urls INTEGER DEFAULT 0,
        successful_urls INTEGER DEFAULT 0,
        failed_urls INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP,
        error_message TEXT
    )
    ''')
    
    # Create scraped_data table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS scraped_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        name TEXT,
        email TEXT,
        phone TEXT,
        country TEXT,
        page_link TEXT UNIQUE,
        website TEXT,
        location TEXT,
        address TEXT,
        likes INTEGER,
        followers INTEGER,
        scrape_time REAL,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status TEXT DEFAULT 'success',
        error_message TEXT,
        FOREIGN KEY (job_id) REFERENCES scraping_jobs (id)
    )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_page_link ON scraped_data(page_link)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_job_id ON scraped_data(job_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_scraped_at ON scraped_data(scraped_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON scraped_data(status)')
    
    conn.commit()
    conn.close()
    
    print(f"Database initialized: {DB_NAME}")

def get_connection():
    """Get a database connection"""
    return sqlite3.connect(DB_NAME)

def save_scraped_data(job_id, data):
    """Save scraped data to database"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
        INSERT OR REPLACE INTO scraped_data 
        (job_id, name, email, phone, country, page_link, website, location, address, likes, followers, scrape_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            job_id,
            data.get('name', ''),
            data.get('email', ''),
            data.get('phone', ''),
            data.get('country', ''),
            data.get('page_link', ''),
            data.get('website', ''),
            data.get('location', ''),
            data.get('address', ''),
            data.get('likes', 0) if data.get('likes') else 0,
            data.get('followers', 0) if data.get('followers') else 0,
            data.get('scrape_time', 0.0)
        ))
        
        conn.commit()
        return cursor.lastrowid
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        conn.close()

def create_job(job_name="Default Job", total_urls=0):
    """Create a new scraping job"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO scraping_jobs (job_name, total_urls, status)
    VALUES (?, ?, 'running')
    ''', (job_name, total_urls))
    
    job_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    return job_id

def update_job_status(job_id, **kwargs):
    """Update job status and statistics"""
    conn = get_connection()
    cursor = conn.cursor()
    
    update_fields = []
    values = []
    
    for key, value in kwargs.items():
        update_fields.append(f"{key} = ?")
        values.append(value)
    
    if update_fields:
        values.append(job_id)
        query = f'''
        UPDATE scraping_jobs 
        SET {', '.join(update_fields)}
        WHERE id = ?
        '''
        cursor.execute(query, values)
        conn.commit()
    
    conn.close()

def get_job_status(job_id):
    """Get job status"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM scraping_jobs WHERE id = ?', (job_id,))
    job = cursor.fetchone()
    
    conn.close()
    return job

def get_scraped_data(job_id=None, limit=100, offset=0):
    """Get scraped data with optional filtering"""
    conn = get_connection()
    cursor = conn.cursor()
    
    if job_id:
        cursor.execute('''
        SELECT * FROM scraped_data 
        WHERE job_id = ? 
        ORDER BY scraped_at DESC 
        LIMIT ? OFFSET ?
        ''', (job_id, limit, offset))
    else:
        cursor.execute("""
        SELECT * FROM scraped_data 
        ORDER BY scraped_at DESC 
        LIMIT ? OFFSET ?
        """, (limit, offset))
    
    data = cursor.fetchall()
    
    # Get column names
    cursor.execute('PRAGMA table_info(scraped_data)')
    columns = [col[1] for col in cursor.fetchall()]
    
    conn.close()
    
    # Convert to list of dictionaries
    result = []
    for row in data:
        result.append(dict(zip(columns, row)))
    
    return result

def get_job_stats():
    """Get overall job statistics"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT 
        COUNT(*) as total_jobs,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_jobs,
        SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) as running_jobs,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_jobs,
        SUM(total_urls) as total_urls_scraped
    FROM scraping_jobs
    ''')
    
    stats = cursor.fetchone()
    conn.close()
    
    return {
        'total_jobs': stats[0] or 0,
        'completed_jobs': stats[1] or 0,
        'running_jobs': stats[2] or 0,
        'failed_jobs': stats[3] or 0,
        'total_urls_scraped': stats[4] or 0
    }

def clear_all_data():
    """Clear all scraped data"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM scraped_data')
    cursor.execute('DELETE FROM scraping_jobs')
    cursor.execute('VACUUM')
    
    conn.commit()
    conn.close()

def export_to_excel(filename='facebook_data_export.xlsx'):
    """Export data to Excel file"""
    import pandas as pd
    from datetime import datetime
    
    conn = get_connection()
    
    # Read all data
    df = pd.read_sql_query('''
    SELECT 
        name, email, phone, country, page_link, website, 
        location, address, likes, followers, scrape_time,
        scraped_at, status
    FROM scraped_data
    ORDER BY scraped_at DESC
    ''', conn)
    
    conn.close()
    
    # Save to Excel
    df.to_excel(filename, index=False)
    return filename

# Initialize database when module is imported
init_database()