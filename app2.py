# app.py
import os
import re
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
import queue
import json

# Import database module
import database as db

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Global state for current job
current_job = {
    'id': None,
    'is_running': False,
    'processed': 0,
    'total': 0,
    'successful': 0,
    'failed': 0,
    'current_url': '',
    'progress': 0,
    'speed': 0
}

# Thread-local storage for WebDriver
thread_local = threading.local()

# Configure patterns for data detection
EMAIL_PATTERN = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
PHONE_PATTERNS = [
    r'(?:\+92|0092|92)?[-\s]?(3\d{2})[-\s]?(\d{7})',
    r'(0?3\d{2})[-\s]?(\d{7})',
    r'(0?3\d{2})\s?(\d{3}\s?\d{4})',
    r'(\(0?3\d{2}\))\s?(\d{7})',
    r'(?:\+971|00971|971)?[-\s]?(\d)[-\s]?(\d{3})[-\s]?(\d{4})',
]

COMPILED_PATTERNS = {
    'email': re.compile(EMAIL_PATTERN, re.IGNORECASE),
    'phone': [re.compile(pattern) for pattern in PHONE_PATTERNS],
    'website': re.compile(r'(https?://[^\s]+|www\.[^\s]+)'),
    'likes': re.compile(r'(\d+[,.]?\d*)\s*(likes|people\s+like\s+this)', re.IGNORECASE),
    'followers': re.compile(r'(\d+[,.]?\d*)\s*(followers|people\s+follow\s+this)', re.IGNORECASE)
}

def get_driver():
    """Get or create a WebDriver instance"""
    if not hasattr(thread_local, 'driver'):
        chrome_options = Options()
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
        
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(f'user-agent={random.choice(user_agents)}')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
            })
            thread_local.driver = driver
        except Exception as e:
            print(f"Error creating driver: {e}")
            return None
    
    return thread_local.driver

def close_driver():
    """Close WebDriver instance"""
    if hasattr(thread_local, 'driver'):
        thread_local.driver.quit()
        del thread_local.driver

def extract_using_patterns(text, pattern_type):
    """Extract data using pre-compiled regex patterns"""
    if not text:
        return ""
    
    if pattern_type == 'email':
        match = COMPILED_PATTERNS['email'].search(text)
        return match.group(0) if match else ""
    
    elif pattern_type == 'phone':
        for pattern in COMPILED_PATTERNS['phone']:
            match = pattern.search(text)
            if match:
                phone = ''.join([g for g in match.groups() if g])
                return normalize_phone(phone)
        return ""
    
    elif pattern_type == 'website':
        match = COMPILED_PATTERNS['website'].search(text)
        return match.group(0) if match else ""
    
    return ""

def normalize_phone(phone):
    """Normalize phone number"""
    if not phone:
        return ""
    cleaned = re.sub(r'[^\d+]', '', phone)
    return cleaned if len(cleaned) >= 8 else ""

def detect_country(phone):
    """Detect country from phone number"""
    if phone.startswith('92') or phone.startswith('+92'):
        return "Pakistan"
    elif phone.startswith('971') or phone.startswith('+971'):
        return "UAE"
    elif phone.startswith('1') or phone.startswith('+1'):
        return "US/Canada"
    return "Unknown"

def scrape_facebook_page(url):
    """Scrape a single Facebook page"""
    driver = get_driver()
    if not driver:
        return None
    
    data = {
        'name': '', 'email': '', 'phone': '', 'country': '',
        'page_link': url, 'website': '', 'location': '',
        'address': '', 'likes': 0, 'followers': 0
    }
    
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        page_text = driver.find_element(By.TAG_NAME, 'body').text
        
        # Extract name
        try:
            name_elem = driver.find_element(By.XPATH, "//h1 | //title")
            data['name'] = name_elem.text.strip()[:100]
        except:
            data['name'] = url.split('/')[-1]
        
        # Extract contact info
        data['email'] = extract_using_patterns(page_text, 'email')
        phone_num = extract_using_patterns(page_text, 'phone')
        data['phone'] = phone_num
        data['country'] = detect_country(phone_num) if phone_num else ""
        data['website'] = extract_using_patterns(page_text, 'website')
        
        # Extract social stats
        likes_match = COMPILED_PATTERNS['likes'].search(page_text)
        followers_match = COMPILED_PATTERNS['followers'].search(page_text)
        
        if likes_match:
            likes_str = likes_match.group(1).replace(',', '').replace('.', '')
            data['likes'] = int(likes_str) if likes_str.isdigit() else 0
        if followers_match:
            followers_str = followers_match.group(1).replace(',', '').replace('.', '')
            data['followers'] = int(followers_str) if followers_str.isdigit() else 0
        
        # Try to get location/address
        try:
            location_elem = driver.find_elements(By.XPATH, 
                "//div[contains(text(), 'Location') or contains(text(), 'Address')]/following-sibling::div")
            if location_elem:
                data['address'] = location_elem[0].text[:200]
        except:
            pass
        
        return data
        
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None
    finally:
        driver.get("about:blank")

def extract_links_from_html(html_content, base_url='https://www.facebook.com'):
    """Extract Facebook links from HTML content"""
    soup = BeautifulSoup(html_content, 'html.parser')
    links = set()
    
    # Look for Facebook links in various formats
    facebook_patterns = [
        r'https?://(www\.)?facebook\.com/[A-Za-z0-9_.-]+/?',
        r'https?://(www\.)?facebook\.com/pages/[^/]+/\d+',
        r'https?://(www\.)?facebook\.com/profile\.php\?id=\d+',
        r'https?://(www\.)?facebook\.com/groups/[A-Za-z0-9_.-]+'
    ]
    
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        for pattern in facebook_patterns:
            if re.match(pattern, href):
                if base_url and not href.startswith('http'):
                    href = urljoin(base_url, href)
                links.add(href)
                break
    
    return list(links)

def update_job_status(status_update):
    """Update current job status and emit via SocketIO"""
    global current_job
    current_job.update(status_update)
    
    # Also update in database if job ID exists
    if current_job['id']:
        db.update_job_status(
            current_job['id'],
            processed_urls=current_job['processed'],
            successful_urls=current_job['successful'],
            failed_urls=current_job['failed']
        )
    
    socketio.emit('status_update', current_job)

def scraping_worker(links, html_content=None, job_name="Scraping Job"):
    """Main scraping worker function"""
    global current_job
    
    all_links = []
    
    # If HTML content is provided, extract links from it
    if html_content:
        extracted_links = extract_links_from_html(html_content)
        all_links.extend(extracted_links)
    
    # Add any direct links
    all_links.extend(links)
    
    if not all_links:
        update_job_status({'is_running': False})
        return
    
    # Create job in database
    job_id = db.create_job(job_name, len(all_links))
    current_job['id'] = job_id
    
    # Update initial status
    update_job_status({
        'is_running': True,
        'total': len(all_links),
        'processed': 0,
        'successful': 0,
        'failed': 0,
        'progress': 0
    })
    
    # Process links with threading
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for link in all_links:
            futures.append(executor.submit(process_single_url, link, job_id))
        
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    # Save to database
                    data_id = db.save_scraped_data(job_id, result)
                    if data_id:
                        result['id'] = data_id
                        socketio.emit('new_data', result)
                        
                        # Update status
                        current_job['successful'] += 1
                    else:
                        current_job['failed'] += 1
                else:
                    current_job['failed'] += 1
                
                current_job['processed'] += 1
                
                # Calculate progress
                progress = (current_job['processed'] / current_job['total']) * 100
                current_job['progress'] = round(progress, 1)
                
                # Calculate speed (items per minute)
                if current_job['processed'] > 0:
                    elapsed_time = time.time() - start_time
                    current_job['speed'] = round((current_job['processed'] / elapsed_time) * 60, 1)
                
                # Emit status update
                update_job_status({})
                
            except Exception as e:
                print(f"Error in future: {e}")
                current_job['failed'] += 1
                current_job['processed'] += 1
                update_job_status({})
    
    # Mark job as completed
    db.update_job_status(
        job_id,
        status='completed',
        completed_at=datetime.now().isoformat()
    )
    
    # Final cleanup
    update_job_status({'is_running': False})
    close_driver()

def process_single_url(url, job_id):
    """Process a single URL and return data"""
    update_job_status({'current_url': url})
    
    start_time = time.time()
    data = scrape_facebook_page(url)
    elapsed = time.time() - start_time
    
    if data:
        data['scrape_time'] = round(elapsed, 2)
        return data
    
    # Save failed attempt to database
    db.save_scraped_data(job_id, {
        'page_link': url,
        'name': '',
        'status': 'failed',
        'error_message': 'Failed to scrape page'
    })
    
    return None

# Flask Routes
@app.route('/')
def index():
    """Render main page"""
    return render_template('index2.html')

@app.route('/api/scrape', methods=['POST'])
def start_scraping():
    """Start scraping process"""
    global current_job
    
    if current_job['is_running']:
        return jsonify({'error': 'Scraping already in progress'}), 400
    
    data = request.json
    links = data.get('links', [])
    html_content = data.get('html_content', '')
    job_name = data.get('job_name', 'Scraping Job')
    
    # Start scraping in background thread
    thread = threading.Thread(
        target=scraping_worker, 
        args=(links, html_content, job_name)
    )
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'message': 'Scraping started', 
        'status': 'processing',
        'job_id': current_job.get('id')
    })

@app.route('/api/stop', methods=['POST'])
def stop_scraping():
    """Stop scraping process"""
    global current_job
    
    if current_job['id']:
        db.update_job_status(
            current_job['id'],
            status='stopped',
            completed_at=datetime.now().isoformat(),
            error_message='Stopped by user'
        )
    
    update_job_status({'is_running': False})
    return jsonify({'message': 'Scraping stopped'})

@app.route('/api/status')
def get_status():
    """Get current scraping status"""
    return jsonify(current_job)

@app.route('/api/jobs')
def get_jobs():
    """Get all scraping jobs"""
    try:
        # This would require adding a function to database.py to get all jobs
        # For now, return current job info
        stats = db.get_job_stats()
        return jsonify({
            'current_job': current_job,
            'stats': stats
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data')
def get_data():
    """Get scraped data"""
    try:
        job_id = request.args.get('job_id', type=int)
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        data = db.get_scraped_data(job_id, limit, offset)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export')
def export_data():
    """Export data as Excel file"""
    try:
        job_id = request.args.get('job_id', type=int)
        filename = db.export_to_excel()
        
        return send_file(
            filename,
            as_attachment=True,
            download_name=f'facebook_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/clear', methods=['POST'])
def clear_data():
    """Clear all scraped data"""
    try:
        db.clear_all_data()
        global current_job
        current_job = {
            'id': None,
            'is_running': False,
            'processed': 0,
            'total': 0,
            'successful': 0,
            'failed': 0,
            'current_url': '',
            'progress': 0,
            'speed': 0
        }
        return jsonify({'message': 'Data cleared successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/upload_html', methods=['POST'])
def upload_html():
    """Upload and process HTML file"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if file and file.filename.endswith('.html'):
        html_content = file.read().decode('utf-8')
        links = extract_links_from_html(html_content)
        
        return jsonify({
            'message': 'File processed successfully',
            'links_found': len(links),
            'links': links[]  # Return first 50 links
        })
    
    return jsonify({'error': 'Invalid file format'}), 400

@app.route('/api/stats')
def get_stats():
    """Get overall statistics"""
    try:
        stats = db.get_job_stats()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# SocketIO Events
@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print('Client connected')
    emit('status_update', current_job)

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')

if __name__ == '__main__':
    print("Starting Facebook Page Scraper UI...")
    print("Database initialized: facebook_scraper.db")
    print("Open your browser and go to: http://localhost:5000")

    socketio.run(app, debug=True, port=5000)
