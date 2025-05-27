# app.py
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from flask_jwt_extended import JWTManager, jwt_required, create_access_token, get_jwt_identity
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg2
import psycopg2.extras
import uuid
import os
import json
import secrets
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import base64
from threading import Thread
import functools
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import logging
from flask import redirect

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
)

# Initialize Flask app
app = Flask(__name__)

# Configure CORS to allow cross-origin requests
CORS(app, 
     resources={r"/*": {"origins": "*"}},
     supports_credentials=True,
     allow_headers=["Content-Type", "Authorization", "X-Requested-With", "Accept", "Origin"],
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
     expose_headers=["Content-Type", "Authorization"])

# Email and SMTP Configuration - better to use environment variables in production
SMTP_SERVER = os.environ.get('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.environ.get('SMTP_PORT', '587'))
SMTP_USERNAME = os.environ.get('SMTP_USERNAME', 'sugil.s@vdartinc.com')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', 'elka vboz rmvq lucw')

# Base URL for your application - used for tracking links
BASE_URL = os.environ.get('BASE_URL', 'http://localhost:5000/')

# Generate a secret key for JWT tokens
generated_key = secrets.token_hex(32)

# Database Configuration - better to use environment variables in production
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME', 'email_app')
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASS = os.environ.get('DB_PASS', 'Admin@123')

# JWT Configuration
app.config['JWT_SECRET_KEY'] = os.environ.get('JWT_SECRET_KEY', generated_key)
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(hours=1)

app.logger.info(f"Using JWT secret key: {app.config['JWT_SECRET_KEY'][:8]}...") # Show first 8 chars for debugging

# Initialize the JWT manager
jwt = JWTManager(app)

# Database Connection Functions
def get_db_connection():
    """Get PostgreSQL database connection using Flask's g object for request scoping"""
    if 'db' not in g:
        g.db = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        g.db.autocommit = False
        # Set cursor factory to return dictionaries
        g.cursor = g.db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return g.db, g.cursor

def get_direct_db_connection():
    """Get a direct database connection (not tied to Flask's g)
    Use this for background threads or scheduled tasks"""
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    conn.autocommit = False
    return conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

@app.teardown_appcontext
def close_db_connection(exception):
    """Close database connection at the end of request"""
    db = g.pop('db', None)
    if db is not None:
        g.pop('cursor', None)
        db.close()

# Initialize Database Tables
def init_db():
    """Create database tables if they don't exist"""
    conn, cur = get_db_connection()
    
    # Create users table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        email VARCHAR(255) NOT NULL UNIQUE,
        password_hash VARCHAR(255) NOT NULL,
        full_name VARCHAR(255) NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        is_active BOOLEAN DEFAULT TRUE
    )
    ''')
    
    # Create groups table (NEW)
    cur.execute('''
    CREATE TABLE IF NOT EXISTS groups (
        group_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(user_id),
        name VARCHAR(255) NOT NULL,
        description TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        is_active BOOLEAN DEFAULT TRUE,
        CONSTRAINT unique_group_name_per_user UNIQUE (user_id, name)
    )
    ''')
    
    # Create email_campaigns table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS email_campaigns (
        campaign_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(user_id),
        campaign_name VARCHAR(255) NOT NULL,
        subject_line VARCHAR(255) NOT NULL,
        from_name VARCHAR(255) NOT NULL,
        from_email VARCHAR(255) NOT NULL,
        reply_to_email VARCHAR(255) NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        scheduled_at TIMESTAMP WITH TIME ZONE,
        sent_at TIMESTAMP WITH TIME ZONE,
        status VARCHAR(50) DEFAULT 'draft',
        is_active BOOLEAN DEFAULT TRUE
    )
    ''')
    
    # Create email_templates table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS email_templates (
        template_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(user_id),
        campaign_id UUID REFERENCES email_campaigns(campaign_id),
        template_name VARCHAR(255) NOT NULL,
        html_content TEXT NOT NULL,
        text_content TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        is_active BOOLEAN DEFAULT TRUE
    )
    ''')
    
    # Create recipients table (UPDATED with group_id)
    cur.execute('''
    CREATE TABLE IF NOT EXISTS recipients (
        recipient_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(user_id),
        group_id UUID REFERENCES groups(group_id) ON DELETE SET NULL,
        email VARCHAR(255) NOT NULL,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        company VARCHAR(255),
        position VARCHAR(255),
        custom_fields JSONB,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        is_active BOOLEAN DEFAULT TRUE
    )
    ''')
    
    # Create campaign_recipients table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS campaign_recipients (
        campaign_recipient_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        campaign_id UUID NOT NULL REFERENCES email_campaigns(campaign_id),
        recipient_id UUID NOT NULL REFERENCES recipients(recipient_id),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        is_active BOOLEAN DEFAULT TRUE,
        CONSTRAINT unique_campaign_recipient UNIQUE (campaign_id, recipient_id)
    )
                
    ''')
    # Create campaign_groups table - this stores the relationship between campaigns and groups
    cur.execute('''
    CREATE TABLE IF NOT EXISTS campaign_groups (
        campaign_group_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        campaign_id UUID NOT NULL REFERENCES email_campaigns(campaign_id),
        group_id UUID NOT NULL REFERENCES groups(group_id),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        is_active BOOLEAN DEFAULT TRUE,
        CONSTRAINT unique_campaign_group UNIQUE (campaign_id, group_id)
    )
    ''')
    
    # Create email_tracking table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS email_tracking (
        tracking_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        campaign_id UUID NOT NULL REFERENCES email_campaigns(campaign_id),
        recipient_id UUID NOT NULL REFERENCES recipients(recipient_id),
        email_status VARCHAR(50) DEFAULT 'pending',
        sent_at TIMESTAMP WITH TIME ZONE,
        delivered_at TIMESTAMP WITH TIME ZONE,
        opened_at TIMESTAMP WITH TIME ZONE,
        clicked_at TIMESTAMP WITH TIME ZONE,
        replied_at TIMESTAMP WITH TIME ZONE,
        bounced_at TIMESTAMP WITH TIME ZONE,
        tracking_pixel_id VARCHAR(255) UNIQUE,
        open_count INTEGER DEFAULT 0,
        click_count INTEGER DEFAULT 0,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        is_active BOOLEAN DEFAULT TRUE
    )
    ''')
    
    # Create url_tracking table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS url_tracking (
        url_tracking_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        tracking_id UUID NOT NULL REFERENCES email_tracking(tracking_id),
        original_url TEXT NOT NULL,
        tracking_url TEXT NOT NULL,
        click_count INTEGER DEFAULT 0,
        first_clicked_at TIMESTAMP WITH TIME ZONE,
        last_clicked_at TIMESTAMP WITH TIME ZONE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        is_active BOOLEAN DEFAULT TRUE
    )
    ''')
    
    # Check if group_id column exists in recipients table, if not add it
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name='recipients' AND column_name='group_id'
    """)
    
    
    if not cur.fetchone():
        app.logger.info("Adding group_id column to recipients table...")
        cur.execute("""
            ALTER TABLE recipients 
            ADD COLUMN group_id UUID REFERENCES groups(group_id) ON DELETE SET NULL
        """)
    
    conn.commit()
    app.logger.info("Database tables initialized")

# Initialize database tables on app startup
with app.app_context():
    init_db()

# Helper Functions
def to_dict(row):
    """Convert a database row to a dictionary"""
    if row is None:
        return None
    return dict(row)

def to_list(rows):
    """Convert database rows to a list of dictionaries"""
    return [dict(row) for row in rows]

def handle_transaction(func):
    """Decorator to handle database transactions and rollbacks"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        conn, cur = get_db_connection()
        try:
            result = func(*args, **kwargs)
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Database error: {str(e)}")
            return jsonify({"error": str(e)}), 500
    return wrapper

from bs4 import BeautifulSoup
import uuid

def add_tracking_links(html_content, campaign_id, tracking_id, base_url):
    """Replace all links in HTML content with tracking links"""
    
    
    soup = BeautifulSoup(html_content, 'html.parser')
    conn = None
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Find all links
        for a_tag in soup.find_all('a', href=True):
            original_url = a_tag['href']
            
            # Skip mailto: links
            if original_url.startswith('mailto:'):
                continue
                
            # Create tracking entry
            url_tracking_id = str(uuid.uuid4())
            tracking_url = f"{base_url}track/click/{tracking_id}/{url_tracking_id}"
            
            # Store original and tracking URLs
            cur.execute("""
                INSERT INTO url_tracking
                (tracking_id, original_url, tracking_url)
                VALUES (%s, %s, %s)
                RETURNING url_tracking_id
            """, (tracking_id, original_url, tracking_url))
            
            # Replace the link
            a_tag['href'] = tracking_url
            
        conn.commit()
        return str(soup)
        
    except Exception as e:
        app.logger.error(f"Error adding tracking links: {str(e)}")
        if conn:
            conn.rollback()
        return html_content  # Return original content on error
    finally:
        if conn:
            conn.close()

def rewrite_links(html_content, tracking_id, base_url):
    """Replace all links in HTML content with tracking links"""
    from bs4 import BeautifulSoup
    import uuid
    
    app.logger.info(f"🔗 Rewriting links for tracking_id: {tracking_id}")
    soup = BeautifulSoup(html_content, 'html.parser')
    conn = None
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        link_count = 0
        # Find all links
        for a_tag in soup.find_all('a', href=True):
            original_url = a_tag['href']
            
            # Skip mailto: links, anchors, and javascript: links
            if original_url.startswith('mailto:') or original_url.startswith('#') or original_url.startswith('javascript:'):
                continue
                
            # Create a unique ID for this link
            url_tracking_id = str(uuid.uuid4())
            
            # Create tracking URL
            tracking_url = f"{base_url}track/click/{tracking_id}/{url_tracking_id}"
            
            # Insert into url_tracking table
            cur.execute("""
                INSERT INTO url_tracking
                (tracking_id, original_url, tracking_url, click_count)
                VALUES (%s, %s, %s, 0)
                RETURNING url_tracking_id
            """, (tracking_id, original_url, tracking_url))
            
            # Get the inserted ID
            db_url_id = cur.fetchone()['url_tracking_id']
            
            # Update tracking URL with actual ID from database
            tracking_url = f"{base_url}track/click/{tracking_id}/{db_url_id}"
            
            # Replace the href attribute
            a_tag['href'] = tracking_url
            link_count += 1
            
        # Commit all the URL tracking entries
        conn.commit()
        app.logger.info(f"✅ Rewrote {link_count} links for tracking_id: {tracking_id}")
        
        # Add JavaScript beacon tracking as a backup for image blocking
        # This only works if the email client allows JavaScript
        js_beacon = soup.new_tag('script')
        js_beacon.string = f"""
            (function() {{
                try {{
                    setTimeout(function() {{
                        var img = new Image();
                        img.onload = function() {{ /* loaded */ }};
                        img.onerror = function() {{ /* error */ }};
                        img.src = '{base_url}track/beacon/{tracking_id}?t=' + new Date().getTime();
                    }}, 1000);
                }} catch(e) {{
                    // Silently fail if JS is blocked
                }}
            }})();
        """
        
        # Add the beacon script to the body
        if soup.body:
            soup.body.append(js_beacon)
        
        # Return the modified HTML
        return str(soup)
        
    except Exception as e:
        app.logger.error(f"❌ Error rewriting links: {str(e)}")
        if conn:
            conn.rollback()
        # If there's an error, return the original HTML
        return html_content
    finally:
        if conn:
            conn.close()

# Email Sending Functions
# def send_email_async(campaign_id, test_mode=False, base_url=None):
    """Asynchronously send emails for a campaign"""
    # Create a new app context for the thread
    with app.app_context():
        # Use a direct connection instead of Flask's g since this runs in a background thread
        conn = None
        try:
            # Get public URL for tracking
            if not base_url:
                base_url = os.environ.get('BASE_URL', 'http://localhost:5000/')
                if not base_url.endswith('/'):
                    base_url += '/'
            
            app.logger.info(f"📧 Starting email sending for campaign {campaign_id}, test_mode={test_mode}, base_url={base_url}")
            
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            conn.autocommit = False
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Get campaign details
            cur.execute("""
                SELECT * FROM email_campaigns WHERE campaign_id = %s
            """, (campaign_id,))
            campaign = cur.fetchone()
            
            if not campaign:
                app.logger.error(f"❌ Campaign {campaign_id} not found")
                return
            
            # Get template for campaign
            cur.execute("""
                SELECT * FROM email_templates 
                WHERE campaign_id = %s AND is_active = TRUE
                LIMIT 1
            """, (campaign_id,))
            template = cur.fetchone()
            
            if not template:
                app.logger.error(f"❌ No active template found for campaign {campaign_id}")
                return
            
            if test_mode:
                # Send only to the campaign owner for testing
                cur.execute("""
                    SELECT * FROM users WHERE user_id = %s
                """, (campaign['user_id'],))
                user = cur.fetchone()
                recipients = [{'email': user['email'], 'recipient_id': None}]
                app.logger.info(f"📧 Test mode: Sending to campaign owner {user['email']}")
            else:
                # Get all direct recipients for this campaign
                cur.execute("""
                    SELECT r.* FROM recipients r
                    JOIN campaign_recipients cr ON r.recipient_id = cr.recipient_id
                    WHERE cr.campaign_id = %s AND cr.is_active = TRUE AND r.is_active = TRUE
                """, (campaign_id,))
                direct_recipients = cur.fetchall()
                
                # Get recipients from groups
                cur.execute("""
                    SELECT r.* FROM recipients r
                    JOIN groups g ON r.group_id = g.group_id
                    JOIN campaign_groups cg ON g.group_id = cg.group_id
                    WHERE cg.campaign_id = %s AND cg.is_active = TRUE AND g.is_active = TRUE AND r.is_active = TRUE
                    AND r.recipient_id NOT IN (
                        SELECT cr.recipient_id FROM campaign_recipients cr 
                        WHERE cr.campaign_id = %s AND cr.is_active = TRUE
                    )
                """, (campaign_id, campaign_id))
                group_recipients = cur.fetchall()
                
                # Combine both sets of recipients, avoiding duplicates
                recipients = list(direct_recipients)
                
                # Add group recipients that aren't already direct recipients
                recipient_ids = set(r['recipient_id'] for r in recipients)
                for recipient in group_recipients:
                    if recipient['recipient_id'] not in recipient_ids:
                        recipients.append(recipient)
                        recipient_ids.add(recipient['recipient_id'])
                
                app.logger.info(f"📧 Sending campaign to {len(recipients)} recipients ({len(direct_recipients)} direct, {len(group_recipients)} from groups)")
            
            # Email configuration
            smtp_server = SMTP_SERVER
            smtp_port = SMTP_PORT
            smtp_username = SMTP_USERNAME
            smtp_password = SMTP_PASSWORD
            
            # Initialize SMTP server connection
            app.logger.info(f"🔌 Connecting to SMTP server {smtp_server}:{smtp_port}")
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.ehlo()
            server.starttls()
            server.login(smtp_username, smtp_password)
            app.logger.info("✅ SMTP connection established")
            
            # Track send counts
            success_count = 0
            failure_count = 0
            
            for recipient in recipients:
                try:
                    # Create unique tracking pixel for this email
                    tracking_pixel_id = str(uuid.uuid4())
                    app.logger.debug(f"🔍 Created tracking pixel ID: {tracking_pixel_id} for {recipient['email']}")
                    
                    # Create tracking entry
                    if not test_mode:
                        cur.execute("""
                            INSERT INTO email_tracking 
                            (campaign_id, recipient_id, tracking_pixel_id, email_status)
                            VALUES (%s, %s, %s, 'sending')
                            RETURNING tracking_id
                        """, (campaign_id, recipient['recipient_id'], tracking_pixel_id))
                        tracking = cur.fetchone()
                        conn.commit()
                        app.logger.debug(f"✅ Created tracking entry: {tracking['tracking_id']}")
                    else:
                        # For test mode, create a temporary tracking ID
                        tracking = {'tracking_id': str(uuid.uuid4())}
                    
                    # Personalize email content
                    html_content = template['html_content']
                    text_content = template.get('text_content', '')
                    
                    if not test_mode and recipient.get('first_name'):
                        html_content = html_content.replace('{{first_name}}', recipient['first_name'])
                        text_content = text_content.replace('{{first_name}}', recipient['first_name'])
                        if recipient.get('last_name'):
                            html_content = html_content.replace('{{last_name}}', recipient['last_name'])
                            text_content = text_content.replace('{{last_name}}', recipient['last_name'])
                    
                    # Add tracking pixel
                    tracking_pixel_url = f"{base_url}track/open/{tracking_pixel_id}"
                    tracking_pixel = f'<img src="{tracking_pixel_url}" width="1" height="1" alt="" style="display:none" />'
                    
                    # First rewrite links for click tracking
                    if not test_mode:
                        html_content = rewrite_links(html_content, tracking['tracking_id'], base_url)
                    
                    # Add the tracking pixel at the very end
                    html_content = html_content + tracking_pixel
                    
                    # Create email message
                    msg = MIMEMultipart('alternative')
                    msg['Subject'] = campaign['subject_line']
                    msg['From'] = f"{campaign['from_name']} <{campaign['from_email']}>"
                    msg['To'] = recipient['email']
                    msg['Reply-To'] = campaign['reply_to_email']
                    # Add important headers for better deliverability
                    msg['List-Unsubscribe'] = f"<mailto:{campaign['reply_to_email']}?subject=Unsubscribe>"
                    
                    # Add text and HTML parts
                    if text_content:
                        part1 = MIMEText(text_content, 'plain')
                        msg.attach(part1)
                    
                    part2 = MIMEText(html_content, 'html')
                    msg.attach(part2)
                    
                    # Send the email
                    server.send_message(msg)
                    success_count += 1
                    
                    # Log that message was sent
                    app.logger.info(f"✅ Email sent to {recipient['email']}")
                    
                    # Update tracking status
                    if not test_mode:
                        cur.execute("""
                            UPDATE email_tracking
                            SET email_status = 'sent', sent_at = NOW(), updated_at = NOW()
                            WHERE tracking_id = %s
                        """, (tracking['tracking_id'],))
                        conn.commit()
                
                except Exception as e:
                    app.logger.error(f"❌ Error sending email to {recipient['email']}: {str(e)}")
                    failure_count += 1
                    if not test_mode:
                        try:
                            cur.execute("""
                                UPDATE email_tracking
                                SET email_status = 'failed', updated_at = NOW()
                                WHERE tracking_id = %s
                            """, (tracking['tracking_id'],))
                            conn.commit()
                        except Exception as ex:
                            app.logger.error(f"❌ Error updating tracking status: {str(ex)}")
                            conn.rollback()
            
            # Close the SMTP connection
            server.quit()
            app.logger.info(f"✅ SMTP connection closed, sent {success_count} emails, {failure_count} failures")
            
            # Update campaign status
            if not test_mode:
                cur.execute("""
                    UPDATE email_campaigns
                    SET status = 'completed', sent_at = NOW()
                    WHERE campaign_id = %s
                """, (campaign_id,))
                conn.commit()
                app.logger.info(f"✅ Campaign {campaign_id} marked as completed")
                
        except Exception as e:
            app.logger.error(f"❌ Error in send_email_async: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
# Find this function in app.py and replace it with this updated version
def send_email_async(campaign_id, test_mode=False, base_url=None):
    """Asynchronously send emails for a campaign with improved tracking"""
    # Create a new app context for the thread
    with app.app_context():
        # Use a direct connection instead of Flask's g since this runs in a background thread
        conn = None
        try:
            # Get public URL for tracking
            if not base_url:
                base_url = os.environ.get('BASE_URL', 'http://localhost:5000/')
                if not base_url.endswith('/'):
                    base_url += '/'
            
            app.logger.info(f"📧 Starting email sending for campaign {campaign_id}, test_mode={test_mode}, base_url={base_url}")
            
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            conn.autocommit = False
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Get campaign details
            cur.execute("""
                SELECT * FROM email_campaigns WHERE campaign_id = %s
            """, (campaign_id,))
            campaign = cur.fetchone()
            
            if not campaign:
                app.logger.error(f"❌ Campaign {campaign_id} not found")
                return
            
            # Get template for campaign
            cur.execute("""
                SELECT * FROM email_templates 
                WHERE campaign_id = %s AND is_active = TRUE
                LIMIT 1
            """, (campaign_id,))
            template = cur.fetchone()
            
            if not template:
                app.logger.error(f"❌ No active template found for campaign {campaign_id}")
                return
            
            if test_mode:
                # Send only to the campaign owner for testing
                cur.execute("""
                    SELECT * FROM users WHERE user_id = %s
                """, (campaign['user_id'],))
                user = cur.fetchone()
                recipients = [{'email': user['email'], 'recipient_id': None}]
                app.logger.info(f"📧 Test mode: Sending to campaign owner {user['email']}")
            else:
                # Get all direct recipients for this campaign
                cur.execute("""
                    SELECT r.* FROM recipients r
                    JOIN campaign_recipients cr ON r.recipient_id = cr.recipient_id
                    WHERE cr.campaign_id = %s AND cr.is_active = TRUE AND r.is_active = TRUE
                """, (campaign_id,))
                direct_recipients = cur.fetchall()
                
                # Get recipients from groups
                cur.execute("""
                    SELECT r.* FROM recipients r
                    JOIN groups g ON r.group_id = g.group_id
                    JOIN campaign_groups cg ON g.group_id = cg.group_id
                    WHERE cg.campaign_id = %s AND cg.is_active = TRUE AND g.is_active = TRUE AND r.is_active = TRUE
                    AND r.recipient_id NOT IN (
                        SELECT cr.recipient_id FROM campaign_recipients cr 
                        WHERE cr.campaign_id = %s AND cr.is_active = TRUE
                    )
                """, (campaign_id, campaign_id))
                group_recipients = cur.fetchall()
                
                # Combine both sets of recipients, avoiding duplicates
                recipients = list(direct_recipients)
                
                # Add group recipients that aren't already direct recipients
                recipient_ids = set(r['recipient_id'] for r in recipients)
                for recipient in group_recipients:
                    if recipient['recipient_id'] not in recipient_ids:
                        recipients.append(recipient)
                        recipient_ids.add(recipient['recipient_id'])
                
                app.logger.info(f"📧 Sending campaign to {len(recipients)} recipients ({len(direct_recipients)} direct, {len(group_recipients)} from groups)")
            
            # Email configuration
            smtp_server = SMTP_SERVER
            smtp_port = SMTP_PORT
            smtp_username = SMTP_USERNAME
            smtp_password = SMTP_PASSWORD
            
            # Initialize SMTP server connection
            app.logger.info(f"🔌 Connecting to SMTP server {smtp_server}:{smtp_port}")
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.ehlo()
            server.starttls()
            server.login(smtp_username, smtp_password)
            app.logger.info("✅ SMTP connection established")
            
            # Track send counts
            success_count = 0
            failure_count = 0
            
            # Import our helper function (will be defined above)
            from bs4 import BeautifulSoup
            
            for recipient in recipients:
                try:
                    # Create unique tracking pixel for this email
                    tracking_pixel_id = str(uuid.uuid4())
                    app.logger.debug(f"🔍 Created tracking pixel ID: {tracking_pixel_id} for {recipient['email']}")
                    
                    # Create tracking entry
                    if not test_mode:
                        cur.execute("""
                            INSERT INTO email_tracking 
                            (campaign_id, recipient_id, tracking_pixel_id, email_status)
                            VALUES (%s, %s, %s, 'sending')
                            RETURNING tracking_id
                        """, (campaign_id, recipient['recipient_id'], tracking_pixel_id))
                        tracking = cur.fetchone()
                        conn.commit()
                        app.logger.debug(f"✅ Created tracking entry: {tracking['tracking_id']}")
                    else:
                        # For test mode, create a temporary tracking ID
                        tracking = {'tracking_id': str(uuid.uuid4())}
                    
                    # Personalize email content
                    html_content = template['html_content']
                    text_content = template.get('text_content', '')
                    
                    if not test_mode and recipient.get('first_name'):
                        html_content = html_content.replace('{{first_name}}', recipient['first_name'])
                        text_content = text_content.replace('{{first_name}}', recipient['first_name'])
                        if recipient.get('last_name'):
                            html_content = html_content.replace('{{last_name}}', recipient['last_name'])
                            text_content = text_content.replace('{{last_name}}', recipient['last_name'])
                    
                    # First rewrite links for click tracking
                    if not test_mode:
                        html_content = rewrite_links(html_content, tracking['tracking_id'], base_url)
                    
                    # Add multiple tracking mechanisms using our new function
                    # The function adds tracking pixels throughout the email for redundancy
                    html_content = add_tracking_elements(html_content, tracking_pixel_id, tracking['tracking_id'], base_url)
                    
                    # Create email message
                    msg = MIMEMultipart('alternative')
                    msg['Subject'] = campaign['subject_line']
                    msg['From'] = f"{campaign['from_name']} <{campaign['from_email']}>"
                    msg['To'] = recipient['email']
                    msg['Reply-To'] = campaign['reply_to_email']
                    # Add important headers for better deliverability
                    msg['List-Unsubscribe'] = f"<mailto:{campaign['reply_to_email']}?subject=Unsubscribe>"
                    
                    # Add text and HTML parts
                    if text_content:
                        part1 = MIMEText(text_content, 'plain')
                        msg.attach(part1)
                    
                    part2 = MIMEText(html_content, 'html')
                    msg.attach(part2)
                    
                    # Send the email
                    server.send_message(msg)
                    success_count += 1
                    
                    # Log that message was sent
                    app.logger.info(f"✅ Email sent to {recipient['email']}")
                    
                    # Update tracking status
                    if not test_mode:
                        cur.execute("""
                            UPDATE email_tracking
                            SET email_status = 'sent', sent_at = NOW(), updated_at = NOW()
                            WHERE tracking_id = %s
                        """, (tracking['tracking_id'],))
                        conn.commit()
                
                except Exception as e:
                    app.logger.error(f"❌ Error sending email to {recipient['email']}: {str(e)}")
                    failure_count += 1
                    if not test_mode:
                        try:
                            cur.execute("""
                                UPDATE email_tracking
                                SET email_status = 'failed', updated_at = NOW()
                                WHERE tracking_id = %s
                            """, (tracking['tracking_id'],))
                            conn.commit()
                        except Exception as ex:
                            app.logger.error(f"❌ Error updating tracking status: {str(ex)}")
                            conn.rollback()
            
            # Close the SMTP connection
            server.quit()
            app.logger.info(f"✅ SMTP connection closed, sent {success_count} emails, {failure_count} failures")
            
            # Update campaign status
            if not test_mode:
                cur.execute("""
                    UPDATE email_campaigns
                    SET status = 'completed', sent_at = NOW()
                    WHERE campaign_id = %s
                """, (campaign_id,))
                conn.commit()
                app.logger.info(f"✅ Campaign {campaign_id} marked as completed")
                
        except Exception as e:
            app.logger.error(f"❌ Error in send_email_async: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()


#Open Tracking function code 
def add_tracking_elements(html_content, tracking_pixel_id, tracking_id, base_url):
    """
    Properly add tracking pixel and beacon to HTML email content
    to reliably track opens even without clicks.
    
    Parameters:
    - html_content: The original HTML content
    - tracking_pixel_id: Unique ID for the tracking pixel
    - tracking_id: ID of the tracking entry in the database
    - base_url: Base URL for the application
    
    Returns:
    - Modified HTML content with tracking elements
    """
    from bs4 import BeautifulSoup
    import logging
    
    # Parse HTML content
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Create tracking pixel
    tracking_pixel_url = f"{base_url}track/open/{tracking_pixel_id}"
    tracking_pixel = soup.new_tag('img', 
                                 src=tracking_pixel_url,
                                 width="1", 
                                 height="1", 
                                 alt="",
                                 style="display:none")
    
    # Create JavaScript beacon as backup
    js_beacon = soup.new_tag('script')
    js_beacon.string = f"""
        (function() {{
            try {{
                // Try to send tracking beacon immediately
                var img = new Image();
                img.src = '{base_url}track/beacon/{tracking_pixel_id}?t=' + new Date().getTime();
                
                // Also try again after a delay to catch delayed loads
                setTimeout(function() {{
                    var img2 = new Image();
                    img2.src = '{base_url}track/beacon/{tracking_pixel_id}?t=' + new Date().getTime() + '&d=1';
                }}, 2000);
            }} catch(e) {{
                // Silently fail if JS is blocked
            }}
        }})();
    """
    
    # Create multiple tracking mechanisms for redundancy
    
    # 1. Add to body in multiple locations if it exists
    if soup.body:
        try:
            # Add at beginning of body in a hidden div
            hidden_div = soup.new_tag('div', style="display:none !important; max-height:0px; overflow:hidden;")
            hidden_div.append(tracking_pixel.copy())
            soup.body.insert(0, hidden_div)
            
            # Add in the middle of content for better chance of loading
            if len(soup.body.contents) > 2:
                middle_index = len(soup.body.contents) // 2
                middle_div = soup.new_tag('div', style="display:none !important;")
                middle_div.append(tracking_pixel.copy())
                soup.body.insert(middle_index, middle_div)
                
            # Add at end of body
            soup.body.append(tracking_pixel.copy())
            soup.body.append(js_beacon)
        except Exception as e:
            logging.error(f"Error adding tracking to body: {str(e)}")
    
    # 2. Add to HTML head if it exists
    if soup.head:
        try:
            # Add a prefetch link to trigger loading
            prefetch = soup.new_tag('link', 
                                   rel="prefetch", 
                                   href=tracking_pixel_url)
            soup.head.append(prefetch)
            
            # Add a style with background image for tracking
            style_tag = soup.new_tag('style')
            style_tag.string = f"""
                body::before {{
                    content: '';
                    background-image: url('{tracking_pixel_url}?s=css');
                    display: none;
                }}
            """
            soup.head.append(style_tag)
        except Exception as e:
            logging.error(f"Error adding tracking to head: {str(e)}")
    
    # 3. If no body or head, add to the root
    if not soup.body and not soup.head:
        try:
            if len(soup.contents) > 0:
                soup.contents[-1].append(tracking_pixel.copy())
            else:
                soup.append(tracking_pixel.copy())
        except Exception as e:
            logging.error(f"Error adding tracking to root: {str(e)}")
    
    # 4. Also add tracking pixel at the very end of the HTML outside any tags
    # This ensures it's included even if email clients restructure the HTML
    html_with_tracking = str(soup)
    
    # Add an extra tracking pixel at the very end as a last resort
    html_with_tracking += f'<!--[if !mso]><!-- --><div style="display:none;max-height:0px;overflow:hidden;"><img src="{tracking_pixel_url}?pos=end" width="1" height="1" alt="" style="display:none" /></div><!--<![endif]-->'
    
    return html_with_tracking



# Email Reply Checking Function
def check_for_replies():
    """Check for email replies and update tracking data"""
    import imaplib
    import email
    from email.header import decode_header
    import time
    
    app.logger.info("Starting scheduled email reply check")
    
    # Email configuration
    username = SMTP_USERNAME
    password = SMTP_PASSWORD
    imap_server = "imap.gmail.com"  # Use your IMAP server
    
    conn = None
    mail = None
    try:
        # Connect to IMAP server
        app.logger.info(f"Connecting to IMAP server: {imap_server}")
        mail = imaplib.IMAP4_SSL(imap_server)
        mail.login(username, password)
        mail.select("INBOX")
        app.logger.info("Successfully connected to IMAP server")
        
        # Search for recent emails (last 24 hours)
        date = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y")
        status, messages = mail.search(None, f'(SINCE {date})')
        message_ids = messages[0].split(b' ')
        app.logger.info(f"Found {len(message_ids)} messages in the last 24 hours")
        
        if not message_ids or message_ids[0] == b'':
            app.logger.info("No messages found to check")
            return
        
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get all campaign subjects for matching
        cur.execute("""
            SELECT campaign_id, subject_line FROM email_campaigns
            WHERE status = 'completed'
        """)
        campaigns = cur.fetchall()
        campaign_subjects = {c['subject_line']: c['campaign_id'] for c in campaigns}
        app.logger.info(f"Loaded {len(campaign_subjects)} campaign subjects for matching")
        
        replies_found = 0
        for mail_id in message_ids:
            try:
                app.logger.debug(f"Checking message ID: {mail_id}")
                status, msg_data = mail.fetch(mail_id, "(RFC822)")
                
                for response in msg_data:
                    if isinstance(response, tuple):
                        msg = email.message_from_bytes(response[1])
                        subject = decode_header(msg["Subject"])[0][0]
                        sender = msg.get("From", "")
                        
                        # Check if it's a reply (subject starts with Re:)
                        if isinstance(subject, bytes):
                            subject = subject.decode()
                        
                        app.logger.debug(f"Processing email - Subject: {subject}, From: {sender}")
                        
                        if subject and subject.lower().startswith("re:"):
                            # Extract original subject by removing "Re: "
                            original_subject = subject[4:].strip()
                            app.logger.info(f"Found reply email - Original subject: {original_subject}")
                            
                            # Check if this matches any of our campaigns
                            campaign_id = None
                            for camp_subject, camp_id in campaign_subjects.items():
                                if original_subject.lower() == camp_subject.lower():
                                    campaign_id = camp_id
                                    break
                            
                            if campaign_id:
                                app.logger.info(f"Matched reply to campaign ID: {campaign_id}")
                                
                                # Find the recipient email from the sender
                                sender_email = None
                                if '<' in sender and '>' in sender:
                                    # Extract email from format "Name <email@example.com>"
                                    sender_email = sender.split('<')[1].split('>')[0].strip()
                                else:
                                    # Just use the whole sender field
                                    sender_email = sender.strip()
                                
                                app.logger.info(f"Extracted sender email: {sender_email}")
                                
                                # Find the recipient in our database
                                cur.execute("""
                                    SELECT r.recipient_id, r.email
                                    FROM recipients r
                                    JOIN campaign_recipients cr ON r.recipient_id = cr.recipient_id
                                    WHERE cr.campaign_id = %s AND r.email = %s
                                """, (campaign_id, sender_email))
                                
                                recipient = cur.fetchone()
                                
                                if recipient:
                                    app.logger.info(f"Found matching recipient: {recipient['email']}")
                                    
                                    # Check if already replied
                                    cur.execute("""
                                        SELECT tracking_id FROM email_tracking
                                        WHERE campaign_id = %s AND recipient_id = %s AND replied_at IS NOT NULL
                                    """, (campaign_id, recipient['recipient_id']))
                                    
                                    already_replied = cur.fetchone()
                                    
                                    if already_replied:
                                        app.logger.info(f"Recipient {recipient['email']} already marked as replied")
                                    else:
                                        # Update tracking status
                                        cur.execute("""
                                            UPDATE email_tracking
                                            SET 
                                                email_status = 'replied',
                                                replied_at = NOW(),
                                                updated_at = NOW()
                                            WHERE campaign_id = %s AND recipient_id = %s
                                            RETURNING tracking_id
                                        """, (campaign_id, recipient['recipient_id']))
                                        
                                        updated = cur.fetchone()
                                        
                                        if updated:
                                            conn.commit()
                                            app.logger.info(f"Successfully marked {recipient['email']} as replied, tracking_id: {updated['tracking_id']}")
                                            replies_found += 1
                                        else:
                                            app.logger.warning(f"No tracking entry found for {recipient['email']} in campaign {campaign_id}")
                                else:
                                    app.logger.warning(f"No matching recipient found for email: {sender_email}")
                            else:
                                app.logger.debug(f"No matching campaign found for subject: {original_subject}")
            except Exception as e:
                app.logger.error(f"Error processing email {mail_id}: {str(e)}")
                # Continue to next email
        
        app.logger.info(f"Reply check complete. Found and processed {replies_found} new replies.")
    
    except Exception as e:
        app.logger.error(f"Error in check_for_replies: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if mail:
            try:
                mail.close()
                mail.logout()
            except:
                pass
        if conn:
            conn.close()

# Safe wrapper for check_for_replies to use with scheduler
def safe_check_for_replies():
    """Safely run the reply check with error handling for the scheduler"""
    try:
        check_for_replies()
    except Exception as e:
        app.logger.error(f"Error in scheduled reply check: {str(e)}")

# Initialize scheduler for checking replies - runs every 10 minutes
scheduler = BackgroundScheduler()
scheduler.add_job(func=safe_check_for_replies, trigger="interval", minutes=1)
scheduler.start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

# API Routes - Authentication
@app.route('/api/register', methods=['POST'])
@handle_transaction
def register():
    """Register a new user"""
    data = request.get_json()
    
    # Validate required fields
    required_fields = ['email', 'password', 'full_name']
    for field in required_fields:
        if field not in data or not data[field]:
            return jsonify({'message': f'Missing required field: {field}'}), 422
    
    conn, cur = get_db_connection()
    
    # Check if user exists
    cur.execute("SELECT * FROM users WHERE email = %s", (data['email'],))
    if cur.fetchone():
        return jsonify({'message': 'Email already registered'}), 400
    
    # Create new user
    password_hash = generate_password_hash(data['password'])
    cur.execute("""
        INSERT INTO users (email, password_hash, full_name)
        VALUES (%s, %s, %s)
        RETURNING user_id, email, full_name
    """, (data['email'], password_hash, data['full_name']))
    
    user = cur.fetchone()
    
    access_token = create_access_token(identity=str(user['user_id']))
    
    return jsonify({
        'message': 'User registered successfully',
        'access_token': access_token,
        'user': {
            'user_id': str(user['user_id']),
            'email': user['email'],
            'full_name': user['full_name']
        }
    }), 201

@app.route('/api/login', methods=['POST'])
@handle_transaction
def login():
    """Login user and return JWT token"""
    data = request.get_json()
    
    # Validate required fields
    required_fields = ['email', 'password']
    for field in required_fields:
        if field not in data or not data[field]:
            return jsonify({'message': f'Missing required field: {field}'}), 422
    
    conn, cur = get_db_connection()
    
    # Find user by email
    cur.execute("SELECT * FROM users WHERE email = %s", (data['email'],))
    user = cur.fetchone()
    
    # Verify credentials
    if not user or not check_password_hash(user['password_hash'], data['password']):
        return jsonify({'message': 'Invalid credentials'}), 401
    
    # Create JWT token
    access_token = create_access_token(identity=str(user['user_id']))
    
    return jsonify({
        'message': 'Login successful',
        'access_token': access_token,
        'user': {
            'user_id': str(user['user_id']),
            'email': user['email'],
            'full_name': user['full_name']
        }
    }), 200

# API Routes - Groups (NEW)
@app.route('/api/groups', methods=['GET'])
@jwt_required()
@handle_transaction
def get_groups():
    """Get all groups for current user"""
    user_id = get_jwt_identity()
    conn, cur = get_db_connection()
    
    cur.execute("""
        SELECT g.*, 
               COUNT(r.recipient_id) as recipient_count
        FROM groups g
        LEFT JOIN recipients r ON g.group_id = r.group_id AND r.is_active = TRUE
        WHERE g.user_id = %s AND g.is_active = TRUE
        GROUP BY g.group_id
        ORDER BY g.name ASC
    """, (user_id,))
    
    groups = cur.fetchall()
    
    result = []
    for group in groups:
        group_data = dict(group)
        group_data['group_id'] = str(group_data['group_id'])
        group_data['user_id'] = str(group_data['user_id'])
        # Note: In JavaScript we expect 'id' not 'group_id' for consistency
        group_data['id'] = group_data['group_id']
        
        # Format dates
        for key in ['created_at', 'updated_at']:
            if key in group_data and group_data[key]:
                group_data[key] = group_data[key].isoformat()
        
        result.append(group_data)
    
    return jsonify(result), 200

@app.route('/api/groups', methods=['POST'])
@jwt_required()
@handle_transaction
def create_group():
    """Create a new group"""
    user_id = get_jwt_identity()
    data = request.get_json()
    
    # Validate required fields
    if 'name' not in data or not data['name']:
        return jsonify({'message': 'Group name is required'}), 422
    
    conn, cur = get_db_connection()
    
    # Check if group with same name already exists for this user
    cur.execute("""
        SELECT * FROM groups
        WHERE user_id = %s AND name = %s AND is_active = TRUE
    """, (user_id, data['name']))
    
    if cur.fetchone():
        return jsonify({'message': 'A group with this name already exists'}), 400
    
    # Create new group
    cur.execute("""
        INSERT INTO groups (user_id, name, description)
        VALUES (%s, %s, %s)
        RETURNING group_id
    """, (
        user_id,
        data['name'],
        data.get('description', '')
    ))
    
    group = cur.fetchone()
    
    return jsonify({
        'message': 'Group created successfully',
        'group_id': str(group['group_id'])
    }), 201

@app.route('/api/groups/<group_id>', methods=['GET'])
@jwt_required()
@handle_transaction
def get_group(group_id):
    """Get a single group by ID"""
    user_id = get_jwt_identity()
    conn, cur = get_db_connection()
    
    # Get group details with recipient count
    cur.execute("""
        SELECT g.*, 
               COUNT(r.recipient_id) as recipient_count
        FROM groups g
        LEFT JOIN recipients r ON g.group_id = r.group_id AND r.is_active = TRUE
        WHERE g.group_id = %s AND g.user_id = %s AND g.is_active = TRUE
        GROUP BY g.group_id
    """, (group_id, user_id))
    
    group = cur.fetchone()
    
    if not group:
        return jsonify({'message': 'Group not found'}), 404
    
    # Get recipients in this group
    cur.execute("""
        SELECT * FROM recipients
        WHERE group_id = %s AND user_id = %s AND is_active = TRUE
        ORDER BY created_at DESC
    """, (group_id, user_id))
    
    recipients = cur.fetchall()
    
    # Format group data
    group_data = dict(group)
    group_data['group_id'] = str(group_data['group_id'])
    group_data['user_id'] = str(group_data['user_id'])
    group_data['id'] = group_data['group_id']
    
    # Format dates
    for key in ['created_at', 'updated_at']:
        if key in group_data and group_data[key]:
            group_data[key] = group_data[key].isoformat()
    
    # Format recipients
    recipient_list = []
    for recipient in recipients:
        recipient_data = dict(recipient)
        recipient_data['recipient_id'] = str(recipient_data['recipient_id'])
        recipient_data['user_id'] = str(recipient_data['user_id'])
        recipient_data['group_id'] = str(recipient_data['group_id']) if recipient_data.get('group_id') else None
        
        # Format dates
        for key in ['created_at', 'updated_at']:
            if key in recipient_data and recipient_data[key]:
                recipient_data[key] = recipient_data[key].isoformat()
        
        recipient_list.append(recipient_data)
    
    group_data['recipients'] = recipient_list
    
    return jsonify(group_data), 200

@app.route('/api/groups/<group_id>/update', methods=['POST'])
@jwt_required()
@handle_transaction
def update_group(group_id):
    """Update a group"""
    user_id = get_jwt_identity()
    data = request.get_json()
    
    # Validate required fields
    if 'name' not in data or not data['name']:
        return jsonify({'message': 'Group name is required'}), 422
    
    conn, cur = get_db_connection()
    
    # Verify group exists and belongs to user
    cur.execute("""
        SELECT * FROM groups
        WHERE group_id = %s AND user_id = %s AND is_active = TRUE
    """, (group_id, user_id))
    
    group = cur.fetchone()
    
    if not group:
        return jsonify({'message': 'Group not found or access denied'}), 404
    
    # Check if another group with same name exists (excluding current group)
    cur.execute("""
        SELECT * FROM groups
        WHERE user_id = %s AND name = %s AND group_id != %s AND is_active = TRUE
    """, (user_id, data['name'], group_id))
    
    if cur.fetchone():
        return jsonify({'message': 'Another group with this name already exists'}), 400
    
    # Update group
    cur.execute("""
        UPDATE groups
        SET 
            name = %s,
            description = %s,
            updated_at = NOW()
        WHERE group_id = %s
        RETURNING group_id
    """, (
        data['name'],
        data.get('description', ''),
        group_id
    ))
    
    updated = cur.fetchone()
    
    return jsonify({
        'message': 'Group updated successfully',
        'group_id': str(updated['group_id'])
    }), 200

@app.route('/api/groups/<group_id>/delete', methods=['POST'])
@jwt_required()
@handle_transaction
def delete_group(group_id):
    """Delete a group"""
    user_id = get_jwt_identity()
    conn, cur = get_db_connection()
    
    # Verify group exists and belongs to user
    cur.execute("""
        SELECT * FROM groups
        WHERE group_id = %s AND user_id = %s AND is_active = TRUE
    """, (group_id, user_id))
    
    group = cur.fetchone()
    
    if not group:
        return jsonify({'message': 'Group not found or access denied'}), 404
    
    # Remove group reference from all recipients (they become ungrouped)
    cur.execute("""
        UPDATE recipients
        SET group_id = NULL, updated_at = NOW()
        WHERE group_id = %s
    """, (group_id,))
    
    # Soft delete the group
    cur.execute("""
        UPDATE groups
        SET is_active = FALSE, updated_at = NOW()
        WHERE group_id = %s
    """, (group_id,))
    
    return jsonify({
        'message': 'Group deleted successfully',
        'group_id': str(group_id)
    }), 200

@app.route('/api/groups/<group_id>/recipients', methods=['POST'])
@jwt_required()
@handle_transaction
def add_recipients_to_group(group_id):
    print("Group Id",group_id)
    """Add recipients to a group"""
    user_id = get_jwt_identity()
    data = request.get_json()
    recipientId = data['recipientIds']
    print(recipientId)
    
    # Validate data
    if not data or 'recipientIds' not in data or not data['recipientIds']:
        return jsonify({'message': 'No recipient IDs provided'}), 400
    
    conn, cur = get_db_connection()
    
    # Verify group exists and belongs to user
    cur.execute("""
        SELECT * FROM groups
        WHERE group_id = %s AND user_id = %s AND is_active = TRUE
    """, (group_id, user_id))
    
    if not cur.fetchone():
        return jsonify({'message': 'Group not found or access denied'}), 404
    
    # Convert recipient_ids to a list if it's a single value
    recipient_ids = data['recipientIds']
    print(recipient_ids)
    if not isinstance(recipient_ids, list):
        recipient_ids = [recipient_ids]
    
    # Update recipients to add them to the group
    updated_count = 0
    for recipient_id in recipient_ids:
        print("recipentId  ->>",recipient_id)
        # Verify recipient belongs to user
        cur.execute("""
            SELECT recipient_id FROM recipients
            WHERE recipient_id = %s AND user_id = %s AND is_active = TRUE
        """, (recipient_id, user_id))
        
        if cur.fetchone():
            cur.execute("""
                UPDATE recipients
                SET group_id = %s, updated_at = NOW()
                WHERE recipient_id = %s
            """, (group_id, recipient_id))
            updated_count += 1
    
    return jsonify({
        'message': f'Added {updated_count} recipients to group',
        'updated_count': updated_count
    }), 200

@app.route('/api/groups/<group_id>/recipients/remove', methods=['POST'])
@jwt_required()
@handle_transaction
def remove_recipients_from_group(group_id):
    """Remove recipients from a group"""
    user_id = get_jwt_identity()
    data = request.get_json()
    
    # Validate data
    if not data or 'recipient_ids' not in data or not data['recipient_ids']:
        return jsonify({'message': 'No recipient IDs provided'}), 400
    
    conn, cur = get_db_connection()
    
    # Verify group exists and belongs to user
    cur.execute("""
        SELECT * FROM groups
        WHERE group_id = %s AND user_id = %s AND is_active = TRUE
    """, (group_id, user_id))
    
    if not cur.fetchone():
        return jsonify({'message': 'Group not found or access denied'}), 404
    
    # Convert recipient_ids to a list if it's a single value
    recipient_ids = data['recipient_ids']
    if not isinstance(recipient_ids, list):
        recipient_ids = [recipient_ids]
    
    # Remove recipients from the group (set group_id to NULL)
    updated_count = 0
    for recipient_id in recipient_ids:
        # Verify recipient belongs to user and is in this group
        cur.execute("""
            SELECT recipient_id FROM recipients
            WHERE recipient_id = %s AND user_id = %s AND group_id = %s AND is_active = TRUE
        """, (recipient_id, user_id, group_id))
        
        if cur.fetchone():
            cur.execute("""
                UPDATE recipients
                SET group_id = NULL, updated_at = NOW()
                WHERE recipient_id = %s
            """, (recipient_id,))
            updated_count += 1
    
    return jsonify({
        'message': f'Removed {updated_count} recipients from group',
        'updated_count': updated_count
    }), 200

# API Routes - Campaigns
@app.route('/api/campaigns', methods=['GET'])
@jwt_required()
@handle_transaction
def get_campaigns():
    """Get all campaigns for current user"""
    user_id = get_jwt_identity()
    conn, cur = get_db_connection()
    
    # Get all campaigns for this user
    cur.execute("""
        SELECT * FROM email_campaigns
        WHERE user_id = %s AND is_active = TRUE
        ORDER BY created_at DESC
    """, (user_id,))
    campaigns = cur.fetchall()
    
    result = []
    for campaign in campaigns:
        # Count recipients
        cur.execute("""
            SELECT COUNT(*) as recipient_count FROM campaign_recipients
            WHERE campaign_id = %s AND is_active = TRUE
        """, (campaign['campaign_id'],))
        recipient_count = cur.fetchone()['recipient_count']
        
        # Get tracking stats
        stats = {
            'sent_count': 0,
            'opened_count': 0,
            'clicked_count': 0,
            'replied_count': 0,
            'open_rate': 0,
            'click_rate': 0,
            'reply_rate': 0
        }
        
        # Only get tracking stats for completed campaigns
        if campaign['status'] == 'completed':
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE sent_at IS NOT NULL) as sent_count,
                    COUNT(*) FILTER (WHERE opened_at IS NOT NULL) as opened_count,
                    COUNT(*) FILTER (WHERE clicked_at IS NOT NULL) as clicked_count,
                    COUNT(*) FILTER (WHERE replied_at IS NOT NULL) as replied_count
                FROM email_tracking
                WHERE campaign_id = %s
            """, (campaign['campaign_id'],))
            tracking = cur.fetchone()
            
            sent_count = tracking['sent_count']
            opened_count = tracking['opened_count']
            clicked_count = tracking['clicked_count']
            replied_count = tracking['replied_count']
            
            stats['sent_count'] = sent_count
            stats['opened_count'] = opened_count
            stats['clicked_count'] = clicked_count
            stats['replied_count'] = replied_count
            
            # Calculate rates
            if sent_count > 0:
                stats['open_rate'] = (opened_count / sent_count) * 100
                stats['click_rate'] = (clicked_count / sent_count) * 100
                stats['reply_rate'] = (replied_count / sent_count) * 100
        
        # Prepare campaign data
        campaign_data = dict(campaign)
        campaign_data['recipient_count'] = recipient_count
        campaign_data['stats'] = stats
        
        # Convert UUIDs to strings for JSON serialization
        campaign_data['campaign_id'] = str(campaign_data['campaign_id'])
        campaign_data['user_id'] = str(campaign_data['user_id'])
        
        # Format dates
        for key in ['created_at', 'scheduled_at', 'sent_at']:
            if key in campaign_data and campaign_data[key]:
                campaign_data[key] = campaign_data[key].isoformat()
        
        result.append(campaign_data)
    
    return jsonify(result), 200


#changed now
@app.route('/api/campaigns', methods=['POST'])
@jwt_required()
@handle_transaction
def create_campaign():
    """Create a new campaign"""
    user_id = get_jwt_identity()
    data = request.get_json()
    
    # Validate required fields
    required_fields = ['campaign_name', 'subject_line', 'from_name', 'from_email', 'reply_to_email']
    for field in required_fields:
        if field not in data or not data[field]:
            return jsonify({'message': f'Missing required field: {field}'}), 422
    
    conn, cur = get_db_connection()
    
    # Create campaign
    cur.execute("""
        INSERT INTO email_campaigns 
        (user_id, campaign_name, subject_line, from_name, from_email, reply_to_email)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING campaign_id
    """, (
        user_id, 
        data['campaign_name'], 
        data['subject_line'], 
        data['from_name'],
        data['from_email'],
        data['reply_to_email']
    ))
    
    campaign = cur.fetchone()
    campaign_id = campaign['campaign_id']
    
    # Create template if provided
    if 'template' in data:
        template_name = data['template'].get('name', 'Default Template')
        html_content = data['template']['html_content']
        text_content = data['template'].get('text_content', '')
        
        cur.execute("""
            INSERT INTO email_templates
            (user_id, campaign_id, template_name, html_content, text_content)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING template_id
        """, (user_id, campaign_id, template_name, html_content, text_content))
    
    # Add individual recipients if provided
    if 'recipients' in data and data['recipients']:
        for recipient_item in data['recipients']:
            # Handle if recipient is a dict or a string ID
            if isinstance(recipient_item, dict) and 'recipient_id' in recipient_item:
                recipient_id = recipient_item['recipient_id']
            else:
                recipient_id = recipient_item
                
            # Verify recipient belongs to user
            cur.execute("""
                SELECT recipient_id FROM recipients
                WHERE recipient_id = %s AND user_id = %s
            """, (recipient_id, user_id))
            
            if cur.fetchone():
                cur.execute("""
                    INSERT INTO campaign_recipients (campaign_id, recipient_id)
                    VALUES (%s, %s)
                    ON CONFLICT (campaign_id, recipient_id) DO NOTHING
                """, (campaign_id, recipient_id))
    
    # Add groups if provided
    if 'groups' in data and data['groups']:
        for group_item in data['groups']:
            # Handle if group is a dict or a string ID
            if isinstance(group_item, dict) and ('id' in group_item or 'group_id' in group_item):
                group_id = group_item.get('id') or group_item.get('group_id')
            else:
                group_id = group_item
                
            # Debug logging to identify the issue
            app.logger.info(f"Processing group: {group_item}, extracted ID: {group_id}")
            
            # Verify group belongs to user
            cur.execute("""
                SELECT group_id FROM groups
                WHERE group_id = %s AND user_id = %s
            """, (group_id, user_id))
            
            if cur.fetchone():
                # Add group to campaign_groups table
                cur.execute("""
                    INSERT INTO campaign_groups (campaign_id, group_id)
                    VALUES (%s, %s)
                    ON CONFLICT (campaign_id, group_id) DO NOTHING
                """, (campaign_id, group_id))
    
    return jsonify({
        'message': 'Campaign created successfully',
        'campaign_id': str(campaign_id)
    }), 201

# @app.route('/api/campaigns/<campaign_id>', methods=['GET'])
# @jwt_required()
# @handle_transaction
# def get_campaign(campaign_id):
#     """Get a single campaign by ID"""
#     user_id = get_jwt_identity()
#     conn, cur = get_db_connection()
    
#     # Get campaign details
#     cur.execute("""
#         SELECT * FROM email_campaigns
#         WHERE campaign_id = %s AND user_id = %s
#     """, (campaign_id, user_id))
    
#     campaign = cur.fetchone()
    
#     if not campaign:
#         return jsonify({'message': 'Campaign not found'}), 404
    
#     # Get template
#     cur.execute("""
#         SELECT * FROM email_templates
#         WHERE campaign_id = %s AND is_active = TRUE
#         LIMIT 1
#     """, (campaign_id,))
#     template = cur.fetchone()
    
#     # Get direct recipients
#     cur.execute("""
#         SELECT r.* FROM recipients r
#         JOIN campaign_recipients cr ON r.recipient_id = cr.recipient_id
#         WHERE cr.campaign_id = %s AND cr.is_active = TRUE AND r.is_active = TRUE
#     """, (campaign_id,))
#     recipients = cur.fetchall()
    
#     # Get groups for this campaign
#     cur.execute("""
#         SELECT g.*, COUNT(r.recipient_id) as recipient_count 
#         FROM groups g
#         JOIN campaign_groups cg ON g.group_id = cg.group_id
#         LEFT JOIN recipients r ON g.group_id = r.group_id AND r.is_active = TRUE
#         WHERE cg.campaign_id = %s AND cg.is_active = TRUE AND g.is_active = TRUE
#         GROUP BY g.group_id
#     """, (campaign_id,))
#     groups = cur.fetchall()
    
#     # Process recipients
#     recipient_list = []
#     for recipient in recipients:
#         recipient_data = dict(recipient)
#         recipient_data['recipient_id'] = str(recipient_data['recipient_id'])
#         recipient_data['user_id'] = str(recipient_data['user_id'])
#         if recipient_data.get('group_id'):
#             recipient_data['group_id'] = str(recipient_data['group_id'])
            
#             # Get group name for this recipient if it has a group
#             cur.execute("""
#                 SELECT name FROM groups WHERE group_id = %s
#             """, (recipient['group_id'],))
#             group_result = cur.fetchone()
#             if group_result:
#                 recipient_data['group_name'] = group_result['name']
        
#         recipient_list.append(recipient_data)
    
#     # Process groups
#     group_list = []
#     for group in groups:
#         group_data = dict(group)
#         group_data['group_id'] = str(group_data['group_id']) 
#         group_data['user_id'] = str(group_data['user_id'])
#         group_data['id'] = group_data['group_id']  # For frontend compatibility
        
#         # Format dates
#         for key in ['created_at', 'updated_at']:
#             if key in group_data and group_data[key]:
#                 group_data[key] = group_data[key].isoformat()
                
#         group_list.append(group_data)
    
#     # Get tracking stats if campaign sent
#     tracking_stats = None
#     if campaign['status'] == 'completed':
#         cur.execute("""
#             SELECT 
#                 COUNT(*) FILTER (WHERE sent_at IS NOT NULL) as sent_count,
#                 COUNT(*) FILTER (WHERE opened_at IS NOT NULL) as opened_count,
#                 COUNT(*) FILTER (WHERE clicked_at IS NOT NULL) as clicked_count,
#                 COUNT(*) FILTER (WHERE replied_at IS NOT NULL) as replied_count
#             FROM email_tracking
#             WHERE campaign_id = %s
#         """, (campaign_id,))
#         overall_stats = cur.fetchone()
        
#         # Get individual recipient tracking
#         cur.execute("""
#             SELECT et.*, r.email, r.first_name, r.last_name 
#             FROM email_tracking et
#             JOIN recipients r ON et.recipient_id = r.recipient_id
#             WHERE et.campaign_id = %s
#         """, (campaign_id,))
#         recipient_tracking = cur.fetchall()
        
#         # Calculate rates
#         sent_count = overall_stats['sent_count']
#         opened_count = overall_stats['opened_count']
#         clicked_count = overall_stats['clicked_count']
#         replied_count = overall_stats['replied_count']
        
#         open_rate = (opened_count / sent_count * 100) if sent_count > 0 else 0
#         click_rate = (clicked_count / sent_count * 100) if sent_count > 0 else 0
#         reply_rate = (replied_count / sent_count * 100) if sent_count > 0 else 0
        
#         # Format recipient tracking data
#         recipient_tracking_list = []
#         for tracking in recipient_tracking:
#             tracking_data = dict(tracking)
#             for key in ['tracking_id', 'campaign_id', 'recipient_id']:
#                 if key in tracking_data and tracking_data[key]:
#                     tracking_data[key] = str(tracking_data[key])
            
#             # Format dates to ISO strings
#             for key in ['sent_at', 'opened_at', 'clicked_at', 'replied_at', 'created_at', 'updated_at']:
#                 if key in tracking_data and tracking_data[key]:
#                     tracking_data[key] = tracking_data[key].isoformat()
            
#             recipient_tracking_list.append(tracking_data)
        
#         tracking_stats = {
#             'overall': {
#                 'sent_count': sent_count,
#                 'opened_count': opened_count,
#                 'clicked_count': clicked_count,
#                 'replied_count': replied_count,
#                 'open_rate': open_rate,
#                 'click_rate': click_rate,
#                 'reply_rate': reply_rate
#             },
#             'recipients': recipient_tracking_list
#         }
    
#     # Prepare template data
#     # Prepare template data
#     template_data = None
#     if template:
#         template_data = dict(template)
#         template_data['template_id'] = str(template_data['template_id'])
#         template_data['user_id'] = str(template_data['user_id'])
#         template_data['campaign_id'] = str(template_data['campaign_id'])
        
#         # Format dates
#         for key in ['created_at', 'updated_at']:
#             if key in template_data and template_data[key]:
#                 template_data[key] = template_data[key].isoformat()
    
#     # Prepare campaign data
#     campaign_data = dict(campaign)
#     campaign_data['campaign_id'] = str(campaign_data['campaign_id'])
#     campaign_data['user_id'] = str(campaign_data['user_id'])
    
#     # Format dates
#     for key in ['created_at', 'scheduled_at', 'sent_at']:
#         if key in campaign_data and campaign_data[key]:
#             campaign_data[key] = campaign_data[key].isoformat()
    
#     # Calculate total recipient count including those from groups
#     total_recipient_count = len(recipient_list)
#     campaign_data['recipient_count'] = total_recipient_count
    
#     result = {
#         **campaign_data,
#         'template': template_data,
#         'recipients': recipient_list,
#         'groups': group_list,
#         'tracking_stats': tracking_stats
#     }
    
#     return jsonify(result), 200

@app.route('/api/campaigns/<campaign_id>', methods=['GET'])
@jwt_required()
@handle_transaction
def get_campaign(campaign_id):
    """Get a single campaign by ID"""
    user_id = get_jwt_identity()
    conn, cur = get_db_connection()
    
    # Get campaign details
    cur.execute("""
        SELECT * FROM email_campaigns
        WHERE campaign_id = %s AND user_id = %s
    """, (campaign_id, user_id))
    
    campaign = cur.fetchone()
    
    if not campaign:
        return jsonify({'message': 'Campaign not found'}), 404
    
    # Get template
    cur.execute("""
        SELECT * FROM email_templates
        WHERE campaign_id = %s AND is_active = TRUE
        LIMIT 1
    """, (campaign_id,))
    template = cur.fetchone()
    
    # Get direct recipients
    cur.execute("""
        SELECT r.*, g.name as group_name
        FROM recipients r
        JOIN campaign_recipients cr ON r.recipient_id = cr.recipient_id
        LEFT JOIN groups g ON r.group_id = g.group_id
        WHERE cr.campaign_id = %s AND cr.is_active = TRUE AND r.is_active = TRUE
    """, (campaign_id,))
    recipients = cur.fetchall()
    
    # Get groups for this campaign with recipient counts
    cur.execute("""
        SELECT g.*, COUNT(r.recipient_id) as recipient_count 
        FROM groups g
        JOIN campaign_groups cg ON g.group_id = cg.group_id
        LEFT JOIN recipients r ON g.group_id = r.group_id AND r.is_active = TRUE
        WHERE cg.campaign_id = %s AND cg.is_active = TRUE AND g.is_active = TRUE
        GROUP BY g.group_id
    """, (campaign_id,))
    groups = cur.fetchall()
    
    # Process recipients
    recipient_list = []
    for recipient in recipients:
        recipient_data = dict(recipient)
        recipient_data['recipient_id'] = str(recipient_data['recipient_id'])
        recipient_data['user_id'] = str(recipient_data['user_id'])
        if recipient_data.get('group_id'):
            recipient_data['group_id'] = str(recipient_data['group_id'])
        
        # Format dates
        for key in ['created_at', 'updated_at']:
            if key in recipient_data and recipient_data[key]:
                recipient_data[key] = recipient_data[key].isoformat()
        
        recipient_list.append(recipient_data)
    
    # Process groups
    group_list = []
    for group in groups:
        group_data = dict(group)
        group_data['group_id'] = str(group_data['group_id'])
        group_data['user_id'] = str(group_data['user_id'])
        group_data['id'] = group_data['group_id']  # For frontend compatibility
        
        # Format dates
        for key in ['created_at', 'updated_at']:
            if key in group_data and group_data[key]:
                group_data[key] = group_data[key].isoformat()
        
        group_list.append(group_data)
    
    # Get tracking stats if campaign sent
    tracking_stats = None
    if campaign['status'] == 'completed':
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE sent_at IS NOT NULL) as sent_count,
                COUNT(*) FILTER (WHERE opened_at IS NOT NULL) as opened_count,
                COUNT(*) FILTER (WHERE clicked_at IS NOT NULL) as clicked_count,
                COUNT(*) FILTER (WHERE replied_at IS NOT NULL) as replied_count
            FROM email_tracking
            WHERE campaign_id = %s
        """, (campaign_id,))
        overall_stats = cur.fetchone()
        
        # Get individual recipient tracking
        cur.execute("""
            SELECT et.*, r.email, r.first_name, r.last_name 
            FROM email_tracking et
            JOIN recipients r ON et.recipient_id = r.recipient_id
            WHERE et.campaign_id = %s
        """, (campaign_id,))
        recipient_tracking = cur.fetchall()
        
        # Calculate rates
        sent_count = overall_stats['sent_count']
        opened_count = overall_stats['opened_count']
        clicked_count = overall_stats['clicked_count']
        replied_count = overall_stats['replied_count']
        
        open_rate = (opened_count / sent_count * 100) if sent_count > 0 else 0
        click_rate = (clicked_count / sent_count * 100) if sent_count > 0 else 0
        reply_rate = (replied_count / sent_count * 100) if sent_count > 0 else 0
        
        # Format recipient tracking data
        recipient_tracking_list = []
        for tracking in recipient_tracking:
            tracking_data = dict(tracking)
            for key in ['tracking_id', 'campaign_id', 'recipient_id']:
                if key in tracking_data and tracking_data[key]:
                    tracking_data[key] = str(tracking_data[key])
            
            # Format dates to ISO strings
            for key in ['sent_at', 'opened_at', 'clicked_at', 'replied_at', 'created_at', 'updated_at']:
                if key in tracking_data and tracking_data[key]:
                    tracking_data[key] = tracking_data[key].isoformat()
            
            recipient_tracking_list.append(tracking_data)
        
        tracking_stats = {
            'overall': {
                'sent_count': sent_count,
                'opened_count': opened_count,
                'clicked_count': clicked_count,
                'replied_count': replied_count,
                'open_rate': open_rate,
                'click_rate': click_rate,
                'reply_rate': reply_rate
            },
            'recipients': recipient_tracking_list
        }
    
    # Prepare template data
    template_data = None
    if template:
        template_data = dict(template)
        template_data['template_id'] = str(template_data['template_id'])
        template_data['user_id'] = str(template_data['user_id'])
        template_data['campaign_id'] = str(template_data['campaign_id'])
        
        # Format dates
        for key in ['created_at', 'updated_at']:
            if key in template_data and template_data[key]:
                template_data[key] = template_data[key].isoformat()
    
    # Prepare campaign data
    campaign_data = dict(campaign)
    campaign_data['campaign_id'] = str(campaign_data['campaign_id'])
    campaign_data['user_id'] = str(campaign_data['user_id'])
    
    # Format dates
    for key in ['created_at', 'scheduled_at', 'sent_at']:
        if key in campaign_data and campaign_data[key]:
            campaign_data[key] = campaign_data[key].isoformat()
    
    # Calculate total recipient count
    # This includes direct recipients and all recipients from the groups
    cur.execute("""
        WITH campaign_direct_recipients AS (
            SELECT COUNT(DISTINCT cr.recipient_id) as count
            FROM campaign_recipients cr
            WHERE cr.campaign_id = %s AND cr.is_active = TRUE
        ),
        campaign_group_recipients AS (
            SELECT COUNT(DISTINCT r.recipient_id) as count
            FROM recipients r
            JOIN groups g ON r.group_id = g.group_id
            JOIN campaign_groups cg ON g.group_id = cg.group_id
            WHERE cg.campaign_id = %s AND cg.is_active = TRUE AND g.is_active = TRUE AND r.is_active = TRUE
            AND r.recipient_id NOT IN (
                SELECT cr.recipient_id FROM campaign_recipients cr 
                WHERE cr.campaign_id = %s AND cr.is_active = TRUE
            )
        )
        SELECT
            (SELECT count FROM campaign_direct_recipients) +
            (SELECT count FROM campaign_group_recipients) as total_count
    """, (campaign_id, campaign_id, campaign_id))
    
    total_count_result = cur.fetchone()
    campaign_data['recipient_count'] = total_count_result['total_count'] if total_count_result else len(recipient_list)
    
    result = {
        **campaign_data,
        'template': template_data,
        'recipients': recipient_list,
        'groups': group_list,
        'tracking_stats': tracking_stats
    }
    
    return jsonify(result), 200

@app.route('/api/campaigns/<campaign_id>/send', methods=['POST'])
@jwt_required()
@handle_transaction
def send_campaign(campaign_id):
    """Send a campaign or test email"""
    user_id = get_jwt_identity()
    data = request.get_json()
    
    # Handle case where no data is sent
    if not data:
        data = {'test_mode': False}
    
    conn, cur = get_db_connection()
    
    # Verify campaign exists and belongs to user
    cur.execute("""
        SELECT * FROM email_campaigns
        WHERE campaign_id = %s AND user_id = %s
    """, (campaign_id, user_id))
    
    campaign = cur.fetchone()
    
    if not campaign:
        return jsonify({'message': 'Campaign not found'}), 404
    
    # Check if campaign can be sent
    if campaign['status'] not in ['draft', 'scheduled']:
        return jsonify({'message': f'Campaign cannot be sent (status: {campaign["status"]})'}), 400
    
    test_mode = data.get('test_mode', False)
    
    # Get base URL from current request
    base_url = request.host_url
    app.logger.info(f"Sending campaign {campaign_id} with base_url: {base_url}, test_mode: {test_mode}")
    
    if test_mode:
        # Send test email without updating campaign status
        Thread(target=send_email_async, args=(campaign_id, True, base_url)).start()
        
        return jsonify({
            'message': 'Test email sending in progress'
        }), 200
    else:
        # Update campaign status
        cur.execute("""
            UPDATE email_campaigns
            SET status = 'sending'
            WHERE campaign_id = %s
        """, (campaign_id,))
        
        # Send emails in background
        Thread(target=send_email_async, args=(campaign_id, False, base_url)).start()
        
        return jsonify({
            'message': 'Campaign sending in progress'
        }), 200

# API Routes - Recipients (UPDATED)
@app.route('/api/recipients', methods=['GET'])
@jwt_required()
@handle_transaction
def get_recipients():
    """Get all recipients for current user"""
    user_id = get_jwt_identity()
    conn, cur = get_db_connection()
    
    cur.execute("""
        SELECT r.*, g.name as group_name
        FROM recipients r
        LEFT JOIN groups g ON r.group_id = g.group_id
        WHERE r.user_id = %s AND r.is_active = TRUE
        ORDER BY r.created_at DESC
    """, (user_id,))
    
    recipients = cur.fetchall()
    
    result = []
    for recipient in recipients:
        recipient_data = dict(recipient)
        recipient_data['recipient_id'] = str(recipient_data['recipient_id'])
        recipient_data['user_id'] = str(recipient_data['user_id'])
        if recipient_data.get('group_id'):
            recipient_data['group_id'] = str(recipient_data['group_id'])
        
        # Format dates
        for key in ['created_at', 'updated_at']:
            if key in recipient_data and recipient_data[key]:
                recipient_data[key] = recipient_data[key].isoformat()
        
        result.append(recipient_data)
    
    return jsonify(result), 200

@app.route('/api/recipients', methods=['POST'])
@jwt_required()
@handle_transaction
def create_recipient():
    """Create a new recipient"""
    user_id = get_jwt_identity()
    data = request.get_json()
    
    # Validate required fields
    if 'email' not in data or not data['email']:
        return jsonify({'message': 'Email is required'}), 422
    
    conn, cur = get_db_connection()
    
    # Check if recipient already exists
    cur.execute("""
        SELECT * FROM recipients
        WHERE user_id = %s AND email = %s AND is_active = TRUE
    """, (user_id, data['email']))
    
    if cur.fetchone():
        return jsonify({'message': 'Recipient with this email already exists'}), 400
    
    # Validate group_id if provided
    group_id = data.get('group_id')
    if group_id:
        cur.execute("""
            SELECT * FROM groups
            WHERE group_id = %s AND user_id = %s AND is_active = TRUE
        """, (group_id, user_id))
        
        if not cur.fetchone():
            return jsonify({'message': 'Invalid group ID'}), 400
    
    # Create new recipient
    custom_fields = json.dumps(data.get('custom_fields', {})) if data.get('custom_fields') else None
    
    cur.execute("""
        INSERT INTO recipients 
        (user_id, email, first_name, last_name, company, position, group_id, custom_fields)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING recipient_id
    """, (
        user_id,
        data['email'],
        data.get('first_name'),
        data.get('last_name'),
        data.get('company'),
        data.get('position'),
        group_id,
        custom_fields
    ))
    
    recipient = cur.fetchone()
    
    return jsonify({
        'message': 'Recipient created successfully',
        'recipient_id': str(recipient['recipient_id'])
    }), 201

@app.route('/api/recipients/bulk', methods=['POST'])
@jwt_required()
@handle_transaction
def create_recipients_bulk():
    """Create multiple recipients at once"""
    user_id = get_jwt_identity()
    data = request.get_json()
    
    # Validate data
    if not data or 'recipients' not in data:
        return jsonify({'message': 'No recipients provided'}), 400
    
    conn, cur = get_db_connection()
    
    created_count = 0
    skipped_count = 0
    
    for recipient_data in data['recipients']:
        # Skip if no email
        if 'email' not in recipient_data or not recipient_data['email']:
            skipped_count += 1
            continue
        
        # Check if recipient already exists
        cur.execute("""
            SELECT * FROM recipients
            WHERE user_id = %s AND email = %s AND is_active = TRUE
        """, (user_id, recipient_data['email']))
        
        if cur.fetchone():
            skipped_count += 1
            continue
        
        # Create new recipient
        custom_fields = json.dumps(recipient_data.get('custom_fields', {})) if recipient_data.get('custom_fields') else None
        
        cur.execute("""
            INSERT INTO recipients 
            (user_id, email, first_name, last_name, company, position, custom_fields)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            user_id,
            recipient_data['email'],
            recipient_data.get('first_name'),
            recipient_data.get('last_name'),
            recipient_data.get('company'),
            recipient_data.get('position'),
            custom_fields
        ))
        
        created_count += 1
    
    return jsonify({
        'message': f'Created {created_count} recipients, skipped {skipped_count} duplicates',
        'created_count': created_count,
        'skipped_count': skipped_count
    }), 201

@app.route('/api/recipients/<recipient_id>', methods=['GET'])
@jwt_required()
@handle_transaction
def get_recipient(recipient_id):
    """Get a single recipient by ID"""
    user_id = get_jwt_identity()
    conn, cur = get_db_connection()
    
    # Get recipient by ID and verify ownership
    cur.execute("""
        SELECT * FROM recipients
        WHERE recipient_id = %s AND user_id = %s AND is_active = TRUE
    """, (recipient_id, user_id))
    
    recipient = cur.fetchone()
    
    if not recipient:
        return jsonify({'message': 'Recipient not found or access denied'}), 404
    
    # Convert to dictionary and handle UUID serialization
    result = dict(recipient)
    result['recipient_id'] = str(result['recipient_id'])
    result['user_id'] = str(result['user_id'])
    if result.get('group_id'):
        result['group_id'] = str(result['group_id'])
    
    # Format dates
    for key in ['created_at', 'updated_at']:
        if key in result and result[key]:
            result[key] = result[key].isoformat()
    
    return jsonify(result), 200

@app.route('/api/recipients/<recipient_id>/update', methods=['POST'])
@jwt_required()
@handle_transaction
def update_recipient(recipient_id):
    """Update an existing recipient"""
    user_id = get_jwt_identity()
    data = request.get_json()
    
    # Validate required fields
    if 'email' not in data or not data['email']:
        return jsonify({'message': 'Email is required'}), 422
    
    conn, cur = get_db_connection()
    
    # Verify recipient exists and belongs to user
    cur.execute("""
        SELECT * FROM recipients
        WHERE recipient_id = %s AND user_id = %s AND is_active = TRUE
    """, (recipient_id, user_id))
    
    recipient = cur.fetchone()
    
    if not recipient:
        return jsonify({'message': 'Recipient not found or access denied'}), 404
    
    # Check if updating to an email that already exists (for another recipient)
    if data['email'] != recipient['email']:
        cur.execute("""
            SELECT * FROM recipients
            WHERE user_id = %s AND email = %s AND recipient_id != %s AND is_active = TRUE
        """, (user_id, data['email'], recipient_id))
        
        if cur.fetchone():
            return jsonify({'message': 'Another recipient with this email already exists'}), 400
    
    # Validate group_id if provided
    group_id = data.get('group_id')
    if group_id == '':  # Handle empty string as None
        group_id = None
    
    if group_id:
        cur.execute("""
            SELECT * FROM groups
            WHERE group_id = %s AND user_id = %s AND is_active = TRUE
        """, (group_id, user_id))
        
        if not cur.fetchone():
            return jsonify({'message': 'Invalid group ID'}), 400
    
    # Handle custom fields as JSON
    custom_fields = json.dumps(data.get('custom_fields', {})) if data.get('custom_fields') else None
    
    # Update recipient
    cur.execute("""
        UPDATE recipients
        SET 
            email = %s,
            first_name = %s,
            last_name = %s,
            company = %s,
            position = %s,
            group_id = %s,
            custom_fields = %s,
            updated_at = NOW()
        WHERE recipient_id = %s
        RETURNING recipient_id
    """, (
        data['email'],
        data.get('first_name'),
        data.get('last_name'),
        data.get('company'),
        data.get('position'),
        group_id,
        custom_fields,
        recipient_id
    ))
    
    updated = cur.fetchone()
    
    return jsonify({
        'message': 'Recipient updated successfully',
        'recipient_id': str(updated['recipient_id'])
    }), 200

@app.route('/api/recipients/<recipient_id>/delete', methods=['POST'])
@jwt_required()
@handle_transaction
def delete_recipient_post(recipient_id):
    """Delete a single recipient by ID using POST method"""
    user_id = get_jwt_identity()
    conn, cur = get_db_connection()
    
    # Verify recipient exists and belongs to user
    cur.execute("""
        SELECT * FROM recipients
        WHERE recipient_id = %s AND user_id = %s
    """, (recipient_id, user_id))
    
    recipient = cur.fetchone()
    
    if not recipient:
        return jsonify({'message': 'Recipient not found or access denied'}), 404
    
    # Soft delete the recipient by setting is_active to FALSE
    cur.execute("""
        UPDATE recipients
        SET is_active = FALSE, updated_at = NOW()
        WHERE recipient_id = %s
    """, (recipient_id,))
    
    # Also remove recipient from any campaign_recipients
    cur.execute("""
        UPDATE campaign_recipients
        SET is_active = FALSE
        WHERE recipient_id = %s
    """, (recipient_id,))
    
    return jsonify({
        'message': 'Recipient deleted successfully',
        'recipient_id': recipient_id
    }), 200

@app.route('/api/recipients/bulk-delete', methods=['POST'])
@jwt_required()
@handle_transaction
def bulk_delete_recipients():
    """Delete multiple recipients at once"""
    user_id = get_jwt_identity()
    data = request.get_json()
    
    # Validate data
    if not data or 'recipient_ids' not in data or not data['recipient_ids']:
        return jsonify({'message': 'No recipient IDs provided'}), 400
    
    conn, cur = get_db_connection()
    
    # Convert recipient_ids to a list if it's a single value
    recipient_ids = data['recipient_ids']
    if not isinstance(recipient_ids, list):
        recipient_ids = [recipient_ids]
    
    # Get recipients that belong to the user
    placeholders = ','.join(['%s'] * len(recipient_ids))
    query_params = recipient_ids + [user_id]
    
    cur.execute(f"""
        SELECT recipient_id FROM recipients
        WHERE recipient_id IN ({placeholders})
        AND user_id = %s
    """, query_params)
    
    valid_recipient_ids = [str(row['recipient_id']) for row in cur.fetchall()]
    
    if not valid_recipient_ids:
        return jsonify({'message': 'No valid recipients found'}), 404
    
    # Soft delete the recipients
    placeholders = ','.join(['%s'] * len(valid_recipient_ids))
    
    cur.execute(f"""
        UPDATE recipients
        SET is_active = FALSE, updated_at = NOW()
        WHERE recipient_id IN ({placeholders})
    """, valid_recipient_ids)
    
    # Remove recipients from any campaign_recipients
    cur.execute(f"""
        UPDATE campaign_recipients
        SET is_active = FALSE
        WHERE recipient_id IN ({placeholders})
    """, valid_recipient_ids)
    
    return jsonify({
        'message': f'Successfully deleted {len(valid_recipient_ids)} recipients',
        'deleted_count': len(valid_recipient_ids),
        'recipient_ids': valid_recipient_ids
    }), 200

# API Routes - Templates
@app.route('/api/templates', methods=['GET'])
@jwt_required()
@handle_transaction
def get_templates():
    """Get all templates for current user"""
    user_id = get_jwt_identity()
    conn, cur = get_db_connection()
    
    cur.execute("""
        SELECT * FROM email_templates
        WHERE user_id = %s AND is_active = TRUE
        ORDER BY created_at DESC
    """, (user_id,))
    
    templates = cur.fetchall()
    
    result = []
    for template in templates:
        template_data = dict(template)
        template_data['template_id'] = str(template_data['template_id'])
        template_data['user_id'] = str(template_data['user_id'])
        if template_data['campaign_id']:
            template_data['campaign_id'] = str(template_data['campaign_id'])
        
        # Format dates
        for key in ['created_at', 'updated_at']:
            if key in template_data and template_data[key]:
                template_data[key] = template_data[key].isoformat()
        
        result.append(template_data)
    
    return jsonify(result), 200

# Tracking routes with enhanced logging and direct database connections
# @app.route('/track/open/<tracking_pixel_id>', methods=['GET'])
# def track_open(tracking_pixel_id):
#     """Track email opens via tracking pixel"""
#     conn = None
#     try:
#         app.logger.info(f"🔍 Tracking pixel accessed: {tracking_pixel_id}")
        
#         conn = psycopg2.connect(
#             host=DB_HOST,
#             database=DB_NAME,
#             user=DB_USER,
#             password=DB_PASS
#         )
#         conn.autocommit = False
#         cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
#         # Find tracking entry
#         cur.execute("""
#             SELECT tracking_id, email_status, opened_at, open_count 
#             FROM email_tracking
#             WHERE tracking_pixel_id = %s
#         """, (tracking_pixel_id,))
        
#         tracking = cur.fetchone()
        
#         if tracking:
#             app.logger.info(f"✅ Found tracking entry: {tracking['tracking_id']}")
#             app.logger.info(f"Current values: status={tracking['email_status']}, opened_at={tracking['opened_at']}, count={tracking['open_count']}")
            
#             # Update tracking data - don't downgrade from 'clicked' to 'opened'
#             cur.execute("""
#                 UPDATE email_tracking
#                 SET 
#                     email_status = CASE
#                         WHEN email_status IN ('sending', 'sent', 'pending') THEN 'opened'
#                         ELSE email_status -- Keep existing status if it's 'clicked' or 'replied'
#                     END,
#                     opened_at = COALESCE(opened_at, NOW()),
#                     open_count = open_count + 1,
#                     updated_at = NOW()
#                 WHERE tracking_id = %s
#                 RETURNING tracking_id, email_status, opened_at, open_count, updated_at
#             """, (tracking['tracking_id'],))
            
#             updated = cur.fetchone()
            
#             # Explicitly commit and confirm
#             conn.commit()
#             app.logger.info(f"✅ UPDATE COMMITTED: status={updated['email_status']}, opened_at={updated['opened_at']}, count={updated['open_count']}")
            
#             # Double-check the update
#             cur.execute("""
#                 SELECT tracking_id, email_status, opened_at, open_count 
#                 FROM email_tracking
#                 WHERE tracking_id = %s
#             """, (tracking['tracking_id'],))
            
#             verification = cur.fetchone()
#             app.logger.info(f"✅ VERIFIED VALUES: status={verification['email_status']}, opened_at={verification['opened_at']}, count={verification['open_count']}")
            
#         else:
#             app.logger.warning(f"⚠️ No tracking entry found for pixel ID: {tracking_pixel_id}")
        
#         # Return a 1x1 transparent pixel
#         pixel = base64.b64decode('R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==')
        
#         return pixel, 200, {
#             'Content-Type': 'image/gif', 
#             'Cache-Control': 'no-cache, no-store, must-revalidate, private',
#             'Pragma': 'no-cache',
#             'Expires': '0'
#         }
    
#     except Exception as e:
#         app.logger.error(f"❌ Error tracking open: {str(e)}")
#         if conn:
#             try:
#                 conn.rollback()
#             except:
#                 pass
#         pixel = base64.b64decode('R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==')
#         return pixel, 200, {'Content-Type': 'image/gif'}
#     finally:
#         if conn:
#             conn.close()

@app.route('/track/open/<tracking_pixel_id>', methods=['GET'])
def track_open(tracking_pixel_id):
    """Track email opens via tracking pixel with enhanced reliability"""
    conn = None
    try:
        # Extract source info if available (for debugging)
        source = request.args.get('s', 'img')
        position = request.args.get('pos', 'unknown')
        user_agent = request.headers.get('User-Agent', 'Unknown')
        
        app.logger.info(f"🔍 Tracking pixel accessed: {tracking_pixel_id} (source: {source}, pos: {position}, UA: {user_agent})")
        
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Find tracking entry
        cur.execute("""
            SELECT tracking_id, campaign_id, recipient_id, email_status, opened_at, open_count 
            FROM email_tracking
            WHERE tracking_pixel_id = %s
        """, (tracking_pixel_id,))
        
        tracking = cur.fetchone()
        
        if tracking:
            app.logger.info(f"✅ Found tracking entry: {tracking['tracking_id']} for campaign {tracking['campaign_id']}")
            app.logger.debug(f"Current values: status={tracking['email_status']}, opened_at={tracking['opened_at']}, count={tracking['open_count']}")
            
            # Update tracking data - don't downgrade from 'clicked' or 'replied' to 'opened'
            cur.execute("""
                UPDATE email_tracking
                SET 
                    email_status = CASE
                        WHEN email_status IN ('sending', 'sent', 'pending', 'failed') THEN 'opened'
                        ELSE email_status -- Keep existing status if it's 'clicked' or 'replied'
                    END,
                    opened_at = COALESCE(opened_at, NOW()),
                    open_count = open_count + 1,
                    updated_at = NOW()
                WHERE tracking_id = %s
                RETURNING tracking_id, email_status, opened_at, open_count, updated_at
            """, (tracking['tracking_id'],))
            
            updated = cur.fetchone()
            
            # Explicitly commit and confirm
            conn.commit()
            app.logger.info(f"✅ UPDATE COMMITTED: status={updated['email_status']}, opened_at={updated['opened_at']}, count={updated['open_count']}")
            
            # Double-check the update (extra validation)
            cur.execute("""
                SELECT tracking_id, email_status, opened_at, open_count 
                FROM email_tracking
                WHERE tracking_id = %s
            """, (tracking['tracking_id'],))
            
            verification = cur.fetchone()
            app.logger.debug(f"✅ VERIFIED VALUES: status={verification['email_status']}, opened_at={verification['opened_at']}, count={verification['open_count']}")
            
        else:
            app.logger.warning(f"⚠️ No tracking entry found for pixel ID: {tracking_pixel_id}")
        
        # Return a 1x1 transparent pixel
        pixel = base64.b64decode('R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==')
        
        return pixel, 200, {
            'Content-Type': 'image/gif', 
            'Cache-Control': 'no-cache, no-store, must-revalidate, private',
            'Pragma': 'no-cache',
            'Expires': '0',
            'Access-Control-Allow-Origin': '*'
        }
    
    except Exception as e:
        app.logger.error(f"❌ Error tracking open: {str(e)}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        # Still return a pixel to avoid broken images
        pixel = base64.b64decode('R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==')
        return pixel, 200, {'Content-Type': 'image/gif'}
    finally:
        if conn:
            conn.close()


@app.route('/track/click/<tracking_id>/<url_tracking_id>', methods=['GET'])
def track_click(tracking_id, url_tracking_id):
    """Track email link clicks and also ensure opens are recorded"""
    conn = None
    try:
        app.logger.info(f"🔍 Click tracking: tracking_id={tracking_id}, url_tracking_id={url_tracking_id}")
        
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Find url tracking entry
        cur.execute("""
            SELECT * FROM url_tracking
            WHERE url_tracking_id = %s
        """, (url_tracking_id,))
        
        url_tracking = cur.fetchone()
        
        if not url_tracking:
            app.logger.warning(f"⚠️ No URL tracking entry found for {url_tracking_id}")
            # Fallback to a safe URL if the tracking entry isn't found
            return redirect("https://www.google.com", code=302)
            
        original_url = url_tracking['original_url']
        app.logger.info(f"✅ Found original URL: {original_url}")
        
        # Update url tracking data
        cur.execute("""
            UPDATE url_tracking
            SET 
                click_count = click_count + 1,
                first_clicked_at = COALESCE(first_clicked_at, NOW()),
                last_clicked_at = NOW()
            WHERE url_tracking_id = %s
            RETURNING url_tracking_id, click_count
        """, (url_tracking_id,))
        
        url_update = cur.fetchone()
        app.logger.info(f"✅ Updated URL tracking: url_id={url_update['url_tracking_id']}, clicks={url_update['click_count']}")
        
        # First check if this tracking ID exists and get its current values
        cur.execute("""
            SELECT tracking_id, email_status, opened_at, open_count, clicked_at, click_count, replied_at
            FROM email_tracking
            WHERE tracking_id = %s
        """, (tracking_id,))
        
        tracking = cur.fetchone()
        
        if tracking:
            app.logger.info(f"✅ Found tracking entry: {tracking['tracking_id']}")
            app.logger.info(f"Current values: status={tracking['email_status']}, opened_at={tracking['opened_at']}, open_count={tracking['open_count']}, clicked_at={tracking['clicked_at']}, click_count={tracking['click_count']}")
            
            # Update email tracking - ensure opened is also recorded
            cur.execute("""
                UPDATE email_tracking
                SET 
                    -- Set status to 'clicked' unless it's already 'replied'
                    email_status = CASE
                        WHEN email_status = 'replied' THEN 'replied'
                        ELSE 'clicked'
                    END,
                    -- Always ensure opened_at is set - clicking means they opened it
                    opened_at = COALESCE(opened_at, NOW()),
                    -- Make sure open_count is at least 1
                    open_count = CASE WHEN open_count = 0 THEN 1 ELSE open_count END,
                    -- Update clicked data
                    clicked_at = COALESCE(clicked_at, NOW()),
                    click_count = click_count + 1,
                    updated_at = NOW()
                WHERE tracking_id = %s
                RETURNING tracking_id, email_status, opened_at, open_count, clicked_at, click_count
            """, (tracking_id,))
            
            tracking_update = cur.fetchone()
            
            conn.commit()
            app.logger.info(f"✅ UPDATE COMMITTED: status={tracking_update['email_status']}, opened_at={tracking_update['opened_at']}, open_count={tracking_update['open_count']}, clicked_at={tracking_update['clicked_at']}, click_count={tracking_update['click_count']}")
            
            # Double-check the update
            cur.execute("""
                SELECT tracking_id, email_status, opened_at, open_count, clicked_at, click_count
                FROM email_tracking
                WHERE tracking_id = %s
            """, (tracking_id,))
            
            verification = cur.fetchone()
            app.logger.info(f"✅ VERIFIED VALUES: status={verification['email_status']}, opens={verification['open_count']}, clicks={verification['click_count']}")
        else:
            app.logger.warning(f"⚠️ No email tracking record found for {tracking_id}")
            conn.commit()
        
        app.logger.info(f"🔄 Redirecting to: {original_url}")
        
        # Redirect to the original URL
        return redirect(original_url, code=302)
        
    except Exception as e:
        app.logger.error(f"❌ Error tracking click: {str(e)}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        # Provide a fallback in case of error
        return redirect("https://www.google.com", code=302)
    finally:
        if conn:
            conn.close()

# Add this new beacon tracking endpoint for JavaScript-based tracking
@app.route('/track/beacon/<tracking_pixel_id>', methods=['GET'])
def track_beacon(tracking_pixel_id):
    """JavaScript-based tracking endpoint as backup for image tracking"""
    conn = None
    try:
        # Get delay flag if this is a delayed beacon
        delayed = request.args.get('d', '0') == '1'
        user_agent = request.headers.get('User-Agent', 'Unknown')
        
        app.logger.info(f"🔍 Beacon tracking accessed: {tracking_pixel_id} (delayed: {delayed}, UA: {user_agent})")
        
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Find tracking entry
        cur.execute("""
            SELECT tracking_id, campaign_id, recipient_id, email_status, opened_at, open_count 
            FROM email_tracking
            WHERE tracking_pixel_id = %s
        """, (tracking_pixel_id,))
        
        tracking = cur.fetchone()
        
        if tracking:
            app.logger.info(f"✅ Found tracking entry for beacon: {tracking['tracking_id']} (campaign: {tracking['campaign_id']})")
            
            # Update tracking data - similar to track_open
            cur.execute("""
                UPDATE email_tracking
                SET 
                    email_status = CASE
                        WHEN email_status IN ('sending', 'sent', 'pending', 'failed') THEN 'opened'
                        ELSE email_status
                    END,
                    opened_at = COALESCE(opened_at, NOW()),
                    open_count = open_count + 1,
                    updated_at = NOW()
                WHERE tracking_id = %s
                RETURNING tracking_id, email_status, opened_at, open_count
            """, (tracking['tracking_id'],))
            
            updated = cur.fetchone()
            conn.commit()
            app.logger.info(f"✅ Beacon tracking updated: status={updated['email_status']}, opens={updated['open_count']}")
        else:
            app.logger.warning(f"⚠️ No tracking entry found for beacon ID: {tracking_pixel_id}")
        
        # Return minimal response with CORS headers to work in any email client
        return jsonify({'status': 'ok'}), 200, {
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
        }
    
    except Exception as e:
        app.logger.error(f"❌ Error tracking beacon: {str(e)}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return jsonify({'status': 'error'}), 200  # Return 200 even on error to avoid JS errors
    finally:
        if conn:
            conn.close()
            
# Manual Reply Marking Endpoint
@app.route('/api/campaigns/<campaign_id>/mark-replied', methods=['POST'])
@jwt_required()
def mark_email_repliedd(campaign_id):
    """Manually mark an email as replied"""
    user_id = get_jwt_identity()
    data = request.get_json()
    conn = None
    
    try:
        # Validate input
        if not data or 'recipient_id' not in data:
            return jsonify({'message': 'Recipient ID is required'}), 400
        
        recipient_id = data['recipient_id']
        app.logger.info(f"Attempting to mark recipient {recipient_id} as replied for campaign {campaign_id}")
        
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Verify campaign belongs to user
        cur.execute("""
            SELECT * FROM email_campaigns
            WHERE campaign_id = %s AND user_id = %s
        """, (campaign_id, user_id))
        
        if not cur.fetchone():
            return jsonify({'message': 'Campaign not found or access denied'}), 404
        
        # Update tracking record
        cur.execute("""
            UPDATE email_tracking
            SET 
                email_status = 'replied',
                replied_at = COALESCE(replied_at, NOW()),
                updated_at = NOW()
            WHERE campaign_id = %s AND recipient_id = %s
            RETURNING tracking_id, replied_at
        """, (campaign_id, recipient_id))
        
        result = cur.fetchone()
        
        if not result:
            app.logger.warning(f"No tracking record found for recipient {recipient_id} in campaign {campaign_id}")
            return jsonify({'message': 'No tracking record found for this recipient'}), 404
        
        # Explicitly commit the transaction
        conn.commit()
        
        app.logger.info(f"Successfully marked recipient {recipient_id} as replied for campaign {campaign_id}")
        
        return jsonify({
            'message': 'Email marked as replied successfully',
            'tracking_id': str(result['tracking_id']),
            'replied_at': result['replied_at'].isoformat()
        }), 200
        
    except Exception as e:
        app.logger.error(f"Error marking email as replied: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({'message': f'Error: {str(e)}'}), 500
    finally:
        if conn:
            conn.close()

# Dashboard routes
@app.route('/api/dashboard', methods=['GET'])
@jwt_required()
@handle_transaction
def get_dashboard_data():
    """Get dashboard overview data"""
    user_id = get_jwt_identity()
    conn, cur = get_db_connection()
    
    # Get counts
    cur.execute("""
        SELECT COUNT(*) as campaign_count
        FROM email_campaigns
        WHERE user_id = %s AND is_active = TRUE
    """, (user_id,))
    campaign_count = cur.fetchone()['campaign_count']
    
    cur.execute("""
        SELECT COUNT(*) as recipient_count
        FROM recipients
        WHERE user_id = %s AND is_active = TRUE
    """, (user_id,))
    recipient_count = cur.fetchone()['recipient_count']
    
    cur.execute("""
        SELECT COUNT(*) as template_count
        FROM email_templates
        WHERE user_id = %s AND is_active = TRUE
    """, (user_id,))
    template_count = cur.fetchone()['template_count']
    
    # Get completed campaigns
    cur.execute("""
        SELECT * FROM email_campaigns
        WHERE user_id = %s AND status = 'completed' AND is_active = TRUE
        ORDER BY sent_at DESC
    """, (user_id,))
    completed_campaigns = cur.fetchall()
    
    # Calculate stats
    total_sent = 0
    total_opened = 0
    total_clicked = 0
    total_replied = 0
    
    campaign_stats = []
    
    for campaign in completed_campaigns:
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE sent_at IS NOT NULL) as sent_count,
                COUNT(*) FILTER (WHERE opened_at IS NOT NULL) as opened_count,
                COUNT(*) FILTER (WHERE clicked_at IS NOT NULL) as clicked_count,
                COUNT(*) FILTER (WHERE replied_at IS NOT NULL) as replied_count
            FROM email_tracking
            WHERE campaign_id = %s
        """, (campaign['campaign_id'],))
        
        tracking = cur.fetchone()
        
        sent_count = tracking['sent_count']
        opened_count = tracking['opened_count']
        clicked_count = tracking['clicked_count']
        replied_count = tracking['replied_count']
        
        total_sent += sent_count
        total_opened += opened_count
        total_clicked += clicked_count
        total_replied += replied_count
        
        open_rate = (opened_count / sent_count * 100) if sent_count > 0 else 0
        click_rate = (clicked_count / sent_count * 100) if sent_count > 0 else 0
        reply_rate = (replied_count / sent_count * 100) if sent_count > 0 else 0
        
        campaign_data = dict(campaign)
        campaign_data['campaign_id'] = str(campaign_data['campaign_id'])
        campaign_data['user_id'] = str(campaign_data['user_id'])
        
        # Format dates
        for key in ['created_at', 'scheduled_at', 'sent_at']:
            if key in campaign_data and campaign_data[key]:
                campaign_data[key] = campaign_data[key].isoformat()
        
        campaign_stats.append({
            **campaign_data,
            'sent_count': sent_count,
            'opened_count': opened_count,
            'clicked_count': clicked_count,
            'replied_count': replied_count,
            'open_rate': open_rate,
            'click_rate': click_rate,
            'reply_rate': reply_rate
        })
    
    overall_open_rate = (total_opened / total_sent * 100) if total_sent > 0 else 0
    overall_click_rate = (total_clicked / total_sent * 100) if total_sent > 0 else 0
    overall_reply_rate = (total_replied / total_sent * 100) if total_sent > 0 else 0
    
    # Get recent campaigns
    cur.execute("""
        SELECT * FROM email_campaigns
        WHERE user_id = %s AND is_active = TRUE
        ORDER BY created_at DESC
        LIMIT 5
    """, (user_id,))
    
    recent_campaigns = cur.fetchall()
    recent_campaign_data = []
    
    for campaign in recent_campaigns:
        campaign_data = dict(campaign)
        campaign_data['campaign_id'] = str(campaign_data['campaign_id'])
        campaign_data['user_id'] = str(campaign_data['user_id'])
        
        # Format dates
        for key in ['created_at', 'scheduled_at', 'sent_at']:
            if key in campaign_data and campaign_data[key]:
                campaign_data[key] = campaign_data[key].isoformat()
        
        recent_campaign_data.append(campaign_data)
    
    # Get recent recipients
    cur.execute("""
        SELECT * FROM recipients
        WHERE user_id = %s AND is_active = TRUE
        ORDER BY created_at DESC
        LIMIT 5
    """, (user_id,))
    
    recent_recipients = cur.fetchall()
    recent_recipient_data = []
    
    for recipient in recent_recipients:
        recipient_data = dict(recipient)
        recipient_data['recipient_id'] = str(recipient_data['recipient_id'])
        recipient_data['user_id'] = str(recipient_data['user_id'])
        
        # Format dates
        for key in ['created_at', 'updated_at']:
            if key in recipient_data and recipient_data[key]:
                recipient_data[key] = recipient_data[key].isoformat()
        
        recent_recipient_data.append(recipient_data)
    
    result = {
        'counts': {
            'campaigns': campaign_count,
            'recipients': recipient_count,
            'templates': template_count,
            'emails_sent': total_sent
        },
        'overall_stats': {
            'total_sent': total_sent,
            'total_opened': total_opened,
            'total_clicked': total_clicked,
            'total_replied': total_replied,
            'open_rate': overall_open_rate,
            'click_rate': overall_click_rate,
            'reply_rate': overall_reply_rate
        },
        'campaign_stats': campaign_stats,
        'recent_campaigns': recent_campaign_data,
        'recent_recipients': recent_recipient_data
    }
    return jsonify(result), 200

# Utility and debugging routes
@app.route('/api/auth-test', methods=['GET'])
@jwt_required()
def auth_test():
    """Test endpoint that requires authentication"""
    current_user_id = get_jwt_identity()
    return jsonify({
        'status': 'success',
        'message': 'Authentication successful',
        'user_id': current_user_id,
        'timestamp': datetime.now().isoformat()
    }), 200

@app.route('/api/health-check', methods=['GET'])
def health_check():
    """Health check endpoint that doesn't require authentication"""
    return jsonify({
        'status': 'success',
        'message': 'API is online',
        'timestamp': datetime.now().isoformat()
    }), 200

# Debugging endpoints
@app.route('/api/debug/tracking/<campaign_id>', methods=['GET'])
@jwt_required()
def debug_tracking(campaign_id):
    """Debug endpoint to directly query tracking data"""
    user_id = get_jwt_identity()
    conn = None
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Verify campaign belongs to user
        cur.execute("""
            SELECT * FROM email_campaigns
            WHERE campaign_id = %s AND user_id = %s
        """, (campaign_id, user_id))
        
        if not cur.fetchone():
            return jsonify({'message': 'Campaign not found or access denied'}), 404
        
        # Get raw tracking data from database
        cur.execute("""
            SELECT 
                et.tracking_id, 
                et.recipient_id, 
                r.email as recipient_email,
                et.email_status,
                et.sent_at,
                et.opened_at,
                et.clicked_at,
                et.replied_at,
                et.open_count,
                et.click_count,
                et.tracking_pixel_id,
                et.created_at,
                et.updated_at
            FROM email_tracking et
            JOIN recipients r ON et.recipient_id = r.recipient_id
            WHERE et.campaign_id = %s
        """, (campaign_id,))
        
        tracking_data = []
        for row in cur.fetchall():
            data = dict(row)
            # Format UUIDs
            data['tracking_id'] = str(data['tracking_id'])
            data['recipient_id'] = str(data['recipient_id'])
            
            # Format dates
            for key in ['sent_at', 'opened_at', 'clicked_at', 'replied_at', 'created_at', 'updated_at']:
                if key in data and data[key]:
                    data[key] = data[key].isoformat()
            
            # Add testing links
            data['test_links'] = {
                'open_url': f"/track/open/{data['tracking_pixel_id']}",
                'beacon_url': f"/track/beacon/{data['tracking_pixel_id']}",
                'click_test': f"/api/debug/test-click/{data['tracking_id']}"
            }
            
            tracking_data.append(data)
        
        # Get URL tracking data too
        cur.execute("""
            SELECT ut.* 
            FROM url_tracking ut
            JOIN email_tracking et ON ut.tracking_id = et.tracking_id
            WHERE et.campaign_id = %s
            ORDER BY ut.created_at DESC
        """, (campaign_id,))
        
        url_data = []
        for row in cur.fetchall():
            data = dict(row)
            # Format UUIDs
            data['url_tracking_id'] = str(data['url_tracking_id'])
            data['tracking_id'] = str(data['tracking_id'])
            
            # Format dates
            for key in ['first_clicked_at', 'last_clicked_at', 'created_at']:
                if key in data and data[key]:
                    data[key] = data[key].isoformat()
            
            # Add the actual tracking URL
            data['click_test_url'] = f"/track/click/{data['tracking_id']}/{data['url_tracking_id']}"
            
            url_data.append(data)
        
        return jsonify({
            'campaign_id': campaign_id,
            'tracking_count': len(tracking_data),
            'tracking_data': tracking_data,
            'url_tracking': url_data,
            'timestamp': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/debug/check-replies', methods=['GET'])
@jwt_required()
def trigger_reply_check():
    """Manually trigger the email reply check"""
    user_id = get_jwt_identity()
    
    # Start reply check in a separate thread to avoid blocking
    thread = Thread(target=check_for_replies)
    thread.start()
    
    return jsonify({
        'message': 'Reply check started in background',
        'user_id': user_id,
        'timestamp': datetime.now().isoformat()
    }), 200

@app.route('/api/debug/track-open/<campaign_id>/<recipient_id>', methods=['GET'])
def debug_track_open(campaign_id, recipient_id):
    """Debug endpoint to manually trigger an open tracking event"""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Find tracking entry
        cur.execute("""
            SELECT * FROM email_tracking
            WHERE campaign_id = %s AND recipient_id = %s
        """, (campaign_id, recipient_id))
        
        tracking = cur.fetchone()
        
        if tracking:
            # Update tracking data
            cur.execute("""
                UPDATE email_tracking
                SET 
                    email_status = 'opened',
                    opened_at = COALESCE(opened_at, NOW()),
                    open_count = open_count + 1,
                    updated_at = NOW()
                WHERE tracking_id = %s
                RETURNING tracking_id, opened_at, open_count
            """, (tracking['tracking_id'],))
            
            updated = cur.fetchone()
            conn.commit()
            
            return jsonify({
                'message': 'Successfully recorded open event',
                'tracking_id': str(updated['tracking_id']),
                'opened_at': updated['opened_at'].isoformat(),
                'open_count': updated['open_count']
            }), 200
        else:
            return jsonify({
                'message': 'No tracking entry found for this campaign and recipient'
            }), 404
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({
            'message': f'Error: {str(e)}'
        }), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/debug/test-click/<tracking_id>', methods=['GET'])
def test_click(tracking_id):
    """Generate a test click for debugging"""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Create a test URL tracking entry
        cur.execute("""
            INSERT INTO url_tracking
            (tracking_id, original_url, tracking_url, click_count)
            VALUES (%s, %s, %s, 0)
            RETURNING url_tracking_id
        """, (tracking_id, 'https://www.google.com', f'http://localhost:5000/track/click/{tracking_id}/test'))
        
        url_tracking_id = cur.fetchone()['url_tracking_id']
        conn.commit()
        
        # Redirect to the click tracking URL
        return redirect(f"/track/click/{tracking_id}/{url_tracking_id}", code=302)
        
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/campaigns/<campaign_id>/groups', methods=['GET'])
@jwt_required()
@handle_transaction
def get_campaign_groups(campaign_id):
    """Get all groups associated with a campaign"""
    user_id = get_jwt_identity()
    conn, cur = get_db_connection()
    
    # Verify campaign belongs to user
    cur.execute("""
        SELECT * FROM email_campaigns
        WHERE campaign_id = %s AND user_id = %s
    """, (campaign_id, user_id))
    
    if not cur.fetchone():
        return jsonify({'message': 'Campaign not found or access denied'}), 404
    
    # Get groups for this campaign with recipient counts
    cur.execute("""
        SELECT g.*, COUNT(r.recipient_id) as recipient_count 
        FROM groups g
        JOIN campaign_groups cg ON g.group_id = cg.group_id
        LEFT JOIN recipients r ON g.group_id = r.group_id AND r.is_active = TRUE
        WHERE cg.campaign_id = %s AND cg.is_active = TRUE AND g.is_active = TRUE
        GROUP BY g.group_id
    """, (campaign_id,))
    
    groups = cur.fetchall()
    
    result = []
    for group in groups:
        group_data = dict(group)
        group_data['group_id'] = str(group_data['group_id'])
        group_data['user_id'] = str(group_data['user_id'])
        group_data['id'] = group_data['group_id']  # For frontend compatibility
        
        # Format dates
        for key in ['created_at', 'updated_at']:
            if key in group_data and group_data[key]:
                group_data[key] = group_data[key].isoformat()
        
        result.append(group_data)
    
    return jsonify(result), 200

@app.route('/api/campaigns/<campaign_id>/groups', methods=['POST'])
@jwt_required()
@handle_transaction
def add_groups_to_campaign(campaign_id):
    """Add groups to a campaign"""
    user_id = get_jwt_identity()
    data = request.get_json()
    
    # Validate data
    if not data or 'group_ids' not in data or not data['group_ids']:
        return jsonify({'message': 'No group IDs provided'}), 400
    
    conn, cur = get_db_connection()
    
    # Verify campaign belongs to user
    cur.execute("""
        SELECT * FROM email_campaigns
        WHERE campaign_id = %s AND user_id = %s
    """, (campaign_id, user_id))
    
    if not cur.fetchone():
        return jsonify({'message': 'Campaign not found or access denied'}), 404
    
    # Convert group_ids to a list if it's a single value
    group_ids = data['group_ids']
    if not isinstance(group_ids, list):
        group_ids = [group_ids]
    
    # Add groups to campaign
    added_count = 0
    for group_id in group_ids:
        # Verify group belongs to user
        cur.execute("""
            SELECT group_id FROM groups
            WHERE group_id = %s AND user_id = %s AND is_active = TRUE
        """, (group_id, user_id))
        
        if cur.fetchone():
            cur.execute("""
                INSERT INTO campaign_groups (campaign_id, group_id)
                VALUES (%s, %s)
                ON CONFLICT (campaign_id, group_id) DO NOTHING
            """, (campaign_id, group_id))
            added_count += 1
    
    return jsonify({
        'message': f'Added {added_count} groups to campaign',
        'added_count': added_count
    }), 200

@app.route('/api/campaigns/<campaign_id>/groups/remove', methods=['POST'])
@jwt_required()
@handle_transaction
def remove_groups_from_campaign(campaign_id):
    """Remove groups from a campaign"""
    user_id = get_jwt_identity()
    data = request.get_json()
    
    # Validate data
    if not data or 'group_ids' not in data or not data['group_ids']:
        return jsonify({'message': 'No group IDs provided'}), 400
    
    conn, cur = get_db_connection()
    
    # Verify campaign belongs to user
    cur.execute("""
        SELECT * FROM email_campaigns
        WHERE campaign_id = %s AND user_id = %s
    """, (campaign_id, user_id))
    
    if not cur.fetchone():
        return jsonify({'message': 'Campaign not found or access denied'}), 404
    
    # Convert group_ids to a list if it's a single value
    group_ids = data['group_ids']
    if not isinstance(group_ids, list):
        group_ids = [group_ids]
    
    # Remove groups from campaign
    placeholders = ','.join(['%s'] * len(group_ids))
    query_params = group_ids + [campaign_id]
    
    cur.execute(f"""
        UPDATE campaign_groups
        SET is_active = FALSE
        WHERE group_id IN ({placeholders}) AND campaign_id = %s
    """, query_params)
    
    return jsonify({
        'message': f'Removed groups from campaign',
        'removed_count': len(group_ids)
    }), 200

@app.route('/api/campaigns/<campaign_id>/update', methods=['POST'])
@jwt_required()
@handle_transaction
def update_campaign(campaign_id):
    """Update an existing campaign"""
    user_id = get_jwt_identity()
    data = request.get_json()
    
    conn, cur = get_db_connection()
    
    # Verify campaign exists and belongs to user
    cur.execute("""
        SELECT * FROM email_campaigns
        WHERE campaign_id = %s AND user_id = %s
    """, (campaign_id, user_id))
    
    campaign = cur.fetchone()
    
    if not campaign:
        return jsonify({'message': 'Campaign not found or access denied'}), 404
    
    # Only allow updating campaigns in draft status
    if campaign['status'] != 'draft':
        return jsonify({'message': f'Cannot update campaign in {campaign["status"]} status'}), 400
    
    # Update campaign fields
    update_fields = []
    update_values = []
    
    if 'campaign_name' in data and data['campaign_name']:
        update_fields.append("campaign_name = %s")
        update_values.append(data['campaign_name'])
    
    if 'subject_line' in data and data['subject_line']:
        update_fields.append("subject_line = %s")
        update_values.append(data['subject_line'])
    
    if 'from_name' in data and data['from_name']:
        update_fields.append("from_name = %s")
        update_values.append(data['from_name'])
    
    if 'from_email' in data and data['from_email']:
        update_fields.append("from_email = %s")
        update_values.append(data['from_email'])
    
    if 'reply_to_email' in data and data['reply_to_email']:
        update_fields.append("reply_to_email = %s")
        update_values.append(data['reply_to_email'])
    
    if update_fields:
        update_fields.append("updated_at = NOW()")
        update_query = f"""
            UPDATE email_campaigns
            SET {', '.join(update_fields)}
            WHERE campaign_id = %s
        """
        update_values.append(campaign_id)
        
        cur.execute(update_query, update_values)
    
    # Update template if provided
    if 'template' in data:
        template_name = data['template'].get('name', 'Default Template')
        html_content = data['template']['html_content']
        text_content = data['template'].get('text_content', '')
        
        # Check if template exists
        cur.execute("""
            SELECT * FROM email_templates
            WHERE campaign_id = %s AND is_active = TRUE
        """, (campaign_id,))
        
        template = cur.fetchone()
        
        if template:
            # Update existing template
            cur.execute("""
                UPDATE email_templates
                SET 
                    template_name = %s,
                    html_content = %s,
                    text_content = %s,
                    updated_at = NOW()
                WHERE template_id = %s
            """, (template_name, html_content, text_content, template['template_id']))
        else:
            # Create new template
            cur.execute("""
                INSERT INTO email_templates
                (user_id, campaign_id, template_name, html_content, text_content)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, campaign_id, template_name, html_content, text_content))
    
    # Update recipients if provided
    if 'recipients' in data:
        # First remove all existing recipients
        cur.execute("""
            UPDATE campaign_recipients
            SET is_active = FALSE
            WHERE campaign_id = %s
        """, (campaign_id,))
        
        # Then add the new recipients
        for recipient_id in data['recipients']:
            # Verify recipient belongs to user
            cur.execute("""
                SELECT recipient_id FROM recipients
                WHERE recipient_id = %s AND user_id = %s
            """, (recipient_id, user_id))
            
            if cur.fetchone():
                cur.execute("""
                    INSERT INTO campaign_recipients (campaign_id, recipient_id)
                    VALUES (%s, %s)
                    ON CONFLICT (campaign_id, recipient_id) DO UPDATE
                    SET is_active = TRUE
                """, (campaign_id, recipient_id))
    
    # Update groups if provided
    if 'groups' in data:
        # First remove all existing groups
        cur.execute("""
            UPDATE campaign_groups
            SET is_active = FALSE
            WHERE campaign_id = %s
        """, (campaign_id,))
        
        # Then add the new groups
        for group_id in data['groups']:
            # Verify group belongs to user
            cur.execute("""
                SELECT group_id FROM groups
                WHERE group_id = %s AND user_id = %s
            """, (group_id, user_id))
            
            if cur.fetchone():
                cur.execute("""
                    INSERT INTO campaign_groups (campaign_id, group_id)
                    VALUES (%s, %s)
                    ON CONFLICT (campaign_id, group_id) DO UPDATE
                    SET is_active = TRUE
                """, (campaign_id, group_id))
    
    return jsonify({
        'message': 'Campaign updated successfully',
        'campaign_id': str(campaign_id)
    }), 200

@app.route('/api/campaigns/<campaign_id>/recipients/<recipient_id>/replied', methods=['POST'])
@jwt_required()
def mark_email_replied(campaign_id, recipient_id):
    """Manually mark an email as replied"""
    user_id = get_jwt_identity()
    conn = None
    
    try:
        app.logger.info(f"Attempting to mark recipient {recipient_id} as replied for campaign {campaign_id}")
        
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Verify campaign belongs to user
        cur.execute("""
            SELECT * FROM email_campaigns
            WHERE campaign_id = %s AND user_id = %s
        """, (campaign_id, user_id))
        
        if not cur.fetchone():
            return jsonify({'message': 'Campaign not found or access denied'}), 404
        
        # Update tracking record
        cur.execute("""
            UPDATE email_tracking
            SET 
                email_status = 'replied',
                replied_at = COALESCE(replied_at, NOW()),
                updated_at = NOW()
            WHERE campaign_id = %s AND recipient_id = %s
            RETURNING tracking_id, replied_at
        """, (campaign_id, recipient_id))
        
        result = cur.fetchone()
        
        if not result:
            app.logger.warning(f"No tracking record found for recipient {recipient_id} in campaign {campaign_id}")
            return jsonify({'message': 'No tracking record found for this recipient'}), 404
        
        # Explicitly commit the transaction
        conn.commit()
        
        app.logger.info(f"Successfully marked recipient {recipient_id} as replied for campaign {campaign_id}")
        
        return jsonify({
            'message': 'Email marked as replied successfully',
            'tracking_id': str(result['tracking_id']),
            'replied_at': result['replied_at'].isoformat()
        }), 200
        
    except Exception as e:
        app.logger.error(f"Error marking email as replied: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({'message': f'Error: {str(e)}'}), 500
    finally:
        if conn:
            conn.close()

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'status': 'error',
        'message': 'The requested URL was not found on the server.'
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'status': 'error',
        'message': 'An internal server error occurred.',
        'details': str(error) if app.debug else None
    }), 500

# Request logging middleware
@app.before_request
def log_request_info():
    if app.debug:
        app.logger.debug('Headers: %s', request.headers)
        app.logger.debug('Body: %s', request.get_data())

@app.after_request
def log_response_info(response):
    if app.debug and response.content_type == 'application/json':
        app.logger.debug('Response: %s', response.get_data())
    return response

# Main entry point
if __name__ == '__main__':
    # Set debug to False in production!
    app.run(debug=True, port=5000)