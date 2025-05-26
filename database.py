import os
import datetime
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Table, MetaData, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Get database URL from environment
database_url = os.environ.get("DATABASE_URL")
if database_url is None:
    # Instead of raising an error, just use a dummy URL for SQLite
    database_url = "sqlite:///:memory:"
    print("No DATABASE_URL found, using in-memory SQLite database")

# Fix potential issue with URL format
if database_url:
    # First handle postgres:// format 
    if database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql://')
    
    # Handle invalid protocol in URL
    if database_url.startswith(('https://', 'http://')):
        # Extract the part after the protocol
        parts = database_url.split('://', 1)
        if len(parts) > 1:
            # Replace with postgresql://
            database_url = 'postgresql://' + parts[1]

# Create database engine with a short timeout
try:
    # Enhanced connection settings for Neon PostgreSQL
    connect_args = {}
    if 'postgresql' in database_url:
        connect_args = {
            "connect_timeout": 10,             # 10 seconds timeout
            "keepalives": 1,                   # Enable keepalives
            "keepalives_idle": 30,             # Seconds before sending keepalive
            "keepalives_interval": 10,         # Seconds between keepalives 
            "keepalives_count": 5,             # Max number of keepalive retries
            "sslmode": 'require'               # Force SSL connection
        }
    
    # Create engine with connection pooling for better reliability
    engine = create_engine(
        database_url, 
        connect_args=connect_args,
        pool_size=5,                         # Connection pool size
        max_overflow=10,                     # Max extra connections
        pool_timeout=30,                     # Connection timeout
        pool_recycle=1800                    # Recycle connections after 30 min
    )
    # Test connection with a quick timeout
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
except Exception as e:
    print(f"Error connecting to database - using fallback: {str(e)}")
    # Instead of exiting, we'll define our own versions of functions that use fallback data
    from db_fallback import save_job_search, get_popular_searches, get_highest_risk_jobs, get_lowest_risk_jobs, get_recent_searches
    
    # Exit the module with fallback functions defined
    import sys
    sys.modules[__name__].__dict__.update(locals())
    exit()
    
# Database connection successful - create tables and set up ORM
Base = declarative_base()

# Define model for job searches
class JobSearch(Base):
    __tablename__ = 'job_searches'
    
    id = Column(Integer, primary_key=True)
    job_title = Column(String(255), nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    year_1_risk = Column(Float)
    year_5_risk = Column(Float)
    risk_category = Column(String(50))
    job_category = Column(String(50))
    
    def __repr__(self):
        return f"<JobSearch(job_title='{self.job_title}', risk_category='{self.risk_category}')>"

# Create tables if they don't exist
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Real implementations of database functions
def save_job_search(job_title, risk_data):
    """
    Save job search data to database
    """
    session = Session()
    try:
        job_search = JobSearch(
            job_title=job_title,
            year_1_risk=risk_data.get('year_1_risk'),
            year_5_risk=risk_data.get('year_5_risk'),
            risk_category=risk_data.get('risk_category'),
            job_category=risk_data.get('job_category')
        )
        session.add(job_search)
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        print(f"Error saving job search: {e}")
        return False
    finally:
        session.close()
        
def get_popular_searches(limit=5):
    """
    Get most popular job searches
    """
    session = Session()
    try:
        # SQL to count job titles and order by count
        query = text("""
            SELECT job_title, COUNT(*) as count 
            FROM job_searches 
            GROUP BY job_title 
            ORDER BY count DESC 
            LIMIT :limit
        """)
        
        result = session.execute(query, {"limit": limit})
        
        # Convert result to list of dictionaries
        popular_searches = [{"job_title": row[0], "count": row[1]} for row in result]
        
        session.close()
        return popular_searches
    except Exception as e:
        print(f"Error getting popular searches: {e}")
        return []
        
def get_highest_risk_jobs(limit=5):
    """
    Get jobs with highest average year 5 risk
    """
    session = Session()
    try:
        # SQL to get highest risk jobs
        query = text("""
            SELECT job_title, AVG(year_5_risk) as avg_risk 
            FROM job_searches 
            GROUP BY job_title 
            HAVING COUNT(*) > 1
            ORDER BY avg_risk DESC 
            LIMIT :limit
        """)
        
        result = session.execute(query, {"limit": limit})
        
        # Convert result to list of dictionaries
        high_risk_jobs = [{"job_title": row[0], "risk": float(row[1])} for row in result]
        
        session.close()
        return high_risk_jobs
    except Exception as e:
        print(f"Error getting highest risk jobs: {e}")
        return []
        
def get_lowest_risk_jobs(limit=5):
    """
    Get jobs with lowest average year 5 risk
    """
    session = Session()
    try:
        # SQL to get lowest risk jobs
        query = text("""
            SELECT job_title, AVG(year_5_risk) as avg_risk 
            FROM job_searches 
            GROUP BY job_title 
            HAVING COUNT(*) > 1
            ORDER BY avg_risk ASC 
            LIMIT :limit
        """)
        
        result = session.execute(query, {"limit": limit})
        
        # Convert result to list of dictionaries
        low_risk_jobs = [{"job_title": row[0], "risk": float(row[1])} for row in result]
        
        session.close()
        return low_risk_jobs
    except Exception as e:
        print(f"Error getting lowest risk jobs: {e}")
        return []
        
def get_recent_searches(limit=10):
    """
    Get recent job searches
    """
    session = Session()
    try:
        # Query recent searches
        recent_searches = session.query(JobSearch).order_by(
            JobSearch.timestamp.desc()
        ).limit(limit).all()
        
        # Convert to list of dictionaries
        results = []
        for search in recent_searches:
            time_diff = datetime.datetime.utcnow() - search.timestamp
            if time_diff.days > 0:
                time_ago = f"{time_diff.days} days ago"
            elif time_diff.seconds // 3600 > 0:
                time_ago = f"{time_diff.seconds // 3600} hours ago"
            elif time_diff.seconds // 60 > 0:
                time_ago = f"{time_diff.seconds // 60} minutes ago"
            else:
                time_ago = "Just now"
                
            results.append({
                "job_title": search.job_title,
                "year_1_risk": search.year_1_risk,
                "year_5_risk": search.year_5_risk,
                "risk_category": search.risk_category,
                "time_ago": time_ago
            })
        
        session.close()
        return results
    except Exception as e:
        print(f"Error getting recent searches: {e}")
        return []

def get_popular_searches(limit=5):
    """
    Get most popular job searches
    """
    try:
        session = Session()
        
        # SQL to count job titles and order by count
        query = text("""
            SELECT job_title, COUNT(*) as count 
            FROM job_searches 
            GROUP BY job_title 
            ORDER BY count DESC 
            LIMIT :limit
        """)
        
        result = session.execute(query, {"limit": limit})
        
        # Convert result to list of dictionaries
        popular_searches = [{"job_title": row[0], "count": row[1]} for row in result]
        
        # Close session
        session.close()
        
        return popular_searches
    except Exception as e:
        print(f"Error getting popular searches: {str(e)}")
        return []

def get_highest_risk_jobs(limit=5):
    """
    Get jobs with highest average year 5 risk
    """
    try:
        session = Session()
        
        # SQL to get highest risk jobs
        query = text("""
            SELECT job_title, AVG(year_5_risk) as avg_risk 
            FROM job_searches 
            GROUP BY job_title 
            HAVING COUNT(*) > 2
            ORDER BY avg_risk DESC 
            LIMIT :limit
        """)
        
        result = session.execute(query, {"limit": limit})
        
        # Convert result to list of dictionaries
        high_risk_jobs = [{"job_title": row[0], "avg_risk": row[1]} for row in result]
        
        # Close session
        session.close()
        
        return high_risk_jobs
    except Exception as e:
        print(f"Error getting highest risk jobs: {str(e)}")
        return []

def get_lowest_risk_jobs(limit=5):
    """
    Get jobs with lowest average year 5 risk
    """
    try:
        session = Session()
        
        # SQL to get lowest risk jobs
        query = text("""
            SELECT job_title, AVG(year_5_risk) as avg_risk 
            FROM job_searches 
            GROUP BY job_title 
            HAVING COUNT(*) > 2
            ORDER BY avg_risk ASC 
            LIMIT :limit
        """)
        
        result = session.execute(query, {"limit": limit})
        
        # Convert result to list of dictionaries
        low_risk_jobs = [{"job_title": row[0], "avg_risk": row[1]} for row in result]
        
        # Close session
        session.close()
        
        return low_risk_jobs
    except Exception as e:
        print(f"Error getting lowest risk jobs: {str(e)}")
        return []

def get_recent_searches(limit=10):
    """
    Get recent job searches
    """
    try:
        session = Session()
        
        # Query for recent searches
        recent_searches = session.query(JobSearch).order_by(
            JobSearch.timestamp.desc()
        ).limit(limit).all()
        
        # Convert to dictionaries
        results = [
            {
                "job_title": search.job_title,
                "year_1_risk": search.year_1_risk,
                "year_5_risk": search.year_5_risk,
                "timestamp": search.timestamp,
                "risk_category": search.risk_category
            }
            for search in recent_searches
        ]
        
        # Close session
        session.close()
        
        return results
    except Exception as e:
        print(f"Error getting recent searches: {str(e)}")
        return []