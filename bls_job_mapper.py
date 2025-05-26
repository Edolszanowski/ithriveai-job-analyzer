"""
BLS Job Mapper Module

This module provides mapping between job titles and BLS Standard Occupational Classification (SOC) codes.
It also handles storing and retrieving BLS data from the database for faster access.
"""

import os
import json
import datetime
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Float, MetaData, insert, select
import bls_connector

# Initialize database connection if available
database_url = os.environ.get('DATABASE_URL')
engine = None

if database_url:
    try:
        # Connection parameters for more reliable connection
        connect_args = {
            "connect_timeout": 10,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
            "sslmode": 'require'
        }
        
        # Create engine with connection pooling
        engine = create_engine(
            database_url, 
            connect_args=connect_args,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )
        
        # Create bls_data table if it doesn't exist
        metadata = MetaData()
        
        # Define the bls_job_data table
        bls_job_data = Table(
            'bls_job_data', 
            metadata,
            Column('id', Integer, primary_key=True),
            Column('occupation_code', String(10), nullable=False),
            Column('job_title', String(255), nullable=False),
            Column('standardized_title', String(255), nullable=False),
            Column('job_category', String(100)),
            Column('current_employment', Integer),
            Column('projected_employment', Integer),
            Column('percent_change', Float),
            Column('annual_job_openings', Integer),
            Column('median_wage', Float),
            Column('last_updated', String(10), nullable=False)
        )
        
        # Create the table if it doesn't exist
        metadata.create_all(engine, checkfirst=True)
        
    except Exception as e:
        print(f"Error setting up database connection in bls_job_mapper: {str(e)}")
        engine = None

# Job title to SOC code mapping
# This is a starting point - the system will build this mapping over time
JOB_TITLE_TO_SOC = {
    "software developer": "15-1252",
    "software engineer": "15-1252",
    "programmer": "15-1251",
    "web developer": "15-1254",
    "registered nurse": "29-1141",
    "nurse": "29-1141",
    "teacher": "25-2021",  # Elementary School Teachers
    "elementary school teacher": "25-2021",
    "high school teacher": "25-2031",
    "lawyer": "23-1011",
    "attorney": "23-1011",
    "doctor": "29-1221",  # Family Medicine Physicians
    "physician": "29-1221",
    "accountant": "13-2011",
    "project manager": "11-3021",
    "product manager": "11-2021",
    "marketing manager": "11-2021",
    "retail salesperson": "41-2031",
    "cashier": "41-2011",
    "customer service representative": "43-4051",
    "truck driver": "53-3032",
    "receptionist": "43-4171",
    "data scientist": "15-2051",
    "data analyst": "15-2041",
    "business analyst": "13-1111",
    "financial analyst": "13-2051",
    "human resources specialist": "13-1071",
    "graphic designer": "27-1024",
    "police officer": "33-3051",
    "chef": "35-1011",
    "cook": "35-2014",
    "waiter": "35-3031",
    "waitress": "35-3031",
    "janitor": "37-2011",
    "administrative assistant": "43-6011",
    "executive assistant": "43-6011",
    "dental hygienist": "29-1292",
    "electrician": "47-2111",
    "plumber": "47-2152",
    "carpenter": "47-2031",
    "construction worker": "47-2061",
    "mechanic": "49-3023",
    "automotive mechanic": "49-3023",
    "taxi driver": "53-3054",
    "uber driver": "53-3054",
    "journalist": "27-3023",
    "reporter": "27-3023",
    "writer": "27-3042",
    "editor": "27-3041",
    "photographer": "27-4021",
    "court reporter": "23-2011",
    "stenographer": "23-2011",
    "digital court reporter": "23-2011",
    "travel agent": "41-4012"
}

# Job category mapping - helps group similar occupations
SOC_TO_CATEGORY = {
    "15-": "Computer and Mathematical",
    "11-": "Management",
    "13-": "Business and Financial",
    "17-": "Architecture and Engineering",
    "19-": "Life, Physical, and Social Science",
    "21-": "Community and Social Service",
    "23-": "Legal",
    "25-": "Educational Instruction",
    "27-": "Arts, Design, Entertainment, Sports, and Media",
    "29-": "Healthcare Practitioners",
    "31-": "Healthcare Support",
    "33-": "Protective Service",
    "35-": "Food Preparation and Serving",
    "37-": "Building and Grounds Cleaning and Maintenance",
    "39-": "Personal Care and Service",
    "41-": "Sales and Related",
    "43-": "Office and Administrative Support",
    "45-": "Farming, Fishing, and Forestry",
    "47-": "Construction and Extraction",
    "49-": "Installation, Maintenance, and Repair",
    "51-": "Production",
    "53-": "Transportation and Material Moving"
}

def get_job_category(occupation_code: str) -> str:
    """
    Get the job category based on SOC code prefix.
    
    Args:
        occupation_code: SOC occupation code (e.g., "15-1252")
        
    Returns:
        Job category string
    """
    for prefix, category in SOC_TO_CATEGORY.items():
        if occupation_code.startswith(prefix):
            return category
    return "General"

def standardize_job_title(title: str) -> str:
    """
    Standardize job title format for consistent mapping.
    
    Args:
        title: Raw job title
        
    Returns:
        Standardized job title
    """
    # Convert to lowercase and strip extra spaces
    standardized = title.lower().strip()
    
    # Remove common suffixes/prefixes that might affect matching
    suffixes = [" i", " ii", " iii", " iv", " v", " specialist", " assistant", " associate", " senior", " junior", " lead"]
    for suffix in suffixes:
        if standardized.endswith(suffix):
            standardized = standardized[:-len(suffix)]
            break
    
    return standardized

def find_occupation_code(job_title: str) -> Tuple[str, str, str]:
    """
    Find SOC occupation code for a job title.
    
    Args:
        job_title: Job title to search for
        
    Returns:
        Tuple of (SOC code, standardized title, job category)
    """
    # Standardize the job title
    std_title = standardize_job_title(job_title)
    
    # Direct lookup in our mapping
    if std_title in JOB_TITLE_TO_SOC:
        soc_code = JOB_TITLE_TO_SOC[std_title]
        category = get_job_category(soc_code)
        return soc_code, job_title, category
    
    # Search for partial matches
    for key, code in JOB_TITLE_TO_SOC.items():
        if key in std_title or std_title in key:
            category = get_job_category(code)
            return code, job_title, category
    
    # If not found, use BLS API to search
    matches = bls_connector.search_occupations(job_title)
    if matches:
        best_match = matches[0]  # Take the first match
        soc_code = best_match["code"]
        std_title = best_match["title"]
        category = get_job_category(soc_code)
        
        # Add to our mapping for future use
        JOB_TITLE_TO_SOC[standardize_job_title(std_title)] = soc_code
        
        return soc_code, std_title, category
    
    # If all else fails, return a generic code and category
    return "00-0000", job_title, "General"

def get_bls_data_from_db(occupation_code: str) -> Optional[Dict[str, Any]]:
    """
    Get BLS data from database if available.
    
    Args:
        occupation_code: SOC occupation code
        
    Returns:
        Dictionary of BLS data or None if not found
    """
    if not engine:
        return None
        
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT * FROM bls_job_data 
                WHERE occupation_code = :code
                ORDER BY last_updated DESC
                LIMIT 1
            """)
            result = conn.execute(query, {"code": occupation_code})
            row = result.fetchone()
            
            if row:
                # Convert SQLAlchemy row to dictionary
                data = dict(row._mapping)
                
                # Check if data is fresh (less than 90 days old)
                last_updated = datetime.datetime.strptime(data["last_updated"], "%Y-%m-%d")
                days_since_update = (datetime.datetime.now() - last_updated).days
                
                if days_since_update < 90:
                    return data
                    
        # Data not found or too old
        return None
        
    except Exception as e:
        print(f"Error retrieving BLS data from database: {str(e)}")
        return None

def save_bls_data_to_db(data: Dict[str, Any]) -> bool:
    """
    Save BLS data to database.
    
    Args:
        data: Dictionary of BLS data
        
    Returns:
        Boolean indicating success
    """
    if not engine:
        return False
        
    try:
        # Set current date as last_updated if not provided
        if "last_updated" not in data:
            data["last_updated"] = datetime.datetime.now().strftime("%Y-%m-%d")
            
        # Insert data into bls_job_data table
        with engine.connect() as conn:
            # Check if record already exists
            query = text("""
                SELECT id FROM bls_job_data 
                WHERE occupation_code = :occupation_code
            """)
            result = conn.execute(query, {"occupation_code": data["occupation_code"]})
            row = result.fetchone()
            
            if row:
                # Update existing record
                update_query = text("""
                    UPDATE bls_job_data
                    SET 
                        job_title = :job_title,
                        standardized_title = :standardized_title,
                        job_category = :job_category,
                        current_employment = :current_employment,
                        projected_employment = :projected_employment,
                        percent_change = :percent_change,
                        annual_job_openings = :annual_job_openings,
                        median_wage = :median_wage,
                        last_updated = :last_updated
                    WHERE occupation_code = :occupation_code
                """)
                conn.execute(update_query, data)
            else:
                # Insert new record
                insert_query = text("""
                    INSERT INTO bls_job_data (
                        occupation_code, job_title, standardized_title, job_category,
                        current_employment, projected_employment, percent_change,
                        annual_job_openings, median_wage, last_updated
                    ) VALUES (
                        :occupation_code, :job_title, :standardized_title, :job_category,
                        :current_employment, :projected_employment, :percent_change,
                        :annual_job_openings, :median_wage, :last_updated
                    )
                """)
                conn.execute(insert_query, data)
                
            conn.commit()
            
        return True
        
    except Exception as e:
        print(f"Error saving BLS data to database: {str(e)}")
        return False

def fetch_bls_data(job_title: str) -> Dict[str, Any]:
    """
    Fetch BLS data for a job title, either from database or API.
    
    Args:
        job_title: Job title to fetch data for
        
    Returns:
        Dictionary with BLS data
    """
    # Find occupation code for job title
    occupation_code, standardized_title, job_category = find_occupation_code(job_title)
    
    # Try to get data from database first
    db_data = get_bls_data_from_db(occupation_code)
    if db_data:
        print(f"Using cached BLS data for {standardized_title} from database")
        return db_data
    
    # If not in database, fetch from BLS API
    print(f"Fetching fresh BLS data for {standardized_title} (SOC: {occupation_code})")
    
    # Get occupation data
    occupation_data = bls_connector.get_occupation_data(occupation_code)
    
    # Get employment projections
    projection_data = bls_connector.get_employment_projection(occupation_code)
    
    # Extract relevant data
    current_employment = projection_data.get("projections", {}).get("current_employment")
    projected_employment = projection_data.get("projections", {}).get("projected_employment")
    percent_change = projection_data.get("projections", {}).get("percent_change")
    annual_job_openings = projection_data.get("projections", {}).get("annual_job_openings")
    median_wage = occupation_data.get("latest_value")
    
    # Prepare data for storage
    bls_data = {
        "occupation_code": occupation_code,
        "job_title": job_title,
        "standardized_title": standardized_title,
        "job_category": job_category,
        "current_employment": current_employment,
        "projected_employment": projected_employment,
        "percent_change": percent_change,
        "annual_job_openings": annual_job_openings,
        "median_wage": median_wage,
        "last_updated": datetime.datetime.now().strftime("%Y-%m-%d")
    }
    
    # Save to database for future use
    save_bls_data_to_db(bls_data)
    
    return bls_data

def generate_risk_factors(job_title: str, job_category: str) -> List[str]:
    """
    Generate AI displacement risk factors for a job based on its category.
    
    Args:
        job_title: Job title
        job_category: Job category
        
    Returns:
        List of risk factors
    """
    # Generic risk factors that apply to many jobs
    generic_risk_factors = [
        "Routine data processing tasks can be automated",
        "Predictable decision-making components can be handled by AI",
        "Standardized documentation and reporting can be automated",
        "Basic customer interactions can be managed by AI systems"
    ]
    
    # Category-specific risk factors
    category_risk_factors = {
        "Computer and Mathematical": [
            "Code generation AI can produce routine programming solutions",
            "Automated testing and debugging reduces manual work",
            "Basic website and application development increasingly automated",
            "Technical documentation can be generated by AI"
        ],
        "Management": [
            "AI tools can handle resource allocation and scheduling",
            "Performance monitoring and reporting can be automated",
            "Basic project tracking requires less human oversight",
            "Standard communication can be drafted by AI"
        ],
        "Business and Financial": [
            "Financial analysis and modeling increasingly automated",
            "Transaction processing and auditing can be handled by AI",
            "Market research data collection and basic analysis automated",
            "Standard financial reporting can be generated by AI"
        ],
        "Legal": [
            "Document review and contract analysis increasingly automated",
            "Legal research can be accelerated with AI assistance",
            "Standard legal document generation handled by AI",
            "Case outcome prediction becoming more automated"
        ],
        "Educational Instruction": [
            "Basic content delivery can be automated through online platforms",
            "Standardized assessment and grading increasingly automated",
            "Administrative tasks can be handled by AI systems",
            "Some tutoring functions can be performed by AI"
        ],
        "Healthcare Practitioners": [
            "Administrative tasks and documentation increasingly automated",
            "Basic diagnostic assistance provided by AI",
            "Patient scheduling and management can be automated",
            "Some monitoring functions can be performed by AI systems"
        ]
    }
    
    # Get category-specific factors or use generic ones
    specific_factors = category_risk_factors.get(job_category, [])
    
    # Combine generic and specific factors, ensuring we have at least 4
    risk_factors = specific_factors + generic_risk_factors
    
    # Return the first 4-5 factors
    return risk_factors[:min(5, len(risk_factors))]

def generate_protective_factors(job_title: str, job_category: str) -> List[str]:
    """
    Generate protective factors against AI displacement based on job category.
    
    Args:
        job_title: Job title
        job_category: Job category
        
    Returns:
        List of protective factors
    """
    # Generic protective factors that apply to many jobs
    generic_protective_factors = [
        "Complex decision-making in ambiguous situations",
        "Creative problem-solving in unpredictable environments",
        "Building relationships and emotional intelligence",
        "Adaptability to changing circumstances and requirements"
    ]
    
    # Category-specific protective factors
    category_protective_factors = {
        "Computer and Mathematical": [
            "Advanced systems architecture and design",
            "Novel algorithm development and implementation",
            "Complex integration of disparate technologies",
            "Innovative problem-solving in unique technical contexts"
        ],
        "Management": [
            "Strategic leadership and organizational vision",
            "Complex stakeholder relationship management",
            "Change management in ambiguous environments",
            "Cultivating team culture and interpersonal dynamics"
        ],
        "Business and Financial": [
            "Complex strategic financial planning",
            "Contextual business judgment requiring broad knowledge",
            "Negotiation and persuasion in high-stakes situations",
            "Novel market opportunity identification"
        ],
        "Legal": [
            "Complex legal strategy development",
            "Persuasive courtroom advocacy",
            "Nuanced client counseling in ambiguous situations",
            "Novel legal theory development"
        ],
        "Educational Instruction": [
            "Personalized teaching adapted to individual student needs",
            "Creating engaging learning environments",
            "Mentoring and developing student potential",
            "Addressing complex behavioral and learning challenges"
        ],
        "Healthcare Practitioners": [
            "Complex clinical judgment in uncertain situations",
            "Empathetic patient care requiring emotional intelligence",
            "Physical assessment and intervention skills",
            "Integrated treatment planning for complex cases"
        ]
    }
    
    # Get category-specific factors or use generic ones
    specific_factors = category_protective_factors.get(job_category, [])
    
    # Combine specific and generic factors, ensuring we have at least 4
    protective_factors = specific_factors + generic_protective_factors
    
    # Return the first 4-5 factors
    return protective_factors[:min(5, len(protective_factors))]

def calculate_ai_risk(job_title: str, job_category: str) -> Dict[str, Any]:
    """
    Calculate AI displacement risk for a job based on its category.
    
    Args:
        job_title: Job title
        job_category: Job category
        
    Returns:
        Dictionary with risk scores and analysis
    """
    # Risk scores by category (1-year and 5-year risks)
    category_risk_scores = {
        "Computer and Mathematical": (30.0, 50.0),
        "Management": (25.0, 45.0),
        "Business and Financial": (35.0, 55.0),
        "Architecture and Engineering": (20.0, 40.0),
        "Life, Physical, and Social Science": (15.0, 35.0),
        "Community and Social Service": (15.0, 30.0),
        "Legal": (25.0, 45.0),
        "Educational Instruction": (20.0, 35.0),
        "Arts, Design, Entertainment, Sports, and Media": (30.0, 50.0),
        "Healthcare Practitioners": (15.0, 30.0),
        "Healthcare Support": (20.0, 40.0),
        "Protective Service": (15.0, 30.0),
        "Food Preparation and Serving": (35.0, 60.0),
        "Building and Grounds Cleaning and Maintenance": (30.0, 55.0),
        "Personal Care and Service": (25.0, 45.0),
        "Sales and Related": (40.0, 65.0),
        "Office and Administrative Support": (45.0, 70.0),
        "Farming, Fishing, and Forestry": (30.0, 50.0),
        "Construction and Extraction": (20.0, 40.0),
        "Installation, Maintenance, and Repair": (25.0, 45.0),
        "Production": (40.0, 65.0),
        "Transportation and Material Moving": (35.0, 60.0)
    }
    
    # Default risk scores if category not found
    default_risk = (35.0, 55.0)
    
    # Get risk scores for the job category
    year_1_risk, year_5_risk = category_risk_scores.get(job_category, default_risk)
    
    # Determine risk category based on 5-year risk
    if year_5_risk < 30:
        risk_category = "Low"
    elif year_5_risk < 50:
        risk_category = "Moderate"
    elif year_5_risk < 70:
        risk_category = "High"
    else:
        risk_category = "Very High"
    
    # Generate risk and protective factors
    risk_factors = generate_risk_factors(job_title, job_category)
    protective_factors = generate_protective_factors(job_title, job_category)
    
    # Create analysis text
    analysis = f"The role of {job_title} faces a {risk_category.lower()} risk of AI displacement. "
    
    if risk_category in ["Low", "Moderate"]:
        analysis += "While certain routine aspects of the role may be automated, the core functions requiring human judgment, creativity, and interpersonal skills remain difficult to replace with AI. "
    else:
        analysis += "Many aspects of this role involve predictable, routine tasks that are increasingly being automated by AI systems. "
    
    analysis += "Professionals in this field can increase their resilience by developing skills in areas that complement AI rather than compete with it."
    
    # Return complete risk assessment
    return {
        "year_1_risk": year_1_risk,
        "year_5_risk": year_5_risk,
        "risk_category": risk_category,
        "risk_factors": risk_factors,
        "protective_factors": protective_factors,
        "analysis": analysis
    }

def get_complete_job_data(job_title: str) -> Dict[str, Any]:
    """
    Get comprehensive job data including BLS statistics and AI risk analysis.
    
    Args:
        job_title: The job title to analyze
        
    Returns:
        Dictionary with combined job data
    """
    # First get the SOC code and category from our mapping
    soc_code, standardized_title, job_category = find_occupation_code(job_title)
    
    # Then fetch BLS data for the job
    bls_data = fetch_bls_data(job_title)
    
    # Use the category from our mapping (more reliable than BLS data)
    job_category = job_category if job_category != "General" else bls_data.get("job_category", "General")
    
    # Calculate AI displacement risk
    risk_data = calculate_ai_risk(job_title, job_category)
    
    # Combine all data
    result = {
        # Job information
        "job_title": bls_data.get("standardized_title", job_title),
        "occupation_code": soc_code,
        "job_category": job_category,
        
        # Employment data
        "employment": bls_data.get("current_employment"),
        "projected_employment": bls_data.get("projected_employment"),
        "employment_change_percent": bls_data.get("percent_change"),
        "annual_job_openings": bls_data.get("annual_job_openings"),
        "median_wage": bls_data.get("median_wage"),
        
        # Risk assessment
        "year_1_risk": risk_data.get("year_1_risk"),
        "year_5_risk": risk_data.get("year_5_risk"),
        "risk_category": risk_data.get("risk_category"),
        "risk_factors": risk_data.get("risk_factors"),
        "protective_factors": risk_data.get("protective_factors"),
        "analysis": risk_data.get("analysis"),
        
        # Metadata
        "last_updated": bls_data.get("last_updated")
    }
    
    return result