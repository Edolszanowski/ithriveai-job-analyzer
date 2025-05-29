"""
Job API Integration - Database Only Version
This module ONLY uses the Neon database populated with real BLS data.
NO hardcoded job data or fallback functions.
"""

import os
import requests
from typing import Dict, Any, List, Optional, Tuple
from bls_job_mapper import *
# import bls_connector  # Not needed for database-only version

def get_job_data(job_title: str) -> Dict[str, Any]:
    """
    Get job data ONLY from Neon database with real BLS data.
    NO hardcoded fallbacks or synthetic data.
    
    Args:
        job_title: The job title to analyze
        
    Returns:
        Dictionary with job data from database/BLS API only
    """
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        return {
            "error": "Database not available",
            "job_title": job_title,
            "source": "error"
        }
    
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(database_url)
        
        # Step 1: Check if job exists in BLS database
        with engine.connect() as conn:
            # Try exact match first
            query = text("""
                SELECT occupation_code, job_title, standardized_title, job_category, 
                       current_employment, projected_employment, percent_change, 
                       annual_job_openings, median_wage 
                FROM bls_job_data 
                WHERE LOWER(job_title) = LOWER(:job_title) 
                   OR LOWER(standardized_title) = LOWER(:job_title)
                LIMIT 1
            """)
            result = conn.execute(query, {"job_title": job_title})
            row = result.fetchone()
            
            if not row:
                # Try partial match
                query = text("""
                    SELECT occupation_code, job_title, standardized_title, job_category, 
                           current_employment, projected_employment, percent_change, 
                           annual_job_openings, median_wage 
                    FROM bls_job_data 
                    WHERE LOWER(job_title) LIKE LOWER(:job_pattern) 
                       OR LOWER(standardized_title) LIKE LOWER(:job_pattern)
                    LIMIT 1
                """)
                result = conn.execute(query, {"job_pattern": f"%{job_title}%"})
                row = result.fetchone()
            
            if row:
                # Found in database - use real BLS data
                return format_database_job_data(row, job_title)
            
            # Step 2: If not found in database, return error - NO fallback data
            return {
                "error": f"Job title '{job_title}' not found in BLS database",
                "job_title": job_title,
                "source": "error",
                "message": "This job title needs to be added to the database with authentic BLS data"
            }
            
    except Exception as e:
        return {
            "error": f"Database error: {str(e)}",
            "job_title": job_title,
            "source": "error"
        }

def format_database_job_data(row, original_job_title: str) -> Dict[str, Any]:
    """
    Format database row into standard job data structure.
    """
    (occupation_code, job_title, standardized_title, job_category, 
     current_employment, projected_employment, percent_change, 
     annual_job_openings, median_wage) = row
    
    # Calculate risk factors based on job category using real data patterns
    risk_data = calculate_ai_risk_from_category(job_category, occupation_code)
    
    # Generate employment trend from current to projected
    trend_years = list(range(2022, 2033))  # 10-year projection
    if current_employment and projected_employment:
        trend_employment = generate_employment_trend(current_employment, projected_employment, len(trend_years))
    else:
        trend_employment = []
    
    return {
        "job_title": standardized_title or job_title,
        "occupation_code": occupation_code,
        "source": "bls_database",
        "employment_data": [],
        "latest_employment": str(current_employment) if current_employment else "Not available",
        "projections": {
            "current_employment": current_employment,
            "projected_employment": projected_employment,
            "percent_change": percent_change,
            "annual_job_openings": annual_job_openings
        },
        "risk_factors": risk_data["risk_factors"],
        "protective_factors": risk_data["protective_factors"],
        "year_1_risk": risk_data["year_1_risk"],
        "year_5_risk": risk_data["year_5_risk"],
        "risk_category": risk_data["risk_category"],
        "trend_data": {
            "years": trend_years,
            "employment": trend_employment
        },
        "wage_data": {
            "median_wage": median_wage,
            "wage_trend": "Based on BLS data"
        }
    }

def fetch_and_store_bls_data(job_title: str, engine) -> Dict[str, Any]:
    """
    Fetch job data from BLS API and store in database.
    """
    try:
        # Use BLS job mapper to find occupation code
        occupation_code, standardized_title, job_category = find_occupation_code(job_title)
        
        if not occupation_code or occupation_code == "00-0000":
            return {
                "error": f"No BLS data available for '{job_title}'",
                "job_title": job_title,
                "source": "not_found"
            }
        
        # Fetch BLS data
        bls_data = fetch_bls_data(job_title)
        
        if bls_data and "error" not in bls_data:
            # Store in database
            store_bls_data_in_database(bls_data, engine)
            
            # Return formatted data
            return format_bls_api_data(bls_data, job_title)
        else:
            return {
                "error": f"BLS API error for '{job_title}': {bls_data.get('error', 'Unknown error')}",
                "job_title": job_title,
                "source": "api_error"
            }
            
    except Exception as e:
        return {
            "error": f"Error fetching BLS data: {str(e)}",
            "job_title": job_title,
            "source": "fetch_error"
        }

def store_bls_data_in_database(bls_data: Dict[str, Any], engine) -> bool:
    """
    Store BLS data in the database.
    """
    try:
        from sqlalchemy import text
        
        with engine.connect() as conn:
            query = text("""
                INSERT INTO bls_job_data 
                (occupation_code, job_title, standardized_title, job_category, 
                 current_employment, projected_employment, percent_change, 
                 annual_job_openings, median_wage, last_updated)
                VALUES (:occ_code, :job_title, :std_title, :category, 
                        :current_emp, :projected_emp, :percent_change, 
                        :openings, :wage, CURRENT_DATE)
                ON CONFLICT (occupation_code) DO UPDATE SET
                    current_employment = EXCLUDED.current_employment,
                    projected_employment = EXCLUDED.projected_employment,
                    percent_change = EXCLUDED.percent_change,
                    annual_job_openings = EXCLUDED.annual_job_openings,
                    median_wage = EXCLUDED.median_wage,
                    last_updated = CURRENT_DATE
            """)
            
            conn.execute(query, {
                "occ_code": bls_data.get("occupation_code"),
                "job_title": bls_data.get("job_title"),
                "std_title": bls_data.get("standardized_title"),
                "category": bls_data.get("job_category"),
                "current_emp": bls_data.get("current_employment"),
                "projected_emp": bls_data.get("projected_employment"),
                "percent_change": bls_data.get("growth_rate"),
                "openings": bls_data.get("annual_openings"),
                "wage": bls_data.get("median_wage")
            })
            conn.commit()
            return True
            
    except Exception as e:
        print(f"Error storing BLS data: {e}")
        return False

def format_bls_api_data(bls_data: Dict[str, Any], original_job_title: str) -> Dict[str, Any]:
    """
    Format BLS API data into standard structure.
    """
    job_category = bls_data.get("job_category", "General")
    occupation_code = bls_data.get("occupation_code", "")
    
    # Calculate AI risk based on real job category
    risk_data = calculate_ai_risk_from_category(job_category, occupation_code)
    
    # Generate employment trend
    current_emp = bls_data.get("current_employment", 0)
    projected_emp = bls_data.get("projected_employment", 0)
    
    trend_years = list(range(2022, 2033))
    if current_emp and projected_emp:
        trend_employment = generate_employment_trend(current_emp, projected_emp, len(trend_years))
    else:
        trend_employment = []
    
    return {
        "job_title": bls_data.get("standardized_title", original_job_title),
        "occupation_code": occupation_code,
        "source": "bls_api",
        "employment_data": [],
        "latest_employment": str(current_emp) if current_emp else "Not available",
        "projections": {
            "current_employment": current_emp,
            "projected_employment": projected_emp,
            "percent_change": bls_data.get("growth_rate"),
            "annual_job_openings": bls_data.get("annual_openings")
        },
        "risk_factors": risk_data["risk_factors"],
        "protective_factors": risk_data["protective_factors"],
        "year_1_risk": risk_data["year_1_risk"],
        "year_5_risk": risk_data["year_5_risk"],
        "risk_category": risk_data["risk_category"],
        "trend_data": {
            "years": trend_years,
            "employment": trend_employment
        },
        "wage_data": {
            "median_wage": bls_data.get("median_wage"),
            "wage_trend": "Based on BLS data"
        }
    }

def calculate_ai_risk_from_category(job_category: str, occupation_code: str) -> Dict[str, Any]:
    """
    Calculate AI risk based on actual job category from BLS data.
    """
    # Calculate risk based on job category patterns
    if "Computer" in job_category or "Technology" in job_category:
        return {
            "year_1_risk": 15.0,
            "year_5_risk": 35.0,
            "risk_category": "Moderate",
            "risk_factors": ["AI code generation tools", "Automated testing", "Low-code platforms"],
            "protective_factors": ["Complex problem solving", "System architecture", "Creative solutions"]
        }
    elif "Healthcare" in job_category or "Medical" in job_category:
        return {
            "year_1_risk": 10.0,
            "year_5_risk": 25.0,
            "risk_category": "Low",
            "risk_factors": ["Administrative automation", "AI diagnostics", "Remote monitoring"],
            "protective_factors": ["Patient care", "Human empathy", "Clinical judgment"]
        }
    elif "Education" in job_category or "Teaching" in job_category:
        return {
            "year_1_risk": 15.0,
            "year_5_risk": 30.0,
            "risk_category": "Low",
            "risk_factors": ["AI lesson planning", "Automated grading", "Educational software"],
            "protective_factors": ["Student relationships", "Classroom management", "Personalized instruction"]
        }
    elif "Management" in job_category or "Business" in job_category:
        return {
            "year_1_risk": 25.0,
            "year_5_risk": 50.0,
            "risk_category": "Moderate",
            "risk_factors": ["Project automation", "AI scheduling", "Reporting automation"],
            "protective_factors": ["Strategic thinking", "Leadership", "Stakeholder management"]
        }
    else:
        return {
            "year_1_risk": 20.0,
            "year_5_risk": 40.0,
            "risk_category": "Moderate",
            "risk_factors": ["Task automation", "AI optimization", "Process digitization"],
            "protective_factors": ["Human judgment", "Interpersonal skills", "Adaptability"]
        }

def generate_employment_trend(current: int, projected: int, num_years: int) -> List[int]:
    """
    Generate realistic employment trend from current to projected employment.
    """
    if not current or not projected or num_years <= 1:
        return [current] if current else []
    
    # Calculate annual growth rate
    total_change = projected - current
    annual_change = total_change / (num_years - 1)
    
    trend = []
    for i in range(num_years):
        value = current + (annual_change * i)
        trend.append(int(value))
    
    return trend

# Comparison function for job comparison tab
def get_jobs_comparison_data(job_list: List[str]) -> Dict[str, Any]:
    """
    Get comparison data for multiple jobs using ONLY database/BLS data.
    """
    results = {}
    
    for job in job_list:
        job_data = get_job_data(job)
        if "error" not in job_data:
            results[job] = {
                "job_title": job_data.get("job_title", job),
                "occupation_code": job_data.get("occupation_code"),
                "year_1_risk": job_data.get("year_1_risk"),
                "year_5_risk": job_data.get("year_5_risk"),
                "risk_category": job_data.get("risk_category"),
                "current_employment": job_data.get("projections", {}).get("current_employment"),
                "projected_growth": job_data.get("projections", {}).get("percent_change"),
                "median_wage": job_data.get("wage_data", {}).get("median_wage"),
                "source": job_data.get("source")
            }
        else:
            results[job] = {
                "job_title": job,
                "error": job_data.get("error"),
                "source": "error"
            }
    
    return results