"""
BLS Employment Data Module
This module provides hardcoded BLS employment statistics for common job titles
to ensure data availability when the API might not return complete information.
"""

# Dictionary of job titles with their BLS employment statistics
EMPLOYMENT_DATA = {
    'project manager': {
        'employment': 571300,
        'annual_job_openings': 47500,
        'employment_change_percent': 9.8,
        'occ_code': '11-3021'
    },
    'web developer': {
        'employment': 190200,
        'annual_job_openings': 21800, 
        'employment_change_percent': 16.3,
        'occ_code': '15-1254'
    },
    'ui developer': {
        'employment': 185700,
        'annual_job_openings': 21000,
        'employment_change_percent': 15.0,
        'occ_code': '15-1255'
    },
    'software engineer': {
        'employment': 1552600,
        'annual_job_openings': 162900,
        'employment_change_percent': 25.6,
        'occ_code': '15-1252'
    },
    'nurse': {
        'employment': 3130600,
        'annual_job_openings': 193100,
        'employment_change_percent': 6.2,
        'occ_code': '29-1141'
    },
    'teacher': {
        'employment': 1789000,
        'annual_job_openings': 156300,
        'employment_change_percent': 3.9,
        'occ_code': '25-2021'
    },
    'dentist': {
        'employment': 146200,
        'annual_job_openings': 5000,
        'employment_change_percent': 3.0,
        'occ_code': '29-1021'
    },
    'cook': {
        'employment': 880000,
        'annual_job_openings': 148700,
        'employment_change_percent': 5.2,
        'occ_code': '35-2014'
    },
    'customer service representative': {
        'employment': 2892900,
        'annual_job_openings': 361700,
        'employment_change_percent': -4.0,
        'occ_code': '43-4051'
    }
}

def get_employment_data(job_title):
    """
    Get BLS employment data for a specific job title
    
    Args:
        job_title (str): The job title to look up
        
    Returns:
        dict: Employment data if available, or None
    """
    # Try exact match first, then case-insensitive match
    job_title_lower = job_title.lower()
    
    if job_title_lower in EMPLOYMENT_DATA:
        return EMPLOYMENT_DATA[job_title_lower]
    
    # Try partial matches
    for title, data in EMPLOYMENT_DATA.items():
        if title in job_title_lower or job_title_lower in title:
            return data
    
    return None