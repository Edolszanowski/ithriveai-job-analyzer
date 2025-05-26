"""
BLS (Bureau of Labor Statistics) API Connector
This module handles communication with the BLS API to fetch official employment data.
"""
import os
import requests
import json
import time
from typing import Dict, List, Any, Optional
import pandas as pd

# Cache to store API responses and reduce API calls
_api_cache = {}

def get_bls_data(series_ids: List[str], start_year: str, end_year: str) -> Dict[str, Any]:
    """
    Fetch data from BLS API for specified series IDs and date range.
    
    Args:
        series_ids: List of BLS series IDs to fetch
        start_year: Starting year for data (format: 'YYYY')
        end_year: Ending year for data (format: 'YYYY')
        
    Returns:
        Dictionary containing the API response
    """
    # Get API key from environment variable
    api_key = os.environ.get('BLS_API_KEY')
    
    # Check if we have a valid API key
    if not api_key:
        print("BLS_API_KEY environment variable is not set. Using sample data.")
        return {
            "status": "success",
            "responseTime": 100,
            "message": [],
            "Results": {
                "series": [
                    {
                        "seriesID": series_ids[0],
                        "data": [
                            {"year": "2020", "period": "A01", "periodName": "Annual", "value": "120000", "footnotes": []},
                            {"year": "2021", "period": "A01", "periodName": "Annual", "value": "125000", "footnotes": []},
                            {"year": "2022", "period": "A01", "periodName": "Annual", "value": "130000", "footnotes": []},
                            {"year": "2023", "period": "A01", "periodName": "Annual", "value": "135000", "footnotes": []},
                            {"year": "2024", "period": "A01", "periodName": "Annual", "value": "140000", "footnotes": []}
                        ]
                    }
                ]
            }
        }
    
    # Create cache key
    cache_key = f"{','.join(sorted(series_ids))}_{start_year}_{end_year}"
    
    # Return cached response if available
    if cache_key in _api_cache:
        return _api_cache[cache_key]
    
    # Define API endpoint
    url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
    
    # Prepare request payload
    payload = {
        "seriesid": series_ids,
        "startyear": start_year,
        "endyear": end_year,
        "registrationkey": api_key
    }
    
    # API request
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()  # Raise exception for HTTP errors
        data = response.json()
        
        # Cache the result
        _api_cache[cache_key] = data
        
        return data
    except requests.exceptions.RequestException as e:
        print(f"BLS API request failed: {e}")
        return {"status": "error", "message": str(e)}

def get_occupation_data(occ_code: str) -> Dict[str, Any]:
    """
    Get employment and wage data for a specific occupation code.
    
    Args:
        occ_code: SOC occupation code (e.g., '15-1252' for software developers)
        
    Returns:
        Dictionary with occupation data including employment and wage statistics
    """
    # BLS API currently doesn't support direct occupation queries through the time series API
    # This is a placeholder for future implementation using the right series IDs
    # For now, we'll use the OES data which would need to be mapped to occupation codes
    
    # Example mapping (would need to be expanded)
    series_mapping = {
        "15-1252": "OEU1025560000000015125201",  # Software developers - employment
        "11-9111": "OEU1025560000000011911101",  # Medical and health services managers - employment
    }
    
    if occ_code in series_mapping:
        series_id = series_mapping[occ_code]
        current_year = time.strftime("%Y")
        data = get_bls_data([series_id], str(int(current_year)-5), current_year)
        return parse_occupation_response(data, occ_code)
    else:
        return {"status": "error", "message": f"No BLS series ID mapping found for occupation code {occ_code}"}

def parse_occupation_response(response_data: Dict[str, Any], occ_code: str) -> Dict[str, Any]:
    """
    Parse BLS API response for occupation data.
    
    Args:
        response_data: Raw API response
        occ_code: The occupation code that was queried
        
    Returns:
        Structured dictionary with relevant occupation data
    """
    if response_data.get("status") != "REQUEST_SUCCEEDED":
        return {
            "status": "error",
            "message": f"BLS API request failed: {response_data.get('message', 'Unknown error')}"
        }
    
    # Extract series data
    results = []
    for series in response_data.get("Results", {}).get("series", []):
        series_id = series.get("seriesID")
        for item in series.get("data", []):
            results.append({
                "series_id": series_id,
                "year": item.get("year"),
                "period": item.get("period"),
                "value": item.get("value"),
                "footnotes": item.get("footnotes")
            })
    
    # Convert to DataFrame for easier analysis
    if results:
        df = pd.DataFrame(results)
        return {
            "status": "success",
            "occupation_code": occ_code,
            "data": df.to_dict(orient="records"),
            "latest_value": df.iloc[0]["value"] if not df.empty else None
        }
    else:
        return {
            "status": "error", 
            "message": "No data found for the requested occupation"
        }

def search_occupations(query: str) -> List[Dict[str, str]]:
    """
    Search for occupation codes matching the query.
    
    Args:
        query: Search terms for occupation
        
    Returns:
        List of matching occupation codes and titles
    """
    # This would normally query the BLS API, but we'll implement a simple lookup for now
    # In a full implementation, this would use the BLS API or a local database of SOC codes
    
    # Sample SOC codes and titles (abbreviated list)
    soc_codes = [
        {"code": "11-1011", "title": "Chief Executives"},
        {"code": "11-2011", "title": "Advertising and Promotions Managers"},
        {"code": "11-3031", "title": "Financial Managers"},
        {"code": "15-1252", "title": "Software Developers"},
        {"code": "15-1211", "title": "Computer Systems Analysts"},
        {"code": "15-1231", "title": "Computer Network Support Specialists"},
        {"code": "25-1011", "title": "Business Teachers, Postsecondary"},
        {"code": "25-2021", "title": "Elementary School Teachers"},
        {"code": "29-1051", "title": "Pharmacists"},
        {"code": "29-1141", "title": "Registered Nurses"},
        {"code": "41-3091", "title": "Sales Representatives of Services"},
        {"code": "43-4051", "title": "Customer Service Representatives"},
        {"code": "43-9021", "title": "Data Entry Keyers"},
        {"code": "53-3032", "title": "Heavy and Tractor-Trailer Truck Drivers"}
    ]
    
    # Simple search implementation
    query = query.lower()
    matches = [item for item in soc_codes if query in item["title"].lower()]
    
    return matches

def get_employment_projection(occ_code: str) -> Dict[str, Any]:
    """
    Get employment projections for an occupation.
    
    Args:
        occ_code: SOC occupation code
        
    Returns:
        Dictionary with employment projection data
    """
    # This would normally query the BLS Employment Projections API
    # Currently implementing a placeholder with sample data
    
    # Sample projections (would be replaced with actual API data)
    projections = {
        "15-1252": {
            "current_employment": 1365500,
            "projected_employment": 1572900,
            "percent_change": 15.2,
            "annual_job_openings": 162900
        },
        "29-1141": {
            "current_employment": 3130600,
            "projected_employment": 3458200,
            "percent_change": 10.5,
            "annual_job_openings": 203200
        },
        "43-9021": {
            "current_employment": 149900,
            "projected_employment": 112400,
            "percent_change": -25.0,
            "annual_job_openings": 14200
        }
    }
    
    if occ_code in projections:
        return {
            "status": "success",
            "occupation_code": occ_code,
            "projections": projections[occ_code]
        }
    else:
        return {
            "status": "error",
            "message": f"No employment projections found for occupation code {occ_code}"
        }

def check_api_connectivity() -> bool:
    """
    Check if the BLS API is accessible with the provided API key.
    
    Returns:
        Boolean indicating whether the API is accessible
    """
    try:
        # Test with a simple API request
        test_data = get_bls_data(["LAUCN040010000000005"], "2020", "2020")
        return test_data.get("status") == "REQUEST_SUCCEEDED"
    except Exception as e:
        print(f"BLS API connectivity check failed: {e}")
        return False