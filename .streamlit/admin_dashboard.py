"""
Admin Dashboard for Job Title Management
Streamlit app for adding job title aliases and managing the database
"""

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os

# Database connection
def get_db_connection():
    """Get database connection"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        st.error("DATABASE_URL environment variable not set")
        return None
    return create_engine(database_url)

def get_existing_jobs():
    """Get all existing jobs from database"""
    engine = get_db_connection()
    if not engine:
        return []
    
    try:
        with engine.connect() as conn:
            query = text("SELECT job_title, occupation_code FROM bls_job_data ORDER BY job_title")
            result = conn.execute(query)
            return [(row[0], row[1]) for row in result.fetchall()]
    except Exception as e:
        st.error(f"Error fetching jobs: {e}")
        return []

def add_job_alias(alias_title, target_job_title):
    """Add a job title alias"""
    engine = get_db_connection()
    if not engine:
        return False
    
    try:
        with engine.connect() as conn:
            # Add to job_titles table for autocomplete
            query = text("INSERT INTO job_titles (title) VALUES (:title)")
            conn.execute(query, {"title": alias_title})
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error adding alias: {e}")
        return False

def search_existing_aliases(search_term):
    """Search existing job title aliases"""
    engine = get_db_connection()
    if not engine:
        return []
    
    try:
        with engine.connect() as conn:
            query = text("SELECT title FROM job_titles WHERE LOWER(title) LIKE LOWER(:search) ORDER BY title")
            result = conn.execute(query, {"search": f"%{search_term}%"})
            return [row[0] for row in result.fetchall()]
    except Exception as e:
        st.error(f"Error searching aliases: {e}")
        return []

# Streamlit App
st.set_page_config(page_title="Job Title Admin Dashboard", page_icon="⚙️")

st.title("⚙️ Job Title Admin Dashboard")
st.markdown("**Manage job title aliases and database entries**")

# Tabs for different functions
tab1, tab2, tab3 = st.tabs(["Add Job Alias", "View Database", "Search Aliases"])

with tab1:
    st.header("Add New Job Title Alias")
    st.markdown("Add alternative job titles that should map to existing database entries")
    
    # Get existing jobs for dropdown
    existing_jobs = get_existing_jobs()
    job_options = [f"{job[0]} ({job[1]})" for job in existing_jobs]
    
    if existing_jobs:
        col1, col2 = st.columns(2)
        
        with col1:
            new_alias = st.text_input("New Job Title Alias", placeholder="e.g., Software Engineer")
            
        with col2:
            target_job = st.selectbox("Maps to Existing Job", job_options)
        
        if st.button("Add Alias", type="primary"):
            if new_alias and target_job:
                target_job_title = target_job.split(" (")[0]  # Extract job title from display string
                
                if add_job_alias(new_alias, target_job_title):
                    st.success(f"✅ Added alias '{new_alias}' → '{target_job_title}'")
                    st.rerun()
            else:
                st.error("Please enter both alias and target job")
    else:
        st.warning("No existing jobs found in database")

with tab2:
    st.header("Current Database Jobs")
    
    existing_jobs = get_existing_jobs()
    if existing_jobs:
        df = pd.DataFrame(existing_jobs, columns=["Job Title", "SOC Code"])
        st.dataframe(df, use_container_width=True)
        st.info(f"Total jobs in database: {len(existing_jobs)}")
    else:
        st.warning("No jobs found in database")

with tab3:
    st.header("Search Job Title Aliases")
    
    search_term = st.text_input("Search aliases", placeholder="Enter search term")
    
    if search_term:
        aliases = search_existing_aliases(search_term)
        if aliases:
            st.write(f"Found {len(aliases)} matching aliases:")
            for alias in aliases:
                st.write(f"• {alias}")
        else:
            st.info("No matching aliases found")

# Footer
st.markdown("---")
st.markdown("**Instructions:**")
st.markdown("1. Use **Add Job Alias** to create alternative titles for existing database jobs")
st.markdown("2. Use **View Database** to see all current job entries")  
st.markdown("3. Use **Search Aliases** to find existing alternative titles")