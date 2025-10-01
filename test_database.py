#!/usr/bin/env python3
"""
Simple test script to verify that database_setup.py worked correctly.
This script performs basic checks to ensure the database is properly set up.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import sys

# Database configuration (should match database_setup.py)
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'augusta_incentives',
    'user': 'postgres',
    'password': 'admin'
}

def test_database_connection():
    """Test if we can connect to the database."""
    print("Testing database connection...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"SUCCESS: Connected to PostgreSQL: {version[:50]}...")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"FAILED: Connection failed: {e}")
        return False

def test_tables_exist():
    """Test if required tables exist."""
    print("\nTesting if tables exist...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Check if companies table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'companies'
            );
        """)
        companies_exists = cursor.fetchone()[0]
        
        # Check if incentives table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'incentives'
            );
        """)
        incentives_exists = cursor.fetchone()[0]
        
        if companies_exists:
            print("SUCCESS: Companies table exists")
        else:
            print("FAILED: Companies table missing")
            
        if incentives_exists:
            print("SUCCESS: Incentives table exists")
        else:
            print("FAILED: Incentives table missing")
            
        cursor.close()
        conn.close()
        return companies_exists and incentives_exists
        
    except Exception as e:
        print(f"FAILED: Error checking tables: {e}")
        return False

def test_table_structure():
    """Test if tables have the expected structure."""
    print("\nTesting table structure...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Check companies table structure
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'companies' 
            ORDER BY ordinal_position;
        """)
        companies_columns = cursor.fetchall()
        
        # Check incentives table structure
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'incentives' 
            ORDER BY ordinal_position;
        """)
        incentives_columns = cursor.fetchall()
        
        print("Companies table columns:")
        for col_name, data_type in companies_columns:
            print(f"  - {col_name}: {data_type}")
            
        print("Incentives table columns:")
        for col_name, data_type in incentives_columns:
            print(f"  - {col_name}: {data_type}")
            
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"FAILED: Error checking table structure: {e}")
        return False

def test_data_counts():
    """Test if data was loaded with reasonable counts."""
    print("\nTesting data counts...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Count companies
        cursor.execute("SELECT COUNT(*) FROM companies;")
        companies_count = cursor.fetchone()[0]
        
        # Count incentives
        cursor.execute("SELECT COUNT(*) FROM incentives;")
        incentives_count = cursor.fetchone()[0]
        
        print(f"Companies loaded: {companies_count:,}")
        print(f"Incentives loaded: {incentives_count:,}")
        
        # Basic sanity checks
        if companies_count > 0:
            print("SUCCESS: Companies data loaded successfully")
        else:
            print("FAILED: No companies data found")
            
        if incentives_count > 0:
            print("SUCCESS: Incentives data loaded successfully")
        else:
            print("FAILED: No incentives data found")
            
        cursor.close()
        conn.close()
        return companies_count > 0 and incentives_count > 0
        
    except Exception as e:
        print(f"FAILED: Error checking data counts: {e}")
        return False

def test_column_statistics():
    """Test column statistics including value counts and unique values."""
    print("\nTesting column statistics...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        def get_column_stats(table_name, col_name, data_type):
            """Get statistics for a single column based on its data type."""
            # Build appropriate WHERE clause based on data type
            if data_type in ['timestamp without time zone', 'timestamp with time zone', 'date', 'numeric', 'decimal', 'integer', 'bigint', 'smallint', 'real', 'double precision']:
                # For timestamp/date/numeric columns, only check for NULL
                where_clause = f"{col_name} IS NOT NULL"
            else:
                # For text columns, check for NULL and empty strings
                where_clause = f"{col_name} IS NOT NULL AND {col_name} != ''"
            
            # Count non-null values
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause};")
            non_null_count = cursor.fetchone()['count']
            
            # Count unique values
            cursor.execute(f"SELECT COUNT(DISTINCT {col_name}) FROM {table_name} WHERE {where_clause};")
            unique_count = cursor.fetchone()['count']
            
            # Count total rows
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            total_count = cursor.fetchone()['count']
            
            return non_null_count, unique_count, total_count
        
        # Get companies table column statistics
        print("Companies table column statistics:")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'companies' 
            ORDER BY ordinal_position;
        """)
        companies_columns = cursor.fetchall()
        
        for col in companies_columns:
            col_name = col['column_name']
            data_type = col['data_type']
            if col_name in ['id', 'created_at', 'updated_at']:  # Skip system columns
                continue
                
            non_null_count, unique_count, total_count = get_column_stats('companies', col_name, data_type)
            print(f"  {col_name}: {non_null_count:,} values, {unique_count:,} unique ({non_null_count/total_count*100:.1f}% filled)")
        
        # Get incentives table column statistics
        print("\nIncentives table column statistics:")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'incentives' 
            ORDER BY ordinal_position;
        """)
        incentives_columns = cursor.fetchall()
        
        for col in incentives_columns:
            col_name = col['column_name']
            data_type = col['data_type']
            if col_name in ['incentive_id']:  # Skip system columns
                continue
                
            non_null_count, unique_count, total_count = get_column_stats('incentives', col_name, data_type)
            print(f"  {col_name}: {non_null_count:,} values, {unique_count:,} unique ({non_null_count/total_count*100:.1f}% filled)")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"FAILED: Error checking column statistics: {e}")
        return False

def test_sample_queries():
    """Test basic sample queries to ensure data integrity."""
    print("\nTesting sample queries...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        def format_field_value(key, value, max_width=150):
            """Format a field value for display, handling None values and long text."""
            if value is None:
                return "None"
            elif isinstance(value, str) and len(value) > max_width:
                return f"{value[:max_width-3]}..."
            else:
                return str(value)
        
        # Test companies query - get all fields
        cursor.execute("SELECT * FROM companies LIMIT 3;")
        companies = cursor.fetchall()
        print("Sample companies:")
        for i, company in enumerate(companies, 1):
            print(f"\n  Company #{i}:")
            for key, value in company.items():
                formatted_value = format_field_value(key, value)
                print(f"    {key}: {formatted_value}")
        
        # Test incentives query - get all fields
        cursor.execute("SELECT * FROM incentives LIMIT 3;")
        incentives = cursor.fetchall()
        print("\nSample incentives:")
        for i, incentive in enumerate(incentives, 1):
            print(f"\n  Incentive #{i}:")
            for key, value in incentive.items():
                formatted_value = format_field_value(key, value)
                print(f"    {key}: {formatted_value}")
        
        # Test for any data with NULL values
        cursor.execute("SELECT COUNT(*) FROM companies WHERE company_name IS NULL OR company_name = '';")
        null_companies = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) FROM incentives WHERE title IS NULL OR title = '';")
        null_incentives = cursor.fetchone()['count']
        
        print(f"\nData integrity checks:")
        print(f"  Companies with missing names: {null_companies}")
        print(f"  Incentives with missing titles: {null_incentives}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"FAILED: Error running sample queries: {e}")
        return False

def main():
    """Run all database tests."""
    print("Augusta Incentives Database Test")
    print("=" * 50)
    
    tests = [
        ("Database Connection", test_database_connection),
        ("Tables Exist", test_tables_exist),
        ("Table Structure", test_table_structure),
        ("Data Counts", test_data_counts),
        ("Column Statistics", test_column_statistics),
        ("Sample Queries", test_sample_queries)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"FAILED: {test_name} failed with exception: {e}")
    
    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("SUCCESS: All tests passed! Database setup appears to be successful.")
        return True
    else:
        print("WARNING: Some tests failed. Please check the database setup.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
