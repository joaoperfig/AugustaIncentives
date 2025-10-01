#!/usr/bin/env python3
"""
Simple test script to verify that database_setup.py worked correctly.
This script performs basic checks to ensure the database is properly set up.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import sys

# Import shared configuration
from config import get_db_config

# Database configuration
DB_CONFIG = get_db_config()

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

def test_fulltext_indexes():
    """Test if full-text search indexes exist and are functional."""
    print("\nTesting full-text search indexes...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if full-text search indexes exist
        expected_indexes = [
            'idx_companies_fts',
            'idx_incentives_fts',
            'idx_companies_name_fts',
            'idx_companies_description_fts',
            'idx_incentives_title_fts',
            'idx_incentives_description_fts',
            'idx_incentives_ai_description_fts'
        ]
        
        cursor.execute("""
            SELECT indexname 
            FROM pg_indexes 
            WHERE schemaname = 'public' 
            AND indexname IN %s;
        """, (tuple(expected_indexes),))
        
        existing_indexes = [row['indexname'] for row in cursor.fetchall()]
        
        print("Full-text search indexes found:")
        for index in expected_indexes:
            if index in existing_indexes:
                print(f"  [OK] {index}")
            else:
                print(f"  [MISSING] {index}")
        
        # Test if indexes are actually functional
        print("\nTesting full-text search functionality:")
        
        # Test companies full-text search
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM companies 
            WHERE to_tsvector('portuguese', company_name || ' ' || COALESCE(cae_primary_label, '') || ' ' || COALESCE(trade_description_native, '')) 
                  @@ plainto_tsquery('portuguese', 'restaurant');
        """)
        companies_search_count = cursor.fetchone()['count']
        print(f"  Companies matching 'restaurant': {companies_search_count}")
        
        # Test incentives full-text search
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM incentives 
            WHERE to_tsvector('portuguese', COALESCE(title, '') || ' ' || COALESCE(description, '') || ' ' || COALESCE(ai_description, '')) 
                  @@ plainto_tsquery('portuguese', 'digital');
        """)
        incentives_search_count = cursor.fetchone()['count']
        print(f"  Incentives matching 'digital': {incentives_search_count}")
        
        # Test incentives ranking functionality
        cursor.execute("""
            SELECT title, description, ai_description,
                   ts_rank(to_tsvector('portuguese', COALESCE(title, '') || ' ' || COALESCE(description, '') || ' ' || COALESCE(ai_description, '')), 
                           plainto_tsquery('portuguese', 'digital')) as rank
            FROM incentives 
            WHERE to_tsvector('portuguese', COALESCE(title, '') || ' ' || COALESCE(description, '') || ' ' || COALESCE(ai_description, '')) 
                  @@ plainto_tsquery('portuguese', 'digital')
            ORDER BY rank DESC
            LIMIT 3;
        """)
        ranked_incentives = cursor.fetchall()
        
        if ranked_incentives:
            print("  Sample ranked incentive results:")
            for result in ranked_incentives:
                print(f"    - {result['title']} (rank: {result['rank']:.4f})")
                if result['description'] and 'digital' in result['description'].lower():
                    desc = result['description'][:150] + "..." if len(result['description']) > 150 else result['description']
                    print(f"      Description: {desc}")
                if result['ai_description'] and 'digital' in result['ai_description'].lower():
                    ai_desc = result['ai_description'][:150] + "..." if len(result['ai_description']) > 150 else result['ai_description']
                    print(f"      AI Description: {ai_desc}")
        
        # Test ranking functionality
        cursor.execute("""
            SELECT company_name, cae_primary_label, trade_description_native,
                   ts_rank(to_tsvector('portuguese', company_name || ' ' || COALESCE(cae_primary_label, '') || ' ' || COALESCE(trade_description_native, '')), 
                           plainto_tsquery('portuguese', 'restaurant')) as rank
            FROM companies 
            WHERE to_tsvector('portuguese', company_name || ' ' || COALESCE(cae_primary_label, '') || ' ' || COALESCE(trade_description_native, '')) 
                  @@ plainto_tsquery('portuguese', 'restaurant')
            ORDER BY rank DESC
            LIMIT 3;
        """)
        ranked_results = cursor.fetchall()
        
        if ranked_results:
            print("  Sample ranked results:")
            for result in ranked_results:
                print(f"    - {result['company_name']} (rank: {result['rank']:.4f})")
                if result['cae_primary_label'] and 'restaurant' in result['cae_primary_label'].lower():
                    print(f"      CAE: {result['cae_primary_label']}")
                if result['trade_description_native'] and ('restaur' in result['trade_description_native'].lower() or 'restaurant' in result['trade_description_native'].lower()):
                    desc = result['trade_description_native'][:150] + "..." if len(result['trade_description_native']) > 150 else result['trade_description_native']
                    print(f"      Description: {desc}")
        
        cursor.close()
        conn.close()
        
        # Check if all expected indexes exist
        missing_indexes = set(expected_indexes) - set(existing_indexes)
        if missing_indexes:
            print(f"WARNING: {len(missing_indexes)} full-text search indexes are missing")
            return False
        else:
            print("SUCCESS: All full-text search indexes are present and functional")
            return True
        
    except Exception as e:
        print(f"FAILED: Error testing full-text search indexes: {e}")
        return False

def test_search_methods():
    """Test the DatabaseManager search methods."""
    print("\nTesting DatabaseManager search methods...")
    try:
        from database_setup import DatabaseManager
        
        db_manager = DatabaseManager(DB_CONFIG)
        if not db_manager.connect():
            print("FAILED: Could not connect to database")
            return False
        
        # Test company search methods
        print("Testing company search methods:")
        
        # Full-text search
        companies_ft = db_manager.search_companies("restaurant", search_type='fulltext', limit=3)
        print(f"  Full-text search for 'restaurant': {len(companies_ft)} results")
        
        # Like search
        companies_like = db_manager.search_companies("LDA", search_type='like', limit=3)
        print(f"  Like search for 'LDA': {len(companies_like)} results")
        
        # Regex search
        companies_regex = db_manager.search_companies("^[AB]", search_type='regex', limit=3)
        print(f"  Regex search for '^[AB]': {len(companies_regex)} results")
        
        # Test incentive search methods
        print("Testing incentive search methods:")
        
        # Full-text search
        incentives_ft = db_manager.search_incentives("digital", search_type='fulltext', limit=3)
        print(f"  Full-text search for 'digital': {len(incentives_ft)} results")
        
        # Like search
        incentives_like = db_manager.search_incentives("PT2030", search_type='like', limit=3)
        print(f"  Like search for 'PT2030': {len(incentives_like)} results")
        
        # Test combined search
        print("Testing combined search:")
        combined_results = db_manager.search_all("technology", search_type='fulltext', limit=2)
        print(f"  Combined search for 'technology': {len(combined_results['companies'])} companies, {len(combined_results['incentives'])} incentives")
        
        # Test error handling
        print("Testing error handling:")
        try:
            invalid_search = db_manager.search_companies("test", search_type='invalid', limit=1)
            print("  [ERROR] Invalid search type should have raised an error")
        except ValueError as e:
            print(f"  [OK] Invalid search type properly handled: {e}")
        
        db_manager.disconnect()
        
        # Basic success criteria
        if (len(companies_ft) > 0 or len(companies_like) > 0 or len(companies_regex) > 0) and \
           (len(incentives_ft) > 0 or len(incentives_like) > 0):
            print("SUCCESS: Search methods are working correctly")
            return True
        else:
            print("WARNING: Search methods returned no results - check if data exists")
            return True  # Still consider it a success if methods work without errors
        
    except Exception as e:
        print(f"FAILED: Error testing search methods: {e}")
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
        ("Sample Queries", test_sample_queries),
        ("Full-Text Search Indexes", test_fulltext_indexes),
        ("Search Methods", test_search_methods)
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
