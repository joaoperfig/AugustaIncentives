
import csv
import json
import os
import sys
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import sql
except ImportError:
    print("psycopg2-binary is required")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_setup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'augusta_incentives',
    'user': 'postgres',  
    'password': 'admin' 
}

class DatabaseManager:
    """Manages PostgreSQL database operations for Augusta Incentives."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """Establish connection to PostgreSQL database."""
        try:
            # First, connect to default postgres database to create our database
            temp_config = self.config.copy()
            temp_config['database'] = 'postgres'
            
            conn = psycopg2.connect(**temp_config)
            conn.autocommit = True
            temp_cursor = conn.cursor()
            
            # Check if database exists, create if not
            temp_cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.config['database'],)
            )
            
            if not temp_cursor.fetchone():
                logger.info(f"Creating database: {self.config['database']}")
                temp_cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(self.config['database'])
                    )
                )
                logger.info("Database created successfully")
            else:
                logger.info(f"Database {self.config['database']} already exists")
            
            temp_cursor.close()
            conn.close()
            
            # Now connect to our specific database
            self.connection = psycopg2.connect(**self.config)
            self.connection.autocommit = False
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            logger.info("Connected to PostgreSQL database successfully")
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")
    
    def create_tables(self):
        """Create database tables for companies and incentives."""
        try:
            # Create companies table
            companies_table_sql = """
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(500) NOT NULL,
                cae_primary_label TEXT,
                trade_description_native TEXT,
                website VARCHAR(500),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Create incentives table
            incentives_table_sql = """
            CREATE TABLE IF NOT EXISTS incentives (
                incentive_id SERIAL PRIMARY KEY,
                title TEXT,
                description TEXT,
                ai_description TEXT,
                document_urls TEXT,
                publication_date TIMESTAMP,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                total_budget DECIMAL(15,2),
                source_link TEXT
            );
            """
            
            # Create indexes for better performance
            indexes_sql = [
                "CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(company_name);",
                "CREATE INDEX IF NOT EXISTS idx_incentives_title ON incentives(title);",
                "CREATE INDEX IF NOT EXISTS idx_incentives_publication_date ON incentives(publication_date);",
                "CREATE INDEX IF NOT EXISTS idx_incentives_start_date ON incentives(start_date);",
                "CREATE INDEX IF NOT EXISTS idx_incentives_end_date ON incentives(end_date);"
            ]
            
            # Create full-text search indexes for enhanced keyword searching
            fulltext_indexes_sql = [
                # Companies full-text search
                "CREATE INDEX IF NOT EXISTS idx_companies_fts ON companies USING gin(to_tsvector('portuguese', company_name || ' ' || COALESCE(cae_primary_label, '') || ' ' || COALESCE(trade_description_native, '')));",
                
                # Incentives full-text search
                "CREATE INDEX IF NOT EXISTS idx_incentives_fts ON incentives USING gin(to_tsvector('portuguese', COALESCE(title, '') || ' ' || COALESCE(description, '') || ' ' || COALESCE(ai_description, '')));",
                
                # Individual field full-text indexes for more targeted searches
                "CREATE INDEX IF NOT EXISTS idx_companies_name_fts ON companies USING gin(to_tsvector('portuguese', company_name));",
                "CREATE INDEX IF NOT EXISTS idx_companies_description_fts ON companies USING gin(to_tsvector('portuguese', COALESCE(trade_description_native, '')));",
                "CREATE INDEX IF NOT EXISTS idx_incentives_title_fts ON incentives USING gin(to_tsvector('portuguese', COALESCE(title, '')));",
                "CREATE INDEX IF NOT EXISTS idx_incentives_description_fts ON incentives USING gin(to_tsvector('portuguese', COALESCE(description, '')));",
                "CREATE INDEX IF NOT EXISTS idx_incentives_ai_description_fts ON incentives USING gin(to_tsvector('portuguese', COALESCE(ai_description, '')));"
            ]
            
            self.cursor.execute(companies_table_sql)
            logger.info("Companies table created/verified")
            
            self.cursor.execute(incentives_table_sql)
            logger.info("Incentives table created/verified")
            
            for index_sql in indexes_sql:
                self.cursor.execute(index_sql)
            
            for fulltext_sql in fulltext_indexes_sql:
                self.cursor.execute(fulltext_sql)
            
            logger.info("Database indexes and full-text search indexes created/verified")
            self.connection.commit()
            
        except psycopg2.Error as e:
            logger.error(f"Error creating tables: {e}")
            self.connection.rollback()
            raise
    
    def load_companies_data(self, file_path: str) -> int:
        """Load companies data from CSV file."""
        try:
            # Clear existing data
            self.cursor.execute("DELETE FROM companies")
            logger.info("Cleared existing companies data")
            
            companies_loaded = 0
            batch_size = 1000
            
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                batch = []
                
                for row in reader:
                    # Clean and prepare data
                    company_data = {
                        'company_name': row.get('company_name', '').strip(),
                        'cae_primary_label': row.get('cae_primary_label', '').strip(),
                        'trade_description_native': row.get('trade_description_native', '').strip(),
                        'website': row.get('website', '').strip()
                    }
                    
                    batch.append(company_data)
                    
                    if len(batch) >= batch_size:
                        self._insert_companies_batch(batch)
                        companies_loaded += len(batch)
                        batch = []
                        logger.info(f"Loaded {companies_loaded} companies...")
                
                # Insert remaining records
                if batch:
                    self._insert_companies_batch(batch)
                    companies_loaded += len(batch)
            
            self.connection.commit()
            logger.info(f"Successfully loaded {companies_loaded} companies")
            return companies_loaded
            
        except Exception as e:
            logger.error(f"Error loading companies data: {e}")
            self.connection.rollback()
            raise
    
    def _insert_companies_batch(self, batch: List[Dict[str, Any]]):
        """Insert a batch of companies into the database."""
        insert_sql = """
        INSERT INTO companies (company_name, cae_primary_label, trade_description_native, website)
        VALUES (%(company_name)s, %(cae_primary_label)s, %(trade_description_native)s, %(website)s)
        """
        self.cursor.executemany(insert_sql, batch)
    
    def load_incentives_data(self, file_path: str) -> int:
        """Load incentives data from CSV file."""
        try:
            # Clear existing data
            self.cursor.execute("DELETE FROM incentives")
            logger.info("Cleared existing incentives data")
            
            incentives_loaded = 0
            batch_size = 100
            
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                batch = []
                
                for row in reader:
                    # Parse dates
                    publication_date = self._parse_timestamp(row.get('publication_date') or row.get('date_publication'))
                    start_date = self._parse_timestamp(row.get('start_date') or row.get('date_start'))
                    end_date = self._parse_timestamp(row.get('end_date') or row.get('date_end'))
                    
                    # Parse numeric fields
                    total_budget = None
                    if row.get('total_budget'):
                        try:
                            total_budget = float(row['total_budget'])
                        except ValueError:
                            logger.warning(f"Invalid total_budget for row {row.get('incentive_id', 'unknown')}")
                    
                    incentive_data = {
                        'title': row.get('title', '').strip(),
                        'description': row.get('description', '').strip(),
                        'ai_description': row.get('ai_description', '').strip(),
                        'document_urls': row.get('document_urls', '').strip(),
                        'publication_date': publication_date,
                        'start_date': start_date,
                        'end_date': end_date,
                        'total_budget': total_budget,
                        'source_link': row.get('source_link', '').strip()
                    }
                    
                    batch.append(incentive_data)
                    
                    if len(batch) >= batch_size:
                        self._insert_incentives_batch(batch)
                        incentives_loaded += len(batch)
                        batch = []
                        logger.info(f"Loaded {incentives_loaded} incentives...")
                
                # Insert remaining records
                if batch:
                    self._insert_incentives_batch(batch)
                    incentives_loaded += len(batch)
            
            self.connection.commit()
            logger.info(f"Successfully loaded {incentives_loaded} incentives")
            return incentives_loaded
            
        except Exception as e:
            logger.error(f"Error loading incentives data: {e}")
            self.connection.rollback()
            raise
    
    def _insert_incentives_batch(self, batch: List[Dict[str, Any]]):
        """Insert a batch of incentives into the database."""
        insert_sql = """
        INSERT INTO incentives (
            title, description, ai_description, document_urls, publication_date,
            start_date, end_date, total_budget, source_link
        ) VALUES (
            %(title)s, %(description)s, %(ai_description)s, %(document_urls)s, %(publication_date)s,
            %(start_date)s, %(end_date)s, %(total_budget)s, %(source_link)s
        )
        """
        self.cursor.executemany(insert_sql, batch)
    
    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse timestamp string to datetime object."""
        if not timestamp_str or timestamp_str.strip() == '':
            return None
        
        try:
            # Handle various timestamp formats
            timestamp_str = timestamp_str.strip()
            if '+' in timestamp_str:
                # Remove timezone info for simplicity
                timestamp_str = timestamp_str.split('+')[0]
            
            return datetime.fromisoformat(timestamp_str.replace(' ', 'T'))
        except ValueError:
            logger.warning(f"Could not parse timestamp: {timestamp_str}")
            return None
    
    def get_table_stats(self) -> Dict[str, int]:
        """Get record counts for all tables."""
        try:
            stats = {}
            
            self.cursor.execute("SELECT COUNT(*) as count FROM companies")
            stats['companies'] = self.cursor.fetchone()['count']
            
            self.cursor.execute("SELECT COUNT(*) as count FROM incentives")
            stats['incentives'] = self.cursor.fetchone()['count']
            
            return stats
            
        except psycopg2.Error as e:
            logger.error(f"Error getting table stats: {e}")
            return {}
    
    def search_companies(self, keywords: str, search_type: str = 'fulltext', limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search companies by keywords.
        
        Args:
            keywords: Search terms
            search_type: 'fulltext', 'like', or 'regex'
            limit: Maximum number of results to return
        """
        try:
            if search_type == 'fulltext':
                # Full-text search using PostgreSQL's tsvector
                query = """
                SELECT id, company_name, cae_primary_label, trade_description_native, website,
                       ts_rank(to_tsvector('portuguese', company_name || ' ' || COALESCE(cae_primary_label, '') || ' ' || COALESCE(trade_description_native, '')), 
                               plainto_tsquery('portuguese', %s)) as rank
                FROM companies 
                WHERE to_tsvector('portuguese', company_name || ' ' || COALESCE(cae_primary_label, '') || ' ' || COALESCE(trade_description_native, '')) 
                      @@ plainto_tsquery('portuguese', %s)
                ORDER BY rank DESC
                LIMIT %s;
                """
                self.cursor.execute(query, (keywords, keywords, limit))
                
            elif search_type == 'like':
                # Pattern matching with ILIKE
                pattern = f"%{keywords}%"
                query = """
                SELECT id, company_name, cae_primary_label, trade_description_native, website
                FROM companies 
                WHERE company_name ILIKE %s 
                   OR cae_primary_label ILIKE %s 
                   OR trade_description_native ILIKE %s
                ORDER BY company_name
                LIMIT %s;
                """
                self.cursor.execute(query, (pattern, pattern, pattern, limit))
                
            elif search_type == 'regex':
                # Regular expression search
                query = """
                SELECT id, company_name, cae_primary_label, trade_description_native, website
                FROM companies 
                WHERE company_name ~* %s 
                   OR cae_primary_label ~* %s 
                   OR trade_description_native ~* %s
                ORDER BY company_name
                LIMIT %s;
                """
                self.cursor.execute(query, (keywords, keywords, keywords, limit))
            
            else:
                raise ValueError(f"Invalid search_type: {search_type}")
            
            results = self.cursor.fetchall()
            return [dict(row) for row in results]
            
        except psycopg2.Error as e:
            logger.error(f"Error searching companies: {e}")
            return []
    
    def search_incentives(self, keywords: str, search_type: str = 'fulltext', limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search incentives by keywords.
        
        Args:
            keywords: Search terms
            search_type: 'fulltext', 'like', or 'regex'
            limit: Maximum number of results to return
        """
        try:
            if search_type == 'fulltext':
                # Full-text search using PostgreSQL's tsvector
                query = """
                SELECT incentive_id, title, description, ai_description, document_urls, 
                       publication_date, start_date, end_date, total_budget, source_link,
                       ts_rank(to_tsvector('portuguese', COALESCE(title, '') || ' ' || COALESCE(description, '') || ' ' || COALESCE(ai_description, '')), 
                               plainto_tsquery('portuguese', %s)) as rank
                FROM incentives 
                WHERE to_tsvector('portuguese', COALESCE(title, '') || ' ' || COALESCE(description, '') || ' ' || COALESCE(ai_description, '')) 
                      @@ plainto_tsquery('portuguese', %s)
                ORDER BY rank DESC
                LIMIT %s;
                """
                self.cursor.execute(query, (keywords, keywords, limit))
                
            elif search_type == 'like':
                # Pattern matching with ILIKE
                pattern = f"%{keywords}%"
                query = """
                SELECT incentive_id, title, description, ai_description, document_urls, 
                       publication_date, start_date, end_date, total_budget, source_link
                FROM incentives 
                WHERE title ILIKE %s 
                   OR description ILIKE %s 
                   OR ai_description ILIKE %s
                ORDER BY publication_date DESC
                LIMIT %s;
                """
                self.cursor.execute(query, (pattern, pattern, pattern, limit))
                
            elif search_type == 'regex':
                # Regular expression search
                query = """
                SELECT incentive_id, title, description, ai_description, document_urls, 
                       publication_date, start_date, end_date, total_budget, source_link
                FROM incentives 
                WHERE title ~* %s 
                   OR description ~* %s 
                   OR ai_description ~* %s
                ORDER BY publication_date DESC
                LIMIT %s;
                """
                self.cursor.execute(query, (keywords, keywords, keywords, limit))
            
            else:
                raise ValueError(f"Invalid search_type: {search_type}")
            
            results = self.cursor.fetchall()
            return [dict(row) for row in results]
            
        except psycopg2.Error as e:
            logger.error(f"Error searching incentives: {e}")
            return []
    
    def search_all(self, keywords: str, search_type: str = 'fulltext', limit: int = 100) -> Dict[str, List[Dict[str, Any]]]:
        """
        Search both companies and incentives by keywords.
        
        Args:
            keywords: Search terms
            search_type: 'fulltext', 'like', or 'regex'
            limit: Maximum number of results to return per table
        """
        return {
            'companies': self.search_companies(keywords, search_type, limit),
            'incentives': self.search_incentives(keywords, search_type, limit)
        }


def main():
    """Main function to set up database and load data."""
    logger.info("Starting Augusta Incentives database setup")
    
    # Check if CSV files exist
    companies_file = os.path.join('data', 'companies.csv')
    incentives_file = os.path.join('data', 'incentives.csv')
    
    if not os.path.exists(companies_file):
        logger.error(f"Companies CSV file not found: {companies_file}")
        return False
    
    if not os.path.exists(incentives_file):
        logger.error(f"Incentives CSV file not found: {incentives_file}")
        return False
    
    # Initialize database manager
    db_manager = DatabaseManager(DB_CONFIG)
    
    try:
        # Connect to database
        if not db_manager.connect():
            logger.error("Failed to connect to database")
            return False
        
        # Create tables
        logger.info("Creating database tables...")
        db_manager.create_tables()
        
        # Load companies data
        logger.info("Loading companies data...")
        companies_count = db_manager.load_companies_data(companies_file)
        
        # Load incentives data
        logger.info("Loading incentives data...")
        incentives_count = db_manager.load_incentives_data(incentives_file)
        
        # Get final statistics
        stats = db_manager.get_table_stats()
        logger.info(f"Database setup completed successfully!")
        logger.info(f"Final statistics: {stats}")
        
        return True
        
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return False
    
    finally:
        db_manager.disconnect()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

