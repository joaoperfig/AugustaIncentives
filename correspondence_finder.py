"""
Correspondence Finder for Augusta Incentives

This script uses OpenAI API and search tools to find the best companies 
for each incentive by analyzing company profiles and incentive requirements.
"""

import os
import sys
import json
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

# Database imports
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import sql
except ImportError:
    print("psycopg2-binary is required")
    sys.exit(1)

# OpenAI imports
try:
    import openai
except ImportError:
    print("openai package is required. Install with: pip install openai")
    sys.exit(1)


# Import shared configuration
from config import get_db_config, get_openai_config, validate_config

# Load configuration
DB_CONFIG = get_db_config()
OPENAI_CONFIG = get_openai_config()

class CorrespondenceFinder:
    """
    Main class for finding correspondence between companies and incentives
    using OpenAI API and database search tools.
    """
    
    def __init__(self, db_config: Dict[str, Any], openai_config: Dict[str, Any]):
        self.db_config = db_config
        self.openai_config = openai_config
        self.connection = None
        self.cursor = None
        
        # Initialize OpenAI client
        if not openai_config.get('api_key'):
            raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY environment variable.")
        
        self.client = openai.OpenAI(api_key=openai_config['api_key'])
    
    def connect_database(self) -> bool:
        """Establish connection to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            return True
        except Exception as e:
            print(f"Database connection failed: {e}")
            return False
    
    def disconnect_database(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def get_all_incentives(self) -> List[Dict[str, Any]]:
        """Retrieve all incentives from the database."""
        try:
            query = """
                SELECT * FROM incentives 
                ORDER BY incentive_id
            """
            self.cursor.execute(query)
            incentives = self.cursor.fetchall()
            return incentives
        except Exception as e:
            print(f"Error retrieving incentives: {e}")
            return []
    
    def search_companies_by_keywords(self, keywords: List[str], limit: int = 25) -> List[Dict[str, Any]]:
        """
        Search for companies using keywords with relevance scoring using optimized full-text search.
        Uses PostgreSQL's full-text search capabilities for much better performance while maintaining
        good result coverage by using a more flexible search approach.
        
        Args:
            keywords: List of keywords to search for
            limit: Maximum number of companies to return
        
        Returns:
            List of matching companies ordered by relevance score
        """
        try:
            if not keywords:
                return []
            
            # First try full-text search with OR combination for best performance
            # Split multi-word keywords and join individual words with OR
            individual_words = []
            for keyword in keywords:
                # Split by spaces and add each word individually
                words = keyword.split()
                individual_words.extend(words)
            
            # Remove duplicates while preserving order and filter out common stop words
            unique_words = []
            seen = set()
            
            # Common Portuguese and English stop words to filter out
            stop_words = {
                'de', 'da', 'do', 'das', 'dos', 'para', 'com', 'em', 'na', 'no', 'nas', 'nos',
                'por', 'sobre', 'entre', 'até', 'desde', 'durante', 'após', 'antes', 'depois',
                'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
                'by', 'from', 'up', 'about', 'into', 'through', 'during', 'before', 'after',
                'e', 'ou', 'mas', 'se', 'que', 'como', 'quando', 'onde', 'porque', 'então',
                'é', 'são', 'foi', 'será', 'tem', 'têm', 'ter', 'terá', 'pode', 'podem',
                'poder', 'poderá', 'deve', 'devem', 'dever', 'deverá', 'vai', 'vão', 'ir',
                'vir', 'virá', 'fazer', 'fez', 'fará', 'dizer', 'disse', 'dirá'
            }
            
            for word in individual_words:
                word_lower = word.lower()
                if word_lower not in seen and word_lower not in stop_words and len(word) > 2:
                    unique_words.append(word)
                    seen.add(word_lower)
            
            # If we don't have enough meaningful words, use original keywords
            if len(unique_words) < 3:
                search_terms = " | ".join(keywords)
            else:
                search_terms = " | ".join(unique_words)
            
            # Use full-text search with ts_rank for relevance scoring
            # Use to_tsquery instead of plainto_tsquery to properly handle boolean operators
            query = """
                SELECT id, company_name, cae_primary_label, trade_description_native, website, created_at, updated_at,
                       ts_rank(to_tsvector('portuguese', company_name || ' ' || COALESCE(cae_primary_label, '') || ' ' || COALESCE(trade_description_native, '')), 
                               to_tsquery('portuguese', %s)) as relevance_score
                FROM companies 
                WHERE to_tsvector('portuguese', company_name || ' ' || COALESCE(cae_primary_label, '') || ' ' || COALESCE(trade_description_native, '')) 
                      @@ to_tsquery('portuguese', %s)
                ORDER BY relevance_score DESC, company_name ASC
                LIMIT %s
            """
            
            self.cursor.execute(query, (search_terms, search_terms, limit))
            companies = self.cursor.fetchall()
            
            # If we don't have enough results, try a more flexible approach with individual keyword searches
            if len(companies) < limit // 2:  # If we have less than half the requested results
                print(f"Full-text search returned {len(companies)} results, trying flexible search...")
                
                # Try with individual keywords using OR
                flexible_query = """
                    SELECT id, company_name, cae_primary_label, trade_description_native, website, created_at, updated_at,
                           ts_rank(to_tsvector('portuguese', company_name || ' ' || COALESCE(cae_primary_label, '') || ' ' || COALESCE(trade_description_native, '')), 
                                   to_tsquery('portuguese', %s)) as relevance_score
                    FROM companies 
                    WHERE to_tsvector('portuguese', company_name || ' ' || COALESCE(cae_primary_label, '') || ' ' || COALESCE(trade_description_native, '')) 
                          @@ to_tsquery('portuguese', %s)
                    ORDER BY relevance_score DESC, company_name ASC
                    LIMIT %s
                """
                
                # Try each individual word and combine results
                all_companies = {}
                for word in unique_words:
                    self.cursor.execute(flexible_query, (word, word, limit))
                    keyword_companies = self.cursor.fetchall()
                    
                    for company in keyword_companies:
                        company_id = company['id']
                        if company_id not in all_companies or company['relevance_score'] > all_companies[company_id]['relevance_score']:
                            all_companies[company_id] = company
                
                # Convert back to list and sort by relevance score
                companies = list(all_companies.values())
                companies.sort(key=lambda x: x['relevance_score'], reverse=True)
                companies = companies[:limit]
            
            # Log search results for debugging
            if companies:
                print(f"Found {len(companies)} companies with keyword matches")
                top_scores = [c['relevance_score'] for c in companies[:5]]
                print(f"Top relevance scores: {top_scores}")
            
            return companies
            
        except Exception as e:
            print(f"Error searching companies by keywords: {e}")
            return []
    
    def extract_keywords_from_incentive(self, incentive: Dict[str, Any]) -> List[str]:
        """
        Use OpenAI to extract search keywords from an incentive using the keywords prompt.
        
        Args:
            incentive: Dictionary containing incentive data
        
        Returns:
            List of keywords for searching companies
        """
        try:
            # Load the keywords prompt
            with open('keywords_prompt.txt', 'r', encoding='utf-8') as f:
                keywords_prompt = f.read()
            
            # Prepare the full prompt with incentive data
            full_prompt = f"""
            {keywords_prompt}
            
            Incentive data:
            Title: {incentive.get('title', 'N/A')}
            Description: {incentive.get('description', 'N/A')}
            AI Description: {incentive.get('ai_description', 'N/A')}
            """
            
            response = self.client.chat.completions.create(
                model=self.openai_config['model'],
                messages=[
                    {"role": "system", "content": "You are a keyword extraction assistant. Follow the prompt instructions exactly and return only a JSON array of keywords."},
                    {"role": "user", "content": full_prompt}
                ],
                max_tokens=self.openai_config['max_tokens'],
                temperature=self.openai_config['temperature']
            )
            
            # Parse OpenAI response
            content = response.choices[0].message.content.strip()
            keywords = json.loads(content)
            
            return keywords
            
        except Exception as e:
            print(f"Error extracting keywords from incentive: {e}")
            return []
    
    def crop_text_field(self, text: str, max_length: int = 500) -> str:
        """
        Crop text field to specified maximum length, adding ellipsis if truncated.
        
        Args:
            text: Text to crop
            max_length: Maximum length of the text
        
        Returns:
            Cropped text with ellipsis if truncated
        """
        if not text:
            return text
        
        text_str = str(text)
        if len(text_str) <= max_length:
            return text_str
        
        return text_str[:max_length-3] + "..."
    
    def rank_companies_for_incentive(self, incentive: Dict[str, Any], companies: List[Dict[str, Any]]) -> List[int]:
        """
        Use OpenAI to rank companies for an incentive using the ranking prompt.
        
        Args:
            incentive: Dictionary containing incentive data
            companies: List of company dictionaries to evaluate (up to 25)
        
        Returns:
            List of top 5 company IDs in descending order of relevance
        """
        try:
            # Load the ranking prompt
            with open('ranking_prompt.txt', 'r', encoding='utf-8') as f:
                ranking_prompt = f.read()
            
            # Prepare company data for analysis with text cropping
            candidates = []
            for company in companies:
                candidate = {
                    'id': company.get('id'),
                    'company_name': self.crop_text_field(company.get('company_name'), 200),
                    'cae_primary_label': self.crop_text_field(company.get('cae_primary_label'), 200),
                    'trade_description_native': self.crop_text_field(company.get('trade_description_native'), 500),
                    'website': self.crop_text_field(company.get('website'), 100),
                    'created_at': company.get('created_at'),
                    'updated_at': company.get('updated_at')
                }
                candidates.append(candidate)
            
            # Prepare cropped incentive data for analysis
            cropped_incentive = {
                'incentive_id': incentive.get('incentive_id'),
                'title': self.crop_text_field(incentive.get('title'), 200),
                'description': self.crop_text_field(incentive.get('description'), 1000),
                'ai_description': self.crop_text_field(incentive.get('ai_description'), 1000),
                'created_at': incentive.get('created_at'),
                'updated_at': incentive.get('updated_at')
            }
            
            # Prepare the full prompt with incentive and company data
            full_prompt = f"""
            {ranking_prompt}
            
            Incentive:
            {json.dumps(cropped_incentive, indent=2, default=str)}
            
            Candidates:
            {json.dumps(candidates, indent=2, default=str)}
            """
            
            response = self.client.chat.completions.create(
                model=self.openai_config['model'],
                messages=[
                    {"role": "system", "content": "You are a company-incentive matching assistant. Follow the prompt instructions exactly and return only a JSON array of 5 company IDs in descending order of relevance."},
                    {"role": "user", "content": full_prompt}
                ],
                max_tokens=self.openai_config['max_tokens'],
                temperature=self.openai_config['temperature']
            )
            
            # Parse OpenAI response
            content = response.choices[0].message.content.strip()
            top_company_ids = json.loads(content)
            
            return top_company_ids
            
        except Exception as e:
            print(f"Error ranking companies for incentive: {e}")
            return []
    
    def process_all_incentives(self) -> Dict[str, Any]:
        """
        Process all incentives and find best company matches using the new workflow:
        1. Extract keywords from each incentive using keywords_prompt.txt
        2. Search for top 25 companies using those keywords
        3. Rank companies using ranking_prompt.txt to get top 5
        4. Save results as JSON
        
        Returns:
            Dictionary with results for each incentive
        """
        print("Starting correspondence finding process")
        
        if not self.connect_database():
            return {}
        
        try:
            incentives = self.get_all_incentives()
            
            if not incentives:
                print("No incentives found in database")
                return {}
            
            results = {}
            
            for i, incentive in enumerate(incentives, 1):
                print(f"Processing incentive {i} of {len(incentives)}: {incentive.get('title')}")
                
                # Initialize timing for this incentive
                incentive_start_time = time.time()
                timing_data = {}
                
                # Step 1: Extract keywords from incentive using keywords prompt
                keyword_start_time = time.time()
                keywords = self.extract_keywords_from_incentive(incentive)
                keyword_end_time = time.time()
                timing_data['keyword_extraction_time'] = round(keyword_end_time - keyword_start_time, 2)
                
                if not keywords:
                    print(f" Keywords: None")
                    print(f" Found 0 companies")
                    print(f" Ranking completed")
                    print(f" Durations:")
                    print(f"   keyword generation: {timing_data['keyword_extraction_time']:.2f} s")
                    print(f"   company search: 0.00 s")
                    print(f"   company ranking: 0.00 s")
                    print()
                    results[incentive['incentive_id']] = {
                        'incentive': incentive,
                        'keywords': [],
                        'top_25_companies': [],
                        'top_5_company_ids': [],
                        'error': 'No keywords extracted',
                        'timing': timing_data
                    }
                    continue
                
                # Step 2: Search for top 25 companies using keywords
                search_start_time = time.time()
                top_25_companies = self.search_companies_by_keywords(keywords, limit=25)
                search_end_time = time.time()
                timing_data['company_search_time'] = round(search_end_time - search_start_time, 2)
                
                if not top_25_companies:
                    print(f" Keywords: {', '.join(keywords) if keywords else 'None'}")
                    print(f" Found 0 companies")
                    print(f" Ranking completed")
                    print(f" Durations:")
                    print(f"   keyword generation: {timing_data['keyword_extraction_time']:.2f} s")
                    print(f"   company search: {timing_data['company_search_time']:.2f} s")
                    print(f"   company ranking: 0.00 s")
                    print()
                    results[incentive['incentive_id']] = {
                        'incentive': incentive,
                        'keywords': keywords,
                        'top_25_companies': [],
                        'top_5_company_ids': [],
                        'error': 'No companies found',
                        'timing': timing_data
                    }
                    continue
                
                # Step 3: Rank companies using ranking prompt to get top 5
                ranking_start_time = time.time()
                top_5_company_ids = self.rank_companies_for_incentive(incentive, top_25_companies)
                ranking_end_time = time.time()
                timing_data['company_ranking_time'] = round(ranking_end_time - ranking_start_time, 2)
                
                # Calculate total processing time
                incentive_end_time = time.time()
                timing_data['total_processing_time'] = round(incentive_end_time - incentive_start_time, 2)
                
                # Store results
                results[incentive['incentive_id']] = {
                    'incentive': incentive,
                    'keywords': keywords,
                    'top_25_companies': top_25_companies,
                    'top_5_company_ids': top_5_company_ids,
                    'timing': timing_data
                }
                
                print(f" Keywords: {', '.join(keywords) if keywords else 'None'}")
                print(f" Found {len(top_25_companies)} companies")
                print(f" Ranking completed")
                print(f" Durations:")
                print(f"   keyword generation: {timing_data['keyword_extraction_time']:.2f} s")
                print(f"   company search: {timing_data['company_search_time']:.2f} s")
                print(f"   company ranking: {timing_data['company_ranking_time']:.2f} s")
                print()
            
            print(f"Completed processing {len(incentives)} incentives")
            return results
            
        finally:
            self.disconnect_database()
    
    def create_simplified_results(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create simplified results with only essential data.
        
        Args:
            results: Full results from process_all_incentives
        
        Returns:
            Simplified results with incentive, keywords, and top 5 companies (id and name only)
        """
        simplified = {}
        
        for incentive_id, data in results.items():
            if 'error' in data:
                # Skip incentives with errors
                continue
            
            # Get the top 5 companies with only id and name
            companies = []
            if 'top_5_company_ids' in data and 'top_25_companies' in data:
                # Create lookup for companies
                company_lookup = {company['id']: company for company in data['top_25_companies']}
                
                for company_id in data['top_5_company_ids']:
                    if company_id in company_lookup:
                        company = company_lookup[company_id]
                        companies.append({
                            'id': company['id'],
                            'name': company['company_name']
                        })
            
            simplified[incentive_id] = {
                'incentive': data['incentive'],
                'keywords': data['keywords'],
                'companies': companies
            }
        
        return simplified
    
    def save_results(self, results: Dict[str, Any], filename: str = None):
        """Save results to JSON file."""
        if not filename:
            filename = "data/correspondence_results.json"
        
        try:
            # Ensure the data directory exists
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"Results saved to {filename}")
        except Exception as e:
            print(f"Error saving results: {e}")


def main():
    """Main function to run the correspondence finder."""
    try:
        # Validate configuration
        if not validate_config():
            print("Configuration validation failed. Please check your secrets.json file.")
            return
        
        # Initialize the correspondence finder
        finder = CorrespondenceFinder(DB_CONFIG, OPENAI_CONFIG)
        
        # Process all incentives
        results = finder.process_all_incentives()
        
        if results:
            # Save debug results (full data)
            finder.save_results(results, "data/correspondence_debug.json")
            
            # Create and save simplified results
            simplified_results = finder.create_simplified_results(results)
            finder.save_results(simplified_results, "data/correspondence_results.json")
            
        else:
            print("No results generated. Check logs for errors.")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
