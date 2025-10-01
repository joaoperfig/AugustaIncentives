#!/usr/bin/env python3

import os
import re
import sys
import json
import logging
import argparse
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import sql
except ImportError:
    print("Error: psycopg2-binary is required. Install with: pip install psycopg2-binary")
    sys.exit(1)

try:
    from openai import OpenAI
except ImportError:
    print("Error: openai is required. Install with: pip install openai")
    sys.exit(1)

# Import shared configuration
from config import get_db_config, get_openai_config, validate_config

# Global verbose flag
VERBOSE = False

def setup_logging(verbose: bool = False):
    """Configure logging based on verbose flag."""
    global VERBOSE
    VERBOSE = verbose
    
    if verbose:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('chatbot.log'),
                logging.StreamHandler()
            ]
        )
    else:
        # Only log errors to file, no console output
        logging.basicConfig(
            level=logging.ERROR,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('chatbot.log')
            ]
        )

logger = logging.getLogger(__name__)

def verbose_log(message: str, level: str = "info"):
    """Log message only if verbose mode is enabled."""
    if VERBOSE:
        if level == "info":
            logger.info(message)
        elif level == "warning":
            logger.warning(message)
        elif level == "error":
            logger.error(message)
        elif level == "debug":
            logger.debug(message)

class DatabaseManager:
    """Manages PostgreSQL database operations for the chatbot."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """Establish connection to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(**self.config)
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            verbose_log("Successfully connected to database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        verbose_log("Database connection closed")
    
    def execute_query(self, query: str) -> Tuple[bool, List[Dict[str, Any]], str]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query string
            
        Returns:
            Tuple of (success, results, error_message)
        """
        try:
            self.cursor.execute(query)
            
            # Check if it's a SELECT query
            if query.strip().upper().startswith('SELECT'):
                results = self.cursor.fetchall()
                # Convert RealDictRow to regular dict
                results = [dict(row) for row in results]
                return True, results, ""
            else:
                # For non-SELECT queries, commit the transaction
                self.connection.commit()
                return True, [], ""
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Query execution failed: {error_msg}")
            return False, [], error_msg

class AugustaIncentivesChatbot:
    """Main chatbot class for incentives and companies database exploration."""
    
    def __init__(self, prompt_file: str = "chatbot_prompt.txt"):
        self.prompt_file = prompt_file
        self.system_prompt = ""
        self.decision_prompt = ""
        self.final_iteration_prompt = ""
        self.db_manager = None
        self.openai_client = None
        self.conversation_history = []
        
        # Load configuration
        self.db_config = get_db_config()
        self.openai_config = get_openai_config()
        
        # Initialize components
        self._load_system_prompt()
        self._load_decision_prompt()
        self._load_final_iteration_prompt()
        self._initialize_database()
        self._initialize_openai()
    
    def _load_system_prompt(self):
        """Load the system prompt from the txt file."""
        try:
            if not os.path.exists(self.prompt_file):
                raise FileNotFoundError(f"Prompt file not found: {self.prompt_file}")
            
            with open(self.prompt_file, 'r', encoding='utf-8') as f:
                self.system_prompt = f.read().strip()
            
            verbose_log(f"System prompt loaded from {self.prompt_file}")
            
        except Exception as e:
            logger.error(f"Failed to load system prompt: {e}")
            raise
    
    def _load_decision_prompt(self):
        """Load the decision prompt from the txt file."""
        try:
            decision_prompt_file = "decision_prompt.txt"
            if not os.path.exists(decision_prompt_file):
                raise FileNotFoundError(f"Decision prompt file not found: {decision_prompt_file}")
            
            with open(decision_prompt_file, 'r', encoding='utf-8') as f:
                self.decision_prompt = f.read().strip()
            
            verbose_log(f"Decision prompt loaded from {decision_prompt_file}")
            
        except Exception as e:
            logger.error(f"Failed to load decision prompt: {e}")
            raise
    
    def _load_final_iteration_prompt(self):
        """Load the final iteration prompt from the txt file."""
        try:
            final_iteration_prompt_file = "final_iteration_prompt.txt"
            if not os.path.exists(final_iteration_prompt_file):
                raise FileNotFoundError(f"Final iteration prompt file not found: {final_iteration_prompt_file}")
            
            with open(final_iteration_prompt_file, 'r', encoding='utf-8') as f:
                self.final_iteration_prompt = f.read().strip()
            
            verbose_log(f"Final iteration prompt loaded from {final_iteration_prompt_file}")
            
        except Exception as e:
            logger.error(f"Failed to load final iteration prompt: {e}")
            raise
    
    def _initialize_database(self):
        """Initialize database connection."""
        self.db_manager = DatabaseManager(self.db_config)
        if not self.db_manager.connect():
            raise ConnectionError("Failed to connect to database")
    
    def _initialize_openai(self):
        """Initialize OpenAI client."""
        if not self.openai_config.get('api_key'):
            logger.warning("OpenAI API key not found. Chat functionality will be limited.")
            return
        
        try:
            self.openai_client = OpenAI(api_key=self.openai_config['api_key'])
            verbose_log("OpenAI client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {e}")
            raise
    
    def _extract_sql_queries(self, text: str) -> List[str]:
        """Extract SQL queries from text that are wrapped in ```sql code blocks."""
        sql_pattern = r'```sql\s*(.*?)\s*```'
        matches = re.findall(sql_pattern, text, re.DOTALL | re.IGNORECASE)
        return [match.strip() for match in matches if match.strip()]
    
    def _execute_sql_queries(self, queries: List[str]) -> List[Dict[str, Any]]:
        """Execute SQL queries and return results."""
        all_results = []
        
        for query in queries:
            verbose_log(f"Executing SQL query: {query[:100]}...")
            success, results, error = self.db_manager.execute_query(query)
            
            if success:
                all_results.append({
                    'query': query,
                    'success': True,
                    'results': results,
                    'row_count': len(results)
                })
                verbose_log(f"Query executed successfully, returned {len(results)} rows")
            else:
                all_results.append({
                    'query': query,
                    'success': False,
                    'error': error,
                    'results': []
                })
                logger.error(f"Query failed: {error}")
        
        return all_results
    
    def _format_query_results(self, query_results: List[Dict[str, Any]]) -> str:
        """Format query results for display."""
        if not query_results:
            return ""
        
        formatted_output = []
        
        for result in query_results:
            if result['success']:
                if result['results']:
                    # Convert results to a readable format
                    formatted_output.append(f"Query executed successfully ({result['row_count']} rows):")
                    
                    # Show first few rows as example
                    for i, row in enumerate(result['results'][:5]):
                        formatted_output.append(f"Row {i+1}: {dict(row)}")
                    
                    if result['row_count'] > 5:
                        formatted_output.append(f"... and {result['row_count'] - 5} more rows")
                else:
                    formatted_output.append("Query executed successfully (no rows returned)")
            else:
                formatted_output.append(f"Query failed: {result['error']}")
        
        return "\n".join(formatted_output)
    
    def _get_ai_response(self, user_message: str) -> str:
        """Get AI response using OpenAI API."""
        if not self.openai_client:
            return "OpenAI API not available. Please configure your API key."
        
        try:
            # Prepare messages for the conversation
            messages = [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": user_message}
            ]
            
            # Add conversation history
            for msg in self.conversation_history[-10:]:  # Keep last 10 messages
                messages.append(msg)
            
            response = self.openai_client.chat.completions.create(
                model=self.openai_config.get('model', 'gpt-3.5-turbo'),
                messages=messages,
                max_tokens=self.openai_config.get('max_tokens', 2000),
                temperature=self.openai_config.get('temperature', 0.3)
            )
            
            ai_response = response.choices[0].message.content
            return ai_response
            
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            return f"Error getting AI response: {e}"
    
    def process_message(self, user_message: str) -> str:
        """Process a user message and return the chatbot's response."""
        verbose_log(f"Processing user message: {user_message[:100]}...")
        
        # Add user message to conversation history
        self.conversation_history.append({"role": "user", "content": user_message})
        
        # Process the message with iterative query support
        return self._process_with_iterative_queries(user_message)
    
    def _process_with_iterative_queries(self, original_user_message: str, max_iterations: int = 5) -> str:
        """Process a message with support for iterative queries."""
        iteration_count = 0
        current_context = original_user_message
        
        while iteration_count < max_iterations:
            iteration_count += 1
            verbose_log(f"Processing iteration {iteration_count} for message: {original_user_message[:100]}...")
            
            # Get AI response (may contain SQL queries)
            ai_response = self._get_ai_response(current_context)
            
            # Add AI response to conversation history
            self.conversation_history.append({"role": "assistant", "content": ai_response})
            
            # Extract SQL queries from the response
            sql_queries = self._extract_sql_queries(ai_response)
            
            # If no SQL queries found, this is the final response
            if not sql_queries:
                verbose_log("No SQL queries found - returning final response")
                return ai_response
            
            # Execute SQL queries
            verbose_log(f"Found {len(sql_queries)} SQL queries to execute")
            query_results = self._execute_sql_queries(sql_queries)
            query_output = self._format_query_results(query_results)
            
            if not query_output:
                # No query results, return the AI response
                return ai_response
            
            # Show results to the model and let it decide next action
            decision_message = f"""The following SQL queries were executed and returned these results:

{query_output}

{self.decision_prompt.format(
                query_output=query_output,
                original_user_message=original_user_message,
                iteration_count=iteration_count,
                max_iterations=max_iterations
            )}"""
            
            # Update context for next iteration
            current_context = decision_message
            
            # Add the decision message to conversation history
            self.conversation_history.append({"role": "user", "content": decision_message})
        
        # If we've reached max iterations, get a final response
        logger.warning(f"Reached maximum iterations ({max_iterations}) for message")
        final_message = self.final_iteration_prompt.format(
            max_iterations=max_iterations,
            original_user_message=original_user_message
        )
        
        final_response = self._get_ai_response(final_message)
        self.conversation_history.append({"role": "user", "content": final_message})
        self.conversation_history.append({"role": "assistant", "content": final_response})
        
        return final_response
    
    def start_chat(self):
        """Start the interactive chat session."""
        print("=" * 60)
        print("Incentives and Companies Chatbot")
        print("=" * 60)
        print("Welcome! I can help you explore the incentives and companies database.")
        print("Ask me about companies, incentives, or any related information.")
        print("Type 'quit', 'exit', or 'bye' to end the conversation.")
        print("=" * 60)
        
        while True:
            try:
                user_input = input("\nYou: ").strip()
                
                if user_input.lower() in ['quit', 'exit', 'bye', 'q']:
                    print("\nGoodbye! Thanks for using the Incentives and Companies Chatbot.")
                    break
                
                if not user_input:
                    continue
                
                print("\nBot: ", end="", flush=True)
                response = self.process_message(user_input)
                print(response)
                
            except KeyboardInterrupt:
                print("\n\nGoodbye! Thanks for using the Incentives and Companies Chatbot.")
                break
            except Exception as e:
                logger.error(f"Error in chat loop: {e}")
                print(f"\nError: {e}")
    
    def cleanup(self):
        """Clean up resources."""
        if self.db_manager:
            self.db_manager.disconnect()

def main():
    """Main function to run the chatbot."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Augusta Incentives Chatbot')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging output')
    args = parser.parse_args()
    
    # Setup logging based on verbose flag
    setup_logging(verbose=args.verbose)
    
    try:
        # Validate configuration
        if not validate_config():
            print("Configuration validation failed. Please check your settings.")
            return 1
        
        # Create and start chatbot
        chatbot = AugustaIncentivesChatbot()
        
        try:
            chatbot.start_chat()
        finally:
            chatbot.cleanup()
        
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"Fatal error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
