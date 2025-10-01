"""
Configuration module for Augusta Incentives project.

This module centralizes all configuration settings including database,
OpenAI API, and other application settings.
"""

import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

def load_secrets(secrets_file: str = 'secrets.json') -> Dict[str, Any]:
    """Load secrets from JSON file."""
    try:
        with open(secrets_file, 'r') as f:
            secrets = json.load(f)
        logger.info(f"Secrets loaded from {secrets_file}")
        return secrets
    except FileNotFoundError:
        logger.error(f"Secrets file {secrets_file} not found")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in secrets file: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading secrets: {e}")
        raise

# Default database configuration
DEFAULT_DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'augusta_incentives',
    'user': 'postgres',  
    'password': 'admin' 
}

# Default OpenAI configuration
DEFAULT_OPENAI_CONFIG = {
    'model': 'gpt-4o', #'gpt-3.5-turbo', 
    'max_tokens': 2000,
    'temperature': 0.3
}

# Load configuration from secrets file
try:
    SECRETS = load_secrets()
    
    # Database configuration
    DB_CONFIG = SECRETS.get('database', DEFAULT_DB_CONFIG)
    
    # OpenAI configuration
    OPENAI_CONFIG = {
        'api_key': SECRETS.get('openai_api_key'),
        **DEFAULT_OPENAI_CONFIG
    }
    
    logger.info("Configuration loaded successfully from secrets file")
    
except Exception as e:
    logger.warning(f"Failed to load secrets, using default configuration: {e}")
    
    # Fallback to default configuration
    DB_CONFIG = DEFAULT_DB_CONFIG
    OPENAI_CONFIG = {
        'api_key': None,
        **DEFAULT_OPENAI_CONFIG
    }

# Application settings
APP_CONFIG = {
    'log_level': 'INFO',
    'log_file': 'augusta_incentives.log',
    'results_dir': 'results',
    'max_matches_per_incentive': 10
}

def get_db_config() -> Dict[str, Any]:
    """Get database configuration."""
    return DB_CONFIG.copy()

def get_openai_config() -> Dict[str, Any]:
    """Get OpenAI configuration."""
    return OPENAI_CONFIG.copy()

def get_app_config() -> Dict[str, Any]:
    """Get application configuration."""
    return APP_CONFIG.copy()

def validate_config() -> bool:
    """Validate that all required configuration is present."""
    errors = []
    
    # Check database config
    required_db_fields = ['host', 'port', 'database', 'user', 'password']
    for field in required_db_fields:
        if not DB_CONFIG.get(field):
            errors.append(f"Missing database configuration: {field}")
    
    # Check OpenAI config
    if not OPENAI_CONFIG.get('api_key'):
        errors.append("Missing OpenAI API key")
    
    if errors:
        logger.error("Configuration validation failed:")
        for error in errors:
            logger.error(f"  - {error}")
        return False
    
    logger.info("Configuration validation passed")
    return True
