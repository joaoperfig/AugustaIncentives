# Augusta Incentives

A Python application for finding correspondence between companies and incentives using AI-powered matching.

## Setup

### Prerequisites
1. **Install Python** (3.7 or higher)
2. **Install PostgreSQL** database
3. **Install requirements:**
   ```bash
   pip install -r requirements.txt
   ```

### Configuration
1. **Create `secrets.json`** in the project root:
   ```json
   {
     "database": {
       "host": "localhost",
       "port": 5432,
       "database": "augusta_incentives",
       "user": "postgres",
       "password": "your_password"
     },
     "openai_api_key": "your_openai_api_key"
   }
   ```

2. **Run database setup:**
   ```bash
   python database_setup.py
   ```

## Usage

### Correspondence Finder
Find matches between companies and incentives:
```bash
python correspondence_finder.py
```

### Chatbot
Interactive chatbot for querying the database:
```bash
python chatbot.py
```
