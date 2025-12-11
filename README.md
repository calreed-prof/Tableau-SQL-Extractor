# Tableau SQL Extractor

Extract SQL queries from Tableau packaged data sources (.tdsx files) - locally or directly from Tableau Server/Cloud.

## Installation

### Windows

1. Clone or download this repo
2. Install dependencies: `pip install requests`
3. Add the repo folder to your PATH:
   - Press `Win + X` → System → Advanced system settings → Environment Variables
   - Edit "Path" under User variables → New → Add folder path
   - Restart terminal

### Mac/Linux

```bash
# Clone the repo
git clone https://github.com/yourusername/tableau-sql-extractor.git
cd tableau-sql-extractor

# Install dependencies
pip install requests

# Make executable (optional)
chmod +x tableau_sql_extractor.py

# Add alias to your shell profile (~/.bashrc, ~/.zshrc, etc.)
alias tableau-sql='python /path/to/tableau_sql_extractor.py'
```

## Usage

```bash
# Extract from local file
tableau-sql mydata.tdsx

# Save SQL to files
tableau-sql mydata.tdsx ./output

# Download from Tableau Server/Cloud
tableau-sql "https://tableau.com/#/site/mysite/datasources/abc123" --token YOUR_TOKEN

# Show help
tableau-sql --help
```

## Features

- ✅ Extract custom SQL from .tdsx files
- ✅ Download directly from Tableau Server/Cloud URLs
- ✅ Save extracted SQL to individual .sql files
- ✅ Support for multiple connections per data source

## Requirements

- Python 3.6+
- `requests` library

## License

MIT