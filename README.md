# Automated Data Warehouse - ETL Pipeline

A complete ETL pipeline that combines weather data from OpenWeatherMap API with population data into a PostgreSQL/SQLite data warehouse.

## 🚀 Features

- **Extract**: Real-time weather data from API + population data from CSV
- **Transform**: Data cleaning, validation, and standardization
- **Load**: Star schema data warehouse with facts and dimensions
- **Automation**: Scheduled daily runs
- **Modular**: Separate extraction, transformation, loading modules

## 📊 Data Model

- **dim_city**: City information and populations
- **dim_date**: Date dimension for time-based analysis
- **fact_weather**: Weather measurements and metrics

## 🛠️ Tech Stack

- Python 3.11
- SQLAlchemy
- PostgreSQL / SQLite
- OpenWeatherMap API
- Pandas for data processing

## 📁 Project Structure
data-warehouse/
├── src/ # ETL source code
├── sql/ # Database schemas and queries
├── config/ # Configuration files
├── logs/ # Application logs
└── data/ # Data files

## 🚦 Quick Start

1. Clone repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up environment variables in `.env`
4. Run: `python main.py`

## 📈 Sample Queries

See `sql/queries.sql` for business intelligence queries including:
- Average temperatures by city
- Weather patterns by month
- Population vs weather correlations
