# HubSpot Big Data Analytics Platform

A full-stack big data analytics platform that generates, processes, and visualizes **4.85 million+ records** across 7 CRM entity types. Features a premium dark-themed dashboard with 20+ interactive charts powered by Chart.js.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA GENERATION                           │
│  generate_data.py → 4.85M records → Parquet (Snappy compression)│
├──────────────────────────────────────────────────────────────────┤
│                        ETL PIPELINE                              │
│  data_pipeline.py → Transform + Feature Engineering → Aggregate  │
│  7 entity processors │ 22 aggregation functions │ MapReduce-style│
├──────────────────────────────────────────────────────────────────┤
│                      ANALYTICS ENGINE                            │
│  analytics.py → JSON-serializable KPIs + chart data providers    │
├──────────────────────────────────────────────────────────────────┤
│                    FLASK API + DASHBOARD                         │
│  app.py → REST API │ Chart.js SPA │ Glassmorphism dark UI        │
└──────────────────────────────────────────────────────────────────┘
```

## Data Scale

| Entity           | Records     | Features                                    |
|------------------|-------------|---------------------------------------------|
| Companies        | 50,000      | Industry, region, revenue, employee count   |
| Contacts         | 500,000     | Lead score, lifecycle stage, engagement     |
| Deals            | 300,000     | Pipeline, stage, probability, velocity      |
| Marketing Events | 1,000,000   | Channel, event type, device, engagement     |
| Email Campaigns  | 800,000     | Campaign type, open/click/bounce rates      |
| Support Tickets  | 200,000     | Priority, SLA compliance, CSAT              |
| Web Analytics    | 2,000,000   | Pages, countries, devices, conversions      |
| **Total**        | **4,850,000** |                                            |

## Dashboard Features

- **8 tabbed sections**: Overview, Revenue & Deals, Marketing, Email, Contacts, Support, Web Analytics, Companies
- **15 KPI cards** with real-time data
- **20+ interactive charts**: line, bar, radar, polar area, doughnut, funnel visualizations
- **Data tables** with inline metric bars
- **Premium dark theme** with glassmorphism, gradient accents, and smooth animations
- **Fully responsive** design for desktop and mobile

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run full pipeline (generate → ETL → dashboard)
python run_pipeline.py

# Or run steps individually:
python run_pipeline.py --generate    # Generate 4.85M records
python run_pipeline.py --etl         # Run ETL pipeline
python run_pipeline.py --dashboard   # Launch dashboard on :5000
```

## Project Structure

```
HubSpot/
├── run_pipeline.py            # One-command pipeline runner
├── requirements.txt           # Python dependencies
├── data/
│   └── generate_data.py       # Vectorized data generator (NumPy)
├── src/
│   ├── config.py              # Global configuration
│   ├── data_pipeline.py       # ETL: transform + 22 aggregations
│   └── analytics.py           # JSON-ready analytics engine
├── dashboard/
│   ├── app.py                 # Flask web server + REST API
│   ├── static/
│   │   ├── css/style.css      # Premium dark theme CSS
│   │   └── js/dashboard.js    # Chart.js rendering engine
│   └── templates/
│       └── index.html         # SPA dashboard template
└── tests/
    ├── test_pipeline.py       # Pipeline & aggregation tests
    └── test_analytics.py      # Analytics formatting tests
```

## Tech Stack

- **Data Processing**: Python, NumPy, Pandas, PyArrow (Parquet)
- **Web Server**: Flask
- **Frontend**: Chart.js 4, Inter font, custom CSS (no frameworks)
- **Testing**: pytest
- **Storage**: Parquet with Snappy compression

## API Endpoints

| Endpoint                   | Description                    |
|----------------------------|--------------------------------|
| `GET /api/dashboard`       | Full dashboard payload (all)   |
| `GET /api/kpis`            | Top-level KPI metrics          |
| `GET /api/revenue-trend`   | Monthly revenue & pipeline     |
| `GET /api/deals/stages`    | Deal pipeline funnel           |
| `GET /api/deals/region`    | Revenue by region              |
| `GET /api/deals/industry`  | Revenue by industry            |
| `GET /api/marketing/trend` | Marketing events trend         |
| `GET /api/email/performance` | Email campaign metrics       |
| `GET /api/web/trend`       | Web sessions & conversions     |
| `GET /api/support/category`| Support ticket analytics       |
| ...and 12 more endpoints   |                                |

## Running Tests

```bash
pytest tests/ -v
```
