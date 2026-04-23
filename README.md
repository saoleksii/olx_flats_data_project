# Data engineering project: olx data scraping with playwright, bs4, git actions; pipeline and analysis using databricks.
## Overview

This project is a data engineering initiative designed to analyze the long-term residential rental market in Kyiv. At its core, it features an automated, asynchronous web scraper that extracts real estate listings from OLX.ua and ingests the raw data directly into a **Databricks** for processing and analytics.

The pipeline ensures idempotency by tracking the last processed listing ID, preventing duplicate data ingestion, and utilizes Playwright to break scraper defence.

## Architecture & Features

- **Asynchronous Scraping**: Utilizes `Playwright` and `BeautifulSoup4` for high-performance, asynchronous data extraction.
- **State Management**: Persists the `last_id.txt` state in Databricks Volumes to seamlessly resume scraping from the latest unseen listing.
- **Bronze Layer Ingestion**: Automatically authenticates and uploads structured JSON payloads into a Databricks Volume using the Databricks SDK.
- **Automated Workflows**: Includes a GitHub Actions CI/CD pipeline (`scrape_pipeline.yml`) to schedule data extraction multiple times a day (7:00, 12:00, 16:00, 21:00 UTC).
- **Anti-Bot Mitigation**: Employs dynamic User-Agent rotation and headless Chromium configurations to reliably fetch data.

## Project Structure

```text
olx_flats_data_project/
├── .github/workflows/
│   └── scrape_pipeline.yml   # GitHub Actions workflow for scheduled runs
├── fetch.py                  # Main asynchronous scraper and Databricks ingestion script
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables (not tracked in git)
└── README.md                 # Project documentation
```

## Setup and Installation

### Prerequisites

- Python 3.13 or higher
- A Databricks Workspace Free Edition
- Github Actions

### 1. Clone the repository

```bash
git clone https://github.com/saoleksii/olx_flats_data_project.git
cd olx_flats_data_project
```

### 2. Create a Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

### 4. Environment Variables Setup

Create a `.env` file in the root directory and configure your Databricks connection:

```env
URL_HOST=https://<your-databricks-workspace-url>
TOKEN=<your-databricks-personal-access-token>
```

> **Note:** Ensure your Databricks target path (`/Volumes/workspace/default/olx_flats_data`) is correctly configured in `fetch.py` or customize it according to your Databricks Unity Catalog structure.

## How to Run

### Running Locally

To execute the scraper manually on your local machine, simply run:

```bash
python fetch.py
```

The script will:
1. Initialize a Playwright headless browser.
2. Fetch the last processed ID from Databricks.
3. Scrape new listings chronologically until it reaches the last processed ID.
4. Upload a new `olx_data_YYYYMMDD_HHMM.json` file to the Databricks Volume.
5. Update the `last_id.txt` in Databricks.

### Automated Runs (GitHub Actions)

The project is pre-configured with a GitHub Actions workflow. The scraper runs automatically on a CRON schedule (4 times a day). 

To enable this, ensure you add the following **Repository Secrets** in your GitHub repository settings (`Settings > Secrets and variables > Actions`):
- `URL_HOST`: Your Databricks workspace URL.
- `TOKEN`: Your Databricks access token.

You can also trigger the workflow manually using the `workflow_dispatch` event from the GitHub Actions tab.

## Data Schema

The exported JSON files contain arrays of property records formatted as follows:

```json
[
    {
        "id": "123456789",
        "title": "Здам 1-кімнатну квартиру в центрі",
        "price": "15 000 грн.",
        "location": "Київ, Печерський - Сьогодні о 14:30",
        "size": "45 м²",
        "url": "https://www.olx.ua/d/uk/obyavlenie/...",
        "time_collected": "2026-04-23T14:35:00.000000",
        "source": "olx_kyiv"
    }
]
```

## Logging

The application maintains comprehensive logging configurations both to the console, offering visibility into pagination progress, encountered records, and Databricks API interactions.
