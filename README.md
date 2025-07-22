# WachtWijzer: Amsterdam City Offices Wait Times

**WachtWijzer** is a web application that provides real-time wait times for all city offices (*stadsloketten*) in Amsterdam. It helps residents and visitors find the office with the shortest queue, view historical wait time patterns, and plan their visits more efficiently.

The project is built with a Python Flask backend, a dynamic Chart.js frontend, and is fully integrated with Google AdSense for monetization.

![WachtWijzer Screenshot](https://i.imgur.com/yB5kP9v.png)

## Key Features

*   **Live Wait Times**: Fetches and displays up-to-the-minute wait times and queue lengths for all Amsterdam city offices.
*   **Recommended Location**: Highlights the office with the shortest current wait time.
*   **Interactive Wait Time Patterns**: A dynamic chart shows historical average wait times, filterable by day of the week and specific locations.
*   **Click-to-Filter**: Users can click on any location card or the chart legend to instantly filter the graph and scroll to the patterns section.
*   **Multi-Language Support**: The UI is translated into Dutch, English, Turkish, and Moroccan Arabic.
*   **Google AdSense & CMP**: Fully integrated with Google AdSense and its Consent Management Platform (CMP) for handling GDPR and privacy regulations.

## Technology Stack

*   **Backend**: Python, Flask
*   **Database**: PostgreSQL (production), SQLite (local development)
*   **Frontend**: HTML5, CSS3, Vanilla JavaScript
*   **Charting**: Chart.js
*   **Deployment**: Deployed on Render (or any WSGI-compatible host)
*   **Data Collection**: Scheduled Python scripts (`cron_data_collector.py`)

---

## Local Development Setup

Follow these steps to run the application on your local machine.

### 1. Prerequisites

*   Python 3.9+
*   Git
*   A running PostgreSQL instance (optional, SQLite can be used for simplicity)

### 2. Clone the Repository

```bash
git clone https://github.com/your-username/StadsloketWachtWijzer.git
cd StadsloketWachtWijzer
```

### 3. Set Up a Virtual Environment

It is highly recommended to use a virtual environment to manage dependencies.

```bash
# Create the virtual environment
python -m venv venv

# Activate it
# On macOS/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate
```

### 4. Install Dependencies

Install all required Python packages from `requirements.txt`.

```bash
pip install -r requirements.txt
```

### 5. Configure Environment Variables

Create a `.env` file in the project root. This file is ignored by Git and will store your local configuration. You can start by copying the example file:

```bash
cp .env.example .env
```

Now, edit the `.env` file. For a quick start with SQLite, it only needs one line:

```env
# .env
DATABASE_URL=sqlite:///local.db
```

If you prefer to use PostgreSQL, update the `DATABASE_URL` accordingly:

```env
DATABASE_URL=postgresql://user:password@host:port/database_name
```

### 6. Initialize the Database

Run the following command to create the necessary database tables.

```bash
python -c "from wait_time_data import create_database; import os; create_database(os.getenv('DATABASE_URL'))"
```

### 7. Run the Application

Start the Flask development server:

```bash
python app.py
```

The application will now be running at **`http://127.0.0.1:5050`**.

### 8. Testing Google's Consent Banner

To force the Google Consent Management Platform (CMP) banner to appear for testing, even if you are outside the EEA, append the `?fc=alwaysshow` query parameter to the URL:

[**http://127.0.0.1:5050/?fc=alwaysshow**](http://127.0.0.1:5050/?fc=alwaysshow) 