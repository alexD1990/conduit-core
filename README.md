# Conduit Core

**An open-source command-line tool (CLI) for declarative, reliable, and testable data ingestion.**

---

## Vision

Conduit Core is designed to be the industry standard for the "Extract & load" part of the modern data stack. Our goal is to be the perfect companion to **dbt**, bringing principles like modularity, version control, and simple configuration to the world of data ingestion.

## The Problem It Solves

The process of getting data into the data warehouse is often a disorganized landscape of custom, brittle Python scripts. Conduit Core solves this by standardizing and simplifying this process, reducing sources of error and maintenance overhead.

## Getting Started: A 4-Step Guide

Follow these steps to set up and run your first data ingestion pipeline with Conduit Core.

### Step 1: Install Conduit Core

First, install the tool using pip:
```bash
pip install conduit-core
```

### Step 2: Define Your Pipeline in ```ingest.yml```

Next, create a file named ```ingest.yml``` in your project's root directory. This file is your main control panel where you declare what data to move, from where, and to where.

Example: Here, we define a pipeline to fetch new user data from an Azure SQL database and save it to a local CSV file.

sources:
  - name: my_azure_db
    type: azuresql

destinations:
  - name: local_csv_output
    type: csv
    path: "./output/azure_users.csv"

resources:
  - name: get_new_users
    source: my_azure_db
    destination: local_csv_output
    incremental_column: "UserID"
    query: "SELECT UserID, Username, Email FROM dbo.Users WHERE UserID > :last_value;"

### Step 3: Configure Your Secrets in .env

For sources that require passwords or API keys, create a .env file in the same directory. This file keeps your secrets safe and out of version control.

Remember to add .env to your .gitignore file!

# Credentials for the 'my_azure_db' source
DB_SERVER=your-server-name.database.windows.net
DB_DATABASE=your-database
DB_USER=your-username
DB_PASSWORD=your-password

### Step 4: Run Your Pipeline

Finally, execute the ingestion from your terminal:

```bash
conduit run
```

# Conduit Core will now:

1. Read your ```ingest.yml``` configuration.

2. Load the necessary secrets from your ```.env file.```

3. Connect to the Azure SQL database.

4. Run the query to fetch only new rows.

5. Write the new data to ./output/azure_users.csv.