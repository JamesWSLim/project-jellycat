<h1 align="center">Jellycat Project</h1>

## Description
Jellycat Price Tracker & Information Project is a web scraping, ETL, and dashboarding project. The goal of this project is to create an interactive and comprehensive dashboard to keep track of daily price, daily availability, and days before restocking of Jellycat products with the combination of different technologies, demonstrating a data pipeline from obtaining data to displaying data and analysis. 
## Jellycat Dashboard (https://project-jellycat.streamlit.app/)

![](streamlit-streamlit_dashboard-2024-01-15-17-01-26.gif)

Here are some applications used in the project:
* **Playwright (web scraping)**: 
    * Cross-Browser Automation: automate browser actions across different browsers, ensuring consistent behavior
    * Headless Mode: You can run Playwright in headless mode for faster execution, suitable for server-side and CI/CD environments
* **PostgreSQL (Database to store scraped data)**:
    * Reliability and Stability
    * ACID Compliance: ensuring data integrity
    * Extensibility: supports a wide range of data types and indexing options
* **Delta Lake with Spark(ETL pipeline)**:
    * Integration with Apache Spark: leverage Spark's speed and performance, scalability, and fault tolerance through resilient distributed datasets (RDDs), providing ACID transactions
    * Time Travel: Access/revert to earlier versions of data for audits, rollbacks, or reproduce
    * Change Data Capture/Feed (CDC): tracks row-level changes between versions for all the data written into tables, which includes row data along with metadata whether the specified row was inserted, deleted, or updated
* **Streamlit with Matplotlib (Dashboard and visualization)**:
    * Data Integration: Streamlit seamlessly integrates with popular data science libraries like Pandas, NumPy, and Matplotlib, enabling you to incorporate data analysis and visualization directly into your app
    * Interactive Data Apps: easily turn your data analysis scripts into interactive web applications without the need for extensive web development knowledge
    * Simple Scripting: create interactive web applications using just a few lines of Python code, enabling rapid development and prototyping