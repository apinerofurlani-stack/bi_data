#This project implements a BI observability solution that extracts metadata from Tableau Server/Online to monitor the usage, health, and staleness of analytics assets

##Architecture

#The pipeline follows an ETL (Extract-Transform-Load) pattern:

#Scheduling: The script is designed to run daily or hourly using an orchestrator (such as Apache Airflow), ensuring that operational visibility remains up to date.

#Credentials: The solution uses Tableau Personal Access Tokens (PAT). Credentials are managed via environment variables or a secrets manager, avoiding the use of plain-text user passwords in the code.

#Idempotency and Duplicates: The PostgreSQL load utilizes the ON CONFLICT (asset_id) DO UPDATE clause. This guarantees that, regardless of how many times the script is executed, records are not duplicated and information (such as status or last_updated) is always current.

#Retry and Failure: The logic includes try-except blocks. In a production environment, the orchestrator (Airflow) manages the retry policy (e.g., 3 retries with exponential backoff) for network or API failures.

#Storage Design: Data is structured within a Semantic Layer schema in PostgreSQL (bi_data). It uses a dimensional table dim_tableau_assets optimized for direct consumption by BI tools.

#Transformations: Business logic (such as the derived status field calculation) is performed in-memory using Pandas, allowing for agile transformation before database persistence


##Monitoring and Alerting

#Stale Dashboards: A weekly automated report to notify asset owners of content with an 'Obsolete' status (e.g., >90 days without usage or updates), prompting them to either certify or decommission the content.

#Refresh Failures: "High Severity" alerts via Slack or Email if critical assets (marked as favorites or having high traffic) show a refresh_status = 'Failed'.

#Usage Anomalies: Monitoring the views_last_30d metric. If the total view volume drops by more than 40% compared to the 3-week moving average, an alert is triggered for potential server incidents or widespread access issues.


##Design Decisions

#Decision: Used psycopg2.extras.execute_values with ON CONFLICT logic instead of Pandas' df.to_sql.
#Justification: While to_sql is easier to implement, it lacks native support for updating existing records (Upsert) in PostgreSQL without either dropping and recreating the table or requiring complex additional libraries. By using execute_values, the script is significantly faster for bulk insertions and maintains referential integrity and asset history. This approach allows specific fields to be updated only when they change, fulfilling the idempotency principle required for mission-critical systems.



Part 2 — Data Governance 

How would you ensure that business metrics are consistently defined across dashboards?
I would implement a Single Source of Truth (SSOT) by creating certified data sources. Each source must have a designated Data Owner responsible for validation. 
Additionally, I would use Tableau's 'Certified Data Sources' feature to visually signal to users which dashboards are powered by audited and official data


How would you detect if a dashboard is using incorrect or outdated data sources?
I would monitor the Tableau Metadata API and cross-reference it with our data warehouse logs. This allows us to identify dashboards querying deprecated or legacy tables, enabling a proactive migration strategy instead of waiting for reports to break


If two teams define the same metric differently (for example Active Users), how would you resolve this?
I would facilitate a meeting between both teams to align definitions based on the global Business Glossary. If a consensus isn't reached, I would escalate to a Data Governance Committee or leadership for a final decision. 
If both metrics must coexist, they will be clearly labeled and documented in our Data Dictionary to prevent confusion.


Part 3 — Dashboard Design Thinking

Global Indicator (Gauge Chart)
Metric: Percentage of dashboards with refresh_status = 'Success'. Provides an immediate snapshot of data reliability. If this drops, the BI team knows there is a systemic issue with the data extracts or server connectivity.

Dashboard Inventory (Treemap)
Metric: status (Active vs. Obsolete). The size of each rectangle represents the volume of dashboards in each state, making it immediately obvious if "Obsolete" content is consuming more space or resources than "Active" content

Data Latency (Heatmap)
Metric: Time elapsed since last_refresh. It ensures that decision-makers are not looking at yesterday's news.

Top 8 Most Active Assets (Bar Chart)
Metric: Ranking by views_last_30d. These are the assets that require the strictest QA and performance optimization since they have the highest business impact.

Average Query Execution Time (Performance Monitor)
Metric: Average dashboard load time (in seconds). If a dashboard takes too long to load, user adoption will drop. This metric identifies technical bottlenecks that require SQL optimization or filter refactoring

Security & Permissions
Metric: Ratio of "Public/Open" vs. "Restricted" access assets. This is critical for Data Compliance and security. Monitoring how many assets contain sensitive information and who has access to them helps prevent data leaks.

