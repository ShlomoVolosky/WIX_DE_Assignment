# WixDEAssignment

This repository serves as a reference architecture and example code for a **production-grade data pipeline** that integrates data from **Polygon.io** and **Frankfurter**. The pipeline covers:
- Extracting data from the two APIs  
- Transforming and normalizing the data  
- Loading it into a dimensionally modeled data warehouse  
- Analyzing results using **Streamlit**  
- Automating and scheduling with **Airflow**  
- Containerizing everything using **Docker** / **docker-compose**  
- Including **unit tests** for major components  

---
## Table of Contents
1. [Project Overview](#project-overview)  
2. [Directory Structure](#directory-structure)  
3. [Components](#components)  
   - [Docker](#docker)  
   - [Airflow](#airflow)  
   - [src/config](#srconfig)  
   - [src/pipeline](#srcpipeline)  
   - [src/warehouse](#srcwarehouse)  
   - [src/tests](#srctests)  
   - [streamlit_app](#streamlit_app)  
4. [Getting Started](#getting-started)  
5. [Usage](#usage)  
6. [Contributing](#contributing)  
7. [License](#license)  

---

## Project Overview
This project demonstrates how you can set up a scalable and maintainable data pipeline for **financial** and **currency exchange** data:

1. **Extract** real-time or historical market data from [Polygon.io](https://polygon.io/) and exchange rate data from [Frankfurter](https://www.frankfurter.app/).  
2. **Transform** and normalize the data, including currency conversions, data cleaning, and joining.  
3. **Load** the processed data into a **dimensionally modeled** data warehouse for optimized querying and analysis.  
4. **Analyze** data in real-time or near real-time with an interactive **Streamlit** application.  
5. **Automate** recurring tasks (extractions, transformations, data loads) using **Airflow**, scheduled to run at desired intervals.  
6. **Containerize** each component (Airflow, Streamlit, database, etc.) to simplify deployment and ensure consistency across environments.  
7. **Test** critical steps of the pipeline using Python unit tests to help ensure quality and maintainability.

---

## Directory Structure
Below is a suggested layout for the repository. Feel free to adapt it to your needs:

