# 📊 EAS503 Project: AI-Powered Sales Analytics Dashboard

## Project Overview
This project is a data science web application built with **Streamlit** that visualizes sales data from a normalized SQL database. It features a secure login system, a two-column dashboard layout for standard reporting, and an integrated **AI Assistant (powered by Groq/LLaMA)** that converts natural language questions into executable SQL queries.

## 🎯 Project Features (Grading Criteria Mapping)

This repository and application are designed to meet the specific requirements of the live demo:

### 1. Database Connectivity 🗄️
- The application connects to a **normalized SQLite database** (`normalized.db`).
- *Note for Grader:* The Render database connection details are available for inspection during the live demo.

### 2. Jupyter Notebook Integration 📓
- A Jupyter Notebook (`database_connection_test.ipynb`) is included in this repository.
- It demonstrates programmatic connection to the database and executes test queries to verify data integrity outside the Streamlit app.

### 3. Streamlit Web App Interface 🖥️
- **Password Protection:** The app is secured via a login portal. Access is restricted until the correct password is provided.
- **Two-Column Layout:** Once logged in, the dashboard utilizes a clean, two-column layout (Sidebar for controls/tabs, Main area for data) to present query controls and results side-by-side.

### 4. AI & Natural Language Processing 🤖
- **"ChatGPT" / LLM Integration:** The app integrates with the **Groq API (LLaMA 3.3)**.
- Users can ask questions in plain English (e.g., *"Who are the top 5 customers?"*), and the AI generates the corresponding SQL query and retrieves the results automatically.

---

## 🛠️ Tech Stack
- **Frontend:** Streamlit
- **Backend/Logic:** Python
- **Database:** SQLite / Render (PostgreSQL)
- **AI/LLM:** Groq API (LLaMA 3.3-70b)
- **Data Manipulation:** Pandas

## 📂 Repository Structure
```text
├── app.py                  # Main Streamlit application (UI & Logic)
├── sujal_codio_project.py  # Backend logic and predefined SQL functions
├── normalized.db           # SQLite Database file
├── database_connection_test.ipynb  # Notebook for DB connection demo
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation