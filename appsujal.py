import os
import streamlit as st
import pandas as pd
import sqlite3
from groq import Groq

# Import your custom modules
from sujal_codio_project import create_connection, ex1, ex2, ex3, ex4, ex5, ex6, ex7, ex8, ex9, ex10, ex11

# --- CONFIGURATION & SETUP ---
st.set_page_config(
    page_title="Enterprise Sales Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

DB_PATH = "normalized.db"
APP_PASSWORD = os.getenv("APP_PASSWORD", "12345678")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# --- AUTHENTICATION STATE MANAGEMENT ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

def check_password():
    if st.session_state["password_input"] == APP_PASSWORD:
        st.session_state.authenticated = True
    else:
        st.error("❌ Incorrect password. Please access denied.")

# --- LOGIN SCREEN (UI LAYER 1) ---
if not st.session_state.authenticated:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown(
            """
            <div style='text-align: center; margin-top: 50px;'>
                <h1>🔒 Secure Analytics Portal</h1>
                <p>Please log in to access the sales database.</p>
            </div>
            """, 
            unsafe_allow_html=True
        )
        st.text_input("Enter Password", type="password", key="password_input", on_change=check_password)
    st.stop() # Halt app execution here if not logged in

# --- MAIN APP LOGIC (Only runs if authenticated) ---

if not GROQ_API_KEY:
    st.error("⚠️ System Alert: GROQ_API_KEY not found in environment variables.")
    st.stop()

groq_client = Groq(api_key=GROQ_API_KEY)

# --- HELPER FUNCTIONS ---
@st.cache_resource
def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    return conn

@st.cache_data
def get_customer_names():
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT DISTINCT FirstName || ' ' || LastName AS Name FROM Customer ORDER BY Name;",
        conn,
    )
    return df["Name"].tolist()

def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query(sql, conn)

# --- SIDEBAR: GLOBAL CONTROLS ---
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/2103/2103633.png", width=50) # Generic analytics icon
    st.title("Control Panel")
    
    st.markdown("### 👤 User Settings")
    customers = get_customer_names()
    selected_customer = st.selectbox("Active Customer Profile", customers)
    
    st.markdown("---")
    st.info(f"**Database:** `{DB_PATH}`\n\n**Status:** Connected ✅")
    
    if st.button("Logout", type="secondary"):
        st.session_state.authenticated = False
        st.rerun()

# --- MAIN DASHBOARD LAYOUT ---
st.title("📈 Enterprise Sales Dashboard")
st.markdown("Analyze sales performance using standard reporting tools or AI-assisted queries.")

# Use Tabs to separate functionality
tab_reports, tab_ai = st.tabs(["📊 Standard Reports", "🤖 AI Analyst"])

# --- TAB 1: PREDEFINED QUERIES ---
with tab_reports:
    col_rep_1, col_rep_2 = st.columns([1, 3])
    
    with col_rep_1:
        st.subheader("Report Type")
        query_option = st.radio(
            "Select Analysis View:",
            [
                "Customer Orders History (ex1)",
                "Individual Sales Total (ex2)",
                "Global Sales Summary (ex3)",
                "Custom SQL Query",
            ],
            label_visibility="collapsed"
        )
    
    with col_rep_2:
        st.subheader("Data Output")
        
        # Logic handling
        sql = ""
        run_data = False
        
        # We trigger the logic immediately based on radio selection, 
        # but for Custom SQL we need a text area.
        if query_option == "Custom SQL Query":
            custom_sql = st.text_area(
                "Enter raw SQL query:", 
                value="SELECT * FROM Customer LIMIT 5;",
                height=150
            )
            if st.button("Execute Custom Query", type="primary"):
                sql = custom_sql
                run_data = True
        else:
            # For predefined options, we run immediately or on a lightweight interaction
            run_data = True
            if query_option == "Customer Orders History (ex1)":
                sql = ex1(get_connection(), selected_customer)
            elif query_option == "Individual Sales Total (ex2)":
                sql = ex2(get_connection(), selected_customer)
            elif query_option == "Global Sales Summary (ex3)":
                sql = ex3(get_connection())

        # Display Section
        if run_data:
            # Show SQL in an expander so it doesn't clutter the view
            with st.expander("🔍 View Generated SQL Source"):
                st.code(sql, language="sql")
            
            try:
                df = run_query(sql)
                # Show metric if it's a single number (often nice for 'Totals')
                if len(df) == 1 and len(df.columns) == 1:
                    val = df.iloc[0, 0]
                    st.metric(label="Calculated Result", value=str(val))
                else:
                    st.dataframe(df, use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(f"Query Execution Failed: {e}")

# --- TAB 2: AI ASSISTANT ---
with tab_ai:
    st.markdown(
        """
        <div style='background-color:#f0f2f6; padding:20px; border-radius:10px; margin-bottom:20px'>
            <h4>🤖 Ask the AI Assistant</h4>
            <p>Describe what you want to know in plain English. The AI will generate the SQL and retrieve the data.</p>
        </div>
        """, unsafe_allow_html=True
    )
    
    nl_question = st.text_input("What insights are you looking for?", placeholder="e.g., Show me the top 5 products by price...")
    
    col_ai_btn, col_ai_space = st.columns([1, 5])
    with col_ai_btn:
        ask_btn = st.button("✨ Generate Insights", type="primary")
        
    if ask_btn:
        if not nl_question.strip():
            st.toast("Please enter a valid question.", icon="⚠️")
        else:
            schema_description = """
            Tables:
            - Region(RegionID, Region)
            - Country(CountryID, Country, RegionID)
            - Customer(CustomerID, FirstName, LastName, Address, City, CountryID)
            - ProductCategory(ProductCategoryID, ProductCategory, ProductCategoryDescription)
            - Product(ProductID, ProductName, ProductUnitPrice, ProductCategoryID)
            - OrderDetail(OrderID, CustomerID, ProductID, OrderDate, QuantityOrdered)
            """

            system_prompt = (
                "You are an assistant that writes SQL for a SQLite database. "
                "Return ONLY a valid SQL SELECT statement. "
                "Do not include explanations, comments, or markdown."
            )

            user_prompt = f"{schema_description}\n\nQuestion:\n{nl_question}\n\nSQL:"

            try:
                with st.spinner("🤖 Analyzing Schema & Generating Logic..."):
                    response = groq_client.chat.completions.create(
                        model="llama-3.3-70b-versatile",
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        temperature=0,
                    )
                    sql_from_ai = response.choices[0].message.content.strip()

                # Clean up AI response
                if sql_from_ai.startswith("```"):
                    sql_from_ai = sql_from_ai.strip("`").strip()
                    if sql_from_ai.lower().startswith("sql"):
                        sql_from_ai = sql_from_ai[3:].strip()

                st.success("Analysis Complete")
                
                # Split result into Code and Data
                res_col1, res_col2 = st.columns(2)
                
                with res_col1:
                    st.caption("Generated SQL Query")
                    st.code(sql_from_ai, language="sql")
                
                with res_col2:
                    st.caption("Query Results")
                    try:
                        df_ai = run_query(sql_from_ai)
                        st.dataframe(df_ai, use_container_width=True)
                    except Exception as e:
                        st.error(f"Execution Error: {e}")

            except Exception as e:
                st.error(f"Groq API Error: {e}")