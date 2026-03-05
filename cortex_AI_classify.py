import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import json
import streamlit as st
import os
import io
import re
import time


# -------------------------------
# Setup Snowflake session
# -------------------------------
session = get_active_session()



# -------------------------------------
# Main Streamlit app
# -------------------------------------

    
st.title("Snowflake Cortex Thematic Classifier")

st.markdown("This App gives no code access to the Snowflake Classify function for csv files. ") 
st.markdown("1) **File Upload**: Upload a csv File to a table in Database: JBS_DATASETS, Schema: APP_STORAGE; using put_stream()")
st.markdown("2) **Classify**: Run AI_Classify; Input Categories and labels, a task description. Only input an example if you are running clssification on a single column of text")
st.markdown("See Snowflake Documentation for more details: [put_stream()](https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/snowpark/api/snowflake.snowpark.FileOperation.put_stream), [AI_CLASSIFY](https://docs.snowflake.com/en/sql-reference/functions/ai_classify)", unsafe_allow_html=True)
st.text("For questions or to give feedback contact apagano@jbsinternational.com")


sessh = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()
st.code(f"Session info; {sessh}")

def q(name):
    return f'"{name}"'




def clean_name(s):
    s = re.sub(r"[^\w]+", "_", s)       # replace non-alphanum with underscore
    return re.sub(r'_+', '_', s).strip('_').upper()


def process_df(df):
    df.columns = [clean_name(c) for c in df.columns]
    #df.to_csv(path, index=False)
    return df


# -------------------------------
# Step 1-3: Database / Schema / Table
# -------------------------------
# databases = [row["name"] for row in session.sql("SHOW DATABASES").collect()]
# demo_dbs = ["JBS_DATASETS", "PROJECT_DB"]
# database = st.selectbox("Select Database", demo_dbs)

# schemas = [row["name"] for row in session.sql(f"SHOW SCHEMAS IN {q(database)}").collect()]
# schema = st.selectbox("Select Schema", schemas)

# tables = [row["name"] for row in session.sql(f"SHOW TABLES IN {q(database)}.{q(schema)}").collect()]
# table = st.selectbox("Select Table", tables)

# full_table = f"{q(database)}.{q(schema)}.{q(table)}"





    
# -------------------------------------
# csv Upload Streamlit app
# -------------------------------------
def csv_uploader():

    # -------------------------
    # Stage settings
    # -------------------------

    stage_name_no_at = "CORTEX_CLASSIFY_APPV1_STAGE"
    
    stage_name = f"@{stage_name_no_at}"

    
    # -------------------------
    # File upload tab
    # -------------------------
    st.header("File Upload")
    st.write(f"Upload files to Snowflake App stage {stage_name_no_at}")

    uploaded_file = st.file_uploader("Choose a file")

    if uploaded_file:
        with st.spinner("Staging..."): 
            file_extension = os.path.splitext(uploaded_file.name)[1].lower()
            try:
                # Create file stream using BytesIO and upload
                file_stream = io.BytesIO(uploaded_file.getvalue())
                session.file.put_stream(
                    file_stream,
                    f"{stage_name}/{uploaded_file.name}",
                    auto_compress=False,
                    overwrite=True
                )
                st.success(f"File '{uploaded_file.name}' staging Successful")
    
                try:
                    uploaded_file_pd = pd.read_csv(uploaded_file)
    
                except Exception as e:
                    st.warning(f"Error occurred while reading the file: {str(e)}")
            except Exception as e:
                st.error(f"Error occurred while uploading file: {str(e)}")
        with st.spinner("Cleaning data and creating table..."):
            try:
                clean_upload_pd = process_df(uploaded_file_pd)
                st.success("Data cleaning Successful")
                table_cols = ",\n    ".join([f'"{c}" VARCHAR' for c in clean_upload_pd.columns])
                create_table_sql = f"CREATE OR REPLACE TABLE INPUT_AI_CLASSIFY_APP (\n    {table_cols}\n);"
                session.sql(create_table_sql).collect()
                st.success("CREATE TABLE Successful")
            except Exception as e:
                st.code(create_table_sql)
                st.error(f"Failed to clean data and create table: : {str(e)}")
        with st.spinner("Copying data into table..."):
            try:
                copy_into_sql = f"""COPY INTO INPUT_AI_CLASSIFY_APP
                FROM @{stage_name_no_at}/{uploaded_file.name}
                FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
                ON_ERROR = 'CONTINUE';"""
                session.sql(copy_into_sql).collect()
                preview_upload = session.sql("SELECT * FROM INPUT_AI_CLASSIFY_APP").to_pandas()
                st.success("COPY INTO Successful")
                st.markdown("### SQL CREATE TABLE EXECUTED:")
                st.code(create_table_sql)
                st.markdown("### SQL COPY INTO EXECUTED:")
                st.code(copy_into_sql)
                st.markdown("### DATA Preview:")
                st.dataframe(preview_upload.head())
            except Exception as e:
                st.code(copy_into_sql)
                st.error(f"Failed to copy into table: : {str(e)}")

   
# -------------------------------------
# Launch CSV Uploader
# -------------------------------------
if __name__ == "__main__":
    csv_uploader() 



full_table = f"INPUT_AI_CLASSIFY_APP"








# -------------------------------
# Step 4: Columns
# -------------------------------
column_info = session.sql(f"SHOW COLUMNS IN {full_table}").collect()
text_columns = [col["column_name"] for col in column_info]

if not text_columns:
    st.warning("No columns found in table.")
    st.stop()

selected_cols = st.multiselect("Select Text Columns", text_columns)

if selected_cols:
    df_preview = session.table(full_table).select(selected_cols).limit(5).to_pandas()
    st.markdown("### Preview of selected columns")
    st.dataframe(df_preview)

# -------------------------------
# AI_CLASSIFY Parameter Builder
# -------------------------------
def build_ai_classify_params():
    # Categories
    if "ai_categories" not in st.session_state:
        st.session_state.ai_categories = []

    st.subheader("Categories")
    with st.form("add_category", clear_on_submit=True):
        c1, c2 = st.columns([1,2])
        with c1: 
            new_label = st.text_input("Category Label")
        with c2:
            new_desc = st.text_input("Description (optional)")
        if st.form_submit_button("Add Category"):
            if new_label.strip():
                d = {"label": new_label.strip()}
                if new_desc.strip():
                    d["description"] = new_desc.strip()
                st.session_state.ai_categories.append(d)

    if st.session_state.ai_categories:
        st.json(st.session_state.ai_categories)

    # Config object
    st.subheader("Config")
    task_description = st.text_input(
        "Task description (optional)",
        placeholder="Explain what the classification should decide..."
    )
    output_mode = st.selectbox("Output mode", ["single", "multi"])

    # Examples
    if "ai_examples" not in st.session_state:
        st.session_state.ai_examples = []

    st.subheader("Examples (Optional Few-shot)")
    with st.form("add_example", clear_on_submit=True):
        ex_input = st.text_input("Input example")
        ex_labels = st.text_input("Labels (comma separated)")
        ex_explanation = st.text_input("Explanation")
        if st.form_submit_button("Add Example"):
            if ex_input.strip():
                st.session_state.ai_examples.append({
                    "input": ex_input.strip(),
                    "labels": [s.strip() for s in ex_labels.split(",")] if ex_labels else [],
                    "explanation": ex_explanation.strip()
                })
    if st.session_state.ai_examples:
        st.json(st.session_state.ai_examples)

    # Build config object
    config_object = {"output_mode": output_mode}
    if task_description:
        config_object["task_description"] = task_description
    if st.session_state.ai_examples:
         config_object["examples"] = st.session_state.ai_examples

    return st.session_state.ai_categories, config_object

categories, config = build_ai_classify_params()

# -------------------------------
# Fully dynamic Cortex run
# -------------------------------
def run_cortex(table, input_cols, categories, config_object):
    if not input_cols:
        st.warning("No input columns selected.")
        return
    if not categories:
        st.warning("No categories provided.")
        return

    # Concatenate multiple columns
    #concat_expr = " || ' ' || ".join([f'"{col}"' for col in input_cols])
    concat_expr = " || ' ' || ".join([f'COALESCE("{c}", \'\')' for c in input_cols])
    # Convert Python objects to Snowflake-compatible JSON (single quotes)
    def to_snowflake_json(obj):
        return json.dumps(obj).replace('"', "'")

    categories_json = to_snowflake_json(categories)
    config_json = to_snowflake_json(config_object)

    # Build SQL
    query = f"""
        SELECT 
            {', '.join([f'"{c}"' for c in input_cols])},
            SNOWFLAKE.CORTEX.AI_CLASSIFY(
                {concat_expr},
                {categories_json},
                {config_json}
            )
        FROM {table}
    """

    st.markdown("### Generated SQL")
    st.code(query)

    df = session.sql(query).to_pandas()
    st.markdown("### Cortex Classification Results")
    st.dataframe(df)

if st.button("Run Cortex Classification"):
    run_cortex(full_table, selected_cols, categories, config)
