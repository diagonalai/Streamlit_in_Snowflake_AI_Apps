import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import json
import re
import os
import io


# -------------------------------
# Setup Snowflake session
# -------------------------------
session = get_active_session()


session.sql("LIST @CORTEX_SEARCH_APPV1_STAGE").collect()


# -------------------------------
# Section 1: File Uploader
# -------------------------------


if "uploaded_file_name" not in st.session_state:
    st.session_state.uploaded_file_name = None


if "chunked_file_name" not in st.session_state:
    st.session_state.chunked_file_name = None



st.title("CORTEX SEARCH RAG")

st.text("This App gives no code access for creating a Cortex SEARCH Service and using it to search using Vector Embeddings for RAG. ") 
st.text("1) Upload a PDF File to Database: JBS_DATASETS, Schema: APP_STORAGE; using put_stream()")
st.text("2) Parse and chunk the Document using PARSE_DOCUMENT(select mode and page_split) and SPLIT_TEXT_RECURSIVE_CHARACTER(select format, chunk_size and overlap) ")
st.text("3) Create the SEARCH service ")
st.markdown("See Snowflake Documentation for more details: [put_stream()](https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/snowpark/api/snowflake.snowpark.FileOperation.put_stream), [PARSE_DOCUMENT](https://docs.snowflake.com/en/sql-reference/functions/parse_document-snowflake-cortex), [SPLIT_TEXT_RECURSIVE_CHARACTER](https://docs.snowflake.com/en/sql-reference/functions/split_text_recursive_character-snowflake-cortex), [CORTEX SEARCH](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)", unsafe_allow_html=True)
st.text("For questions or to give feedback contact apagano@jbsinternational.com")
# -------------------------------------
# Main Streamlit app
# -------------------------------------
def file_upload():
    #st.title("Snowflake File Management App")
    sessh = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()
    st.code(f"Session info; {sessh}")
    # -------------------------
    # Stage settings
    # -------------------------

    stage_name = f"@CORTEX_SEARCH_APPV1_STAGE"


    # -------------------------
    # File upload tab
    # -------------------------
    st.header("File Upload")
    st.write("Upload files to Snowflake stage.")

    uploaded_file = st.file_uploader("Choose a file")

    if uploaded_file:
        try:
            # Create file stream using BytesIO and upload
            file_stream = io.BytesIO(uploaded_file.getvalue())
            session.file.put_stream(
                file_stream,
                f"{stage_name}/{uploaded_file.name}",
                auto_compress=False,
                overwrite=True
            )
            st.session_state['uploaded_file_name'] = uploaded_file.name
            #file_name = uploaded_file.name
            st.success(f"File '{uploaded_file.name}' has been uploaded successfully! File type '{type(uploaded_file)}'")

        except Exception as e:
            st.error(f"Error occurred while uploading file: {str(e)}")


# -------------------------------------
# Launch app
# -------------------------------------
# if __name__ == "__main__":
file_upload() 


#files_all_in = session.sql(f"LIST @CORTEX_COMPLETE_APPV1_STAGE").collect()

#st.markdown(files_all_in)

# -------------------------------
# Section 2: Complete App
# -------------------------------




def q(name):
    return f'"{name}"'

# -------------------------------
# Step 1-3: Database / Schema / Stage / File
# -------------------------------

database = f"JBS_DATASETS"
schema = f"APP_STORAGE"
stage = "CORTEX_SEARCH_APPV1_STAGE"

# all_files = [row["name"] for row in session.sql(f"ls @{q(database)}.{q(schema)}.{q(stage)}").collect()]
# files = [f for f in all_files if f != "cortex_complete_appv1_stage/ignore_me.txt"]

file = st.session_state.uploaded_file_name

if file:
    full_filepath = f"{q(database)}.{q(schema)}.{q(file)}"
    
    safe_filename = re.sub(r'[^A-Za-z0-9_]', '_', file).upper()
    parsed_table = f'PARSED_{safe_filename}'
    full_tablename = f'{database}.{schema}.{parsed_table}'
    
    parted_filename = re.findall(r'[^/]+', file)
    clean_filename = parted_filename[-1]

    # -------------------------------
    # Step 4: Parse Text
    # -------------------------------
    st.subheader("Parse and Chunk Document")
    st.write("Select PARSE_DOCUMENT options. Settings are set to default for easy use.")
    
    modes = ['OCR', 'LAYOUT']
    mode = st.selectbox("Mode", modes)
    
    splits = ['FALSE', 'TRUE']
    split = st.selectbox("Page Split", splits)
    
    #'page_filter': [{'start': 0, 'end': 1}]}
    options = {'mode': mode, 'page_split': split}

    st.write("Select chunking options;")

    formats = ['none', 'markdown']
    format = st.selectbox("Select format", formats)
    chunk_size = st.slider("chunk_size(An integer specifying the maximum number of characters in each chunk.)", min_value=0, max_value=3600, value=1800, step=300)
    overlap = st.slider("overlap(An integer that specifies the number of characters to overlap between consecutive chunks.)", min_value=0, max_value=500, value=250, step=50)
    
    #st.write(type(format))
    
    
    sql_parse = f"""
    CREATE OR REPLACE TABLE PARSED_{safe_filename} AS
    SELECT
            SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                '@{database}.{schema}.{stage}',
                '{clean_filename}',
                {options}
                ) AS parsed_text
    """

    sql_chunk_table = f"""CREATE OR REPLACE TABLE CHUNKED_{safe_filename} (
        CHUNK VARCHAR
    );"""


    sql_chunk = f"""INSERT INTO CHUNKED_{safe_filename} (CHUNK)
    SELECT
        c.value::STRING
    FROM
        PARSED_{safe_filename},
        LATERAL FLATTEN( input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER (
            parsed_text::STRING,
            '{format}',
            {chunk_size},
            {overlap}
        )) c;"""

    

    st.subheader("SQL Code")
    st.code(f"""{sql_parse}

    
    {sql_chunk_table} 

    
    {sql_chunk} """)

  
    if st.button("Parse and Chunk"):
        with st.spinner("Parsing Document"):
            try:
                session.sql(sql_parse).collect()
                st.success("PDF parsed Successfully")
            except Exception as e:
                st.error(f"Document Parsing Failed; {str(e)}")
        with st.spinner("Chunking Document"):
            try:
                session.sql(sql_chunk_table).collect()
                session.sql(sql_chunk).collect()
                st.success("PDF chunked Successfully")
                st.session_state.chunked_file_name = f"CHUNKED_{safe_filename}"
            except Exception as e:
                st.error(f"Document Chunking Failed; {str(e)}")

                
if st.session_state.chunked_file_name:
    st.success("PDF parsed and chunked Successfully")
    chunked_df = session.sql(f"SELECT * FROM CHUNKED_{safe_filename}").to_pandas()
    st.dataframe(chunked_df)

    st.subheader("Create Search Service")
    
    sql_create_search = f"""CREATE OR REPLACE CORTEX SEARCH SERVICE {safe_filename}_SEARCH_SERVICE
    ON chunk
    WAREHOUSE = jbs_datasets_wh_xs
    TARGET_LAG = '1 minute'
    EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
    AS (
    SELECT
        chunk
    FROM CHUNKED_{safe_filename}
    );"""


    sql_share_with_public = f"""GRANT USAGE ON CORTEX SEARCH SERVICE {safe_filename}_SEARCH_SERVICE TO ROLE public;"""

    st.subheader("SQL Code")
    st.code(f"""{sql_create_search} 
    
    {sql_share_with_public}""")
    
    if st.button(f"Create SEARCH service"):
        with st.spinner("Creating SEARCH Service"):
            try:
                session.sql(sql_create_search).collect()
                st.success(f"SEARCH service created")
            except Exception as e:
                st.error(f"Failed to create SEARCH service")

            try:
                session.sql(sql_share_with_public).collect()
                st.success(f"SEARCH service shared with PUBLIC role")
                st.markdown(
                f"Access SEARCH service here: [{safe_filename}_SEARCH_SERVICE](https://app.snowflake.com/qoeygzs/akb11987/#/cortex/search/playground/databases/JBS_DATASETS/schemas/APP_STORAGE/services/{safe_filename}_SEARCH_SERVICE)"
                )
                st.markdown(
                f"Review Vector Embeddings: [Vector Embeddings](https://app.snowflake.com/qoeygzs/akb11987/#/cortex/search/databases/JBS_DATASETS/schemas/APP_STORAGE/services/{safe_filename}_SEARCH_SERVICE/preview)"
                )
            except Exception as e:
                st.error(f"Failed to share SEARCH service with PUBLIC role")
    