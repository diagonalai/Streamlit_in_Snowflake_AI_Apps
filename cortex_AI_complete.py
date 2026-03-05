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


session.sql("LIST @CORTEX_COMPLETE_APPV1_STAGE").collect()


# -------------------------------
# Section 1: File Uploader
# -------------------------------


if "uploaded_file_name" not in st.session_state:
    st.session_state.uploaded_file_name = None


st.title("Snowflake Complete for Thematic Analysis")

st.markdown("This App gives no code access to the Snowflake Complete function for PDF files. ") 
st.markdown("1) **File Upload**: Upload a PDF File to Database: JBS_DATASETS, Schema: APP_STORAGE; using put_stream()")
st.markdown("2) **Parse Text**: Parse the Document using PARSE_DOCUMENT; Select mode, page_split")
st.markdown("3) **Complete**: Run Complete using AI_COMPLETE; Input a Prompt, Select Temperature, Top_p and max_tokens")
st.markdown("See Snowflake Documentation for more details: [put_stream()](https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/snowpark/api/snowflake.snowpark.FileOperation.put_stream), [PARSE_DOCUMENT](https://docs.snowflake.com/en/sql-reference/functions/parse_document-snowflake-cortex), [AI_COMPLETE](https://docs.snowflake.com/en/sql-reference/functions/ai_complete-single-string)", unsafe_allow_html=True)
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

    stage_name = f"@CORTEX_COMPLETE_APPV1_STAGE"


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
stage = "CORTEX_COMPLETE_APPV1_STAGE"

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
    st.header("Parse Text")
    st.markdown("Select settings to parse the text. If you are unsure, you can leave the settings at deafult.")
    modes = ['OCR', 'LAYOUT']
    mode = st.selectbox("Select Mode", modes)
    
    
    splits = ['FALSE', 'TRUE']
    split = st.selectbox("Select Page Split", splits)
    
    
    
    #'page_filter': [{'start': 0, 'end': 1}]}
    options = {'mode': mode, 'page_split': split}
    
    
    
    sql = f"""
    CREATE OR REPLACE TABLE {full_tablename} AS
    SELECT
        *,
            SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                '@{database}.{schema}.{stage}',
                '{clean_filename}',
                {options}
                ) AS parsed_text
    """

    st.subheader("SQL")
    st.code(sql)
    if st.button("Parse Document"):
        with st.spinner("Parsing Document"):
            try:
                session.sql(sql).collect()
                st.success("PDF parsed and table created")
            
                df = session.sql(f"SELECT * FROM {full_tablename}").to_pandas()
                st.session_state.parsed_text = df.iloc[0, 1]
            except Exception as e:
                st.error("Document Parsing Failed.")

if 'parsed_text' in st.session_state:
    st.subheader("Parsed Text")
    st.code(st.session_state.parsed_text)
# -------------------------------
# Step 5: AI_COMPLETE Parameter Builder
# -------------------------------

    st.header("Cortex Complete")
    st.markdown("Select settings for Cortex Complete. If you are unsure, you can select a mistral LLM model and leave the other settings at deafult.")

    core_models = [
        "claude-4-opus",
        "claude-4-sonnet",
        "claude-3-7-sonnet",
        "claude-3-5-sonnet",
        "deepseek-r1",
        "llama3-8b",
        "llama3-70b",
        "llama3.1-8b",
        "llama3.1-70b",
        "llama3.1-405b",
        "llama3.3-70b",
        "llama4-maverick",
        "llama4-scout",
        "mistral-large",
        "mistral-large2",
        "mistral-7b",
        "mixtral-8x7b",
        "openai-gpt-4.1",
        "openai-o4-mini",
        "snowflake-arctic",
        "snowflake-llama-3.1-405b",
        "snowflake-llama-3.3-70b"
    ]
    
    finetuned_models = [row["name"] for row in session.sql(f"SHOW MODELS IN ACCOUNT").collect()]
    all_models = core_models + finetuned_models
    model = st.selectbox("Select LLM Model", core_models)
    
    # system_prompt = st.text_input("System Prompt",
    #           placeholder="Background information and instructions for a response style..."
    #       )
    
    # user_prompt = st.text_input("User Prompt",
    #           placeholder="A prompt provided by the user..."
    #       )
    
    prompt = st.text_input("Prompt",
             placeholder="A prompt provided by the user..."
         )
    
    # prompt_dev = [
    #     {'role': 'system', 'content': system_prompt},
    #     {'role': 'user', 'content': user_prompt}
    #  ]
    
    #full_prompt_dev = f"{prompt} {st.session_state.parsed_text}"     #Makes a string for input into the sql
    
    
    def sql_escape(s):
        return s.replace("'", "''")
    
    full_prompt_dev = sql_escape(
        f"{prompt}\n\nDOCUMENT CONTENT:\n{st.session_state.parsed_text}"
    )
    
#    st.markdown(type(full_prompt_dev))
    
    temperature = st.slider("Temperature (Increases the randomness of the output of the language model.)", min_value=0, max_value=10, value=0, step=1)
    temperature = temperature / 10
    
    top_p = st.slider("Top_p (Restricts the set of possible tokens that the model outputs)", min_value=0, max_value=10, value=0, step=1)
    top_p = top_p / 10
    
    max_tokens = st.slider("max_tokens (Sets the maximum number of output tokens in the response. Small values can result in truncated responses. Default: 4096 Maximum allowed value: 8192)", min_value=0, max_value=8192, value=4096, step=512)
    max_tokens = float(max_tokens)
    
    response_type = 'json'
    response_format = {'type': response_type}
    # -------------------------------
    # # Step 6: AI_COMPLETE CALL
    # -------------------------------
    
    
    sql = f"""
        SELECT AI_COMPLETE(
            '{model}',
            '{full_prompt_dev}',
            OBJECT_CONSTRUCT(
                'temperature', {temperature},
                'top_p', {top_p},
                'max_tokens', {max_tokens}
                                )
            ) AS LLM_SCORE
            
        """
    st.code(sql)
    if st.button("Run Cortex Complete"):
        with st.spinner("Running Cortex Complete"):
            try:
                res = session.sql(sql).to_pandas()
                st.dataframe(res)
                cell_value = res["LLM_SCORE"].iloc[0]  # row 0
                st.markdown("### Response")
                st.markdown(cell_value)
            except Exception as e:
                 st.error(f"Cortex Failed. This may occur if the document is too large for the model, or if the LLM model may not be available in your region. See AI_Complete documentation for model / region availability. (minstral 7-b and minstral-large are available in the US) {str(e)}")
    
# if 'parsed_text' in st.session_state:
#     if st.button("Clear Stage"):
#         try:
#             session.sql(f"REMOVE @{database}.{schema}.{stage}/{clean_filename}")
#             st.success(f"Stage Cleared")
            
#         except Exception as e:
#             st.error(f"Error Clearing Stage: {str(e)}")
        

