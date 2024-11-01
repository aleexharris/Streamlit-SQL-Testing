import streamlit as st
import duckdb
import pandas as pd
import time
from code_editor import code_editor
from collections import deque
from minio import Minio
from streamlit_ace import st_ace

from consts import (
    QUERY_CACHE_LEN,
    MINIO_HOST,
    MINIO_PORT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    LIVE,
)
from query import Query, Sql

st.set_page_config(page_title="budgetdune")


def init_session_state():
    if "query_history" not in st.session_state:
        st.session_state.query_history = deque(maxlen=QUERY_CACHE_LEN)
    if "duck_conn" not in st.session_state:
        conn = duckdb.connect(":memory:")

        if LIVE:
            try:
                minio_client = Minio(
                    f"{MINIO_HOST}:{MINIO_PORT}",
                    access_key=MINIO_ACCESS_KEY,
                    secret_key=MINIO_SECRET_KEY,
                    secure=False,
                )
                objects = minio_client.list_objects("processed")
                for obj in objects:
                    dataset_name = obj.object_name.split("/")[0]
                    conn.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {dataset_name} AS 
                        SELECT * FROM read_parquet('s3://processed/{dataset_name}/*');
                    """
                    )
            except Exception as e:
                st.error(f"Failed to connect to Minio: {e}")
        else:
            conn.execute(
                """
                CREATE TABLE test_data AS 
                SELECT * FROM (
                    VALUES 
                    (1, 'test1'),
                    (2, 'test2'),
                    (3, 'test3')
                ) AS t(id, name);
            """
            )

        st.session_state.duck_conn = conn


def create_page(conn):
    st.title("Budget Dune Dashboard")

    with st.sidebar:
        st.header("Query History")
        for query in reversed(st.session_state.query_history):
            timestamp = time.strftime("%H:%M:%S", time.localtime(query.ts))
            status_indicator = (
                "🟢"
                if query.status == "completed"
                else "🔴" if query.status == "failed" else "🟠"
            )
            with st.expander(
                f"{status_indicator} [{timestamp}] - ID: 0x{query.id[:3]}...{query.id[-3:]}"
            ):
                st.code(query.sql)
                if st.button("Restore", key=f"restore_{query.id}"):
                    st.session_state["editor_value"] = str(query.sql)
                    st.session_state["ace_key"] = f"sql_ace_{time.time()}"
                    st.rerun()

    st.divider()
    st.write("Press Ctrl + Enter to run a preview of your query.")

    cur = conn.cursor()
    if "editor_value" not in st.session_state:
        st.session_state["editor_value"] = "SELECT * FROM test_data;"
    initial_value = st.session_state["editor_value"]
    st.write(f"Editor state SQL: {st.session_state['editor_value']}")
    ace_key = st.session_state.get("ace_key", "sql_ace")
    response_text = st_ace(
        value=initial_value,
        language="sql",
        theme="monokai",
        key=ace_key,
        height=400,
        show_gutter=True,
        show_print_margin=True,
        wrap=True,
        font_size=14,
        tab_size=2,
    )

    if response_text:
        for query_string in response_text.split(";"):
            if query_string.strip() == "":
                continue

            query = Query.from_str(query_string + ";")

            col1, col2 = st.columns([4, 1])
            with col1:
                try:
                    cur.execute(query.sql.preview())
                    df = cur.fetch_df()
                    query.preview = df
                    st.write(df)
                    st.session_state.query_history.append(query)
                except Exception as e:
                    st.error(e)
                    query.status = "failed"
                    query.error_message = str(e)

            with col2:
                if st.button("Run Full Query", key=f"run_{query.id}"):
                    with st.spinner("Running full query..."):
                        try:
                            # TODO: Implement the background job and email .csv functionality
                            st.success("Query submitted! Results will be emailed to you.")
                        except Exception as e:
                            st.error(f"Failed to run query: {e}")

    if st.button("Reset"):
        st.session_state.clear()
        st.rerun()


def main():
    init_session_state()
    conn = st.session_state.duck_conn
    create_page(conn)


if __name__ == "__main__":
    main()
