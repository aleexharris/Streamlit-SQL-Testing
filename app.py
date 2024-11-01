import streamlit as st
import duckdb
import pandas as pd
from dataclasses import dataclass
import hashlib
import time
from code_editor import code_editor

st.set_page_config(page_title="budgetdune")


class Sql(str):
    def preview(self) -> str:
        query = self.strip().rstrip(';')
        if query.upper().strip().endswith('LIMIT'):
            query = query.rsplit('LIMIT', 1)[0].strip()
        elif 'LIMIT' in query.upper():
            query = query.rsplit('LIMIT', 1)[0].strip()
        return f"{query}\nLIMIT 100;"


@dataclass
class Query:
    ts: int
    id: str
    sql: Sql
    preview: None | pd.DataFrame

    def from_str(s: str):
        ts = int(time.time())
        id = hashlib.sha256(f"{ts}:{s}".encode()).hexdigest()
        return Query(ts, id, Sql(s), None)


def get_db_connection():
    if "duck_conn" not in st.session_state:
        st.session_state["duck_conn"] = duckdb.connect(":memory:")
    return st.session_state["duck_conn"]


def create_page(conn):
    st.title("Budget Dune Dashboard")
    st.divider()
    st.write("Press Ctrl + Enter to run a preview of your query.")
    cur = conn.cursor()
    response = code_editor(code="SELECT * FROM dune;", lang="sql", key="editor")
    for query_string in response["text"].split(";"):
        if query_string.strip() == "":
            continue
        
        query = Query.from_str(query_string)

        try:
            cur.execute(query.sql.preview())
            df = cur.fetch_df()
            query.preview = df
            st.write(df)
        except Exception as e:
            st.error(e)

    if st.button("Reset"):
        st.cache_resource.clear()
        st.session_state["editor"]["text"] = ""
        st.rerun()


def main():
    conn = get_db_connection()
    create_page(conn)

if __name__ == "__main__":
    main()
