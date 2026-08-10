# ==============================================================
# app.py
# Databricks App (Streamlit) frontend for the AI Job Hunting Copilot.
# Deploy via Databricks Apps: `databricks apps deploy` with this file
# as the entrypoint, plus an app.yaml declaring the Streamlit command.
# ==============================================================

from datetime import datetime

import pandas as pd
import streamlit as st

import agent_tools as tools
from agent import run_agent_turn
from lakebase import get_connection as get_conn

st.set_page_config(page_title="AI Job Hunting Copilot", layout="wide")


# --- session state ---------------------------------------------------
if "user_id" not in st.session_state:
    st.session_state.user_id = None
if "chat_history" not in st.session_state:
    st.session_state.chat_history = None

st.title("🧭 AI Job Hunting Copilot")

tab_profile, tab_search, tab_pipeline, tab_chat = st.tabs(
    ["👤 Profile Setup", "🔍 Semantic Job Search", "📋 Application Pipeline", "💬 Chat with Agent"]
)

# ======================================================================
# TAB 1: Candidate Profile Setup
# ======================================================================
with tab_profile:
    st.subheader("Set up your candidate profile")

    with st.form("profile_form"):
        name = st.text_input("Name")
        email = st.text_input("Email")
        target_roles = st.text_input("Target roles (comma-separated)")
        target_salary_min = st.number_input("Minimum target salary", min_value=0, step=5000)
        preferred_locations = st.text_input("Preferred locations (comma-separated)")
        resume_text = st.text_area("Paste your resume text", height=250)
        primary_skills = st.text_input("Primary skills (comma-separated)")
        experience_summary = st.text_area("Experience summary", height=150)
        submitted = st.form_submit_button("Save Profile")

    if submitted:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO job_copilot.users (name, email, target_roles, target_salary_min, preferred_locations)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (email) DO UPDATE SET
                    target_roles = EXCLUDED.target_roles,
                    target_salary_min = EXCLUDED.target_salary_min,
                    preferred_locations = EXCLUDED.preferred_locations
                RETURNING user_id
                """,
                (name, email, target_roles.split(","), target_salary_min, preferred_locations.split(",")),
            )
            user_id = cur.fetchone()["user_id"]

            cur.execute(
                """
                INSERT INTO job_copilot.profiles (user_id, resume_text, primary_skills, experience_summary)
                VALUES (%s, %s, %s, %s)
                """,
                (user_id, resume_text, primary_skills.split(","), experience_summary),
            )
            conn.commit()
            st.session_state.user_id = str(user_id)
        st.success(f"Profile saved. user_id = {st.session_state.user_id}")

# ======================================================================
# TAB 2: Semantic Job Search & Match Explanation
# ======================================================================
with tab_search:
    st.subheader("Search jobs by meaning, not just keywords")

    if not st.session_state.user_id:
        st.info("Set up your profile first in the Profile Setup tab.")
    else:
        query = st.text_input(
            "Describe what you're looking for",
            placeholder="e.g. remote backend roles that don't require 5+ years of Kubernetes experience",
        )
        if st.button("Search") and query:
            results = tools.search_and_rank_jobs(st.session_state.user_id, query, top_k=10)
            for job in results:
                with st.expander(f"{job['title']} — {job['company']} ({job['location']})"):
                    st.write(job["clean_description"][:600] + "...")
                    st.write(f"**Salary range:** {job.get('salary_range', 'n/a')}")
                    if st.button("Explain match", key=f"explain-{job['job_id']}"):
                        explanation = tools.explain_job_match(st.session_state.user_id, job["job_id"])
                        st.json(explanation)
                    if st.button("Save job", key=f"save-{job['job_id']}"):
                        with get_conn() as conn, conn.cursor() as cur:
                            cur.execute(
                                "INSERT INTO job_copilot.saved_jobs (user_id, job_id) VALUES (%s, %s) "
                                "ON CONFLICT DO NOTHING",
                                (st.session_state.user_id, job["job_id"]),
                            )
                            conn.commit()
                        st.success("Saved.")

# ======================================================================
# TAB 3: Active Application Pipeline (Kanban / Table view)
# ======================================================================
with tab_pipeline:
    st.subheader("Your application pipeline")

    if not st.session_state.user_id:
        st.info("Set up your profile first in the Profile Setup tab.")
    else:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.application_id, j.title, j.company, a.stage, a.applied_date, a.updated_at
                FROM job_copilot.applications a
                JOIN job_copilot.job_postings j ON j.job_id = a.job_id
                WHERE a.user_id = %s
                ORDER BY a.updated_at DESC
                """,
                (st.session_state.user_id,),
            )
            rows = cur.fetchall()

        view_mode = st.radio("View", ["Kanban", "Table"], horizontal=True)
        stages = ["saved", "applied", "interviewing", "rejected", "offer"]

        if view_mode == "Table":
            st.dataframe(pd.DataFrame(rows))
        else:
            cols = st.columns(len(stages))
            for col, stage in zip(cols, stages):
                with col:
                    st.markdown(f"**{stage.title()}**")
                    for r in [r for r in rows if r["stage"] == stage]:
                        st.markdown(f"- {r['title']} @ {r['company']}")

        st.divider()
        st.markdown("**Update a stage**")
        app_id = st.text_input("Application ID")
        new_stage = st.selectbox("New stage", stages)
        if st.button("Update stage") and app_id:
            result = tools.update_application_stage(app_id, new_stage)
            st.success(f"Updated: {result}")

# ======================================================================
# TAB 4: Interactive Chat with the AI Agent
# ======================================================================
with tab_chat:
    st.subheader("Chat with your Job Hunting Copilot")

    if not st.session_state.user_id:
        st.info("Set up your profile first in the Profile Setup tab.")
    else:
        for msg in (st.session_state.chat_history or [])[1:]:
            if msg.get("role") in ("user", "assistant") and msg.get("content"):
                with st.chat_message(msg["role"]):
                    st.write(msg["content"])

        user_msg = st.chat_input("Ask about roles, drafts, or your pipeline...")
        if user_msg:
            with st.chat_message("user"):
                st.write(user_msg)
            result = run_agent_turn(st.session_state.user_id, user_msg, st.session_state.chat_history)
            st.session_state.chat_history = result["history"]
            with st.chat_message("assistant"):
                st.write(result["reply"])