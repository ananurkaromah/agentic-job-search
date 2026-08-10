# ==============================================================
# agent.py
# Orchestrates the AI Job Hunting Copilot agent: wires agent_tools.py
# functions as callable tools for a Databricks Foundation Model
# (e.g. databricks-meta-llama-3-3-70b-instruct or databricks-dbrx-instruct)
# via the OpenAI-compatible Foundation Model API, with function calling.
# ==============================================================

import json
import os

from openai import OpenAI

import agent_tools as tools

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]          # e.g. https://<workspace>.databricks.com
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
FM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"{DATABRICKS_HOST}/serving-endpoints",
)

SYSTEM_PROMPT = """You are the AI Job Hunting Copilot, an assistant that helps a job
seeker search for roles, understand match quality, track applications, draft
tailored materials, and log interview notes. Always call a tool to fetch or
write real data rather than guessing. Be concise and concrete."""

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "search_and_rank_jobs",
            "description": "Semantic search over job postings ranked by relevance.",
            "parameters": {
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                    "query_text": {"type": "string"},
                    "top_k": {"type": "integer", "default": 10},
                },
                "required": ["user_id", "query_text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explain_job_match",
            "description": "Get a job posting plus the user's profile to explain fit.",
            "parameters": {
                "type": "object",
                "properties": {"user_id": {"type": "string"}, "job_id": {"type": "string"}},
                "required": ["user_id", "job_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "surface_stale_applications",
            "description": "List applications with no update in N days.",
            "parameters": {
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                    "days_inactive": {"type": "integer", "default": 14},
                },
                "required": ["user_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_application_stage",
            "description": "Move an application to a new pipeline stage.",
            "parameters": {
                "type": "object",
                "properties": {
                    "application_id": {"type": "string"},
                    "new_stage": {
                        "type": "string",
                        "enum": ["saved", "applied", "interviewing", "rejected", "offer"],
                    },
                },
                "required": ["application_id", "new_stage"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "draft_tailored_materials",
            "description": "Assemble grounded context for a cover letter or resume bullets.",
            "parameters": {
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                    "job_id": {"type": "string"},
                    "material_type": {"type": "string", "enum": ["cover_letter", "resume_bullets"]},
                },
                "required": ["user_id", "job_id", "material_type"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_interview_note",
            "description": "Record an interview or recruiter note against an application.",
            "parameters": {
                "type": "object",
                "properties": {
                    "application_id": {"type": "string"},
                    "stage": {"type": "string"},
                    "note_text": {"type": "string"},
                    "follow_up_date": {"type": "string", "description": "YYYY-MM-DD, optional"},
                },
                "required": ["application_id", "stage", "note_text"],
            },
        },
    },
]

TOOL_DISPATCH = {
    "search_and_rank_jobs": tools.search_and_rank_jobs,
    "explain_job_match": tools.explain_job_match,
    "surface_stale_applications": tools.surface_stale_applications,
    "update_application_stage": tools.update_application_stage,
    "draft_tailored_materials": tools.draft_tailored_materials,
    "add_interview_note": tools.add_interview_note,
}


def run_agent_turn(user_id: str, user_message: str, history: list[dict] | None = None) -> dict:
    """Runs one conversational turn, executing tool calls until the model
    returns a final text answer. Returns {'reply': str, 'history': list}."""
    messages = history or [{"role": "system", "content": SYSTEM_PROMPT}]
    messages.append({"role": "user", "content": user_message})

    for _ in range(5):  # cap tool-call loops
        response = client.chat.completions.create(
            model=FM_ENDPOINT,
            messages=messages,
            tools=TOOL_SCHEMAS,
            tool_choice="auto",
            max_tokens=1000,
        )
        choice = response.choices[0].message

        if not choice.tool_calls:
            messages.append({"role": "assistant", "content": choice.content})
            return {"reply": choice.content, "history": messages}

        messages.append({"role": "assistant", "content": choice.content, "tool_calls": choice.tool_calls})

        for call in choice.tool_calls:
            fn_name = call.function.name
            fn_args = json.loads(call.function.arguments)
            fn_args.setdefault("user_id", user_id)
            try:
                result = TOOL_DISPATCH[fn_name](**fn_args)
            except Exception as exc:
                result = {"error": str(exc)}

            messages.append({
                "role": "tool",
                "tool_call_id": call.id,
                "content": json.dumps(result, default=str),
            })

    return {"reply": "I ran into trouble completing that — please try rephrasing.", "history": messages}
