"""
mcp_client.py — Equifiz MF MCP Client (v4.2 — per-intent calls)

Changes from v4.1
─────────────────────────────────────────────────────────────────────
graph.py v10.0 now calls run_mcp_query() once PER INTENT with a
single-entity PRE_RESOLVED block.  Each call is already focused:
one entity, one tool family, one intent description.

Key changes:
1. _EntityRegistry.parse_injected — still works for single-entity
   blocks (one unnamed entity block).  Multi-entity blocks with
   "Entity N:" headers still supported for backward compatibility.

2. entity_label injection — ONLY added to tool schemas when the
   current request is genuinely multi-entity (is_multi=True).
   For per-intent calls is_multi is always False → no entity_label
   added → no more ValidationError on resolver tools.

3. get_semantic_tools — properly implemented using ChromaDB to do
   per-call tool selection scoped to the intent's tool family.
   Prefers tools whose required_db_param matches the entity family
   detected from PRE_RESOLVED.

4. _build_system_prompt — concise; includes the single resolved
   entity's IDs so the LLM never needs to hallucinate them.

5. _ensure_codes_for_tool — cross-domain injection guard retained
   from v4.1 (prevents mf_schcode leaking into a co_code tool).

All v4.1 fixes retained unchanged.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Optional

import chromadb
from chromadb.utils import embedding_functions
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from rich.console import Console
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
CHROMA_PATH   = Path(__file__).parent / "chroma_db"
MAX_ROUNDS    = 15

LLM_BACKEND  = os.getenv("LLM_BACKEND", "groq").lower()
GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")
OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
EMBED_MODEL  = "embeddinggemma:latest"

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger  = logging.getLogger("equifiz_mf_client")
console = Console()

# ── ChromaDB ───────────────────────────────────────────────────────────────────
chroma_client  = chromadb.PersistentClient(path=str(CHROMA_PATH))
ollama_ef      = embedding_functions.OllamaEmbeddingFunction(
    model_name=EMBED_MODEL, url=f"{OLLAMA_HOST}/api/embeddings"
)
tool_collection = chroma_client.get_collection(
    name="equifiz_tools", embedding_function=ollama_ef
)

# ── Constants ──────────────────────────────────────────────────────────────────
_NUMERIC_PARAMS = {"mf_schcode", "mf_cocode", "co_code", "index_code"}
_PARAM_TO_FAMILY = {
    "co_code":    "stock",
    "mf_schcode": "mf_scheme",
    "mf_cocode":  "mf_amc",
    "isin":       "etf",
    "index_code": "index",
}
_FAMILY_TO_PARAM = {v: k for k, v in _PARAM_TO_FAMILY.items()}

# Resolver tools never need entity-level IDs — skip injection + entity_label
_RESOLVER_TOOLS = {
    "resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes",
    "resolve_nse_symbol", "get_company_details", "search_companies",
}


# ══════════════════════════════════════════════════════════════════════════════
# Tool param index
# ══════════════════════════════════════════════════════════════════════════════

class _ToolParamIndex:
    def __init__(self):
        self._index: dict[str, dict] = {}

    def build(self, raw_tools: list):
        for tool in raw_tools:
            schema   = tool.inputSchema or {}
            props    = schema.get("properties", {})
            required = set(schema.get("required", []))

            # Unwrap nested params
            if "params" in props and len(props) == 1:
                inner    = props["params"]
                props    = inner.get("properties", {})
                required = set(inner.get("required", []))

            required_db = {p for p in required if p in _PARAM_TO_FAMILY}
            optional_db = {
                p for p in props
                if p in _PARAM_TO_FAMILY and p not in required_db
            }

            # Determine tool family from first DB param
            family = "general"
            for p in list(required_db) + list(optional_db):
                family = _PARAM_TO_FAMILY.get(p, "general")
                break

            self._index[tool.name] = {
                "required_db_params": required_db,
                "optional_db_params": optional_db,
                "family":             family,
                "all_params":         set(props.keys()),
            }

        db_bound = sum(
            1 for v in self._index.values()
            if v["required_db_params"] or v["optional_db_params"]
        )
        console.print(
            f"  [ToolParamIndex] Indexed {len(self._index)} tools. "
            f"DB-bound: {db_bound}"
        )

    def get_required_db_params(self, t: str) -> set:
        return self._index.get(t, {}).get("required_db_params", set())

    def get_family(self, t: str) -> str:
        return self._index.get(t, {}).get("family", "general")

    def get_all_params(self, t: str) -> set:
        return self._index.get(t, {}).get("all_params", set())


# ══════════════════════════════════════════════════════════════════════════════
# Entity info + registry
# ══════════════════════════════════════════════════════════════════════════════

class _EntityInfo:
    __slots__ = (
        "label", "entity_type", "query_intent", "recommended_tool",
        "mf_schcode", "mf_cocode", "co_code", "isin", "index_code",
        "company_name", "amc_name", "scheme_name",
    )

    def __init__(self, label: str = ""):
        self.label            = label
        self.entity_type      = "general"
        self.query_intent     = ""
        self.recommended_tool = ""
        self.mf_schcode = self.mf_cocode = self.co_code = None
        self.isin       = self.index_code = None
        self.company_name = self.amc_name = self.scheme_name = ""

    def get_code_for_param(self, param: str):
        if param in _PARAM_TO_FAMILY:
            return getattr(self, param, None)
        return None

    def __repr__(self):
        codes = {
            p: getattr(self, p)
            for p in ("co_code", "mf_schcode", "mf_cocode", "isin", "index_code")
            if getattr(self, p) is not None
        }
        return f"<Entity '{self.label}' type={self.entity_type} codes={codes}>"


class _EntityRegistry:
    def __init__(self):
        self.entities: list[_EntityInfo] = []
        self._primary = _EntityInfo("(primary)")

    def parse_injected(self, query: str):
        block = re.search(
            r"<PRE_RESOLVED>(.*?)</PRE_RESOLVED>", query, re.S | re.I
        )
        if not block:
            return

        current: Optional[_EntityInfo] = None

        for line in block.group(1).splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # "Entity N: <label>" header → new entity
            em = re.match(r"Entity\s+\d+:\s*(.+)", line, re.I)
            if em:
                current = _EntityInfo(em.group(1).strip())
                self.entities.append(current)
                continue

            # First line without an Entity header → unnamed single entity
            if current is None:
                current = _EntityInfo("(primary)")
                self.entities.append(current)

            kv = re.match(r"([\w_]+)\s*=\s*(.+)", line)
            if not kv:
                continue

            key = kv.group(1).lower()
            val = re.sub(r"\s*\(.*?\)\s*$", "", kv.group(2)).strip()
            val = re.sub(r"\s*#.*$", "", val).strip()    # strip inline comments

            try:
                if key in _NUMERIC_PARAMS:
                    setattr(current, key, int(float(val)))
                elif key in _PARAM_TO_FAMILY:
                    setattr(current, key, val)
                elif hasattr(current, key):
                    setattr(current, key, val)
            except (ValueError, AttributeError):
                pass

        if self.entities:
            self._primary = self.entities[0]

    @property
    def is_multi(self) -> bool:
        return len(self.entities) > 1

    def find_entity_for_tool(
        self, tool_name: str, tool_family: str, label_hint: Optional[str]
    ) -> _EntityInfo:
        if label_hint:
            for e in self.entities:
                if e.label.lower() == label_hint.lower():
                    return e
        for e in self.entities:
            if e.recommended_tool == tool_name:
                return e
        for e in self.entities:
            if e.entity_type == tool_family:
                return e
        return self._primary


# ══════════════════════════════════════════════════════════════════════════════
# Semantic tool selection (per-call, family-aware)
# ══════════════════════════════════════════════════════════════════════════════

def get_semantic_tools(
    query:    str,
    tools:    list[dict],
    top_k:    int,
    family:   str = "general",
) -> list[dict]:
    """
    Select top_k tools from `tools` using ChromaDB semantic search on `query`.
    When family != "general", also boosts tools whose name prefix matches
    the target family (from _TOOL_FAMILY_HINTS).
    """
    try:
        count = tool_collection.count()
        if count == 0:
            return tools[:top_k]

        res = tool_collection.query(
            query_texts=[query],
            n_results=min(top_k * 3, count),
            include=["metadatas", "distances"],
        )

        if not (res["ids"] and res["ids"][0]):
            return tools[:top_k]

        # Build a score map from Chroma results
        score_map: dict[str, float] = {}
        for tool_id, dist in zip(res["ids"][0], res["distances"][0]):
            score_map[tool_id] = 1.0 - dist

        # Optionally boost family-matching tools
        if family != "general":
            from graph import _TOOL_FAMILY_HINTS  # import here to avoid circular dep
            for tid in list(score_map):
                for prefix, fam in _TOOL_FAMILY_HINTS.items():
                    if tid.startswith(prefix) and fam == family:
                        score_map[tid] = score_map[tid] + 0.15
                        break

        # Re-rank the provided tools by score
        tool_names_in_list = {t["function"]["name"] for t in tools}
        ranked = sorted(
            [t for t in tools if t["function"]["name"] in score_map],
            key=lambda t: score_map.get(t["function"]["name"], 0),
            reverse=True,
        )

        # Append unseen tools at the end so we never drop something important
        seen = {t["function"]["name"] for t in ranked}
        ranked += [t for t in tools if t["function"]["name"] not in seen]

        selected = ranked[:top_k]
        console.print(
            f"  [Semantic Router] selected {len(selected)} tools "
            f"(top_k={top_k}, family={family})"
        )
        return selected

    except Exception as exc:
        console.print(f"  [Semantic Router] failed ({exc}); using first {top_k} tools")
        return tools[:top_k]


# ══════════════════════════════════════════════════════════════════════════════
# Code injection guard
# ══════════════════════════════════════════════════════════════════════════════

async def _ensure_codes_for_tool(
    tool_name:   str,
    tool_args:   dict,
    registry:    _EntityRegistry,
    param_index: _ToolParamIndex,
    session,
    messages:    list,
) -> tuple[dict, list]:
    updated_args = dict(tool_args)
    label_hint   = updated_args.pop("entity_label", None)
    required_db  = param_index.get_required_db_params(tool_name)
    tool_family  = param_index.get_family(tool_name)

    if not required_db:
        return updated_args, messages

    entity = registry.find_entity_for_tool(tool_name, tool_family, label_hint)

    for param in required_db:
        # 1. Sanitize hallucinated "null" strings
        if param in updated_args:
            if str(updated_args[param]).lower() in {"null", "none", "", "nan"}:
                updated_args.pop(param)

        # 2. Inject correct ID only when entity family matches
        if param not in updated_args or updated_args.get(param) is None:
            if entity and (
                entity.entity_type == tool_family or tool_family == "general"
            ):
                val = entity.get_code_for_param(param)
                if val is not None:
                    updated_args[param] = (
                        int(val) if param in _NUMERIC_PARAMS else val
                    )

    console.print(
        f"    Code injection: tool={tool_name} family={tool_family} "
        f"entity={entity.label} needs={required_db}"
    )
    return updated_args, messages


# ══════════════════════════════════════════════════════════════════════════════
# System prompt builder
# ══════════════════════════════════════════════════════════════════════════════

def _build_system_prompt(registry: _EntityRegistry, is_multi: bool) -> str:
    lines = [
        "You are a financial analyst assistant for Indian markets.",
        "Use the pre-resolved IDs provided. NEVER pass 'null' or empty strings "
        "for required integer IDs.",
        "",
    ]

    if is_multi:
        lines.append(
            "This is a multi-entity request. Include 'entity_label' in tool "
            "calls to route codes correctly."
        )

    for i, e in enumerate(registry.entities, start=1):
        codes_str = ", ".join(
            f"{p}={getattr(e, p)}"
            for p in ("co_code", "mf_schcode", "mf_cocode", "isin", "index_code")
            if getattr(e, p) is not None
        )
        rec = f" | recommended_tool={e.recommended_tool}" if e.recommended_tool else ""
        lines.append(
            f"Entity {i}: {e.label} | type={e.entity_type}"
            + (f" | {codes_str}" if codes_str else "")
            + rec
        )

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# OpenAI-schema tool builder
# ══════════════════════════════════════════════════════════════════════════════

def _to_openai_tool(tool, add_entity_label: bool) -> dict:
    schema   = tool.inputSchema or {}
    props    = dict(schema.get("properties", {}))
    required = list(schema.get("required", []))

    # Unwrap nested params
    if "params" in props and len(props) == 1:
        inner    = props["params"]
        props    = dict(inner.get("properties", {}))
        required = list(inner.get("required", []))

    # Force integer type for numeric DB params (prevents "null" string bug)
    for k in list(props.keys()):
        if k in _NUMERIC_PARAMS:
            props[k] = {**props.get(k, {}), "type": "integer"}

    # entity_label only for genuine multi-entity calls
    if add_entity_label and tool.name not in _RESOLVER_TOOLS:
        props["entity_label"] = {
            "type":        "string",
            "description": "Label of the entity this tool call is for (e.g. 'Reliance Industries')",
        }

    return {
        "type":     "function",
        "function": {
            "name":        tool.name,
            "description": tool.description or "",
            "parameters":  {
                "type":       "object",
                "properties": props,
                "required":   required,
            },
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# LLM chat call
# ══════════════════════════════════════════════════════════════════════════════

def _chat(messages: list, tools: list, backend: str):
    if backend == "groq":
        from groq import Groq
        client = Groq(api_key=os.environ["GROQ_API_KEY"])
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=messages,
            tools=tools,
            tool_choice="auto",
        )
        return response.choices[0]

    # Ollama fallback
    import httpx
    payload = {
        "model":    OLLAMA_MODEL,
        "messages": messages,
        "tools":    tools,
        "stream":   False,
    }
    resp = httpx.post(
        f"{OLLAMA_HOST}/api/chat",
        json=payload,
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()

    class _FakeMessage:
        def __init__(self, d):
            self.content    = d.get("content", "")
            self.tool_calls = d.get("tool_calls") or []

    class _FakeChoice:
        def __init__(self, d):
            self.message = _FakeMessage(d.get("message", {}))

    return _FakeChoice(data)


# ══════════════════════════════════════════════════════════════════════════════
# Core session logic
# ══════════════════════════════════════════════════════════════════════════════

async def _run_query_with_session(
    query:   str,
    session: ClientSession,
    backend: str,
) -> str:
    registry = _EntityRegistry()
    registry.parse_injected(query)

    # Per-intent calls from graph.py v10.0 are always single-entity
    is_multi = registry.is_multi

    # Determine the entity family for tool filtering
    entity_family = "general"
    if registry.entities:
        entity_family = registry.entities[0].entity_type

    # Detect recommended_tool hint
    recommended_tool = ""
    if registry.entities:
        recommended_tool = registry.entities[0].recommended_tool

    # Extract the intent description from the query for semantic search
    intent_match = re.search(
        r"query_intent\s*=\s*(.+)", query, re.I
    )
    search_query = (
        intent_match.group(1).strip() if intent_match else query
    )
    # Prefer the part after "User Query:" if present
    user_query_match = re.search(r"User Query:\s*(.+)", query, re.I | re.S)
    if user_query_match:
        search_query = user_query_match.group(1).strip()

    # Build tool list
    raw_tools   = (await session.list_tools()).tools
    param_index = _ToolParamIndex()
    param_index.build(raw_tools)

    all_manifest = [
        _to_openai_tool(t, add_entity_label=is_multi)
        for t in raw_tools
    ]

    top_k = 14 if is_multi else 8
    current_tools = get_semantic_tools(
        query=search_query,
        tools=all_manifest,
        top_k=top_k,
        family=entity_family,
    )

    # If there's a recommended tool, ensure it's in the list
    if recommended_tool:
        tool_names_present = {t["function"]["name"] for t in current_tools}
        if recommended_tool not in tool_names_present:
            for t in all_manifest:
                if t["function"]["name"] == recommended_tool:
                    current_tools.insert(0, t)
                    console.print(
                        f"  [Semantic Router] pinned recommended_tool={recommended_tool}"
                    )
                    break

    messages = [
        {
            "role":    "system",
            "content": _build_system_prompt(registry, is_multi),
        },
        {
            "role":    "user",
            "content": query,
        },
    ]

    for round_num in range(1, MAX_ROUNDS + 1):
        console.print(f"  ── Round {round_num} ──")

        choice = await asyncio.get_event_loop().run_in_executor(
            None, _chat, messages, current_tools, backend
        )
        msg = choice.message

        if not msg.tool_calls:
            return msg.content or "No data."

        for tc in msg.tool_calls:
            fn   = tc.function.name
            args = (
                json.loads(tc.function.arguments)
                if isinstance(tc.function.arguments, str)
                else tc.function.arguments
            )

            # Pre-sanitize numeric params before injection
            for p in _NUMERIC_PARAMS:
                if p in args:
                    v = str(args[p]).lower()
                    if v in {"null", "none", "", "nan"}:
                        args.pop(p)
                    else:
                        try:
                            args[p] = int(float(v))
                        except ValueError:
                            args.pop(p)

            # Inject resolved codes (skip for resolver tools)
            if fn not in _RESOLVER_TOOLS:
                args, messages = await _ensure_codes_for_tool(
                    fn, args, registry, param_index, session, messages
                )

            console.print(f"  🛠  {fn}  args={args}")

            try:
                res = await session.call_tool(fn, args)
                txt = res.content[0].text if res.content else "Success"
            except Exception as exc:
                txt = f"Error: {exc}"

            console.print(f"  ✔ {fn}: {len(txt)} chars")

            messages.append({
                "role":    "assistant",
                "content": None,
                "tool_calls": [{
                    "id":   tc.id,
                    "type": "function",
                    "function": {
                        "name":      fn,
                        "arguments": json.dumps(args),
                    },
                }],
            })
            messages.append({
                "role":        "tool",
                "tool_call_id": tc.id,
                "content":     txt,
            })

    return "Max rounds reached without a final answer."


# ══════════════════════════════════════════════════════════════════════════════
# Public entry point
# ══════════════════════════════════════════════════════════════════════════════

async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
    backend = backend or LLM_BACKEND
    async with stdio_client(
        StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
    ) as (r, w):
        async with ClientSession(r, w) as session:
            await session.initialize()
            return await _run_query_with_session(query, session, backend)


if __name__ == "__main__":
    asyncio.run(run_mcp_query(" ".join(sys.argv[1:])))