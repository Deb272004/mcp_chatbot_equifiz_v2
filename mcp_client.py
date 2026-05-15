# """
# mcp_client.py — Equifiz MF MCP Client (v6.0)

# Key fixes from v5.0
# ───────────────────
# FIX A — Ollama Round-2 400 Bad Request (definitive fix)
#   Root cause: Ollama is strict about the assistant message format on replay.
#   Solution: Mirror exactly what equifiz_client.py does — which works for
#   multi-turn. The assistant message stores tool_calls with full OpenAI-style
#   dict (id, type, function), but the "arguments" field is kept as a dict
#   (not a JSON string) for Ollama. Tool result messages omit tool_call_id
#   for Ollama (use role="tool" + content only).

# FIX B — Multi-intent via sequential per-intent calls in ONE session
#   Instead of building one giant merged query (which confuses the LLM and
#   produces only one tool call), we now call _run_single_intent() for each
#   intent sequentially inside the same MCP session. This guarantees each
#   intent gets its own clean agentic loop with full MAX_ROUNDS budget.

# FIX C — Code injection only for tools that actually need DB codes
#   _inject_codes() now skips injection entirely if the tool has no required
#   DB params in its schema. Tools like get_top_gainers, get_open_ipos, etc.
#   go straight to execution without any code-lookup overhead.

# FIX D — No ChromaDB re-indexing on chat run
#   Chroma is queried read-only. Indexing is done by index_tools.py separately.

# FIX E — Result size: synthesis now receives ALL intent results concatenated,
#   not just the first 311 chars. Each intent result is stored separately and
#   joined for the synthesis node.
# """

# from __future__ import annotations

# import asyncio
# import json
# import logging
# import os
# import re
# import sys
# from concurrent.futures import ThreadPoolExecutor
# from pathlib import Path
# from typing import Optional

# from mcp import ClientSession, StdioServerParameters
# from mcp.client.stdio import stdio_client
# from rich.console import Console
# from rich.panel import Panel
# from dotenv import load_dotenv

# load_dotenv()

# # ── Config ──────────────────────────────────────────────────────────────────────

# SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
# CHROMA_PATH   = Path(__file__).parent / "chroma_db"
# MAX_ROUNDS    = 15

# LLM_BACKEND  = os.getenv("LLM_BACKEND", "ollama").lower()
# OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
# OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
# GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")

# logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
# logger  = logging.getLogger("equifiz_mf_client")
# console = Console()

# _LLM_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="llm_worker")

# # ── ChromaDB (read-only — indexing done by index_tools.py) ─────────────────────
# from chroma_singleton import get_chroma_collection
# tool_collection = get_chroma_collection()

# # ── Constants ───────────────────────────────────────────────────────────────────

# _NUMERIC_PARAMS = {"mf_schcode", "mf_cocode", "co_code", "index_code"}

# # These tools resolve entity names → DB codes; don't try to inject codes into them
# _RESOLVER_TOOLS = {
#     "resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes",
#     "resolve_nse_symbol", "get_company_details", "search_companies",
# }

# # Only these params ever need DB code injection
# _DB_PARAMS = {"co_code", "mf_schcode", "mf_cocode", "isin", "index_code"}

# _PARAM_TO_FAMILY: dict[str, str] = {
#     "co_code":    "stock",
#     "mf_schcode": "mf_scheme",
#     "mf_cocode":  "mf_amc",
#     "isin":       "etf",
#     "index_code": "index",
# }

# _FAMILY_TO_PARAM: dict[str, str] = {v: k for k, v in _PARAM_TO_FAMILY.items()}

# _RESOLVER_FOR_PARAM = {
#     "mf_schcode": "resolve_mf_scheme",
#     "mf_cocode":  "resolve_mf_fund",
#     "co_code":    "resolve_nse_symbol",
# }


# # ══════════════════════════════════════════════════════════════════════════════
# # Ollama-safe message builders
# # (mirroring the working pattern from equifiz_client.py)
# # ══════════════════════════════════════════════════════════════════════════════

# def _assistant_msg_with_tools(content: Optional[str], tool_calls_raw: list) -> dict:
#     """
#     Build an assistant message that Ollama accepts on replay.

#     Ollama expects tool_calls as a list of dicts with keys:
#       {"id": "...", "type": "function", "function": {"name": "...", "arguments": <dict>}}

#     IMPORTANT: "arguments" must be a DICT (not a JSON string) for Ollama.
#     Content must never be None — use "" instead.
#     """
#     return {
#         "role":       "assistant",
#         "content":    content or "",
#         "tool_calls": tool_calls_raw,
#     }


# def _assistant_msg_text(content: str) -> dict:
#     return {"role": "assistant", "content": content or ""}


# def _tool_result_msg(tc_id: str, content: str, backend: str) -> dict:
#     """
#     Tool result message.
#     Ollama: omit tool_call_id (causes 400 on round 2 if included).
#     Groq/OpenAI: include tool_call_id.
#     """
#     if backend == "ollama":
#         return {"role": "tool", "content": content}
#     return {"role": "tool", "tool_call_id": tc_id, "content": content}


# # ══════════════════════════════════════════════════════════════════════════════
# # Normalised tool call
# # ══════════════════════════════════════════════════════════════════════════════

# class _ToolCall:
#     __slots__ = ("id", "name", "arguments", "raw")

#     def __init__(self, tc_id: str, name: str, arguments: dict, raw: dict):
#         self.id        = tc_id
#         self.name      = name
#         self.arguments = arguments   # always a dict
#         self.raw       = raw         # Ollama-replay-safe dict for history


# class _LLMResponse:
#     __slots__ = ("content", "tool_calls")

#     def __init__(self, content: str, tool_calls: list[_ToolCall]):
#         self.content    = content or ""
#         self.tool_calls = tool_calls


# # ══════════════════════════════════════════════════════════════════════════════
# # LLM wrappers
# # ══════════════════════════════════════════════════════════════════════════════

# def _chat_ollama(messages: list, tools: list) -> _LLMResponse:
#     """
#     Calls Ollama /api/chat.

#     We store each tool_call in the history as:
#       {
#         "id":   "<tc_id>",
#         "type": "function",
#         "function": {"name": "...", "arguments": <dict>}   ← dict, NOT string
#       }
#     This matches the working pattern in equifiz_client.py.
#     """
#     import urllib.request

#     payload = {
#         "model":    OLLAMA_MODEL,
#         "messages": messages,
#         "tools":    tools,
#         "stream":   False,
#         "options":  {"temperature": 0.0},
#     }
#     req = urllib.request.Request(
#         f"{OLLAMA_HOST}/api/chat",
#         data=json.dumps(payload).encode(),
#         headers={"Content-Type": "application/json"},
#         method="POST",
#     )
#     raw  = json.loads(urllib.request.urlopen(req, timeout=600).read().decode())
#     omsg = raw.get("message", {})

#     content     = omsg.get("content") or ""
#     raw_tc_list = omsg.get("tool_calls") or []

#     tool_calls: list[_ToolCall] = []
#     for i, tc in enumerate(raw_tc_list):
#         fn   = tc.get("function", {})
#         name = fn.get("name", f"unknown_{i}")
#         args = fn.get("arguments", {})
#         if isinstance(args, str):
#             try:
#                 args = json.loads(args)
#             except json.JSONDecodeError:
#                 args = {}
#         tc_id = tc.get("id") or f"tc-{name}-{i}"

#         # Build the replay-safe dict (arguments as dict, not string)
#         replay_dict = {
#             "id":   tc_id,
#             "type": "function",
#             "function": {"name": name, "arguments": args},
#         }
#         tool_calls.append(_ToolCall(tc_id, name, args, replay_dict))

#     return _LLMResponse(content, tool_calls)


# def _chat_groq(messages: list, tools: list) -> _LLMResponse:
#     from groq import Groq
#     client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
#     choice = client.chat.completions.create(
#         model=GROQ_MODEL,
#         messages=messages,
#         tools=tools,
#         tool_choice="auto",
#     ).choices[0]
#     msg = choice.message

#     tool_calls: list[_ToolCall] = []
#     for tc in (msg.tool_calls or []):
#         args = {}
#         if isinstance(tc.function.arguments, str):
#             try:
#                 args = json.loads(tc.function.arguments)
#             except json.JSONDecodeError:
#                 args = {}
#         else:
#             args = tc.function.arguments or {}

#         replay_dict = {
#             "id":   tc.id,
#             "type": "function",
#             "function": {"name": tc.function.name, "arguments": tc.function.arguments},
#         }
#         tool_calls.append(_ToolCall(tc.id, tc.function.name, args, replay_dict))

#     return _LLMResponse(msg.content or "", tool_calls)


# def _chat(messages: list, tools: list, backend: str) -> _LLMResponse:
#     if backend == "ollama":
#         return _chat_ollama(messages, tools)
#     return _chat_groq(messages, tools)


# # ══════════════════════════════════════════════════════════════════════════════
# # Tool param index (built from live MCP schema at session start)
# # ══════════════════════════════════════════════════════════════════════════════

# class ToolParamIndex:
#     def __init__(self):
#         self._idx: dict[str, dict] = {}

#     def build(self, raw_tools: list) -> None:
#         for tool in raw_tools:
#             schema   = tool.inputSchema or {}
#             props    = schema.get("properties", {})
#             required = set(schema.get("required", []))

#             # Unwrap nested "params" object if that's the only property
#             if "params" in props and len(props) == 1:
#                 inner    = props["params"]
#                 props    = inner.get("properties", {})
#                 required = set(inner.get("required", []))

#             # Only care about DB-code params
#             req_db = required & _DB_PARAMS
#             opt_db = (set(props.keys()) & _DB_PARAMS) - required

#             self._idx[tool.name] = {
#                 "req_db": req_db,
#                 "opt_db": opt_db,
#                 "all":    set(props.keys()),
#             }

#         db_count = sum(1 for v in self._idx.values() if v["req_db"])
#         console.print(f"  [ToolParamIndex] {len(self._idx)} tools indexed, {db_count} need DB codes")

#     def required_db(self, name: str) -> set[str]:
#         return self._idx.get(name, {}).get("req_db", set())

#     def needs_codes(self, name: str) -> bool:
#         """True only if this tool actually requires a DB code param."""
#         return bool(self.required_db(name))


# # ══════════════════════════════════════════════════════════════════════════════
# # Entity registry (parsed from PRE_RESOLVED block)
# # ══════════════════════════════════════════════════════════════════════════════

# class Entity:
#     __slots__ = (
#         "label", "entity_type", "query_intent", "recommended_tool",
#         "co_code", "mf_schcode", "mf_cocode", "isin", "index_code",
#         "company_name", "amc_name", "scheme_name",
#     )

#     def __init__(self, label: str = ""):
#         self.label            = label
#         self.entity_type      = "general"
#         self.query_intent     = ""
#         self.recommended_tool = ""
#         self.co_code:    Optional[int] = None
#         self.mf_schcode: Optional[int] = None
#         self.mf_cocode:  Optional[int] = None
#         self.isin:       Optional[str] = None
#         self.index_code: Optional[int] = None
#         self.company_name = ""
#         self.amc_name     = ""
#         self.scheme_name  = ""

#     def code(self, param: str):
#         return getattr(self, param, None)

#     def has_code(self) -> bool:
#         return any(self.code(p) is not None for p in _DB_PARAMS)

#     def name_hint(self) -> str:
#         return self.scheme_name or self.amc_name or self.company_name or self.label


# class EntityRegistry:
#     def __init__(self):
#         self.entities: list[Entity] = []

#     def parse(self, query: str) -> None:
#         m     = re.search(r"<PRE_RESOLVED>(.*?)</PRE_RESOLVED>", query, re.S | re.I)
#         block = m.group(1) if m else query
#         current: Optional[Entity] = None

#         for raw in block.splitlines():
#             line = raw.strip()
#             if not line or line.startswith("#"):
#                 continue

#             em = re.match(r"Entity\s+\d+:\s*(.+)", line, re.I)
#             if em:
#                 current = Entity(em.group(1).strip())
#                 self.entities.append(current)
#                 continue

#             if current is None:
#                 current = Entity("(primary)")
#                 self.entities.append(current)

#             kv = re.match(r"([\w_]+)\s*=\s*(.+)", line)
#             if not kv:
#                 continue
#             key = kv.group(1).lower()
#             val = re.sub(r"\s*\(.*?\)\s*$", "", kv.group(2)).strip()
#             # strip inline comments like "  # stock tools only"
#             val = re.sub(r"\s*#.*$", "", val).strip()
#             self._set(current, key, val)

#         if not self.entities:
#             e = Entity("(primary)")
#             for pat, attr in [
#                 (r"mf_schcode[:=]\s*(\d+)", "mf_schcode"),
#                 (r"mf_cocode[:=]\s*(\d+)",  "mf_cocode"),
#                 (r"co_code[:=]\s*(\d+)",    "co_code"),
#                 (r"isin[:=]\s*([A-Z0-9]+)", "isin"),
#                 (r"index_code[:=]\s*(\d+)", "index_code"),
#             ]:
#                 match = re.search(pat, block, re.I)
#                 if match:
#                     self._set(e, attr, match.group(1))
#             if e.has_code():
#                 self.entities.append(e)

#         for e in self.entities:
#             console.print(f"  [Registry] {e.label} ({e.entity_type}) codes={self._code_summary(e)}")

#     @staticmethod
#     def _code_summary(e: Entity) -> str:
#         parts = []
#         for p in ("co_code", "mf_schcode", "mf_cocode", "isin", "index_code"):
#             v = e.code(p)
#             if v is not None:
#                 parts.append(f"{p}={v}")
#         return ", ".join(parts) or "none"

#     @staticmethod
#     def _set(e: Entity, key: str, val: str) -> None:
#         try:
#             if key in ("co_code", "mf_schcode", "mf_cocode", "index_code"):
#                 setattr(e, key, int(val))
#             elif key == "isin":
#                 e.isin = val
#             elif key == "entity_type":
#                 e.entity_type = val
#             elif key == "query_intent":
#                 e.query_intent = val
#             elif key == "recommended_tool":
#                 e.recommended_tool = val
#             elif key == "company_name":
#                 e.company_name = val
#             elif key == "amc_name":
#                 e.amc_name = val
#             elif key == "scheme_name":
#                 e.scheme_name = val
#         except (ValueError, TypeError):
#             pass

#     def find(self, tool_name: str, required_db: set[str], label_hint: Optional[str] = None) -> Optional[Entity]:
#         if label_hint:
#             for e in self.entities:
#                 if e.label.lower() == label_hint.lower():
#                     return e
#         for e in self.entities:
#             if e.recommended_tool == tool_name:
#                 return e
#         # Match by which codes this entity has vs what the tool needs
#         for param in required_db:
#             family = _PARAM_TO_FAMILY.get(param)
#             for e in self.entities:
#                 if e.entity_type == family and e.code(param) is not None:
#                     return e
#         for param in required_db:
#             for e in self.entities:
#                 if e.code(param) is not None:
#                     return e
#         return self.entities[0] if self.entities else None

#     def update_from_resolver(self, tool_name: str, result: str) -> None:
#         if "RESOLVED" not in result:
#             return
#         mapping = {
#             "resolve_mf_scheme":   ("mf_schcode", r"mf_schcode\s*[:=]\s*(\d+)"),
#             "resolve_mf_fund":     ("mf_cocode",  r"mf_cocode\s*[:=]\s*(\d+)"),
#             "resolve_nse_symbol":  ("co_code",    r"co_code\s*[:=]\s*(\d+)"),
#             "get_company_details": ("co_code",    r"co_code\s*[:=]\s*(\d+)"),
#         }
#         entry = mapping.get(tool_name)
#         if not entry:
#             return
#         attr, pat = entry
#         m = re.search(pat, result, re.I)
#         if not m:
#             return
#         val = int(m.group(1))
#         for e in self.entities:
#             if e.code(attr) is None:
#                 setattr(e, attr, val)
#                 console.print(f"  [Registry] updated {e.label}: {attr}={val}")
#                 break


# # ══════════════════════════════════════════════════════════════════════════════
# # Semantic tool selection (read-only Chroma)
# # ══════════════════════════════════════════════════════════════════════════════

# _CORE_TOOLS = {
#     "resolve_mf_scheme", "resolve_mf_fund",
#     "resolve_nse_symbol", "search_companies",
# }


# def get_semantic_tools(query: str, all_tools: list[dict], top_k: int = 8) -> list[dict]:
#     if tool_collection is None:
#         return all_tools
#     try:
#         results       = tool_collection.query(query_texts=[query], n_results=top_k)
#         matched_names = set(results["ids"][0]) | _CORE_TOOLS
#         selected      = [t for t in all_tools if t["function"]["name"] in matched_names]
#         console.print(
#             f"  [Semantic] {len(selected)} tools selected from {len(all_tools)} "
#             f"(top_k={top_k})"
#         )
#         return selected
#     except Exception as e:
#         console.print(f"  [yellow]ChromaDB query error: {e} — using all tools[/yellow]")
#         return all_tools


# # ══════════════════════════════════════════════════════════════════════════════
# # Tool schema builder
# # ══════════════════════════════════════════════════════════════════════════════

# def _to_openai_tool(tool) -> dict:
#     schema   = tool.inputSchema or {}
#     props    = dict(schema.get("properties", {}))
#     required = list(schema.get("required", []))

#     if "params" in props and len(props) == 1:
#         inner    = props["params"]
#         props    = dict(inner.get("properties", {}))
#         required = list(inner.get("required", []))

#     props = {k: dict(v) for k, v in props.items()}
#     for k, v in props.items():
#         if "description" not in v:
#             v["description"] = f"Parameter: {k}"
#         if k in _NUMERIC_PARAMS:
#             v["type"] = "integer"

#     return {
#         "type": "function",
#         "function": {
#             "name":        tool.name,
#             "description": (tool.description or tool.name).strip(),
#             "parameters":  {"type": "object", "properties": props, "required": required},
#         },
#     }


# # ══════════════════════════════════════════════════════════════════════════════
# # Code injection — ONLY for tools that actually require DB codes
# # ══════════════════════════════════════════════════════════════════════════════

# async def _inject_codes(
#     tool_name:   str,
#     tool_args:   dict,
#     required_db: set[str],
#     registry:    EntityRegistry,
#     session:     ClientSession,
#     messages:    list,
#     entity_hint: str,
#     backend:     str,
# ) -> dict:
#     """
#     Fill missing required DB params using registry codes.
#     If still missing after registry lookup, auto-call the resolver tool.
#     Skipped entirely if required_db is empty (no-code tools like get_top_gainers).
#     """
#     if not required_db:
#         return tool_args  # nothing to inject — skip completely

#     label_hint = tool_args.pop("entity_label", None)
#     entity     = registry.find(tool_name, required_db, label_hint)

#     for param in required_db:
#         # Already present and valid — just coerce type
#         if param in tool_args and tool_args[param] is not None:
#             try:
#                 if param in _NUMERIC_PARAMS:
#                     tool_args[param] = int(float(str(tool_args[param])))
#             except (ValueError, TypeError):
#                 tool_args.pop(param)
#             continue

#         # Try registry
#         value = entity.code(param) if entity else None

#         # Auto-resolve if still missing
#         if value is None:
#             resolver = _RESOLVER_FOR_PARAM.get(param)
#             hint     = _clean(entity.name_hint() if entity else entity_hint)
#             if resolver and hint:
#                 console.print(f"  [yellow]⚡ Auto-resolving {param} for '{hint[:60]}'[/yellow]")
#                 try:
#                     res = await session.call_tool(resolver, {"query": hint})
#                     txt = res.content[0].text if res.content else ""
#                     if "RESOLVED" in txt:
#                         registry.update_from_resolver(resolver, txt)
#                         value = (entity.code(param) if entity else None)
#                         if value is None and registry.entities:
#                             value = registry.entities[0].code(param)

#                         auto_id = f"auto-{param}-{value}"
#                         # Append to history in Ollama-safe format
#                         messages.append(_assistant_msg_with_tools("", [{
#                             "id":   auto_id,
#                             "type": "function",
#                             "function": {"name": resolver, "arguments": {"query": hint}},
#                         }]))
#                         messages.append(_tool_result_msg(auto_id, txt, backend))
#                         console.print(f"    [green]✔ {param}={value}[/green]")
#                     else:
#                         console.print(f"  [red]⚠ Could not resolve {param}[/red]")
#                 except Exception as e:
#                     console.print(f"  [red]Auto-resolve failed ({param}): {e}[/red]")

#         if value is not None:
#             try:
#                 if param in _NUMERIC_PARAMS:
#                     value = int(float(str(value)))
#             except (ValueError, TypeError):
#                 pass
#             tool_args[param] = value

#     return tool_args


# def _clean(text: str) -> str:
#     text = re.sub(r"<PRE_RESOLVED>.*?</PRE_RESOLVED>", "", str(text), flags=re.S | re.I)
#     text = re.sub(r"(?i)(User Query|Instruction|Context|RECOMMENDED TOOL):", "", text)
#     return re.sub(r"\[.*?\]", "", text).strip()


# # ══════════════════════════════════════════════════════════════════════════════
# # System prompt builder
# # ══════════════════════════════════════════════════════════════════════════════

# def _system_prompt(registry: EntityRegistry) -> str:
#     base = """\
# You are a financial analyst assistant for Indian markets with tools covering
# equities, mutual funds, ETFs, and indices.

# RULES:
# - Verified numeric IDs are provided in the query. Use them DIRECTLY without
#   calling resolver tools (resolve_mf_scheme, resolve_nse_symbol, etc.) unless
#   an ID is genuinely absent from the query.
# - Call exactly ONE tool that answers the user's question. After getting the
#   tool result, return the data as your final answer — do NOT call more tools
#   unless the first tool explicitly says it needs a follow-up.
# - Never fabricate data. If a tool returns nothing, say so.
# - Return ALL numeric values from tools verbatim. Do NOT describe what a tool
#   returns — return the actual data values.
# - For tools that need no entity codes (e.g. get_top_gainers, get_open_ipos),
#   call them directly with any required non-code params only.
# """
#     if not registry.entities:
#         return base

#     block = "\n\nPRE-RESOLVED ENTITY CODES (use these directly):\n"
#     for i, e in enumerate(registry.entities, 1):
#         block += f"\nEntity {i}: {e.label} ({e.entity_type})\n"
#         if e.query_intent:
#             block += f"  data needed: {e.query_intent}\n"
#         if e.recommended_tool:
#             block += f"  recommended tool: {e.recommended_tool}\n"
#         for attr in ("co_code", "mf_schcode", "mf_cocode", "isin", "index_code"):
#             v = e.code(attr)
#             if v is not None:
#                 block += f"  {attr}={v}\n"
#     block += "\nUse these codes directly. Do not call resolver tools for them."
#     return base + block


# # ══════════════════════════════════════════════════════════════════════════════
# # Meta-description guard
# # ══════════════════════════════════════════════════════════════════════════════

# def _is_meta(text: str) -> bool:
#     if not text or not text.strip():
#         return True
#     lower = text.lower()
#     if any(p in lower for p in [
#         "the function call returns", "the output includes", "the result contains",
#         "the data includes", "this tool returns", "returns the following",
#         "includes market capitalization",
#     ]):
#         console.print("  [yellow]⚠ Meta-description detected — using raw results[/yellow]")
#         return True
#     nums = re.findall(r"\b\d[\d,\.]*\b", text)
#     if len(nums) < 2:
#         console.print(f"  [yellow]⚠ Only {len(nums)} numeric values — likely meta[/yellow]")
#         return True
#     return False


# # ══════════════════════════════════════════════════════════════════════════════
# # Single-intent agentic loop
# # ══════════════════════════════════════════════════════════════════════════════

# async def _run_single_intent(
#     enriched_query: str,
#     session:        ClientSession,
#     backend:        str,
#     raw_tools:      list,
#     param_index:    ToolParamIndex,
#     all_schemas:    list[dict],
# ) -> str:
#     """
#     Run one full agentic loop for a single intent within an existing session.
#     Returns the raw tool result string (not LLM-synthesised prose).
#     """
#     registry    = EntityRegistry()
#     registry.parse(enriched_query)
#     entity_hint = re.split(r"\[(PRE-RESOLVED|VERIFIED|SUGGESTED|RECOMMENDED)", enriched_query)[0].strip()

#     active_tools = get_semantic_tools(enriched_query, all_schemas, top_k=6)

#     messages: list[dict] = [
#         {"role": "system", "content": _system_prompt(registry)},
#         {"role": "user",   "content": enriched_query},
#     ]

#     tool_results: list[str] = []
#     loop = asyncio.get_event_loop()

#     for round_num in range(1, MAX_ROUNDS + 1):
#         console.print(f"  [dim]── Round {round_num} ──[/dim]")

#         try:
#             response: _LLMResponse = await asyncio.wait_for(
#                 loop.run_in_executor(_LLM_EXECUTOR, _chat, messages, active_tools, backend),
#                 timeout=600,
#             )
#         except asyncio.TimeoutError:
#             logger.error(f"LLM timed out at round {round_num}")
#             break
#         except Exception as e:
#             logger.error(f"LLM error at round {round_num}: {e}")
#             break

#         if response.content:
#             console.print(f"  [magenta]LLM:[/magenta] {response.content[:200]}")

#         # No tool calls → final LLM answer
#         if not response.tool_calls:
#             if response.content and not _is_meta(response.content):
#                 return response.content
#             break  # fall through to raw results

#         # Append assistant message with tool calls (Ollama-safe format)
#         messages.append(
#             _assistant_msg_with_tools(
#                 response.content,
#                 [tc.raw for tc in response.tool_calls],
#             )
#         )

#         # Execute each tool call
#         for tc in response.tool_calls:
#             args = dict(tc.arguments)

#             # Coerce numeric params from LLM output
#             for param in _NUMERIC_PARAMS:
#                 if param in args:
#                     try:
#                         args[param] = int(float(str(args[param])))
#                     except (ValueError, TypeError):
#                         args.pop(param, None)

#             # Inject missing DB codes ONLY if this tool needs them
#             if tc.name not in _RESOLVER_TOOLS and param_index.needs_codes(tc.name):
#                 required_db = param_index.required_db(tc.name)
#                 args = await _inject_codes(
#                     tc.name, args, required_db,
#                     registry, session, messages, entity_hint, backend,
#                 )
#             elif "entity_label" in args:
#                 args.pop("entity_label")  # clean up even if no injection

#             console.print(
#                 "  [cyan]🛠  " + tc.name + "[/cyan]  args={"
#                 + ", ".join(f"{k}={v}" for k, v in args.items())
#                 + "}"
#             )

#             try:
#                 res = await asyncio.wait_for(session.call_tool(tc.name, args), timeout=120)
#                 txt = res.content[0].text if res.content else "Empty response"
#                 console.print(f"  [green]✔ {tc.name}[/green]: {len(txt)} chars")

#                 if tc.name in _RESOLVER_TOOLS:
#                     registry.update_from_resolver(tc.name, txt)
#                 else:
#                     tool_results.append(f"[{tc.name}]\n{txt}")

#             except asyncio.TimeoutError:
#                 txt = f"Error: {tc.name} timed out."
#                 console.print(f"  [red]✗ {tc.name} timed out[/red]")
#             except Exception as e:
#                 txt = f"Tool Error: {e}"
#                 console.print(f"  [red]✗ {tc.name}: {e}[/red]")

#             # Append tool result (Ollama-safe: no tool_call_id)
#             messages.append(_tool_result_msg(tc.id, txt, backend))

#     # Return raw tool data if LLM gave no usable prose
#     if tool_results:
#         return "\n\n".join(tool_results)
#     return "(No data returned)"


# # ══════════════════════════════════════════════════════════════════════════════
# # Intent query builder helpers
# # ══════════════════════════════════════════════════════════════════════════════

# def _build_intent_query(intent: dict) -> str:
#     """
#     Build the enriched query string for a single intent, embedding
#     all pre-resolved codes in a PRE_RESOLVED block.
#     """
#     codes       = intent.get("resolved_codes") or {}
#     entity_type = intent.get("entity_type", "general")
#     tool_hint   = intent.get("tool_hint", "")
#     scheme_name = intent.get("scheme_name", "") or ""
#     amc_name    = intent.get("amc_name", "") or ""
#     intent_desc = intent.get("intent_description", "")
#     entity      = intent.get("entity", "")

#     lines: list[str] = []
#     if entity_type and entity_type != "general":
#         lines.append(f"entity_type={entity_type}")
#     if intent_desc:
#         lines.append(f"query_intent={intent_desc}")
#     if tool_hint:
#         lines.append(f"recommended_tool={tool_hint}")

#     _COMMENTS = {
#         "co_code":    "stock/equity tools only",
#         "mf_schcode": "MF scheme tools only",
#         "mf_cocode":  "AMC/fund-house tools only",
#         "isin":       "ETF tools only",
#         "index_code": "index tools only",
#     }
#     for param, val in codes.items():
#         comment = f"  # {_COMMENTS[param]}" if param in _COMMENTS else ""
#         lines.append(f"{param}={val}{comment}")

#     if scheme_name:
#         lines.append(f"scheme_name={scheme_name}")
#     if amc_name:
#         lines.append(f"amc_name={amc_name}")

#     if entity:
#         lines.insert(0, f"Entity 1: {entity}")

#     pre_block = ("<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>\n") if lines else ""
#     sub_query = intent_desc or entity

#     enriched = f"{pre_block}User Query: {sub_query}"
#     if tool_hint:
#         enriched += f"\n\n[RECOMMENDED TOOL: {tool_hint}]"
#     return enriched


# # ══════════════════════════════════════════════════════════════════════════════
# # Public API
# # ══════════════════════════════════════════════════════════════════════════════

# async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
#     """Single-intent query — full agentic loop."""
#     b             = (backend or LLM_BACKEND).lower()
#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()
#             raw_tools   = (await session.list_tools()).tools
#             param_index = ToolParamIndex()
#             param_index.build(raw_tools)
#             all_schemas = [_to_openai_tool(t) for t in raw_tools]
#             return await _run_single_intent(
#                 query, session, b, raw_tools, param_index, all_schemas
#             )


# async def run_mcp_query_multi(
#     intents: list[dict], backend: Optional[str] = None
# ) -> list[dict]:
#     """
#     Multi-intent query — each intent runs its own agentic loop sequentially
#     within a SINGLE shared MCP session (one server process, one DB connection).

#     Returns the intents list with 'mcp_result' populated for each.
#     """
#     b             = (backend or LLM_BACKEND).lower()
#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])

#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()

#             # Build tool index ONCE for the session
#             raw_tools   = (await session.list_tools()).tools
#             param_index = ToolParamIndex()
#             param_index.build(raw_tools)
#             all_schemas = [_to_openai_tool(t) for t in raw_tools]

#             results = list(intents)  # shallow copy

#             for idx, intent in enumerate(results):
#                 entity = intent.get("entity", f"intent_{idx}")
#                 console.print(Panel(
#                     f"[bold blue]Intent {idx+1}/{len(results)}:[/bold blue] "
#                     f"{entity} — {intent.get('intent_description', '')[:80]}",
#                     title="MCP Intent",
#                 ))

#                 enriched_query = _build_intent_query(intent)

#                 try:
#                     mcp_result = await _run_single_intent(
#                         enriched_query, session, b,
#                         raw_tools, param_index, all_schemas,
#                     )
#                 except Exception as exc:
#                     console.print(f"  [red]Intent [{entity}] failed: {exc}[/red]")
#                     mcp_result = f"Error fetching data for {entity}: {exc}"

#                 results[idx] = {**intent, "mcp_result": mcp_result}
#                 console.print(
#                     f"  [green]✔ Intent [{entity}][/green] "
#                     f"→ {len(mcp_result)} chars result"
#                 )

#     return results


# # ── Trim helper (unchanged from v5) ───────────────────────────────────────────

# MAX_CHARS_PER_INTENT = 4000
# MAX_TOTAL_CHARS      = 12000


# def trim_results(results: list[dict]) -> list[dict]:
#     out = [dict(r) for r in results]
#     for r in out:
#         if len(r.get("mcp_result", "")) > MAX_CHARS_PER_INTENT:
#             r["mcp_result"] = r["mcp_result"][:MAX_CHARS_PER_INTENT] + "\n...[trimmed]"
#     while sum(len(r.get("mcp_result", "")) for r in out) > MAX_TOTAL_CHARS:
#         longest = max(out, key=lambda r: len(r.get("mcp_result", "")))
#         if len(longest.get("mcp_result", "")) <= 200:
#             break
#         longest["mcp_result"] = longest["mcp_result"][:-500] + "\n...[trimmed]"
#     return out


# # ── CLI ────────────────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     if len(sys.argv) > 1:
#         asyncio.run(run_mcp_query(" ".join(sys.argv[1:])))




"""
mcp_client.py — Equifiz MF MCP Client (v6.1)

Key fix from v6.0
─────────────────────────────────────────────────────────────────────
FIX F — Per-intent asyncio timeout inside the shared session.

Root cause of the original bug:
  run_mcp_query_multi() ran all intents sequentially with NO per-intent
  timeout. Intent 2's Round-2 inner LLM call (summarising 3847 chars of
  BSE announcements) took long. The outer future.result(timeout=600) in
  graph.py fired and replaced ALL results — including intent 1 which had
  already completed — with "MCP call timed out."

Fix:
  Each intent is now wrapped in asyncio.wait_for(
      _run_single_intent(...),
      timeout=INTENT_TIMEOUT_SECS   # default 150s per intent
  )
  Results are stored immediately as each intent finishes.
  A timeout on intent 2 CANNOT affect intent 1's already-saved result.
  The shared MCP session (one server process, one DB connection) is kept —
  no extra handshake latency from spawning separate sessions per intent.

graph.py change:
  The outer ThreadPoolExecutor timeout in node_mcp_tool_call_intents can
  now be a generous ceiling (e.g. 600s total) because per-intent timeouts
  handle the real granularity. Revert graph.py to v11.2's single-thread
  approach — the fix lives here now, not in the graph.

All v6.0 logic preserved otherwise.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from rich.console import Console
from rich.panel import Panel
from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────────────

SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
CHROMA_PATH   = Path(__file__).parent / "chroma_store"
MAX_ROUNDS    = 15

# Per-intent timeout in seconds.
# Intent 2 timing out will NOT affect intent 1's already-saved result.
INTENT_TIMEOUT_SECS = 150

LLM_BACKEND  = os.getenv("LLM_BACKEND", "ollama").lower()
OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger  = logging.getLogger("equifiz_mf_client")
console = Console()

_LLM_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="llm_worker")

# ── ChromaDB (read-only — indexing done by index_tools.py) ─────────────────────
from chroma_singleton import get_chroma_collection
tool_collection = get_chroma_collection()

# ── Constants ───────────────────────────────────────────────────────────────────

_NUMERIC_PARAMS = {"mf_schcode", "mf_cocode", "co_code", "index_code"}

_RESOLVER_TOOLS = {
    "resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes",
    "resolve_nse_symbol", "get_company_details", "search_companies",
}

_DB_PARAMS = {"co_code", "mf_schcode", "mf_cocode", "isin", "index_code"}

_PARAM_TO_FAMILY: dict[str, str] = {
    "co_code":    "stock",
    "mf_schcode": "mf_scheme",
    "mf_cocode":  "mf_amc",
    "isin":       "etf",
    "index_code": "index",
}

_FAMILY_TO_PARAM: dict[str, str] = {v: k for k, v in _PARAM_TO_FAMILY.items()}

_RESOLVER_FOR_PARAM = {
    "mf_schcode": "resolve_mf_scheme",
    "mf_cocode":  "resolve_mf_fund",
    "co_code":    "resolve_nse_symbol",
}


# ══════════════════════════════════════════════════════════════════════════════
# Ollama-safe message builders  (unchanged from v6.0)
# ══════════════════════════════════════════════════════════════════════════════

def _assistant_msg_with_tools(content: Optional[str], tool_calls_raw: list) -> dict:
    return {
        "role":       "assistant",
        "content":    content or "",
        "tool_calls": tool_calls_raw,
    }


def _assistant_msg_text(content: str) -> dict:
    return {"role": "assistant", "content": content or ""}


def _tool_result_msg(tc_id: str, content: str, backend: str) -> dict:
    if backend == "ollama":
        return {"role": "tool", "content": content}
    return {"role": "tool", "tool_call_id": tc_id, "content": content}


# ══════════════════════════════════════════════════════════════════════════════
# Normalised tool call  (unchanged from v6.0)
# ══════════════════════════════════════════════════════════════════════════════

class _ToolCall:
    __slots__ = ("id", "name", "arguments", "raw")

    def __init__(self, tc_id: str, name: str, arguments: dict, raw: dict):
        self.id        = tc_id
        self.name      = name
        self.arguments = arguments
        self.raw       = raw


class _LLMResponse:
    __slots__ = ("content", "tool_calls")

    def __init__(self, content: str, tool_calls: list[_ToolCall]):
        self.content    = content or ""
        self.tool_calls = tool_calls


# ══════════════════════════════════════════════════════════════════════════════
# LLM wrappers  (unchanged from v6.0)
# ══════════════════════════════════════════════════════════════════════════════

def _chat_ollama(messages: list, tools: list) -> _LLMResponse:
    import urllib.request

    payload = {
        "model":    OLLAMA_MODEL,
        "messages": messages,
        "tools":    tools,
        "stream":   False,
        "options":  {"temperature": 0.0},
    }
    req = urllib.request.Request(
        f"{OLLAMA_HOST}/api/chat",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    raw  = json.loads(urllib.request.urlopen(req, timeout=600).read().decode())
    omsg = raw.get("message", {})

    content     = omsg.get("content") or ""
    raw_tc_list = omsg.get("tool_calls") or []

    tool_calls: list[_ToolCall] = []
    for i, tc in enumerate(raw_tc_list):
        fn   = tc.get("function", {})
        name = fn.get("name", f"unknown_{i}")
        args = fn.get("arguments", {})
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except json.JSONDecodeError:
                args = {}
        tc_id = tc.get("id") or f"tc-{name}-{i}"

        replay_dict = {
            "id":   tc_id,
            "type": "function",
            "function": {"name": name, "arguments": args},
        }
        tool_calls.append(_ToolCall(tc_id, name, args, replay_dict))

    return _LLMResponse(content, tool_calls)


def _chat_groq(messages: list, tools: list) -> _LLMResponse:
    from groq import Groq
    client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
    choice = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=messages,
        tools=tools,
        tool_choice="auto",
    ).choices[0]
    msg = choice.message

    tool_calls: list[_ToolCall] = []
    for tc in (msg.tool_calls or []):
        args = {}
        if isinstance(tc.function.arguments, str):
            try:
                args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                args = {}
        else:
            args = tc.function.arguments or {}

        replay_dict = {
            "id":   tc.id,
            "type": "function",
            "function": {"name": tc.function.name, "arguments": tc.function.arguments},
        }
        tool_calls.append(_ToolCall(tc.id, tc.function.name, args, replay_dict))

    return _LLMResponse(msg.content or "", tool_calls)


def _chat(messages: list, tools: list, backend: str) -> _LLMResponse:
    if backend == "ollama":
        return _chat_ollama(messages, tools)
    return _chat_groq(messages, tools)


# ══════════════════════════════════════════════════════════════════════════════
# Tool param index  (unchanged from v6.0)
# ══════════════════════════════════════════════════════════════════════════════

class ToolParamIndex:
    def __init__(self):
        self._idx: dict[str, dict] = {}

    def build(self, raw_tools: list) -> None:
        for tool in raw_tools:
            schema   = tool.inputSchema or {}
            props    = schema.get("properties", {})
            required = set(schema.get("required", []))

            if "params" in props and len(props) == 1:
                inner    = props["params"]
                props    = inner.get("properties", {})
                required = set(inner.get("required", []))

            req_db = required & _DB_PARAMS
            opt_db = (set(props.keys()) & _DB_PARAMS) - required

            self._idx[tool.name] = {
                "req_db": req_db,
                "opt_db": opt_db,
                "all":    set(props.keys()),
            }

        db_count = sum(1 for v in self._idx.values() if v["req_db"])
        console.print(f"  [ToolParamIndex] {len(self._idx)} tools indexed, {db_count} need DB codes")

    def required_db(self, name: str) -> set[str]:
        return self._idx.get(name, {}).get("req_db", set())

    def needs_codes(self, name: str) -> bool:
        return bool(self.required_db(name))


# ══════════════════════════════════════════════════════════════════════════════
# Entity registry  (unchanged from v6.0)
# ══════════════════════════════════════════════════════════════════════════════

class Entity:
    __slots__ = (
        "label", "entity_type", "query_intent", "recommended_tool",
        "co_code", "mf_schcode", "mf_cocode", "isin", "index_code",
        "company_name", "amc_name", "scheme_name",
    )

    def __init__(self, label: str = ""):
        self.label            = label
        self.entity_type      = "general"
        self.query_intent     = ""
        self.recommended_tool = ""
        self.co_code:    Optional[int] = None
        self.mf_schcode: Optional[int] = None
        self.mf_cocode:  Optional[int] = None
        self.isin:       Optional[str] = None
        self.index_code: Optional[int] = None
        self.company_name = ""
        self.amc_name     = ""
        self.scheme_name  = ""

    def code(self, param: str):
        return getattr(self, param, None)

    def has_code(self) -> bool:
        return any(self.code(p) is not None for p in _DB_PARAMS)

    def name_hint(self) -> str:
        return self.scheme_name or self.amc_name or self.company_name or self.label


class EntityRegistry:
    def __init__(self):
        self.entities: list[Entity] = []

    def parse(self, query: str) -> None:
        m     = re.search(r"<PRE_RESOLVED>(.*?)</PRE_RESOLVED>", query, re.S | re.I)
        block = m.group(1) if m else query
        current: Optional[Entity] = None

        for raw in block.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            em = re.match(r"Entity\s+\d+:\s*(.+)", line, re.I)
            if em:
                current = Entity(em.group(1).strip())
                self.entities.append(current)
                continue

            if current is None:
                current = Entity("(primary)")
                self.entities.append(current)

            kv = re.match(r"([\w_]+)\s*=\s*(.+)", line)
            if not kv:
                continue
            key = kv.group(1).lower()
            val = re.sub(r"\s*\(.*?\)\s*$", "", kv.group(2)).strip()
            val = re.sub(r"\s*#.*$", "", val).strip()
            self._set(current, key, val)

        if not self.entities:
            e = Entity("(primary)")
            for pat, attr in [
                (r"mf_schcode[:=]\s*(\d+)", "mf_schcode"),
                (r"mf_cocode[:=]\s*(\d+)",  "mf_cocode"),
                (r"co_code[:=]\s*(\d+)",    "co_code"),
                (r"isin[:=]\s*([A-Z0-9]+)", "isin"),
                (r"index_code[:=]\s*(\d+)", "index_code"),
            ]:
                match = re.search(pat, block, re.I)
                if match:
                    self._set(e, attr, match.group(1))
            if e.has_code():
                self.entities.append(e)

        for e in self.entities:
            console.print(f"  [Registry] {e.label} ({e.entity_type}) codes={self._code_summary(e)}")

    @staticmethod
    def _code_summary(e: Entity) -> str:
        parts = []
        for p in ("co_code", "mf_schcode", "mf_cocode", "isin", "index_code"):
            v = e.code(p)
            if v is not None:
                parts.append(f"{p}={v}")
        return ", ".join(parts) or "none"

    @staticmethod
    def _set(e: Entity, key: str, val: str) -> None:
        try:
            if key in ("co_code", "mf_schcode", "mf_cocode", "index_code"):
                setattr(e, key, int(val))
            elif key == "isin":
                e.isin = val
            elif key == "entity_type":
                e.entity_type = val
            elif key == "query_intent":
                e.query_intent = val
            elif key == "recommended_tool":
                e.recommended_tool = val
            elif key == "company_name":
                e.company_name = val
            elif key == "amc_name":
                e.amc_name = val
            elif key == "scheme_name":
                e.scheme_name = val
        except (ValueError, TypeError):
            pass

    def find(self, tool_name: str, required_db: set[str], label_hint: Optional[str] = None) -> Optional[Entity]:
        if label_hint:
            for e in self.entities:
                if e.label.lower() == label_hint.lower():
                    return e
        for e in self.entities:
            if e.recommended_tool == tool_name:
                return e
        for param in required_db:
            family = _PARAM_TO_FAMILY.get(param)
            for e in self.entities:
                if e.entity_type == family and e.code(param) is not None:
                    return e
        for param in required_db:
            for e in self.entities:
                if e.code(param) is not None:
                    return e
        return self.entities[0] if self.entities else None

    def update_from_resolver(self, tool_name: str, result: str) -> None:
        if "RESOLVED" not in result:
            return
        mapping = {
            "resolve_mf_scheme":   ("mf_schcode", r"mf_schcode\s*[:=]\s*(\d+)"),
            "resolve_mf_fund":     ("mf_cocode",  r"mf_cocode\s*[:=]\s*(\d+)"),
            "resolve_nse_symbol":  ("co_code",    r"co_code\s*[:=]\s*(\d+)"),
            "get_company_details": ("co_code",    r"co_code\s*[:=]\s*(\d+)"),
        }
        entry = mapping.get(tool_name)
        if not entry:
            return
        attr, pat = entry
        m = re.search(pat, result, re.I)
        if not m:
            return
        val = int(m.group(1))
        for e in self.entities:
            if e.code(attr) is None:
                setattr(e, attr, val)
                console.print(f"  [Registry] updated {e.label}: {attr}={val}")
                break


# ══════════════════════════════════════════════════════════════════════════════
# Semantic tool selection  (unchanged from v6.0)
# ══════════════════════════════════════════════════════════════════════════════

_CORE_TOOLS = {
    "resolve_mf_scheme", "resolve_mf_fund",
    "resolve_nse_symbol", "search_companies",
}


def get_semantic_tools(query: str, all_tools: list[dict], top_k: int = 8) -> list[dict]:
    if tool_collection is None:
        return all_tools
    try:
        results       = tool_collection.query(query_texts=[query], n_results=top_k)
        matched_names = set(results["ids"][0]) | _CORE_TOOLS
        selected      = [t for t in all_tools if t["function"]["name"] in matched_names]
        console.print(
            f"  [Semantic] {len(selected)} tools selected from {len(all_tools)} "
            f"(top_k={top_k})"
        )
        return selected
    except Exception as e:
        console.print(f"  [yellow]ChromaDB query error: {e} — using all tools[/yellow]")
        return all_tools


# ══════════════════════════════════════════════════════════════════════════════
# Tool schema builder  (unchanged from v6.0)
# ══════════════════════════════════════════════════════════════════════════════

def _to_openai_tool(tool) -> dict:
    schema   = tool.inputSchema or {}
    props    = dict(schema.get("properties", {}))
    required = list(schema.get("required", []))

    if "params" in props and len(props) == 1:
        inner    = props["params"]
        props    = dict(inner.get("properties", {}))
        required = list(inner.get("required", []))

    props = {k: dict(v) for k, v in props.items()}
    for k, v in props.items():
        if "description" not in v:
            v["description"] = f"Parameter: {k}"
        if k in _NUMERIC_PARAMS:
            v["type"] = "integer"

    return {
        "type": "function",
        "function": {
            "name":        tool.name,
            "description": (tool.description or tool.name).strip(),
            "parameters":  {"type": "object", "properties": props, "required": required},
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# Code injection  (unchanged from v6.0)
# ══════════════════════════════════════════════════════════════════════════════

async def _inject_codes(
    tool_name:   str,
    tool_args:   dict,
    required_db: set[str],
    registry:    EntityRegistry,
    session:     ClientSession,
    messages:    list,
    entity_hint: str,
    backend:     str,
) -> dict:
    if not required_db:
        return tool_args

    label_hint = tool_args.pop("entity_label", None)
    entity     = registry.find(tool_name, required_db, label_hint)

    for param in required_db:
        if param in tool_args and tool_args[param] is not None:
            try:
                if param in _NUMERIC_PARAMS:
                    tool_args[param] = int(float(str(tool_args[param])))
            except (ValueError, TypeError):
                tool_args.pop(param)
            continue

        value = entity.code(param) if entity else None

        if value is None:
            resolver = _RESOLVER_FOR_PARAM.get(param)
            hint     = _clean(entity.name_hint() if entity else entity_hint)
            if resolver and hint:
                console.print(f"  [yellow]⚡ Auto-resolving {param} for '{hint[:60]}'[/yellow]")
                try:
                    res = await session.call_tool(resolver, {"query": hint})
                    txt = res.content[0].text if res.content else ""
                    if "RESOLVED" in txt:
                        registry.update_from_resolver(resolver, txt)
                        value = (entity.code(param) if entity else None)
                        if value is None and registry.entities:
                            value = registry.entities[0].code(param)

                        auto_id = f"auto-{param}-{value}"
                        messages.append(_assistant_msg_with_tools("", [{
                            "id":   auto_id,
                            "type": "function",
                            "function": {"name": resolver, "arguments": {"query": hint}},
                        }]))
                        messages.append(_tool_result_msg(auto_id, txt, backend))
                        console.print(f"    [green]✔ {param}={value}[/green]")
                    else:
                        console.print(f"  [red]⚠ Could not resolve {param}[/red]")
                except Exception as e:
                    console.print(f"  [red]Auto-resolve failed ({param}): {e}[/red]")

        if value is not None:
            try:
                if param in _NUMERIC_PARAMS:
                    value = int(float(str(value)))
            except (ValueError, TypeError):
                pass
            tool_args[param] = value

    return tool_args


def _clean(text: str) -> str:
    text = re.sub(r"<PRE_RESOLVED>.*?</PRE_RESOLVED>", "", str(text), flags=re.S | re.I)
    text = re.sub(r"(?i)(User Query|Instruction|Context|RECOMMENDED TOOL):", "", text)
    return re.sub(r"\[.*?\]", "", text).strip()


# ══════════════════════════════════════════════════════════════════════════════
# System prompt builder  (unchanged from v6.0)
# ══════════════════════════════════════════════════════════════════════════════

def _system_prompt(registry: EntityRegistry) -> str:
    base = """\
You are a financial analyst assistant for Indian markets with tools covering
equities, mutual funds, ETFs, and indices.

RULES:
- Verified numeric IDs are provided in the query. Use them DIRECTLY without
  calling resolver tools (resolve_mf_scheme, resolve_nse_symbol, etc.) unless
  an ID is genuinely absent from the query.
- Call exactly ONE tool that answers the user's question. After getting the
  tool result, return the data as your final answer — do NOT call more tools
  unless the first tool explicitly says it needs a follow-up.
- Never fabricate data. If a tool returns nothing, say so.
- Return ALL numeric values from tools verbatim. Do NOT describe what a tool
  returns — return the actual data values.
- For tools that need no entity codes (e.g. get_top_gainers, get_open_ipos),
  call them directly with any required non-code params only.
"""
    if not registry.entities:
        return base

    block = "\n\nPRE-RESOLVED ENTITY CODES (use these directly):\n"
    for i, e in enumerate(registry.entities, 1):
        block += f"\nEntity {i}: {e.label} ({e.entity_type})\n"
        if e.query_intent:
            block += f"  data needed: {e.query_intent}\n"
        if e.recommended_tool:
            block += f"  recommended tool: {e.recommended_tool}\n"
        for attr in ("co_code", "mf_schcode", "mf_cocode", "isin", "index_code"):
            v = e.code(attr)
            if v is not None:
                block += f"  {attr}={v}\n"
    block += "\nUse these codes directly. Do not call resolver tools for them."
    return base + block


# ══════════════════════════════════════════════════════════════════════════════
# Meta-description guard  (unchanged from v6.0)
# ══════════════════════════════════════════════════════════════════════════════

def _is_meta(text: str) -> bool:
    if not text or not text.strip():
        return True
    lower = text.lower()
    if any(p in lower for p in [
        "the function call returns", "the output includes", "the result contains",
        "the data includes", "this tool returns", "returns the following",
        "includes market capitalization",
    ]):
        console.print("  [yellow]⚠ Meta-description detected — using raw results[/yellow]")
        return True
    nums = re.findall(r"\b\d[\d,\.]*\b", text)
    if len(nums) < 2:
        console.print(f"  [yellow]⚠ Only {len(nums)} numeric values — likely meta[/yellow]")
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# Single-intent agentic loop  (unchanged from v6.0)
# ══════════════════════════════════════════════════════════════════════════════

async def _run_single_intent(
    enriched_query: str,
    session:        ClientSession,
    backend:        str,
    raw_tools:      list,
    param_index:    ToolParamIndex,
    all_schemas:    list[dict],
) -> str:
    registry    = EntityRegistry()
    registry.parse(enriched_query)
    entity_hint = re.split(r"\[(PRE-RESOLVED|VERIFIED|SUGGESTED|RECOMMENDED)", enriched_query)[0].strip()

    active_tools = get_semantic_tools(enriched_query, all_schemas, top_k=6)

    messages: list[dict] = [
        {"role": "system", "content": _system_prompt(registry)},
        {"role": "user",   "content": enriched_query},
    ]

    tool_results: list[str] = []
    loop = asyncio.get_event_loop()

    for round_num in range(1, MAX_ROUNDS + 1):
        console.print(f"  [dim]── Round {round_num} ──[/dim]")

        try:
            response: _LLMResponse = await asyncio.wait_for(
                loop.run_in_executor(_LLM_EXECUTOR, _chat, messages, active_tools, backend),
                timeout=600,
            )
        except asyncio.TimeoutError:
            logger.error(f"LLM timed out at round {round_num}")
            break
        except Exception as e:
            logger.error(f"LLM error at round {round_num}: {e}")
            break

        if response.content:
            console.print(f"  [magenta]LLM:[/magenta] {response.content[:200]}")

        if not response.tool_calls:
            if response.content and not _is_meta(response.content):
                return response.content
            break

        messages.append(
            _assistant_msg_with_tools(
                response.content,
                [tc.raw for tc in response.tool_calls],
            )
        )

        for tc in response.tool_calls:
            args = dict(tc.arguments)

            for param in _NUMERIC_PARAMS:
                if param in args:
                    try:
                        args[param] = int(float(str(args[param])))
                    except (ValueError, TypeError):
                        args.pop(param, None)

            if tc.name not in _RESOLVER_TOOLS and param_index.needs_codes(tc.name):
                required_db = param_index.required_db(tc.name)
                args = await _inject_codes(
                    tc.name, args, required_db,
                    registry, session, messages, entity_hint, backend,
                )
            elif "entity_label" in args:
                args.pop("entity_label")

            console.print(
                "  [cyan]🛠  " + tc.name + "[/cyan]  args={"
                + ", ".join(f"{k}={v}" for k, v in args.items())
                + "}"
            )

            try:
                res = await asyncio.wait_for(session.call_tool(tc.name, args), timeout=120)
                txt = res.content[0].text if res.content else "Empty response"
                console.print(f"  [green]✔ {tc.name}[/green]: {len(txt)} chars")

                if tc.name in _RESOLVER_TOOLS:
                    registry.update_from_resolver(tc.name, txt)
                else:
                    tool_results.append(f"[{tc.name}]\n{txt}")

            except asyncio.TimeoutError:
                txt = f"Error: {tc.name} timed out."
                console.print(f"  [red]✗ {tc.name} timed out[/red]")
            except Exception as e:
                txt = f"Tool Error: {e}"
                console.print(f"  [red]✗ {tc.name}: {e}[/red]")

            messages.append(_tool_result_msg(tc.id, txt, backend))

    if tool_results:
        return "\n\n".join(tool_results)
    return "(No data returned)"


# ══════════════════════════════════════════════════════════════════════════════
# Intent query builder  (unchanged from v6.0)
# ══════════════════════════════════════════════════════════════════════════════

def _build_intent_query(intent: dict) -> str:
    codes       = intent.get("resolved_codes") or {}
    entity_type = intent.get("entity_type", "general")
    tool_hint   = intent.get("tool_hint", "")
    scheme_name = intent.get("scheme_name", "") or ""
    amc_name    = intent.get("amc_name", "") or ""
    intent_desc = intent.get("intent_description", "")
    entity      = intent.get("entity", "")

    lines: list[str] = []
    if entity_type and entity_type != "general":
        lines.append(f"entity_type={entity_type}")
    if intent_desc:
        lines.append(f"query_intent={intent_desc}")
    if tool_hint:
        lines.append(f"recommended_tool={tool_hint}")

    _COMMENTS = {
        "co_code":    "stock/equity tools only",
        "mf_schcode": "MF scheme tools only",
        "mf_cocode":  "AMC/fund-house tools only",
        "isin":       "ETF tools only",
        "index_code": "index tools only",
    }
    for param, val in codes.items():
        comment = f"  # {_COMMENTS[param]}" if param in _COMMENTS else ""
        lines.append(f"{param}={val}{comment}")

    if scheme_name:
        lines.append(f"scheme_name={scheme_name}")
    if amc_name:
        lines.append(f"amc_name={amc_name}")

    if entity:
        lines.insert(0, f"Entity 1: {entity}")

    pre_block = ("<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>\n") if lines else ""
    sub_query = intent_desc or entity

    enriched = f"{pre_block}User Query: {sub_query}"
    if tool_hint:
        enriched += f"\n\n[RECOMMENDED TOOL: {tool_hint}]"
    return enriched


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
    """Single-intent query — full agentic loop."""
    b             = (backend or LLM_BACKEND).lower()
    server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            raw_tools   = (await session.list_tools()).tools
            param_index = ToolParamIndex()
            param_index.build(raw_tools)
            all_schemas = [_to_openai_tool(t) for t in raw_tools]
            return await _run_single_intent(
                query, session, b, raw_tools, param_index, all_schemas
            )


async def run_mcp_query_multi(
    intents: list[dict], backend: Optional[str] = None
) -> list[dict]:
    """
    Multi-intent query — each intent runs its own agentic loop sequentially
    within a SINGLE shared MCP session (one server process, one DB connection).

    KEY CHANGE from v6.0:
    Each intent is now wrapped in asyncio.wait_for(..., timeout=INTENT_TIMEOUT_SECS).
    This means:
      - Intent 1 completes and its result is saved immediately.
      - If intent 2's Round-2 LLM call is slow and times out, only intent 2
        gets the timeout error — intent 1's result is completely unaffected.
      - The outer future.result(timeout=600) in graph.py becomes a safe
        ceiling, not the actual per-intent guard.
    """
    b             = (backend or LLM_BACKEND).lower()
    server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            raw_tools   = (await session.list_tools()).tools
            param_index = ToolParamIndex()
            param_index.build(raw_tools)
            all_schemas = [_to_openai_tool(t) for t in raw_tools]

            results = list(intents)  # shallow copy to avoid mutating input

            for idx, intent in enumerate(results):
                entity = intent.get("entity", f"intent_{idx}")
                console.print(Panel(
                    f"[bold blue]Intent {idx+1}/{len(results)}:[/bold blue] "
                    f"{entity} — {intent.get('intent_description', '')[:80]}",
                    title="MCP Intent",
                ))

                enriched_query = _build_intent_query(intent)

                try:
                    # ── THE FIX: per-intent asyncio timeout ──────────────────
                    # _run_single_intent is awaited with a hard wall-clock limit.
                    # If it exceeds INTENT_TIMEOUT_SECS, asyncio.TimeoutError is
                    # raised HERE — stored as mcp_result for this intent only.
                    # Previously completed intents are already in results[] and
                    # are completely unaffected.
                    mcp_result = await asyncio.wait_for(
                        _run_single_intent(
                            enriched_query, session, b,
                            raw_tools, param_index, all_schemas,
                        ),
                        timeout=INTENT_TIMEOUT_SECS,
                    )
                except asyncio.TimeoutError:
                    console.print(
                        f"  [red]⏱ Intent [{entity}] timed out after "
                        f"{INTENT_TIMEOUT_SECS}s[/red]"
                    )
                    mcp_result = (
                        f"Data fetch timed out for '{entity}' "
                        f"(>{INTENT_TIMEOUT_SECS}s). Other sub-questions answered normally."
                    )
                except Exception as exc:
                    console.print(f"  [red]Intent [{entity}] failed: {exc}[/red]")
                    mcp_result = f"Error fetching data for {entity}: {exc}"

                # Save immediately — this result is safe regardless of future intents
                results[idx] = {**intent, "mcp_result": mcp_result}
                console.print(
                    f"  [green]✔ Intent [{entity}][/green] "
                    f"→ {len(mcp_result)} chars result"
                )

    return results


# ── Trim helper  (unchanged from v6.0) ────────────────────────────────────────

MAX_CHARS_PER_INTENT = 4000
MAX_TOTAL_CHARS      = 12000


def trim_results(results: list[dict]) -> list[dict]:
    out = [dict(r) for r in results]
    for r in out:
        if len(r.get("mcp_result", "")) > MAX_CHARS_PER_INTENT:
            r["mcp_result"] = r["mcp_result"][:MAX_CHARS_PER_INTENT] + "\n...[trimmed]"
    while sum(len(r.get("mcp_result", "")) for r in out) > MAX_TOTAL_CHARS:
        longest = max(out, key=lambda r: len(r.get("mcp_result", "")))
        if len(longest.get("mcp_result", "")) <= 200:
            break
        longest["mcp_result"] = longest["mcp_result"][:-500] + "\n...[trimmed]"
    return out


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1:
        asyncio.run(run_mcp_query(" ".join(sys.argv[1:])))