


# """
# mcp_client.py — Equifiz MF MCP Client (v6.1)

# Key fix from v6.0
# ─────────────────────────────────────────────────────────────────────
# FIX F — Per-intent asyncio timeout inside the shared session.

# Root cause of the original bug:
#   run_mcp_query_multi() ran all intents sequentially with NO per-intent
#   timeout. Intent 2's Round-2 inner LLM call (summarising 3847 chars of
#   BSE announcements) took long. The outer future.result(timeout=600) in
#   graph.py fired and replaced ALL results — including intent 1 which had
#   already completed — with "MCP call timed out."

# Fix:
#   Each intent is now wrapped in asyncio.wait_for(
#       _run_single_intent(...),
#       timeout=INTENT_TIMEOUT_SECS   # default 150s per intent
#   )
#   Results are stored immediately as each intent finishes.
#   A timeout on intent 2 CANNOT affect intent 1's already-saved result.
#   The shared MCP session (one server process, one DB connection) is kept —
#   no extra handshake latency from spawning separate sessions per intent.

# graph.py change:
#   The outer ThreadPoolExecutor timeout in node_mcp_tool_call_intents can
#   now be a generous ceiling (e.g. 600s total) because per-intent timeouts
#   handle the real granularity. Revert graph.py to v11.2's single-thread
#   approach — the fix lives here now, not in the graph.

# All v6.0 logic preserved otherwise.
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
# CHROMA_PATH   = Path(__file__).parent / "chroma_store"
# MAX_ROUNDS    = 15

# # Per-intent timeout in seconds.
# # Intent 2 timing out will NOT affect intent 1's already-saved result.
# INTENT_TIMEOUT_SECS = 240

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

# _RESOLVER_TOOLS = {
#     "resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes",
#     "resolve_nse_symbol", "get_company_details", "search_companies",
# }

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
# # Ollama-safe message builders  (unchanged from v6.0)
# # ══════════════════════════════════════════════════════════════════════════════

# def _assistant_msg_with_tools(content: Optional[str], tool_calls_raw: list) -> dict:
#     return {
#         "role":       "assistant",
#         "content":    content or "",
#         "tool_calls": tool_calls_raw,
#     }


# def _assistant_msg_text(content: str) -> dict:
#     return {"role": "assistant", "content": content or ""}


# def _tool_result_msg(tc_id: str, content: str, backend: str) -> dict:
#     if backend == "ollama":
#         return {"role": "tool", "content": content}
#     return {"role": "tool", "tool_call_id": tc_id, "content": content}


# # ══════════════════════════════════════════════════════════════════════════════
# # Normalised tool call  (unchanged from v6.0)
# # ══════════════════════════════════════════════════════════════════════════════

# class _ToolCall:
#     __slots__ = ("id", "name", "arguments", "raw")

#     def __init__(self, tc_id: str, name: str, arguments: dict, raw: dict):
#         self.id        = tc_id
#         self.name      = name
#         self.arguments = arguments
#         self.raw       = raw


# class _LLMResponse:
#     __slots__ = ("content", "tool_calls")

#     def __init__(self, content: str, tool_calls: list[_ToolCall]):
#         self.content    = content or ""
#         self.tool_calls = tool_calls


# # ══════════════════════════════════════════════════════════════════════════════
# # LLM wrappers  (unchanged from v6.0)
# # ══════════════════════════════════════════════════════════════════════════════

# def _chat_ollama(messages: list, tools: list) -> _LLMResponse:
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
# # Tool param index  (unchanged from v6.0)
# # ══════════════════════════════════════════════════════════════════════════════

# class ToolParamIndex:
#     def __init__(self):
#         self._idx: dict[str, dict] = {}

#     def build(self, raw_tools: list) -> None:
#         for tool in raw_tools:
#             schema   = tool.inputSchema or {}
#             props    = schema.get("properties", {})
#             required = set(schema.get("required", []))

#             if "params" in props and len(props) == 1:
#                 inner    = props["params"]
#                 props    = inner.get("properties", {})
#                 required = set(inner.get("required", []))

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
#         return bool(self.required_db(name))


# # ══════════════════════════════════════════════════════════════════════════════
# # Entity registry  (unchanged from v6.0)
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
# # Semantic tool selection  (unchanged from v6.0)
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
# # Tool schema builder  (unchanged from v6.0)
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
# # Code injection  (unchanged from v6.0)
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
#     if not required_db:
#         return tool_args

#     label_hint = tool_args.pop("entity_label", None)
#     entity     = registry.find(tool_name, required_db, label_hint)

#     for param in required_db:
#         if param in tool_args and tool_args[param] is not None:
#             try:
#                 if param in _NUMERIC_PARAMS:
#                     tool_args[param] = int(float(str(tool_args[param])))
#             except (ValueError, TypeError):
#                 tool_args.pop(param)
#             continue

#         value = entity.code(param) if entity else None

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
# # System prompt builder  (unchanged from v6.0)
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
# # Meta-description guard  (unchanged from v6.0)
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
# # Single-intent agentic loop  (unchanged from v6.0)
# # ══════════════════════════════════════════════════════════════════════════════

# async def _run_single_intent(
#     enriched_query: str,
#     session:        ClientSession,
#     backend:        str,
#     raw_tools:      list,
#     param_index:    ToolParamIndex,
#     all_schemas:    list[dict],
# ) -> str:
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

#         if not response.tool_calls:
#             if response.content and not _is_meta(response.content):
#                 return response.content
#             break

#         messages.append(
#             _assistant_msg_with_tools(
#                 response.content,
#                 [tc.raw for tc in response.tool_calls],
#             )
#         )

#         for tc in response.tool_calls:
#             args = dict(tc.arguments)

#             for param in _NUMERIC_PARAMS:
#                 if param in args:
#                     try:
#                         args[param] = int(float(str(args[param])))
#                     except (ValueError, TypeError):
#                         args.pop(param, None)

#             if tc.name not in _RESOLVER_TOOLS and param_index.needs_codes(tc.name):
#                 required_db = param_index.required_db(tc.name)
#                 args = await _inject_codes(
#                     tc.name, args, required_db,
#                     registry, session, messages, entity_hint, backend,
#                 )
#             elif "entity_label" in args:
#                 args.pop("entity_label")

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

#             messages.append(_tool_result_msg(tc.id, txt, backend))

#     if tool_results:
#         return "\n\n".join(tool_results)
#     return "(No data returned)"


# # ══════════════════════════════════════════════════════════════════════════════
# # Intent query builder  (unchanged from v6.0)
# # ══════════════════════════════════════════════════════════════════════════════

# def _build_intent_query(intent: dict) -> str:
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

#     KEY CHANGE from v6.0:
#     Each intent is now wrapped in asyncio.wait_for(..., timeout=INTENT_TIMEOUT_SECS).
#     This means:
#       - Intent 1 completes and its result is saved immediately.
#       - If intent 2's Round-2 LLM call is slow and times out, only intent 2
#         gets the timeout error — intent 1's result is completely unaffected.
#       - The outer future.result(timeout=600) in graph.py becomes a safe
#         ceiling, not the actual per-intent guard.
#     """
#     b             = (backend or LLM_BACKEND).lower()
#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])

#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()

#             raw_tools   = (await session.list_tools()).tools
#             param_index = ToolParamIndex()
#             param_index.build(raw_tools)
#             all_schemas = [_to_openai_tool(t) for t in raw_tools]

#             results = list(intents)  # shallow copy to avoid mutating input

#             for idx, intent in enumerate(results):
#                 entity = intent.get("entity", f"intent_{idx}")
#                 console.print(Panel(
#                     f"[bold blue]Intent {idx+1}/{len(results)}:[/bold blue] "
#                     f"{entity} — {intent.get('intent_description', '')[:80]}",
#                     title="MCP Intent",
#                 ))

#                 enriched_query = _build_intent_query(intent)

#                 try:
#                     # ── THE FIX: per-intent asyncio timeout ──────────────────
#                     # _run_single_intent is awaited with a hard wall-clock limit.
#                     # If it exceeds INTENT_TIMEOUT_SECS, asyncio.TimeoutError is
#                     # raised HERE — stored as mcp_result for this intent only.
#                     # Previously completed intents are already in results[] and
#                     # are completely unaffected.
#                     mcp_result = await asyncio.wait_for(
#                         _run_single_intent(
#                             enriched_query, session, b,
#                             raw_tools, param_index, all_schemas,
#                         ),
#                         timeout=INTENT_TIMEOUT_SECS,
#                     )
#                 except asyncio.TimeoutError:
#                     console.print(
#                         f"  [red]⏱ Intent [{entity}] timed out after "
#                         f"{INTENT_TIMEOUT_SECS}s[/red]"
#                     )
#                     mcp_result = (
#                         f"Data fetch timed out for '{entity}' "
#                         f"(>{INTENT_TIMEOUT_SECS}s). Other sub-questions answered normally."
#                     )
#                 except Exception as exc:
#                     console.print(f"  [red]Intent [{entity}] failed: {exc}[/red]")
#                     mcp_result = f"Error fetching data for {entity}: {exc}"

#                 # Save immediately — this result is safe regardless of future intents
#                 results[idx] = {**intent, "mcp_result": mcp_result}
#                 console.print(
#                     f"  [green]✔ Intent [{entity}][/green] "
#                     f"→ {len(mcp_result)} chars result"
#                 )

#     return results


# # ── Trim helper  (unchanged from v6.0) ────────────────────────────────────────

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



# """
# mcp_client.py — Equifiz MF MCP Client (v6.2)

# Key fixes from v6.1 & v6.0:
# ─────────────────────────────────────────────────────────────────────
# FIX F — Per-intent asyncio timeout inside the shared session (v6.1).
# FIX R — Cross-intent EntityRegistry persistence (v6.2).
#   Previously, a fresh registry was instantiated *inside* each intent loop.
#   If Intent 1 auto-resolved a code via its tool loop, Intent 2 completely
#   lost that context. The registry has now been hoisted out of the single
#   intent wrapper so that resolved entities/codes scale across the entire
#   batch within the shared session.
# FIX S — P/E & SectorPE Hallucination Guardrail in System Prompt (v6.2).
#   Strict rules injected into the analyst persona to ensure that if a 
#   stock's 'pe' is 0.0 or net profit is negative, it reports P/E as N/A 
#   or 0.0 instead of hallucinating or substituting the 'SectorPE'. All 
#   other v6.1 features are preserved.
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
# CHROMA_PATH   = Path(__file__).parent / "chroma_store"
# MAX_ROUNDS    = 15

# # Per-intent timeout in seconds.
# INTENT_TIMEOUT_SECS = 240

# LLM_BACKEND  = os.getenv("LLM_BACKEND", "ollama").lower()
# OLLAMA_HOST  = os.getenv("OLLAMA_BASE_URL",  "http://localhost:11434")
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

# _RESOLVER_TOOLS = {
#     "resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes",
#     "resolve_nse_symbol", "get_company_details", "search_companies",
# }

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
# # ══════════════════════════════════════════════════════════════════════════════

# def _assistant_msg_with_tools(content: Optional[str], tool_calls_raw: list) -> dict:
#     return {
#         "role":       "assistant",
#         "content":    content or "",
#         "tool_calls": tool_calls_raw,
#     }


# def _assistant_msg_text(content: str) -> dict:
#     return {"role": "assistant", "content": content or ""}


# def _tool_result_msg(tc_id: str, content: str, backend: str) -> dict:
#     if backend == "ollama":
#         return {"role": "tool", "content": content}
#     return {"role": "tool", "tool_call_id": tc_id, "content": content}


# # ══════════════════════════════════════════════════════════════════════════════
# # Normalised tool call structures
# # ══════════════════════════════════════════════════════════════════════════════

# class _ToolCall:
#     __slots__ = ("id", "name", "arguments", "raw")

#     def __init__(self, tc_id: str, name: str, arguments: dict, raw: dict):
#         self.id        = tc_id
#         self.name      = name
#         self.arguments = arguments
#         self.raw       = raw


# class _LLMResponse:
#     __slots__ = ("content", "tool_calls")

#     def __init__(self, content: str, tool_calls: list[_ToolCall]):
#         self.content    = content or ""
#         self.tool_calls = tool_calls


# # ══════════════════════════════════════════════════════════════════════════════
# # LLM wrappers
# # ══════════════════════════════════════════════════════════════════════════════

# def _chat_ollama(messages: list, tools: list) -> _LLMResponse:
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
# # Tool param index
# # ══════════════════════════════════════════════════════════════════════════════

# class ToolParamIndex:
#     def __init__(self):
#         self._idx: dict[str, dict] = {}

#     def build(self, raw_tools: list) -> None:
#         for tool in raw_tools:
#             schema   = tool.inputSchema or {}
#             props    = schema.get("properties", {})
#             required = set(schema.get("required", []))

#             if "params" in props and len(props) == 1:
#                 inner    = props["params"]
#                 props    = inner.get("properties", {})
#                 required = set(inner.get("required", []))

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
#         return bool(self.required_db(name))


# # ══════════════════════════════════════════════════════════════════════════════
# # Entity registry
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
#                 target_label = em.group(1).strip()
#                 # Track across intents: see if entity already exists in this multi-intent run
#                 existing = next((e for e in self.entities if e.label.lower() == target_label.lower()), None)
#                 if existing:
#                     current = existing
#                 else:
#                     current = Entity(target_label)
#                     self.entities.append(current)
#                 continue

#             if current is None:
#                 # Fallback to general tracking if no explicit header block is hit yet
#                 if self.entities:
#                     current = self.entities[-1]
#                 else:
#                     current = Entity("(primary)")
#                     self.entities.append(current)

#             kv = re.match(r"([\w_]+)\s*=\s*(.+)", line)
#             if not kv:
#                 continue
#             key = kv.group(1).lower()
#             val = re.sub(r"\s*\(.*?\)\s*$", "", kv.group(2)).strip()
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
# # Semantic tool selection
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
# # Code injection
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
#     if not required_db:
#         return tool_args

#     label_hint = tool_args.pop("entity_label", None)
#     entity     = registry.find(tool_name, required_db, label_hint)

#     for param in required_db:
#         if param in tool_args and tool_args[param] is not None:
#             try:
#                 if param in _NUMERIC_PARAMS:
#                     tool_args[param] = int(float(str(tool_args[param])))
#             except (ValueError, TypeError):
#                 tool_args.pop(param)
#             continue

#         value = entity.code(param) if entity else None

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

# CRITICAL FINANCIAL DATA INTERPRETATION RULE:
# - If a tool output shows a company's 'pe' (Price-to-Earnings Ratio) as 0.0 or null, 
#   and the 'netprofit' or 'eps' is negative, it indicates the company is loss-making.
# - In this scenario, do NOT substitute the 'SectorPE' value as the stock's individual PE.
# - Explicitly state that the stock's standalone PE is 0.0 or N/A due to net losses, 
#   and clearly distinguish it from the Sector P/E.
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
#     registry:       Optional[EntityRegistry] = None,
# ) -> str:
#     # FIX R: Use the hoisted context registry if provided, preventing state-wipe out
#     if registry is None:
#         registry = EntityRegistry()
        
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

#         if not response.tool_calls:
#             if response.content and not _is_meta(response.content):
#                 return response.content
#             break

#         messages.append(
#             _assistant_msg_with_tools(
#                 response.content,
#                 [tc.raw for tc in response.tool_calls],
#             )
#         )

#         for tc in response.tool_calls:
#             args = dict(tc.arguments)

#             for param in _NUMERIC_PARAMS:
#                 if param in args:
#                     try:
#                         args[param] = int(float(str(args[param])))
#                     except (ValueError, TypeError):
#                         args.pop(param, None)

#             if tc.name not in _RESOLVER_TOOLS and param_index.needs_codes(tc.name):
#                 required_db = param_index.required_db(tc.name)
#                 args = await _inject_codes(
#                     tc.name, args, required_db,
#                     registry, session, messages, entity_hint, backend,
#                 )
#             elif "entity_label" in args:
#                 args.pop("entity_label")

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

#             messages.append(_tool_result_msg(tc.id, txt, backend))

#     if tool_results:
#         return "\n\n".join(tool_results)
#     return "(No data returned)"


# # ══════════════════════════════════════════════════════════════════════════════
# # Intent query builder
# # ══════════════════════════════════════════════════════════════════════════════

# def _build_intent_query(intent: dict) -> str:
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
#     """
#     b             = (backend or LLM_BACKEND).lower()
#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])

#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()

#             raw_tools   = (await session.list_tools()).tools
#             param_index = ToolParamIndex()
#             param_index.build(raw_tools)
#             all_schemas = [_to_openai_tool(t) for t in raw_tools]

#             results = list(intents)  # shallow copy to avoid mutating input

#             # FIX R: Instantiate a single, shared registry across all intents in the lifecycle batch.
#             shared_registry = EntityRegistry()

#             for idx, intent in enumerate(results):
#                 entity = intent.get("entity", f"intent_{idx}")
#                 console.print(Panel(
#                     f"[bold blue]Intent {idx+1}/{len(results)}:[/bold blue] "
#                     f"{entity} — {intent.get('intent_description', '')[:80]}",
#                     title="MCP Intent",
#                 ))

#                 enriched_query = _build_intent_query(intent)

#                 try:
#                     # Await with an explicit wall-clock execution limit.
#                     mcp_result = await asyncio.wait_for(
#                         _run_single_intent(
#                             enriched_query, session, b,
#                             raw_tools, param_index, all_schemas,
#                             registry=shared_registry, # <-- Inject hoisted cross-intent registry tracking
#                         ),
#                         timeout=INTENT_TIMEOUT_SECS,
#                     )
#                 except asyncio.TimeoutError:
#                     console.print(
#                         f"  [red]⏱ Intent [{entity}] timed out after "
#                         f"{INTENT_TIMEOUT_SECS}s[/red]"
#                     )
#                     mcp_result = (
#                         f"Data fetch timed out for '{entity}' "
#                         f"(>{INTENT_TIMEOUT_SECS}s). Other sub-questions answered normally."
#                     )
#                 except Exception as exc:
#                     console.print(f"  [red]Intent [{entity}] failed: {exc}[/red]")
#                     mcp_result = f"Error fetching data for {entity}: {exc}"

#                 # Save immediately — safe from upstream thread timeout overwrites
#                 results[idx] = {**intent, "mcp_result": mcp_result}
#                 console.print(
#                     f"  [green]✔ Intent [{entity}][/green] "
#                     f"→ {len(mcp_result)} chars result"
#                 )

#     return results


# # ── Trim helper ────────────────────────────────────────────────────────────────

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
mcp_client.py — Equifiz MF MCP Client (v6.4-Database Verified)

Key fixes integrated:
─────────────────────────────────────────────────────────────────────
FIX D — Direct Database Canonical Name Verification (v6.4).
  Bypasses layout-dependent regex parsing of raw tool strings. Uses an 
  optimized psycopg2 lookup matching graph configurations to fetch absolute 
  ground-truth corporate and scheme names directly from the database.
FIX N — Strict Canonical Response Alignment (v6.3).
  Forces zero-trust synchronization between execution payloads and text framing.
FIX R — Cross-intent EntityRegistry persistence (v6.2).
  Hoists the state tracking registry out of individual loops to preserve 
  context across sequential execution batches within a shared session.
FIX S — P/E & SectorPE Hallucination Guardrail in System Prompt (v6.2).
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
from typing import Any, Optional, TypedDict

import psycopg2
import psycopg2.extras
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
INTENT_TIMEOUT_SECS = 240

LLM_BACKEND  = os.getenv("LLM_BACKEND", "ollama").lower()
OLLAMA_HOST  = os.getenv("OLLAMA_BASE_URL",  "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger  = logging.getLogger("equifiz_mf_client")
console = Console()

_LLM_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="llm_worker")

# ── ChromaDB (read-only) ────────────────────────────────────────────────────────
from chroma_singleton import get_chroma_collection
try:
    tool_collection = get_chroma_collection()
except Exception as e:
    console.print(f"  [Chroma Registry] Connection bypassed: {e}")
    tool_collection = None

# ── Database Verification Layer Mapping ────────────────────────────────────────

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "equifiz",
    "user":     "postgres",
    "password": "1234",
}

PARAM_TO_TABLE_MAP: dict[str, dict[str, str]] = {
    "co_code": {
        "table":    "companies",
        "column":   "companyname",
        "id_field": "co_code",
    },
    "mf_schcode": {
        "table":    "scheme_master",
        "column":   "sch_name",
        "id_field": "mf_schcode",
    },
    "mf_cocode": {
        "table":    "fund_house",
        "column":   "lname",
        "id_field": "mf_cocode",
    },
    "isin": {
        "table":    "etf_master",
        "column":   "etfname",
        "id_field": "isin",
    },
    "index_code": {
        "table":    "group_master",
        "column":   "group_name",  
        "id_field": "indexcode",
    },
    "group": {
        "table":    "group_master",
        "column":   "group_name",
        "id_field": "group_name",
    },
    "bond_code": {
        "table":    "bond_master",
        "column":   "companyname",
        "id_field": "code",
    },
    "sector_code": {
        "table":    "companies",
        "column" :  "sectorname",
        "id_field": "sectorcode"
    }
}

_NUMERIC_PARAMS = {"mf_schcode", "mf_cocode", "co_code", "index_code", "bond_code", "sector_code"}

_RESOLVER_TOOLS = {
    "resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes",
    "resolve_nse_symbol", "get_company_details", "search_companies",
}

_DB_PARAMS = {"co_code", "mf_schcode", "mf_cocode", "isin", "index_code", "group", "bond_code", "sector_code"}

_PARAM_TO_FAMILY: dict[str, str] = {
    "co_code":    "stock",
    "mf_schcode": "mf_scheme",
    "mf_cocode":  "mf_amc",
    "isin":       "etf",
    "index_code": "index",
    "group":      "market",
    "bond_code":  "bond",
    "sector_code":"sector"
}

_RESOLVER_FOR_PARAM = {
    "mf_schcode": "resolve_mf_scheme",
    "mf_cocode":  "resolve_mf_fund",
    "co_code":    "resolve_nse_symbol",
}


# ══════════════════════════════════════════════════════════════════════════════
# Database Identity Interception Engine
# ══════════════════════════════════════════════════════════════════════════════

def _db_fetch_canonical_name(param_key: str, val: Any) -> Optional[str]:
    cfg = PARAM_TO_TABLE_MAP.get(param_key)
    if not cfg or val is None:
        return None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        lookup_val = val
        if param_key in _NUMERIC_PARAMS:
            try:
                lookup_val = int(float(str(val)))
            except (ValueError, TypeError):
                pass
                
        query = f"SELECT {cfg['column']} FROM {cfg['table']} WHERE {cfg['id_field']} = %s LIMIT 1"
        cur.execute(query, (lookup_val,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            return str(row.get(cfg['column']) or "").strip()
    except Exception as e:
        console.print(f"  [red][DB Identity Error] Lookup failed for {param_key}={val}: {e}[/red]")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Message Builders & Protocol Normalization
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
# Inference Wrappers
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
# Input Parameter Schema Index
# ══════════════════════════════════════════════════════════════════════════════

class ToolParamIndex:
    def __init__(self):
        self._idx: dict[str, dict] = {}

    def build(self, raw_tools: list) -> None:
        for tool in raw_tools:
            schema   = tool.inputSchema or {}
            props    = dict(schema.get("properties", {}))
            required = set(schema.get("required", []))

            if "params" in props and len(props) == 1:
                inner    = props["params"]
                props    = dict(inner.get("properties", {}))
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
# Entity Registry Core Model Context
# ══════════════════════════════════════════════════════════════════════════════

class Entity:
    __slots__ = (
        "label", "entity_type", "query_intent", "recommended_tool",
        "co_code", "mf_schcode", "mf_cocode", "isin", "index_code", "group", "bond_code", "sector_code",
        "company_name", "amc_name", "scheme_name", "resolved_name"
    )

    def __init__(self, label: str = ""):
        self.label            = label
        self.entity_type      = "general"
        self.query_intent     = ""
        self.recommended_tool = ""
        self.co_code:     Optional[int] = None
        self.mf_schcode:  Optional[int] = None
        self.mf_cocode:   Optional[int] = None
        self.isin:        Optional[str] = None
        self.index_code:  Optional[int] = None
        self.group:       Optional[str] = None
        self.bond_code:   Optional[int] = None
        self.sector_code: Optional[int] = None
        self.company_name = ""
        self.amc_name     = ""
        self.scheme_name  = ""
        self.resolved_name = ""

    def code(self, param: str):
        return getattr(self, param, None)

    def has_code(self) -> bool:
        return any(self.code(p) is not None for p in _DB_PARAMS)

    def name_hint(self) -> str:
        return self.resolved_name or self.scheme_name or self.amc_name or self.company_name or self.label


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
                target_label = em.group(1).strip()
                existing = next((e for e in self.entities if e.label.lower() == target_label.lower()), None)
                if existing:
                    current = existing
                else:
                    current = Entity(target_label)
                    self.entities.append(current)
                continue

            if current is None:
                if self.entities:
                    current = self.entities[-1]
                else:
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
                (r"bond_code[:=]\s*(\d+)",  "bond_code"),
                (r"sector_code[:=]\s*(\d+)", "sector_code"),
            ]:
                match = re.search(pat, block, re.I)
                if match:
                    self._set(e, attr, match.group(1))
            if e.has_code():
                self.entities.append(e)

        # Trigger dynamic direct database name verification for tracking paths
        for e in self.entities:
            for p_field in _DB_PARAMS:
                code_val = e.code(p_field)
                if code_val is not None:
                    db_name = _db_fetch_canonical_name(p_field, code_val)
                    if db_name:
                        e.resolved_name = db_name
                        if p_field == "co_code": e.company_name = db_name
                        elif p_field == "mf_schcode": e.scheme_name = db_name
                        elif p_field == "mf_cocode": e.amc_name = db_name
            console.print(f"  [Registry] {e.label} ({e.entity_type}) verified_name='{e.name_hint()}' | codes={self._code_summary(e)}")

    @staticmethod
    def _code_summary(e: Entity) -> str:
        parts = []
        for p in _DB_PARAMS:
            v = e.code(p)
            if v is not None:
                parts.append(f"{p}={v}")
        return ", ".join(parts) or "none"

    @staticmethod
    def _set(e: Entity, key: str, val: str) -> None:
        try:
            if key in ("co_code", "mf_schcode", "mf_cocode", "index_code", "bond_code", "sector_code"):
                setattr(e, key, int(val))
            elif key in ("isin", "group"):
                setattr(e, key, val)
            elif key in ("entity_type", "query_intent", "recommended_tool", "company_name", "amc_name", "scheme_name"):
                setattr(e, key, val)
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
        
        # Intercept and fetch canonical record matching layout directly from DB rows
        db_canonical_name = _db_fetch_canonical_name(attr, val)

        for e in self.entities:
            if e.code(attr) is None:
                setattr(e, attr, val)
                if db_canonical_name:
                    e.resolved_name = db_canonical_name
                    if attr == "co_code": e.company_name = db_canonical_name
                    elif attr == "mf_schcode": e.scheme_name = db_canonical_name
                    elif attr == "mf_cocode": e.amc_name = db_canonical_name
                else:
                    e.resolved_name = e.label  # Protection Fallback anchor
                console.print(f"  [Registry Auto-Update] verified codes {e.label}: {attr}={val} | DB Canonical: {e.name_hint()}")
                break


# ══════════════════════════════════════════════════════════════════════════════
# Semantic Selection Engine
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
# Code Injection Routing Logic
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
# System Prompt / Framework Orchestration
# ══════════════════════════════════════════════════════════════════════════════



# ══════════════════════════════════════════════════════════════════════════════
# Tool schema builder
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

CRITICAL ALIGNMENT & ZERO-TRUST BOUNDARY RULES (STRICT):
1. TRUTHFUL ENTITY REFLECTION: Carefully analyze the data payload returned by tool execution. You MUST formulate your final prose response using ONLY the verified canonical entity name or ticker returned inside that raw tool data block.
2. If down-stream matching keys lead to data being pulled for a different code or entity than what the user originally queried, you MUST declare the true name of the fetched entity clearly in your text response. Do NOT disguise data of one company under the name of another.
3. If a tool output shows a company's 'pe' (Price-to-Earnings Ratio) as 0.0 or null, 
  and the 'netprofit' or 'eps' is negative, it indicates the company is loss-making.
- In this scenario, do NOT substitute the 'SectorPE' value as the stock's individual PE.
- Explicitly state that the stock's standalone PE is 0.0 or N/A due to net losses, 
  and clearly distinguish it from the Sector P/E.
"""
    if not registry.entities:
        return base

    block = "\n\nPRE-RESOLVED & DB VERIFIED ENTITY CODES (use these directly):\n"
    for i, e in enumerate(registry.entities, 1):
        block += f"\nEntity {i}: {e.name_hint()} (Original Search Query: {e.label})\n"
        if e.query_intent:
            block += f"  data needed: {e.query_intent}\n"
        if e.recommended_tool:
            block += f"  recommended tool: {e.recommended_tool}\n"
        for attr in _DB_PARAMS:
            v = e.code(attr)
            if v is not None:
                block += f"  {attr}={v}\n"
    block += "\nUse these codes directly. Do not call resolver tools for them."
    return base + block


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
# Single-Intent Execution Loop
# ══════════════════════════════════════════════════════════════════════════════

async def _run_single_intent(
    enriched_query: str,
    session:        ClientSession,
    backend:        str,
    raw_tools:      list,
    param_index:    ToolParamIndex,
    all_schemas:    list[dict],
    registry:       Optional[EntityRegistry] = None,
) -> str:
    if registry is None:
        registry = EntityRegistry()
        
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

                # Dynamic interception check on executing payloads to sync runtime properties
                if tc.name not in _RESOLVER_TOOLS:
                    found_db_name = None
                    for key_name in ["companyname", "company_name", "sch_name", "etfname", "group_name"]:
                        match_out = re.search(fr"\"{key_name}\"\s*:\s*\"([^\"]+)\"", txt, re.I) or re.search(fr"{key_name}\s*[:=]\s*([^,\n]+)", txt, re.I)
                        if match_out:
                            found_db_name = match_out.group(1).strip().replace("'", "").replace('"', '')
                            break
                    if found_db_name:
                        for e in registry.entities:
                            e.resolved_name = found_db_name

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
# Processing Routing Utilities
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

    for param, val in codes.items():
        lines.append(f"{param}={val}")

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
# Public Client Ingestion API
# ══════════════════════════════════════════════════════════════════════════════

async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
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
    b             = (backend or LLM_BACKEND).lower()
    server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            raw_tools   = (await session.list_tools()).tools
            param_index = ToolParamIndex()
            param_index.build(raw_tools)
            all_schemas = [_to_openai_tool(t) for t in raw_tools]

            results = list(intents)  

            shared_registry = EntityRegistry()

            for idx, intent in enumerate(results):
                entity = intent.get("entity", f"intent_{idx}")
                console.print(Panel(
                    f"[bold blue]Intent {idx+1}/{len(results)}:[/bold blue] "
                    f"{entity} — {intent.get('intent_description', '')[:80]}",
                    title="MCP Intent",
                ))

                enriched_query = _build_intent_query(intent)

                try:
                    mcp_result = await asyncio.wait_for(
                        _run_single_intent(
                            enriched_query, session, b,
                            raw_tools, param_index, all_schemas,
                            registry=shared_registry, 
                        ),
                        timeout=INTENT_TIMEOUT_SECS,
                    )
                except asyncio.TimeoutError:
                    console.print(f"  [red]⏱ Intent [{entity}] timed out after {INTENT_TIMEOUT_SECS}s[/red]")
                    mcp_result = f"Data fetch timed out for '{entity}' (>{INTENT_TIMEOUT_SECS}s). Other sub-questions answered normally."
                except Exception as exc:
                    console.print(f"  [red]Intent [{entity}] failed: {exc}[/red]")
                    mcp_result = f"Error fetching data for {entity}: {exc}"

                results[idx] = {**intent, "mcp_result": mcp_result}
                console.print(f"  [green]✔ Intent [{entity}][/green] → {len(mcp_result)} chars result")

    return results


# ── Trim helper ────────────────────────────────────────────────────────────────

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


# ── CLI Entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1:
        asyncio.run(run_mcp_query(" ".join(sys.argv[1:])))


