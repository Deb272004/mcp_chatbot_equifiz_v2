# """
# mcp_client.py — Equifiz MF MCP Client (v2)

# Key changes from v1:
# ──────────────────────────────────────────────────────────────────────
# 1. RESOLVER GUARD — a middleware layer sits between the LLM tool-call
#    decision and actual tool execution. Before ANY data tool is executed,
#    the guard checks whether the required code (mf_schcode / mf_cocode /
#    co_code) is already known in the session. If not, it automatically
#    injects a resolver call BEFORE the data tool runs.
#    This eliminates the "LLM skips resolver" failure mode entirely.

# 2. PRE-INJECTED CODES — when called from graph.py's node_mcp_tool_call,
#    the query contains "[PRE-RESOLVED CODES]" lines. The client parses
#    these and pre-populates the session code map, so resolver tools are
#    never needed at all for graph-routed queries.

# 3. run_mcp_query() — new async function, the public API consumed by
#    graph.py's node_mcp_tool_call. Returns the final answer string.

# 4. SYSTEM PROMPT hardening — even stronger guardrails for standalone CLI use.
# """

# from __future__ import annotations

# import asyncio
# import json
# import logging
# import os
# import re
# import sys
# from pathlib import Path
# from typing import Optional

# from mcp import ClientSession, StdioServerParameters
# from mcp.client.stdio import stdio_client
# from rich.console import Console
# from rich.markdown import Markdown
# from rich.panel import Panel
# from rich.rule import Rule
# from dotenv import load_dotenv

# load_dotenv()

# # ── Config ─────────────────────────────────────────────────────────────────────

# SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
# MAX_TOKENS    = 4096
# MAX_ROUNDS    = 15          # slightly higher to allow resolver → data tool in same session

# LLM_BACKEND  = os.getenv("LLM_BACKEND", "groq").lower()
# GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")
# OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
# OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")

# logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
# logger = logging.getLogger("equifiz_mf_client")
# console = Console()

# # ── Tool metadata: which tools need which code ─────────────────────────────────

# # Maps tool_name → the parameter name that holds the required code
# _TOOL_NEEDS_MF_SCHCODE: dict[str, str] = {
#     "get_scheme_nav":        "mf_schcode",
#     "get_investment_details":"mf_schcode",
#     "get_expense_ratio":     "mf_schcode",
#     "get_avg_maturity":      "mf_schcode",
#     "get_scheme_aum":        "mf_schcode",
#     "get_nav_historical":    "mf_schcode",
#     "get_scheme_returns":    "mf_schcode",
#     "get_lumpsum_returns":   "mf_schcode",
#     "get_scheme_sip_details":"mf_schcode",
#     "get_mf_holdings":       "mf_schcode",
#     "get_sector_allocation": "mf_schcode",
#     "get_asset_allocation":  "mf_schcode",
#     "get_portfolio_changes": "mf_schcode",
#     "get_mcap_allocation":   "mf_schcode",
#     "get_most_bought_sold":  "mf_schcode",
#     "get_scheme_ratios":     "mf_schcode",
#     "get_dividend_details":  "mf_schcode",
#     "get_bse_star_scheme":   "mf_schcode",
# }

# _TOOL_NEEDS_MF_COCODE: dict[str, str] = {
#     "get_fund_categories": "mf_cocode",
#     "get_schemes_by_amc":  "mf_cocode",
#     "get_fund_profile":    "mf_cocode",
# }

# _TOOL_NEEDS_CO_CODE: dict[str, str] = {
#     "get_funds_holding_company": "co_code",
# }

# # compare_schemes needs a comma-separated list of mf_schcodes
# _TOOL_NEEDS_SCHCODE_LIST = {"compare_schemes"}

# # Resolver tool names
# _RESOLVER_TOOLS = {"resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes"}


# # ── Session code registry ─────────────────────────────────────────────────────

# class _CodeRegistry:
#     """
#     Tracks resolved codes within a single query session.
#     Populated either from:
#       a) Pre-injected codes in the query string (graph path), or
#       b) Resolver tool results (standalone CLI path).
#     """

#     def __init__(self):
#         self.mf_schcode: Optional[int]  = None
#         self.mf_cocode:  Optional[int]  = None
#         self.co_code:    Optional[int]  = None
#         self.schcode_list: list[int]    = []   # for compare_schemes
#         self.scheme_name: Optional[str] = None
#         self.amc_name:    Optional[str] = None

#     def parse_injected(self, query: str) -> None:
#         """
#         Extract pre-resolved codes from the enriched query string
#         produced by graph.py's node_mcp_tool_call.

#         Looks for lines like:
#           mf_schcode=12345 (scheme: HDFC Flexi Cap Fund - Direct (Growth))
#           mf_cocode=67 (AMC: HDFC Mutual Fund)
#           co_code=891
#         """
#         for line in query.splitlines():
#             line = line.strip()
#             m = re.match(r"mf_schcode\s*=\s*(\d+)", line)
#             if m:
#                 self.mf_schcode = int(m.group(1))
#                 name_m = re.search(r"\(scheme:\s*(.+?)\)", line)
#                 if name_m:
#                     self.scheme_name = name_m.group(1).strip()
#                 logger.info(f"  [CodeRegistry] mf_schcode={self.mf_schcode} (pre-injected)")
#                 continue
#             m = re.match(r"mf_cocode\s*=\s*(\d+)", line)
#             if m:
#                 self.mf_cocode = int(m.group(1))
#                 name_m = re.search(r"\(AMC:\s*(.+?)\)", line)
#                 if name_m:
#                     self.amc_name = name_m.group(1).strip()
#                 logger.info(f"  [CodeRegistry] mf_cocode={self.mf_cocode} (pre-injected)")
#                 continue
#             m = re.match(r"co_code\s*=\s*(\d+)", line)
#             if m:
#                 self.co_code = int(m.group(1))
#                 logger.info(f"  [CodeRegistry] co_code={self.co_code} (pre-injected)")

#     def update_from_resolver_result(self, tool_name: str, result_text: str) -> None:
#         """
#         Parse resolver tool output and store resolved codes.
#         Expected format (from mf_equifiz_server.py):
#           RESOLVED
#           Scheme    : HDFC Flexi Cap Fund ...
#           mf_schcode: 12345
#         or:
#           RESOLVED
#           AMC Name  : HDFC Mutual Fund
#           mf_cocode : 67
#         """
#         if "RESOLVED" not in result_text:
#             return

#         if tool_name == "resolve_mf_scheme":
#             m = re.search(r"mf_schcode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE)
#             if m:
#                 self.mf_schcode = int(m.group(1))
#                 logger.info(f"  [CodeRegistry] mf_schcode={self.mf_schcode} (from resolver)")
#             m2 = re.search(r"Scheme\s*:\s*(.+)", result_text)
#             if m2:
#                 self.scheme_name = m2.group(1).strip()
#             # Also capture mf_cocode if present
#             m3 = re.search(r"mf_cocode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE)
#             if m3 and not self.mf_cocode:
#                 self.mf_cocode = int(m3.group(1))

#         elif tool_name == "resolve_mf_fund":
#             m = re.search(r"mf_cocode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE)
#             if m:
#                 self.mf_cocode = int(m.group(1))
#                 logger.info(f"  [CodeRegistry] mf_cocode={self.mf_cocode} (from resolver)")
#             m2 = re.search(r"AMC Name\s*:\s*(.+)", result_text)
#             if m2:
#                 self.amc_name = m2.group(1).strip()

#     def get(self, code_type: str) -> Optional[int]:
#         return getattr(self, code_type, None)

#     def has_all_for_tool(self, tool_name: str) -> bool:
#         """Returns True if all required codes for this tool are already known."""
#         if tool_name in _TOOL_NEEDS_MF_SCHCODE:
#             return self.mf_schcode is not None
#         if tool_name in _TOOL_NEEDS_MF_COCODE:
#             return self.mf_cocode is not None
#         if tool_name in _TOOL_NEEDS_CO_CODE:
#             return self.co_code is not None
#         if tool_name in _TOOL_NEEDS_SCHCODE_LIST:
#             return len(self.schcode_list) >= 2
#         return True  # tool has no code requirement


# # ── Resolver guard ─────────────────────────────────────────────────────────────

# async def _ensure_codes_for_tool(
#     tool_name: str,
#     tool_args: dict,
#     registry: _CodeRegistry,
#     session: ClientSession,
#     messages: list[dict],
#     entity_hint: str,
# ) -> tuple[dict, list[dict]]:
#     """
#     RESOLVER GUARD — the heart of the fix.

#     Before executing `tool_name`, check whether the required code is
#     already in `registry`. If not, automatically call the appropriate
#     resolver tool and update the registry.

#     Returns:
#       - updated tool_args with the resolved code injected
#       - updated messages list (resolver tool call + result appended)
#     """
#     updated_args = dict(tool_args)

#     # ── mf_schcode guard ──────────────────────────────────────────────────
#     if tool_name in _TOOL_NEEDS_MF_SCHCODE and registry.mf_schcode is None:
#         hint = (
#             tool_args.get("query")
#             or registry.scheme_name
#             or entity_hint
#         )
#         console.print(f"  [yellow]⚡ Auto-resolving mf_schcode for '{hint}'[/yellow]")
#         resolve_args = {"query": hint}
#         try:
#             result = await session.call_tool("resolve_mf_scheme", resolve_args)
#             result_text = result.content[0].text if result.content else ""
#             registry.update_from_resolver_result("resolve_mf_scheme", result_text)
#             # Append resolver call to message history so LLM has context
#             messages.append({
#                 "role": "assistant",
#                 "content": None,
#                 "tool_calls": [{
#                     "id": "auto-resolve-schcode",
#                     "type": "function",
#                     "function": {"name": "resolve_mf_scheme", "arguments": json.dumps(resolve_args)},
#                 }],
#             })
#             messages.append({
#                 "role": "tool",
#                 "tool_call_id": "auto-resolve-schcode",
#                 "content": result_text,
#             })
#             console.print(
#                 f"  [green]✓ mf_schcode resolved: {registry.mf_schcode}[/green]"
#                 if registry.mf_schcode
#                 else "  [red]✗ Could not resolve mf_schcode[/red]"
#             )
#         except Exception as e:
#             console.print(f"  [red]✗ Auto-resolve failed: {e}[/red]")

#     # ── Inject mf_schcode into tool args ──────────────────────────────────
#     if tool_name in _TOOL_NEEDS_MF_SCHCODE and registry.mf_schcode is not None:
#         updated_args["mf_schcode"] = registry.mf_schcode

#     # ── mf_cocode guard ───────────────────────────────────────────────────
#     if tool_name in _TOOL_NEEDS_MF_COCODE and registry.mf_cocode is None:
#         hint = (
#             tool_args.get("query")
#             or registry.amc_name
#             or entity_hint
#         )
#         console.print(f"  [yellow]⚡ Auto-resolving mf_cocode for '{hint}'[/yellow]")
#         resolve_args = {"query": hint}
#         try:
#             result = await session.call_tool("resolve_mf_fund", resolve_args)
#             result_text = result.content[0].text if result.content else ""
#             registry.update_from_resolver_result("resolve_mf_fund", result_text)
#             messages.append({
#                 "role": "assistant",
#                 "content": None,
#                 "tool_calls": [{
#                     "id": "auto-resolve-cocode",
#                     "type": "function",
#                     "function": {"name": "resolve_mf_fund", "arguments": json.dumps(resolve_args)},
#                 }],
#             })
#             messages.append({
#                 "role": "tool",
#                 "tool_call_id": "auto-resolve-cocode",
#                 "content": result_text,
#             })
#         except Exception as e:
#             console.print(f"  [red]✗ Auto-resolve mf_cocode failed: {e}[/red]")

#     if tool_name in _TOOL_NEEDS_MF_COCODE and registry.mf_cocode is not None:
#         updated_args["mf_cocode"] = registry.mf_cocode

#     # ── co_code guard (passed through from graph) ─────────────────────────
#     if tool_name in _TOOL_NEEDS_CO_CODE and registry.co_code is not None:
#         updated_args["co_code"] = registry.co_code

#     # ── compare_schemes: build comma-separated schcode list ───────────────
#     if tool_name in _TOOL_NEEDS_SCHCODE_LIST:
#         raw = updated_args.get("mf_schcodes", "")
#         # If the LLM passed something, validate; otherwise use registry
#         if not raw and registry.schcode_list:
#             updated_args["mf_schcodes"] = ",".join(str(c) for c in registry.schcode_list)

#     return updated_args, messages


# # ── System prompt ──────────────────────────────────────────────────────────────

# SYSTEM_PROMPT = """\
# You are a mutual fund assistant for Indian markets.

# MANDATORY WORKFLOW — follow these rules without exception:

# RULE 1 — RESOLVER TOOLS MUST COME FIRST:
#   Before calling get_scheme_nav, get_investment_details, get_expense_ratio,
#   get_avg_maturity, get_scheme_returns, get_mf_holdings, get_sector_allocation,
#   get_asset_allocation, get_scheme_ratios, get_dividend_details,
#   get_lumpsum_returns, get_nav_historical, get_scheme_sip_details,
#   get_mcap_allocation, get_most_bought_sold, get_bse_star_scheme,
#   get_portfolio_changes, or compare_schemes —
#   you MUST first call resolve_mf_scheme to get mf_schcode.

# RULE 2 — FOR AMC TOOLS:
#   Before calling get_fund_categories, get_schemes_by_amc, get_fund_profile —
#   you MUST first call resolve_mf_fund to get mf_cocode.

# RULE 3 — USE ONLY RESOLVED CODES:
#   NEVER invent or guess mf_schcode, mf_cocode, or co_code values.
#   Only use values returned by resolver tools.

# RULE 4 — IF CODES ARE PRE-INJECTED:
#   When the query contains [PRE-RESOLVED CODES], those codes are already
#   verified. Call data tools directly with those codes — do NOT call
#   resolver tools again.

# RULE 5 — ANSWER ONLY FROM TOOL DATA:
#   Do not fabricate data. If a tool returns "NOT FOUND", say so clearly.
# """


# # ── Tool schema helper ─────────────────────────────────────────────────────────

# def _to_openai_tool(tool) -> dict:
#     schema   = tool.inputSchema or {}
#     props    = schema.get("properties", {})
#     required = schema.get("required", [])
    
#     # If the schema is nested under 'params', flatten it
#     if list(props.keys()) == ["params"]:
#         inner    = props["params"]
#         props    = inner.get("properties", {})
#         required = inner.get("required", [])

#     # --- OLLAMA SANITIZATION FIX ---
#     # 1. Ensure every property has a description (Ollama can be picky here)
#     for k, v in props.items():
#         if "description" not in v or not v["description"]:
#             v["description"] = f"Parameter {k}"
            
#     # 2. Ollama 400 fix: Only include 'required' if there actually ARE required fields
#     sanitized_parameters = {
#         "type": "object",
#         "properties": props
#     }
#     if required:
#         sanitized_parameters["required"] = required
#     # -------------------------------

#     return {
#         "type": "function",
#         "function": {
#             "name":        tool.name,
#             "description": (tool.description or f"Tool {tool.name}").strip(),
#             "parameters":  sanitized_parameters,
#         },
#     }


# # ── LLM backends ───────────────────────────────────────────────────────────────

# def _groq_chat(messages: list, tools: list):
#     from groq import Groq
#     client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
#     resp = client.chat.completions.create(
#         model=GROQ_MODEL, max_tokens=MAX_TOKENS,
#         tools=tools, tool_choice="auto", messages=messages,
#     )
#     return resp.choices[0]


# def _ollama_chat(messages: list, tools: list):
#     import urllib.request

#     raw = urllib.request.urlopen(
#         urllib.request.Request(
#             f"{OLLAMA_HOST}/api/chat",
#             data=json.dumps({
#                 "model": OLLAMA_MODEL, "messages": messages,
#                 "tools": tools, "stream": False,
#                 "options": {"num_predict": MAX_TOKENS, "temperature": 0.1},
#             }).encode(),
#             headers={"Content-Type": "application/json"},
#             method="POST",
#         ),
#         timeout=180,
#     ).read().decode()

#     data    = json.loads(raw)
#     omsg    = data.get("message", {})
#     tc_raw  = omsg.get("tool_calls") or []
#     content = omsg.get("content") or None

#     class _TC:
#         def __init__(self, d):
#             fn = d.get("function", {})
#             self.id = d.get("id", f"tc-{fn.get('name','')}")
#             args = fn.get("arguments", {})
#             self.function = type("F", (), {
#                 "name":      fn.get("name", ""),
#                 "arguments": json.dumps(args) if isinstance(args, dict) else (args or "{}"),
#             })()

#     class _Msg:
#         def __init__(self, c, tcs):
#             self.content    = c
#             self.tool_calls = [_TC(t) for t in tcs] if tcs else None

#     class _Choice:
#         def __init__(self, c, tcs, reason):
#             self.message       = _Msg(c, tcs)
#             self.finish_reason = reason

#     console.print(f"  [dim]↳ finish={data.get('done_reason','?')} tool_calls={len(tc_raw)}[/dim]")
#     return _Choice(content, tc_raw, data.get("done_reason", "stop"))


# def _chat(messages, tools, backend):
#     return _ollama_chat(messages, tools) if backend == "ollama" else _groq_chat(messages, tools)


# # ── Inline tool-call fallback ──────────────────────────────────────────────────

# _TC_RE = re.compile(
#     r"<tool_call>\s*(\{.*?\})\s*</tool_call>"
#     r"|```(?:json)?\s*(\{[^`]*\"name\"\s*:[^`]*\})\s*```",
#     re.DOTALL,
# )

# def _extract_inline_tool_calls(content: str) -> list[dict]:
#     results = []
#     for m in _TC_RE.finditer(content):
#         raw = m.group(1) or m.group(2)
#         try:
#             obj = json.loads(raw)
#             if "name" in obj:
#                 args = obj.get("arguments") or obj.get("parameters") or {}
#                 results.append({"name": obj["name"], "arguments": args})
#         except json.JSONDecodeError:
#             pass
#     return results


# # ── Extract entity hint from query ────────────────────────────────────────────

# def _extract_entity_hint(query: str) -> str:
#     """
#     Pull the most useful search hint from the query for auto-resolver calls.
#     Strips the PRE-RESOLVED CODES block if present, returns the user-facing
#     part of the query.
#     """
#     # Strip injected code block
#     clean = re.split(r"\[PRE-RESOLVED CODES", query)[0].strip()
#     # Strip [SUGGESTED TOOL: ...] suffix
#     clean = re.split(r"\[SUGGESTED TOOL:", clean)[0].strip()
#     return clean


# # # ── Core query loop ────────────────────────────────────────────────────────────

# # async def _run_query_with_session(
# #     query: str,
# #     session: ClientSession,
# #     backend: str,
# # ) -> str:
# #     """
# #     Core agentic loop with resolver guard.
# #     v5 Optimized: Thread-safe LLM calls and strict argument validation 
# #     to prevent TaskGroup crashes.
# #     """
# #     registry = _CodeRegistry()

# #     # 1. Parse any pre-injected codes (from graph.py's node_mcp_tool_call)
# #     if "[PRE-RESOLVED CODES" in query:
# #         registry.parse_injected(query)

# #     entity_hint = _extract_entity_hint(query)

# #     # 2. Setup tools and initial message state
# #     tools = [_to_openai_tool(t) for t in (await session.list_tools()).tools]
# #     messages = [
# #         {"role": "system", "content": SYSTEM_PROMPT},
# #         {"role": "user",   "content": query},
# #     ]

# #     for round_num in range(1, MAX_ROUNDS + 1):
# #         console.print(f"  [dim]── Round {round_num} ──[/dim]")

# #         # 3. THREAD-SAFE LLM CALL
# #         # Offload blocking network I/O to a thread to keep the asyncio loop alive
# #         loop = asyncio.get_event_loop()
# #         try:
# #             choice = await loop.run_in_executor(
# #                 None, _chat, messages, tools, backend
# #             )
# #         except Exception as e:
# #             logger.error(f"LLM call failed at round {round_num}: {e}")
# #             return f"(LLM error: {e})"
        
# #         msg = choice.message

# #         # 4. TOOL CALL EXTRACTION & FALLBACK
# #         tc_list = list(msg.tool_calls or [])
# #         if not tc_list and msg.content and backend == "ollama":
# #             inline = _extract_inline_tool_calls(msg.content)
# #             if inline:
# #                 console.print("  [yellow]⚠ Inline tool call extracted[/yellow]")
# #                 # Normalize inline calls to match the expected object structure
# #                 class _STC:
# #                     def __init__(self, name, args):
# #                         self.id = f"syn-{name}-{round_num}"
# #                         self.function = type("F", (), {
# #                             "name": name,
# #                             "arguments": json.dumps(args) if isinstance(args, dict) else args,
# #                         })()
# #                 tc_list = [_STC(e["name"], e["arguments"]) for e in inline]

# #         # 5. EARLY EXIT (No tool calls = Final Answer)
# #         if not tc_list:
# #             return msg.content or "(Empty response)"

# #         # 6. UPDATE HISTORY (Assistant Turn)
# #         # Ensure arguments are stored correctly for the conversation state
# #         messages.append({
# #             "role": "assistant",
# #             "content": msg.content or None,
# #             "tool_calls": [
# #                 {
# #                     "id": tc.id, 
# #                     "type": "function",
# #                     "function": {
# #                         "name": tc.function.name,
# #                         "arguments": (
# #                             tc.function.arguments 
# #                             if isinstance(tc.function.arguments, str) 
# #                             else json.dumps(tc.function.arguments)
# #                         ),
# #                     },
# #                 }
# #                 for tc in tc_list
# #             ],
# #         })

# #         # 7. EXECUTE TOOL CALLS
# #         for tc in tc_list:
# #             fn_name = tc.function.name
            
# #             # --- CRITICAL FIX: Ensure fn_args is a DICT before calling tool ---
# #             fn_args = tc.function.arguments
# #             if isinstance(fn_args, str):
# #                 try:
# #                     fn_args = json.loads(fn_args)
# #                 except json.JSONDecodeError:
# #                     console.print(f"  [red]✗ Failed to parse JSON args for {fn_name}[/red]")
# #                     fn_args = {} # Fallback to empty to avoid crashing TaskGroup

# #             # 8. RESOLVER GUARD
# #             # Inject mf_schcode/mf_cocode if missing
# #             if fn_name not in _RESOLVER_TOOLS:
# #                 fn_args, messages = await _ensure_codes_for_tool(
# #                     fn_name, fn_args, registry, session, messages, entity_hint
# #                 )

# #             console.print(f"  [dim]🛠 [bold]{fn_name}[/bold]({fn_args})[/dim]")

# #             # 9. PHYSICAL TOOL EXECUTION
# #             try:
# #                 # Use the sanitized fn_args (dictionary)
# #                 result = await session.call_tool(fn_name, fn_args)
# #                 result_text = result.content[0].text if result.content else ""
# #                 console.print(f"  [green]✓ {len(result_text)} chars[/green]")

# #                 # Update local session registry if this was a resolver tool
# #                 if fn_name in _RESOLVER_TOOLS:
# #                     registry.update_from_resolver_result(fn_name, result_text)

# #             except Exception as e:
# #                 # Catching here prevents a sub-exception from killing the TaskGroup
# #                 result_text = f"Tool execution error: {str(e)}"
# #                 console.print(f"  [red]✗ {e}[/red]")

# #             # 10. UPDATE HISTORY (Tool Result)
# #             messages.append({
# #                 "role":         "tool",
# #                 "tool_call_id": tc.id,
# #                 "content":      result_text,
# #             })

# #     return "(Max rounds reached without a final answer)"

# # ── Core query loop ────────────────────────────────────────────────────────────

# async def _run_query_with_session(
#     query: str,
#     session: ClientSession,
#     backend: str,
# ) -> str:
#     """
#     v5 Optimized: Core agentic loop with resolver guard.
#     Solves Timeout, TaskGroup crashes, and Ollama Round 2 errors.
#     """
#     registry = _CodeRegistry()

#     # 1. Handle pre-injected codes from LangGraph
#     if "[PRE-RESOLVED CODES" in query:
#         registry.parse_injected(query)

#     entity_hint = _extract_entity_hint(query)

#     # 2. Setup initial state
#     tools = [_to_openai_tool(t) for t in (await session.list_tools()).tools]
#     messages = [
#         {"role": "system", "content": SYSTEM_PROMPT},
#         {"role": "user",   "content": query},
#     ]

#     for round_num in range(1, MAX_ROUNDS + 1):
#         console.print(f"  [dim]── Round {round_num} ──[/dim]")

#         # 3. NON-BLOCKING LLM EXECUTION
#         loop = asyncio.get_event_loop()
#         try:
#             # We use wait_for here to enforce a hard 10-minute limit on the model response
#             choice = await asyncio.wait_for(
#                 loop.run_in_executor(None, _chat, messages, tools, backend),
#                 timeout=600  # Change this to 600
#             )
#         except asyncio.TimeoutError:
#             logger.error(f"LLM call timed out after 10 minutes at round {round_num}")
#             return "Error: The model took too long to respond. Please try a simpler query."
#         except Exception as e:
#             logger.error(f"LLM call failed: {e}")
#             return f"(LLM error: {e})"
        
#         msg = choice.message

#         # 4. TOOL CALL EXTRACTION
#         tc_list = list(msg.tool_calls or [])
#         if not tc_list and msg.content and backend == "ollama":
#             inline = _extract_inline_tool_calls(msg.content)
#             if inline:
#                 console.print("  [yellow]⚠ Inline tool call extracted[/yellow]")
#                 class _STC:
#                     def __init__(self, name, args):
#                         self.id = f"syn-{name}-{round_num}"
#                         self.function = type("F", (), {
#                             "name": name,
#                             "arguments": json.dumps(args) if isinstance(args, dict) else args,
#                         })()
#                 tc_list = [_STC(e["name"], e["arguments"]) for e in inline]

#         # 5. FINAL ANSWER CHECK
#         if not tc_list:
#             return msg.content or "(Empty response)"

#         # 6. HISTORY UPDATE (Assistant Turn)
#         # Fix: Arguments MUST be stringified for Ollama Round 2 history
#         messages.append({
#             "role": "assistant",
#             "content": msg.content or None,
#             "tool_calls": [
#                 {
#                     "id": tc.id, "type": "function",
#                     "function": {
#                         "name": tc.function.name,
#                         "arguments": (
#                             tc.function.arguments 
#                             if isinstance(tc.function.arguments, str) 
#                             else json.dumps(tc.function.arguments)
#                         ),
#                     },
#                 }
#                 for tc in tc_list
#             ],
#         })

#         # 7. EXECUTION LOOP
#         for tc in tc_list:
#             fn_name = tc.function.name
            
#             # --- ARGUMENT HYGIENE: Convert string to dict to prevent TaskGroup crash ---
#             fn_args = tc.function.arguments
#             if isinstance(fn_args, str):
#                 try:
#                     fn_args = json.loads(fn_args)
#                 except json.JSONDecodeError:
#                     fn_args = {}

#             # 8. RESOLVER GUARD: Inject codes if LLM skipped them
#             if fn_name not in _RESOLVER_TOOLS:
#                 fn_args, messages = await _ensure_codes_for_tool(
#                     fn_name, fn_args, registry, session, messages, entity_hint
#                 )

#             console.print(f"  [dim]🛠 [bold]{fn_name}[/bold]({fn_args})[/dim]")

#             # 9. TOOL EXECUTION (WITH 10-MIN TIMEOUT)
#             try:
#                 # session.call_tool is async, so we wrap it in wait_for
#                 result = await asyncio.wait_for(
#                     session.call_tool(fn_name, fn_args),
#                     timeout=600  # 10 Minutes
#                 )
#                 result_text = result.content[0].text if result.content else ""
#                 console.print(f"  [green]✓ {len(result_text)} chars[/green]")

#                 if fn_name in _RESOLVER_TOOLS:
#                     registry.update_from_resolver_result(fn_name, result_text)

#             except asyncio.TimeoutError:
#                 result_text = "Error: Tool execution timed out after 10 minutes."
#                 console.print(f"  [red]✗ {fn_name} Timed Out[/red]")
#             except Exception as e:
#                 result_text = f"Tool error: {str(e)}"
#                 console.print(f"  [red]✗ {e}[/red]")

#             # 10. HISTORY UPDATE (Tool Turn)
#             messages.append({
#                 "role":         "tool",
#                 "tool_call_id": tc.id,
#                 "content":      result_text,
#             })

#     return "(Max rounds reached without a final answer)"

# # ── Public API (called by graph.py) ───────────────────────────────────────────

# async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
#     """
#     Public async function consumed by graph.py's node_mcp_tool_call.

#     Parameters
#     ----------
#     query   : Enriched query string (may contain [PRE-RESOLVED CODES] block).
#     backend : "groq" or "ollama". Defaults to LLM_BACKEND env var.

#     Returns
#     -------
#     Final answer string from the MCP agent.
#     """
#     b = (backend or LLM_BACKEND).lower()
#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()
#             return await _run_query_with_session(query, session, b)


# # ── CLI (standalone use) ───────────────────────────────────────────────────────

# async def _cli_run(queries: list[str], backend: str) -> None:
#     label = f"Ollama · {OLLAMA_MODEL}" if backend == "ollama" else f"Groq · {GROQ_MODEL}"
#     console.print(Panel.fit(
#         f"[bold cyan]Equifiz MF Assistant[/bold cyan]\n[dim]{label}[/dim]",
#         border_style="cyan",
#     ))

#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()
#             for q in queries:
#                 console.print(Rule(f"[bold]Query:[/bold] {q}", style="blue"))
#                 answer = await _run_query_with_session(q, session, backend)
#                 console.print(Rule("Answer", style="green"))
#                 console.print(
#                     Markdown(answer) if ("##" in answer or "**" in answer) else answer
#                 )


# def main() -> None:
#     args    = sys.argv[1:]
#     backend = LLM_BACKEND

#     if "--backend" in args:
#         idx     = args.index("--backend")
#         backend = args[idx + 1]
#         args    = [a for i, a in enumerate(args) if i != idx and i != idx + 1]

#     if not args:
#         console.print(
#             "[yellow]Usage:[/yellow] python mcp_client.py [--backend groq|ollama] \"your query\""
#         )
#         sys.exit(0)

#     asyncio.run(_cli_run(args, backend))


# if __name__ == "__main__":
#     main()

# #new
# """
# mcp_client.py — Equifiz MF MCP Client (v3)

# Key changes from v2:
# ──────────────────────────────────────────────────────────────────────
# 1. SEMANTIC TOOL ROUTER — Integrates ChromaDB and Ollama Embeddings.
#    Instead of loading all 30+ tools into the LLM context, it searches 
#    the vector store to find the top 5 most relevant tools.

# 2. RESOLVER GUARD — Automatically injects mf_schcode/mf_cocode if the
#    LLM attempts to call a data tool without them.

# 3. GRAPH INTEGRATION — Parses [PRE-RESOLVED CODES] injected by graph.py.
# """

# from __future__ import annotations

# import asyncio
# import json
# import logging
# import os
# import re
# import sys
# from pathlib import Path
# from typing import Optional

# import chromadb
# from chromadb.utils import embedding_functions
# from mcp import ClientSession, StdioServerParameters
# from mcp.client.stdio import stdio_client
# from rich.console import Console
# from rich.markdown import Markdown
# from rich.panel import Panel
# from rich.rule import Rule
# from dotenv import load_dotenv

# load_dotenv()

# # ── Config ─────────────────────────────────────────────────────────────────────

# SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
# CHROMA_PATH   = Path(__file__).parent / "chroma_db"
# MAX_TOKENS    = 4096
# MAX_ROUNDS    = 15

# LLM_BACKEND   = os.getenv("LLM_BACKEND", "groq").lower()
# GROQ_MODEL    = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")
# OLLAMA_HOST   = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
# OLLAMA_MODEL  = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
# EMBED_MODEL   = "embeddinggemma:latest"

# logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
# logger = logging.getLogger("equifiz_mf_client")
# console = Console()

# # ── ChromaDB Setup ─────────────────────────────────────────────────────────────

# chroma_client = chromadb.PersistentClient(path=str(CHROMA_PATH))
# ollama_ef = embedding_functions.OllamaEmbeddingFunction(
#     model_name=EMBED_MODEL,
#     url=f"{OLLAMA_HOST}/api/embeddings",
# )
# tool_collection = chroma_client.get_collection(name="equifiz_tools", embedding_function=ollama_ef)

# # ── Tool metadata ──────────────────────────────────────────────────────────────

# _TOOL_NEEDS_MF_SCHCODE = {
#     "get_scheme_nav", "get_investment_details", "get_expense_ratio",
#     "get_avg_maturity", "get_scheme_aum", "get_nav_historical",
#     "get_scheme_returns", "get_lumpsum_returns", "get_scheme_sip_details",
#     "get_mf_holdings", "get_sector_allocation", "get_asset_allocation",
#     "get_portfolio_changes", "get_mcap_allocation", "get_most_bought_sold",
#     "get_scheme_ratios", "get_dividend_details", "get_bse_star_scheme",
# }

# _TOOL_NEEDS_MF_COCODE = {
#     "get_fund_categories", "get_schemes_by_amc", "get_fund_profile",
# }

# _TOOL_NEEDS_CO_CODE = {"get_funds_holding_company"}
# _RESOLVER_TOOLS = {"resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes"}

# # ── Semantic Search Helper ─────────────────────────────────────────────────────

# def get_semantic_tools(query: str, all_openai_tools: list[dict], top_k: int = 3) -> list[dict]:
#     """
#     Finds relevant tools using ChromaDB embeddings.
#     Always includes core resolvers as a safety measure.
#     """
#     # Force include resolvers so the model can always map names to IDs
#     CORE_PLUMBING = {"resolve_mf_scheme", "resolve_mf_fund"}
    
#     # Query ChromaDB
#     results = tool_collection.query(
#         query_texts=[query],
#         n_results=top_k
#     )
    
#     matched_names = set(results['ids'][0]) | CORE_PLUMBING
    
#     # Filter the full tool manifest provided by the MCP server
#     filtered = [t for t in all_openai_tools if t["function"]["name"] in matched_names]
#     return filtered

# # ── Session code registry ─────────────────────────────────────────────────────

# class _CodeRegistry:
#     def __init__(self):
#         self.mf_schcode: Optional[int]  = None
#         self.mf_cocode:  Optional[int]  = None
#         self.co_code:    Optional[int]  = None
#         self.scheme_name: Optional[str] = None
#         self.amc_name:    Optional[str] = None

#     def parse_injected(self, query: str) -> None:
#         for line in query.splitlines():
#             line = line.strip()
#             if m := re.match(r"mf_schcode\s*=\s*(\d+)", line):
#                 self.mf_schcode = int(m.group(1))
#                 if name_m := re.search(r"\(scheme:\s*(.+?)\)", line):
#                     self.scheme_name = name_m.group(1).strip()
#             elif m := re.match(r"mf_cocode\s*=\s*(\d+)", line):
#                 self.mf_cocode = int(m.group(1))
#                 if name_m := re.search(r"\(AMC:\s*(.+?)\)", line):
#                     self.amc_name = name_m.group(1).strip()
#             elif m := re.match(r"co_code\s*=\s*(\d+)", line):
#                 self.co_code = int(m.group(1))

#     def update_from_resolver_result(self, tool_name: str, result_text: str) -> None:
#         if "RESOLVED" not in result_text: return
#         if tool_name == "resolve_mf_scheme":
#             if m := re.search(r"mf_schcode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE):
#                 self.mf_schcode = int(m.group(1))
#         elif tool_name == "resolve_mf_fund":
#             if m := re.search(r"mf_cocode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE):
#                 self.mf_cocode = int(m.group(1))

# # ── Resolver guard ─────────────────────────────────────────────────────────────

# async def _ensure_codes_for_tool(
#     tool_name: str, tool_args: dict, registry: _CodeRegistry,
#     session: ClientSession, messages: list[dict], entity_hint: str
# ) -> tuple[dict, list[dict]]:
    
#     updated_args = dict(tool_args)

#     # Resolve mf_schcode if missing
#     if tool_name in _TOOL_NEEDS_MF_SCHCODE and registry.mf_schcode is None:
#         hint = tool_args.get("query") or registry.scheme_name or entity_hint
#         console.print(f"  [yellow]⚡ Auto-resolving mf_schcode for '{hint}'[/yellow]")
#         res = await session.call_tool("resolve_mf_scheme", {"query": hint})
#         txt = res.content[0].text if res.content else ""
#         registry.update_from_resolver_result("resolve_mf_scheme", txt)
#         messages.append({
#             "role": "assistant", "content": None,
#             "tool_calls": [{"id": "auto-sch", "type": "function", "function": {"name": "resolve_mf_scheme", "arguments": json.dumps({"query": hint})}}]
#         })
#         messages.append({"role": "tool", "tool_call_id": "auto-sch", "content": txt})

#     if tool_name in _TOOL_NEEDS_MF_SCHCODE and registry.mf_schcode:
#         updated_args["mf_schcode"] = registry.mf_schcode

#     # Resolve mf_cocode if missing
#     if tool_name in _TOOL_NEEDS_MF_COCODE and registry.mf_cocode is None:
#         hint = tool_args.get("query") or registry.amc_name or entity_hint
#         console.print(f"  [yellow]⚡ Auto-resolving mf_cocode for '{hint}'[/yellow]")
#         res = await session.call_tool("resolve_mf_fund", {"query": hint})
#         txt = res.content[0].text if res.content else ""
#         registry.update_from_resolver_result("resolve_mf_fund", txt)
#         messages.append({
#             "role": "assistant", "content": None,
#             "tool_calls": [{"id": "auto-co", "type": "function", "function": {"name": "resolve_mf_fund", "arguments": json.dumps({"query": hint})}}]
#         })
#         messages.append({"role": "tool", "tool_call_id": "auto-co", "content": txt})

#     if tool_name in _TOOL_NEEDS_MF_COCODE and registry.mf_cocode:
#         updated_args["mf_cocode"] = registry.mf_cocode

#     if tool_name in _TOOL_NEEDS_CO_CODE and registry.co_code:
#         updated_args["co_code"] = registry.co_code

#     return updated_args, messages

# # ── Core system prompt ─────────────────────────────────────────────────────────

# SYSTEM_PROMPT = """You are a mutual fund assistant for Indian markets. 
# MANDATORY: Use resolved codes from previous tool calls. Never guess IDs."""

# # ── Helpers ────────────────────────────────────────────────────────────────────

# def _to_openai_tool(tool) -> dict:
#     schema = tool.inputSchema or {}
#     props = schema.get("properties", {})
#     required = schema.get("required", [])
#     if list(props.keys()) == ["params"]:
#         props = props["params"].get("properties", {})
#         required = props["params"].get("required", [])
    
#     for k, v in props.items():
#         if "description" not in v: v["description"] = f"Param {k}"

#     return {
#         "type": "function",
#         "function": {
#             "name": tool.name,
#             "description": (tool.description or tool.name).strip(),
#             "parameters": {"type": "object", "properties": props, "required": required}
#         }
#     }

# def _chat(messages, tools, backend):
#     if backend == "ollama":
#         import urllib.request
#         req = urllib.request.Request(
#             f"{OLLAMA_HOST}/api/chat",
#             data=json.dumps({"model": OLLAMA_MODEL, "messages": messages, "tools": tools, "stream": False, "options": {"temperature": 0.1}}).encode(),
#             headers={"Content-Type": "application/json"}, method="POST"
#         )
#         data = json.loads(urllib.request.urlopen(req, timeout=600).read().decode())
#         omsg = data.get("message", {})
        
#         class _TC:
#             def __init__(self, d):
#                 self.id = d.get("id", "tc-id")
#                 self.function = type("F", (), {"name": d["function"]["name"], "arguments": json.dumps(d["function"]["arguments"])})()
        
#         class _Choice:
#             def __init__(self):
#                 self.message = type("M", (), {"content": omsg.get("content"), "tool_calls": [_TC(t) for t in omsg.get("tool_calls", [])] or None})()
#         return _Choice()
#     else:
#         from groq import Groq
#         client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
#         return client.chat.completions.create(model=GROQ_MODEL, messages=messages, tools=tools, tool_choice="auto").choices[0]

# def _extract_entity_hint(query: str) -> str:
#     return re.split(r"\[PRE-RESOLVED CODES", query)[0].strip()

# # ── Main Query Loop ───────────────────────────────────────────────────────────

# async def _run_query_with_session(query: str, session: ClientSession, backend: str) -> str:
#     registry = _CodeRegistry()
#     if "[PRE-RESOLVED CODES" in query:
#         registry.parse_injected(query)

#     entity_hint = _extract_entity_hint(query)

#     # 1. Load all potential tools from server
#     raw_tools = (await session.list_tools()).tools
#     all_manifest = [_to_openai_tool(t) for t in raw_tools]

#     # 2. SEMANTIC FILTERING - Use ChromaDB to narrow down tools
#     current_tools = get_semantic_tools(query, all_manifest, top_k=5)
#     logger.info(f"  [dim]Semantic Router: selected {len(current_tools)} relevant tools[/dim]")

#     messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": query}]

#     for round_num in range(1, MAX_ROUNDS + 1):
#         logger.info(f"  [dim]── Round {round_num} ──[/dim]")
        
#         loop = asyncio.get_event_loop()
#         try:
#             choice = await asyncio.wait_for(
#                 loop.run_in_executor(None, _chat, messages, current_tools, backend),
#                 timeout=600
#             )
#         except Exception as e: return f"Error: {e}"

#         msg = choice.message
#         if not msg.tool_calls: return msg.content or "(Empty response)"

#         # Append Assistant Turn
#         messages.append({
#             "role": "assistant", "content": msg.content,
#             "tool_calls": [{"id": tc.id, "type": "function", "function": {"name": tc.function.name, "arguments": tc.function.arguments}} for tc in msg.tool_calls]
#         })

#         # Execute Tools
#         for tc in msg.tool_calls:
#             fn_name, fn_args = tc.function.name, json.loads(tc.function.arguments)
#             logger.info(f"  [cyan]📤 Sending to Server:[/cyan] {fn_name} args={fn_args}")
#             # Resolver Guard
#             if fn_name not in _RESOLVER_TOOLS:
#                 fn_args, messages = await _ensure_codes_for_tool(fn_name, fn_args, registry, session, messages, entity_hint)

#             console.print(f"  [dim]🛠 [bold]{fn_name}[/bold][/dim]")
#             try:
#                 res = await asyncio.wait_for(session.call_tool(fn_name, fn_args), timeout=600)
#                 txt = res.content[0].text if res.content else ""
#                 if fn_name in _RESOLVER_TOOLS: registry.update_from_resolver_result(fn_name, txt)
#             except Exception as e: txt = f"Error: {e}"

#             messages.append({"role": "tool", "tool_call_id": tc.id, "content": txt})

#     return "(Max rounds reached)"

# async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
#     b = (backend or LLM_BACKEND).lower()
#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()
#             return await _run_query_with_session(query, session, b)

# if __name__ == "__main__":
#     if len(sys.argv) > 1:
#         asyncio.run(run_mcp_query(sys.argv[1]))

"""
mcp_client.py — Equifiz MF MCP Client (v3.1)
Optimized for LangGraph Integration and High-Precision ID Injection.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Optional, Any

import chromadb
from chromadb.utils import embedding_functions
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.rule import Rule
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────

SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
CHROMA_PATH   = Path(__file__).parent / "chroma_db"
MAX_TOKENS    = 4096
MAX_ROUNDS    = 15

LLM_BACKEND   = os.getenv("LLM_BACKEND", "groq").lower()
GROQ_MODEL    = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")
OLLAMA_HOST   = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
OLLAMA_MODEL  = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
EMBED_MODEL   = "embeddinggemma:latest"

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("equifiz_mf_client")
console = Console()

# ── ChromaDB Setup ─────────────────────────────────────────────────────────────

chroma_client = chromadb.PersistentClient(path=str(CHROMA_PATH))
ollama_ef = embedding_functions.OllamaEmbeddingFunction(
    model_name=EMBED_MODEL,
    url=f"{OLLAMA_HOST}/api/embeddings",
)
tool_collection = chroma_client.get_collection(name="equifiz_tools", embedding_function=ollama_ef)

# ── Tool metadata ──────────────────────────────────────────────────────────────

_TOOL_NEEDS_MF_SCHCODE = {
    "get_scheme_nav", "get_investment_details", "get_expense_ratio",
    "get_avg_maturity", "get_scheme_aum", "get_nav_historical",
    "get_scheme_returns", "get_lumpsum_returns", "get_scheme_sip_details",
    "get_mf_holdings", "get_sector_allocation", "get_asset_allocation",
    "get_portfolio_changes", "get_mcap_allocation", "get_most_bought_sold",
    "get_scheme_ratios", "get_dividend_details", "get_bse_star_scheme",
}

_TOOL_NEEDS_MF_COCODE = {
    "get_fund_categories", "get_schemes_by_amc", "get_fund_profile",
}

_TOOL_NEEDS_CO_CODE = {"get_funds_holding_company"}
_RESOLVER_TOOLS = {"resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes"}

# ── Semantic Search Helper ─────────────────────────────────────────────────────

def get_semantic_tools(query: str, all_openai_tools: list[dict], top_k: int = 7) -> list[dict]:
    """Finds relevant tools using ChromaDB embeddings. Always includes core resolvers."""
    CORE_PLUMBING = {"resolve_mf_scheme", "resolve_mf_fund"}
    
    try:
        results = tool_collection.query(query_texts=[query], n_results=top_k)
        matched_names = set(results['ids'][0]) | CORE_PLUMBING
    except Exception as e:
        console.print(f"ChromaDB Query Failed: {e}")
        matched_names = CORE_PLUMBING # Safety fallback

    return [t for t in all_openai_tools if t["function"]["name"] in matched_names]

# ── Session code registry ─────────────────────────────────────────────────────

class _CodeRegistry:
    def __init__(self):
        self.mf_schcode: Optional[int]  = None
        self.mf_cocode:  Optional[int]  = None
        self.co_code:    Optional[int]  = None
        self.scheme_name: Optional[str] = None
        self.amc_name:    Optional[str] = None

    def parse_injected(self, query: str) -> None:
        """Parses [VERIFIED ENTITY IDs] or [PRE-RESOLVED CODES] from graph.py."""
        for line in query.splitlines():
            line = line.strip().replace("- ", "") # Handle bullet points
            if m := re.search(r"mf_schcode[:=]\s*(\d+)", line, re.I):
                self.mf_schcode = int(m.group(1))
                console.print(f"Registry: Injected mf_schcode={self.mf_schcode}")
            elif m := re.search(r"mf_cocode[:=]\s*(\d+)", line, re.I):
                self.mf_cocode = int(m.group(1))
            elif m := re.search(r"co_code[:=]\s*(\d+)", line, re.I):
                self.co_code = int(m.group(1))

    def update_from_resolver_result(self, tool_name: str, result_text: str) -> None:
        if "RESOLVED" not in result_text: return
        if tool_name == "resolve_mf_scheme":
            if m := re.search(r"mf_schcode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE):
                self.mf_schcode = int(m.group(1))
        elif tool_name == "resolve_mf_fund":
            if m := re.search(r"mf_cocode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE):
                self.mf_cocode = int(m.group(1))

# ── Resolver guard ─────────────────────────────────────────────────────────────

# async def _ensure_codes_for_tool(
#     tool_name: str, tool_args: dict, registry: _CodeRegistry,
#     session: ClientSession, messages: list[dict], entity_hint: str
# ) -> tuple[dict, list[dict]]:
    
#     updated_args = dict(tool_args)

#     # 1. MF_SCHCODE Logic
#     if tool_name in _TOOL_NEEDS_MF_SCHCODE:
#         if registry.mf_schcode is None:
#             hint = tool_args.get("query") or entity_hint
#             console.print(f"  [yellow]⚡ Auto-resolving mf_schcode for '{hint}'[/yellow]")
#             res = await session.call_tool("resolve_mf_scheme", {"query": hint})
#             txt = res.content[0].text if res.content else ""
#             registry.update_from_resolver_result("resolve_mf_scheme", txt)
#             messages.append({
#                 "role": "assistant", "content": None,
#                 "tool_calls": [{"id": "auto-sch", "type": "function", "function": {"name": "resolve_mf_scheme", "arguments": json.dumps({"query": hint})}}]
#             })
#             messages.append({"role": "tool", "tool_call_id": "auto-sch", "content": txt})
        
#         # FORCE INJECTION: Overwrite arg with verified registry value
#         if registry.mf_schcode is not None:
#             updated_args["mf_schcode"] = registry.mf_schcode

#     # 2. MF_COCODE Logic
#     if tool_name in _TOOL_NEEDS_MF_COCODE:
#         if registry.mf_cocode is None:
#             hint = tool_args.get("query") or entity_hint
#             console.print(f"  [yellow]⚡ Auto-resolving mf_cocode for '{hint}'[/yellow]")
#             res = await session.call_tool("resolve_mf_fund", {"query": hint})
#             txt = res.content[0].text if res.content else ""
#             registry.update_from_resolver_result("resolve_mf_fund", txt)
#             messages.append({
#                 "role": "assistant", "content": None,
#                 "tool_calls": [{"id": "auto-co", "type": "function", "function": {"name": "resolve_mf_fund", "arguments": json.dumps({"query": hint})}}]
#             })
#             messages.append({"role": "tool", "tool_call_id": "auto-co", "content": txt})

#         if registry.mf_cocode is not None:
#             updated_args["mf_cocode"] = registry.mf_cocode

#     # 3. CO_CODE Logic
#     if tool_name in _TOOL_NEEDS_CO_CODE and registry.co_code is not None:
#         updated_args["co_code"] = registry.co_code

#     return updated_args, messages


# async def _ensure_codes_for_tool(
#     tool_name: str, tool_args: dict, registry: _CodeRegistry,
#     session: ClientSession, messages: list[dict], entity_hint: str
# ) -> tuple[dict, list[dict]]:
#     """
#     Intercepts tool calls to inject missing numeric IDs (mf_schcode, mf_cocode) 
#     by triggering silent resolver calls if the registry is empty.
#     """
#     updated_args = dict(tool_args)

#     # Helper to clean hints (removes common noise that confuses fuzzy search)
#     def clean_hint(h):
#         return re.sub(r"(mf_schcode|mf_cocode|co_code)[:=]\s*", "", str(h), flags=re.I).strip()

#     # 1. MF_SCHCODE Logic (Scheme Level)
#     if tool_name in _TOOL_NEEDS_MF_SCHCODE:
#         if registry.mf_schcode is None:
#             # Try to find a hint in current args, otherwise use global entity_hint
#             raw_hint = tool_args.get("query") or tool_args.get("sch_name") or entity_hint
#             hint = clean_hint(raw_hint)
            
#             console.print(f"  [yellow]⚡ Auto-resolving mf_schcode for '{hint}'[/yellow]")
            
#             try:
#                 res = await session.call_tool("resolve_mf_scheme", {"query": hint})
#                 txt = res.content[0].text if res.content else ""
                
#                 # Update registry with the newly found ID
#                 registry.update_from_resolver_result("resolve_mf_scheme", txt)
                
#                 # Update conversation history so LLM "sees" the resolution happened
#                 messages.append({
#                     "role": "assistant", "content": None,
#                     "tool_calls": [{
#                         "id": f"auto-sch-{registry.mf_schcode}", 
#                         "type": "function", 
#                         "function": {"name": "resolve_mf_scheme", "arguments": json.dumps({"query": hint})}
#                     }]
#                 })
#                 messages.append({"role": "tool", "tool_call_id": f"auto-sch-{registry.mf_schcode}", "content": txt})
#             except Exception as e:
#                 logger.error(f"Auto-resolution failed: {e}")

#         # FORCE INJECTION: Overwrite the argument with the verified registry value
#         if registry.mf_schcode is not None:
#             updated_args["mf_schcode"] = registry.mf_schcode

#     # 2. MF_COCODE Logic (AMC Level)
#     if tool_name in _TOOL_NEEDS_MF_COCODE:
#         if registry.mf_cocode is None:
#             raw_hint = tool_args.get("query") or tool_args.get("amc_name") or entity_hint
#             hint = clean_hint(raw_hint)
            
#             console.print(f"  [yellow]⚡ Auto-resolving mf_cocode for '{hint}'[/yellow]")
            
#             try:
#                 res = await session.call_tool("resolve_mf_fund", {"query": hint})
#                 txt = res.content[0].text if res.content else ""
                
#                 registry.update_from_resolver_result("resolve_mf_fund", txt)
                
#                 messages.append({
#                     "role": "assistant", "content": None,
#                     "tool_calls": [{
#                         "id": f"auto-co-{registry.mf_cocode}", 
#                         "type": "function", 
#                         "function": {"name": "resolve_mf_fund", "arguments": json.dumps({"query": hint})}
#                     }]
#                 })
#                 messages.append({"role": "tool", "tool_call_id": f"auto-co-{registry.mf_cocode}", "content": txt})
#             except Exception as e:
#                 logger.error(f"Auto-resolution failed: {e}")

#         if registry.mf_cocode is not None:
#             updated_args["mf_cocode"] = registry.mf_cocode

#     # 3. CO_CODE Logic (Corporate/Company Level)
#     if tool_name in _TOOL_NEEDS_CO_CODE and registry.co_code is not None:
#         updated_args["co_code"] = registry.co_code

#     return updated_args, messages

async def _ensure_codes_for_tool(
    tool_name: str, tool_args: dict, registry: _CodeRegistry,
    session: ClientSession, messages: list[dict], entity_hint: str
) -> tuple[dict, list[dict]]:
    """
    Intercepts tool calls to inject missing numeric IDs.
    Enhanced with 'Prompt-Noise' filtering to prevent 400 Bad Request errors.
    """
    updated_args = dict(tool_args)

    # def clean_hint(h):
    #     """
    #     Strips out LangGraph boilerplate and ID labels so the resolver 
    #     sees only the fund name (e.g., 'DSP Bond Fund').
    #     """
    #     h_str = str(h)
    #     # 1. Remove LangGraph/System headers: Context, Instruction, User Query, etc.
    #     # This fixes the issue where 'Instruction:...' was being sent to the fuzzy search.
    #     h_str = re.sub(r"(?i)(Context|Instruction|User Query|RECOMMENDED TOOL):.*?\n", "", h_str, flags=re.S)
        
    #     # 2. Strip numeric labels like 'mf_schcode: 770' or 'mf_cocode='
    #     h_str = re.sub(r"(?i)(mf_schcode|mf_cocode|co_code)[:=]\s*", "", h_str).strip()
        
    #     # 3. Final polish: remove lingering brackets and extra whitespace
    #     h_str = h_str.replace("[", "").replace("]", "").strip()
    #     return h_str

    # Inside mcp_client.py -> _ensure_codes_for_tool
    
    def clean_hint(h):
        h_str = str(h)
        
        # 1. Remove everything inside <PRE_RESOLVED> tags
        h_str = re.sub(r"<PRE_RESOLVED>.*?</PRE_RESOLVED>", "", h_str, flags=re.S | re.I)
        
        # 2. Remove "User Query:" or "Instruction:" prefixes
        h_str = re.sub(r"(?i)(User Query|Instruction|Context|RECOMMENDED TOOL):", "", h_str)
        
        # 3. Strip any remaining bracketed metadata
        h_str = re.sub(r"\[.*?\]", "", h_str)

        cleaned = h_str.strip()
        console.print(f"    [dim]Cleaned hint for resolver:[/dim] [italic]'{cleaned}'[/italic]")
        return cleaned

    # 1. MF_SCHCODE Logic (Scheme Level)
    if tool_name in _TOOL_NEEDS_MF_SCHCODE:
        if registry.mf_schcode is None:
            # Check current args first, fallback to entity_hint
            raw_hint = tool_args.get("query") or tool_args.get("sch_name") or entity_hint
            hint = clean_hint(raw_hint)
            
            console.print(f"  [yellow]⚡ Auto-resolving mf_schcode for clean hint: '{hint}'[/yellow]")
            
            try:
                res = await session.call_tool("resolve_mf_scheme", {"query": hint})
                txt = res.content[0].text if res.content else ""
                
                # We only update if we actually found a match
                if "RESOLVED" in txt:
                    registry.update_from_resolver_result("resolve_mf_scheme", txt)
                    console.print(f"    [green]✔ Success:[/green] Resolved to mf_schcode: [bold]{registry.mf_schcode}[/bold]")
                    messages.append({
                        "role": "assistant", "content": None,
                        "tool_calls": [{
                            "id": f"auto-sch-{registry.mf_schcode}", 
                            "type": "function", 
                            "function": {"name": "resolve_mf_scheme", "arguments": json.dumps({"query": hint})}
                        }]
                    })
                    messages.append({"role": "tool", "tool_call_id": f"auto-sch-{registry.mf_schcode}", "content": txt})
                else:
                    console.print(f"  [red]⚠ Could not resolve scheme for: {hint}[/red]")
            except Exception as e:
                console.print(f"Auto-resolution failed: {e}")

        # FORCE INJECTION: Overwrite arg with verified registry value
        if registry.mf_schcode is not None:
            updated_args["mf_schcode"] = registry.mf_schcode

    # 2. MF_COCODE Logic (AMC Level)
    if tool_name in _TOOL_NEEDS_MF_COCODE:
        if registry.mf_cocode is None:
            raw_hint = tool_args.get("query") or tool_args.get("amc_name") or entity_hint
            hint = clean_hint(raw_hint)
            
            console.print(f"  [yellow]⚡ Auto-resolving mf_cocode for clean hint: '{hint}'[/yellow]")
            
            try:
                res = await session.call_tool("resolve_mf_fund", {"query": hint})
                txt = res.content[0].text if res.content else ""
                
                if "RESOLVED" in txt:
                    registry.update_from_resolver_result("resolve_mf_fund", txt)
                    
                    messages.append({
                        "role": "assistant", "content": None,
                        "tool_calls": [{
                            "id": f"auto-co-{registry.mf_cocode}", 
                            "type": "function", 
                            "function": {"name": "resolve_mf_fund", "arguments": json.dumps({"query": hint})}
                        }]
                    })
                    messages.append({"role": "tool", "tool_call_id": f"auto-co-{registry.mf_cocode}", "content": txt})
            except Exception as e:
                console.print(f"Auto-resolution failed: {e}")

        if registry.mf_cocode is not None:
            updated_args["mf_cocode"] = registry.mf_cocode

    # 3. CO_CODE Logic (Corporate/Company Level)
    if tool_name in _TOOL_NEEDS_CO_CODE and registry.co_code is not None:
        updated_args["co_code"] = registry.co_code

    return updated_args, messages

# ── Core system prompt ─────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a mutual fund analyst. 
Verified IDs are provided in context. You MUST use these numeric IDs for tool calls.
Do not call resolution tools if an ID is already present in the context."""

# ── Helpers ────────────────────────────────────────────────────────────────────

def _to_openai_tool(tool) -> dict:
    schema = tool.inputSchema or {}
    props = schema.get("properties", {})
    required = schema.get("required", [])
    
    # Safe flattening for FastMCP 'params' wrapper
    if "params" in props and len(props) == 1:
        inner = props["params"]
        props = inner.get("properties", {})
        required = inner.get("required", [])
    
    for k, v in props.items():
        if "description" not in v: v["description"] = f"Param {k}"

    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": (tool.description or tool.name).strip(),
            "parameters": {"type": "object", "properties": props, "required": required}
        }
    }

def _chat(messages, tools, backend):
    if backend == "ollama":
        import urllib.request
        req = urllib.request.Request(
            f"{OLLAMA_HOST}/api/chat",
            data=json.dumps({
                "model": OLLAMA_MODEL, 
                "messages": messages, 
                "tools": tools, 
                "stream": False, 
                "options": {"temperature": 0.0} # Low temp for precision
            }).encode(),
            headers={"Content-Type": "application/json"}, method="POST"
        )
        data = json.loads(urllib.request.urlopen(req, timeout=600).read().decode())
        omsg = data.get("message", {})
        
        class _TC:
            def __init__(self, d):
                self.id = d.get("id", "tc-id")
                # Normalize arguments to string for the Registry parser
                args = d["function"]["arguments"]
                self.function = type("F", (), {
                    "name": d["function"]["name"], 
                    "arguments": json.dumps(args) if isinstance(args, dict) else args
                })()
        
        class _Choice:
            def __init__(self):
                self.message = type("M", (), {
                    "content": omsg.get("content"), 
                    "tool_calls": [_TC(t) for t in omsg.get("tool_calls", [])] or None
                })()
        return _Choice()
    else:
        from groq import Groq
        client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
        return client.chat.completions.create(model=GROQ_MODEL, messages=messages, tools=tools, tool_choice="auto").choices[0]

def _extract_entity_hint(query: str) -> str:
    # Strip all metadata tags for the resolver hint
    clean = re.split(r"\[(PRE-RESOLVED|VERIFIED|SUGGESTED)", query)[0].strip()
    return clean

# ── Main Query Loop ───────────────────────────────────────────────────────────

async def _run_query_with_session(query: str, session: ClientSession, backend: str) -> str:
    registry = _CodeRegistry()
    registry.parse_injected(query)
    
    console.print(Panel(f"[bold blue]Incoming Query Context:[/bold blue]\n{query}", title="LLM Input"))

    entity_hint = _extract_entity_hint(query)
    raw_tools = (await session.list_tools()).tools
    all_manifest = [_to_openai_tool(t) for t in raw_tools]

    # SEMANTIC FILTERING
    current_tools = get_semantic_tools(query, all_manifest, top_k=7)
    console.print(f"  [Semantic Router] selected {len(current_tools)} tools")

    messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": query}]

    for round_num in range(1, MAX_ROUNDS + 1):
        loop = asyncio.get_event_loop()
        try:
            choice = await asyncio.wait_for(
                loop.run_in_executor(None, _chat, messages, current_tools, backend),
                timeout=600
            )
        except Exception as e: return f"LLM Error: {e}"

        msg = choice.message
        if msg.content:
            console.print(f"\n[bold magenta]Assistant:[/bold magenta] {msg.content}")

        if not msg.tool_calls: 
            return msg.content or "(No data found)"

        # Append Assistant Turn (Fixed for Ollama dictionary arguments)
        messages.append({
            "role": "assistant", "content": msg.content,
            "tool_calls": [
                {
                    "id": tc.id, "type": "function", 
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments}
                } for tc in msg.tool_calls
            ]
        })

        # Execute Tools
        for tc in msg.tool_calls:
            fn_name = tc.function.name
            # Argument Hygiene: Handle string or dict
            fn_args = tc.function.arguments
            if isinstance(fn_args, str):
                try: fn_args = json.loads(fn_args)
                except: fn_args = {}

            # RESOLVER GUARD: Force Verified IDs into arguments
            if fn_name not in _RESOLVER_TOOLS:
                fn_args, messages = await _ensure_codes_for_tool(fn_name, fn_args, registry, session, messages, entity_hint)

            console.print(f"  [cyan]🛠 Executing:[/cyan] [bold]{fn_name}[/bold] with {fn_args}")
            console.print(f"  [cyan]🛠 Final Tool Call:[/cyan] [bold]{fn_name}[/bold]([italic]{json.dumps(fn_args)}[/italic])")
            try:
                res = await asyncio.wait_for(session.call_tool(fn_name, fn_args), timeout=600)
                txt = res.content[0].text if res.content else "Empty response"
                if fn_name in _RESOLVER_TOOLS: registry.update_from_resolver_result(fn_name, txt)
            except Exception as e: txt = f"Tool Error: {e}"

            messages.append({"role": "tool", "tool_call_id": tc.id, "content": txt})

    return "(Process exceeded max rounds)"

async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
    b = (backend or LLM_BACKEND).lower()
    server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            return await _run_query_with_session(query, session, b)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        asyncio.run(run_mcp_query(" ".join(sys.argv[1:])))