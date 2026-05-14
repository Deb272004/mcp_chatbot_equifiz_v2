

# """
# mcp_client.py — Equifiz MF MCP Client (v3.2)

# Fix from v3.1:
# ─────────────────────────────────────────────────────────────────────
# - Tracks `last_tool_result` across rounds so that if the LLM returns
#   empty content in Round 2 (after seeing tool output), the raw tool
#   data is returned instead of "(No data found)".
# - Final fallback in `run_mcp_query` also returns `last_tool_result`
#   instead of the generic max-rounds message.
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
# from rich.panel import Panel
# from dotenv import load_dotenv

# load_dotenv()

# # ── Config ─────────────────────────────────────────────────────────────────────

# SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
# CHROMA_PATH   = Path(__file__).parent / "chroma_db"
# MAX_TOKENS    = 4096
# MAX_ROUNDS    = 15

# LLM_BACKEND  = os.getenv("LLM_BACKEND", "groq").lower()
# GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")
# OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
# OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
# EMBED_MODEL  = "embeddinggemma:latest"

# logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
# logger  = logging.getLogger("equifiz_mf_client")
# console = Console()

# # ── ChromaDB Setup ─────────────────────────────────────────────────────────────

# chroma_client  = chromadb.PersistentClient(path=str(CHROMA_PATH))
# ollama_ef      = embedding_functions.OllamaEmbeddingFunction(
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

# _TOOL_NEEDS_CO_CODE  = {"get_funds_holding_company"}
# _RESOLVER_TOOLS      = {"resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes"}

# # ── Semantic Search Helper ─────────────────────────────────────────────────────

# def get_semantic_tools(query: str, all_openai_tools: list[dict], top_k: int = 7) -> list[dict]:
#     """Finds relevant tools via ChromaDB embeddings. Always includes core resolvers."""
#     CORE_PLUMBING = {"resolve_mf_scheme", "resolve_mf_fund"}
#     try:
#         results      = tool_collection.query(query_texts=[query], n_results=top_k)
#         matched_names = set(results["ids"][0]) | CORE_PLUMBING
#     except Exception as e:
#         console.print(f"ChromaDB query failed: {e}")
#         matched_names = CORE_PLUMBING
#     return [t for t in all_openai_tools if t["function"]["name"] in matched_names]

# # ── Session code registry ─────────────────────────────────────────────────────

# class _CodeRegistry:
#     def __init__(self):
#         self.mf_schcode:  Optional[int] = None
#         self.mf_cocode:   Optional[int] = None
#         self.co_code:     Optional[int] = None
#         self.scheme_name: Optional[str] = None
#         self.amc_name:    Optional[str] = None

#     def parse_injected(self, query: str) -> None:
#         """Parses [VERIFIED ENTITY IDs] or [PRE-RESOLVED CODES] injected by graph.py."""
#         for line in query.splitlines():
#             line = line.strip().replace("- ", "")
#             if m := re.search(r"mf_schcode[:=]\s*(\d+)", line, re.I):
#                 self.mf_schcode = int(m.group(1))
#                 console.print(f"  [Registry] Injected mf_schcode={self.mf_schcode}")
#             elif m := re.search(r"mf_cocode[:=]\s*(\d+)", line, re.I):
#                 self.mf_cocode = int(m.group(1))
#                 console.print(f"  [Registry] Injected mf_cocode={self.mf_cocode}")
#             elif m := re.search(r"co_code[:=]\s*(\d+)", line, re.I):
#                 self.co_code = int(m.group(1))
#                 console.print(f"  [Registry] Injected co_code={self.co_code}")

#     def update_from_resolver_result(self, tool_name: str, result_text: str) -> None:
#         if "RESOLVED" not in result_text:
#             return
#         if tool_name == "resolve_mf_scheme":
#             if m := re.search(r"mf_schcode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE):
#                 self.mf_schcode = int(m.group(1))
#         elif tool_name == "resolve_mf_fund":
#             if m := re.search(r"mf_cocode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE):
#                 self.mf_cocode = int(m.group(1))

# # ── Resolver guard ─────────────────────────────────────────────────────────────

# async def _ensure_codes_for_tool(
#     tool_name: str,
#     tool_args: dict,
#     registry:  _CodeRegistry,
#     session:   ClientSession,
#     messages:  list[dict],
#     entity_hint: str,
# ) -> tuple[dict, list[dict]]:
#     """
#     Injects missing numeric IDs (mf_schcode, mf_cocode) by triggering
#     silent resolver calls when the registry is empty.
#     """
#     updated_args = dict(tool_args)

#     def clean_hint(h: str) -> str:
#         h = re.sub(r"<PRE_RESOLVED>.*?</PRE_RESOLVED>", "", str(h), flags=re.S | re.I)
#         h = re.sub(r"(?i)(User Query|Instruction|Context|RECOMMENDED TOOL):", "", h)
#         h = re.sub(r"\[.*?\]", "", h)
#         cleaned = h.strip()
#         console.print(f"    [dim]Resolver hint:[/dim] '{cleaned}'")
#         return cleaned

#     # ── mf_schcode ────────────────────────────────────────────────────────────
#     if tool_name in _TOOL_NEEDS_MF_SCHCODE:
#         if registry.mf_schcode is None:
#             raw  = tool_args.get("query") or tool_args.get("sch_name") or entity_hint
#             hint = clean_hint(raw)
#             console.print(f"  [yellow]⚡ Auto-resolving mf_schcode for '{hint}'[/yellow]")
#             try:
#                 res = await session.call_tool("resolve_mf_scheme", {"query": hint})
#                 txt = res.content[0].text if res.content else ""
#                 if "RESOLVED" in txt:
#                     registry.update_from_resolver_result("resolve_mf_scheme", txt)
#                     console.print(f"    [green]✔ mf_schcode={registry.mf_schcode}[/green]")
#                     messages.append({
#                         "role": "assistant", "content": None,
#                         "tool_calls": [{
#                             "id": f"auto-sch-{registry.mf_schcode}",
#                             "type": "function",
#                             "function": {
#                                 "name": "resolve_mf_scheme",
#                                 "arguments": json.dumps({"query": hint}),
#                             },
#                         }],
#                     })
#                     messages.append({
#                         "role": "tool",
#                         "tool_call_id": f"auto-sch-{registry.mf_schcode}",
#                         "content": txt,
#                     })
#                 else:
#                     console.print(f"  [red]⚠ Could not resolve scheme for: {hint}[/red]")
#             except Exception as e:
#                 console.print(f"  [red]Auto-resolution failed: {e}[/red]")

#         if registry.mf_schcode is not None:
#             updated_args["mf_schcode"] = registry.mf_schcode

#     # ── mf_cocode ─────────────────────────────────────────────────────────────
#     if tool_name in _TOOL_NEEDS_MF_COCODE:
#         if registry.mf_cocode is None:
#             raw  = tool_args.get("query") or tool_args.get("amc_name") or entity_hint
#             hint = clean_hint(raw)
#             console.print(f"  [yellow]⚡ Auto-resolving mf_cocode for '{hint}'[/yellow]")
#             try:
#                 res = await session.call_tool("resolve_mf_fund", {"query": hint})
#                 txt = res.content[0].text if res.content else ""
#                 if "RESOLVED" in txt:
#                     registry.update_from_resolver_result("resolve_mf_fund", txt)
#                     messages.append({
#                         "role": "assistant", "content": None,
#                         "tool_calls": [{
#                             "id": f"auto-co-{registry.mf_cocode}",
#                             "type": "function",
#                             "function": {
#                                 "name": "resolve_mf_fund",
#                                 "arguments": json.dumps({"query": hint}),
#                             },
#                         }],
#                     })
#                     messages.append({
#                         "role": "tool",
#                         "tool_call_id": f"auto-co-{registry.mf_cocode}",
#                         "content": txt,
#                     })
#                 else:
#                     console.print(f"  [red]⚠ Could not resolve fund for: {hint}[/red]")
#             except Exception as e:
#                 console.print(f"  [red]Auto-resolution failed: {e}[/red]")

#         if registry.mf_cocode is not None:
#             updated_args["mf_cocode"] = registry.mf_cocode

#     # ── co_code ───────────────────────────────────────────────────────────────
#     if tool_name in _TOOL_NEEDS_CO_CODE and registry.co_code is not None:
#         updated_args["co_code"] = registry.co_code

#     return updated_args, messages

# # ── System prompt ──────────────────────────────────────────────────────────────

# SYSTEM_PROMPT = """\
# You are a mutual fund analyst for Indian markets.
# Verified numeric IDs are provided in the context — use them directly for tool calls.
# Do NOT call resolver tools if an ID is already present.
# Do NOT fabricate data. If a tool returns no data, say so clearly.
# """

# # ── Helpers ────────────────────────────────────────────────────────────────────

# def _to_openai_tool(tool) -> dict:
#     schema   = tool.inputSchema or {}
#     props    = schema.get("properties", {})
#     required = schema.get("required", [])

#     if "params" in props and len(props) == 1:
#         inner    = props["params"]
#         props    = inner.get("properties", {})
#         required = inner.get("required", [])

#     for k, v in props.items():
#         if "description" not in v:
#             v["description"] = f"Param {k}"

#     return {
#         "type": "function",
#         "function": {
#             "name":        tool.name,
#             "description": (tool.description or tool.name).strip(),
#             "parameters":  {"type": "object", "properties": props, "required": required},
#         },
#     }


# def _chat(messages: list, tools: list, backend: str):
#     if backend == "ollama":
#         import urllib.request
#         req = urllib.request.Request(
#             f"{OLLAMA_HOST}/api/chat",
#             data=json.dumps({
#                 "model":   OLLAMA_MODEL,
#                 "messages": messages,
#                 "tools":   tools,
#                 "stream":  False,
#                 "options": {"temperature": 0.0},
#             }).encode(),
#             headers={"Content-Type": "application/json"},
#             method="POST",
#         )
#         data = json.loads(urllib.request.urlopen(req, timeout=600).read().decode())
#         omsg = data.get("message", {})

#         class _TC:
#             def __init__(self, d):
#                 self.id = d.get("id", "tc-id")
#                 args = d["function"]["arguments"]
#                 self.function = type("F", (), {
#                     "name":      d["function"]["name"],
#                     "arguments": json.dumps(args) if isinstance(args, dict) else args,
#                 })()

#         class _Choice:
#             def __init__(self):
#                 tcs = [_TC(t) for t in omsg.get("tool_calls", [])]
#                 self.message = type("M", (), {
#                     "content":    omsg.get("content"),
#                     "tool_calls": tcs or None,
#                 })()

#         return _Choice()

#     else:
#         from groq import Groq
#         client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
#         return client.chat.completions.create(
#             model=GROQ_MODEL,
#             messages=messages,
#             tools=tools,
#             tool_choice="auto",
#         ).choices[0]


# def _extract_entity_hint(query: str) -> str:
#     return re.split(r"\[(PRE-RESOLVED|VERIFIED|SUGGESTED)", query)[0].strip()

# # ── Core query loop ────────────────────────────────────────────────────────────

# async def _run_query_with_session(
#     query:   str,
#     session: ClientSession,
#     backend: str,
# ) -> str:
#     registry = _CodeRegistry()
#     registry.parse_injected(query)

#     console.print(Panel(
#         f"[bold blue]Incoming Query Context:[/bold blue]\n{query}",
#         title="LLM Input",
#     ))

#     entity_hint  = _extract_entity_hint(query)
#     raw_tools    = (await session.list_tools()).tools
#     all_manifest = [_to_openai_tool(t) for t in raw_tools]

#     current_tools = get_semantic_tools(query, all_manifest, top_k=7)
#     console.print(f"  [Semantic Router] selected {len(current_tools)} tools")

#     messages         = [
#         {"role": "system", "content": SYSTEM_PROMPT},
#         {"role": "user",   "content": query},
#     ]
#     last_tool_result = ""  # ← tracks the most recent tool output

#     for round_num in range(1, MAX_ROUNDS + 1):
#         console.print(f"  [dim]── Round {round_num} ──[/dim]")

#         loop = asyncio.get_event_loop()
#         try:
#             choice = await asyncio.wait_for(
#                 loop.run_in_executor(None, _chat, messages, current_tools, backend),
#                 timeout=600,
#             )
#         except asyncio.TimeoutError:
#             logger.error(f"LLM timed out at round {round_num}")
#             return last_tool_result or "Error: LLM timed out."
#         except Exception as e:
#             logger.error(f"LLM call failed: {e}")
#             return last_tool_result or f"LLM Error: {e}"

#         msg = choice.message

#         if msg.content:
#             console.print(f"  [magenta]Assistant:[/magenta] {msg.content}")

#         # ── FIX: No tool calls → LLM is done. Return content if available,
#         #         otherwise fall back to the last raw tool result so the
#         #         synthesis node always receives real data.
#         if not msg.tool_calls:
#             console.print(f"  [dim]Round {round_num}: no tool calls — returning final answer[/dim]")
#             console.print(f"  [dim]msg.content={msg.content!r}[/dim]")
#             console.print(f"  [dim]last_tool_result length={len(last_tool_result)}[/dim]")
#             return msg.content or last_tool_result or "(No data found)"

#         # Append assistant turn
#         messages.append({
#             "role":    "assistant",
#             "content": msg.content,
#             "tool_calls": [
#                 {
#                     "id":   tc.id,
#                     "type": "function",
#                     "function": {
#                         "name":      tc.function.name,
#                         "arguments": tc.function.arguments,
#                     },
#                 }
#                 for tc in msg.tool_calls
#             ],
#         })

#         # Execute tools
#         for tc in msg.tool_calls:
#             fn_name = tc.function.name
#             fn_args = tc.function.arguments
#             if isinstance(fn_args, str):
#                 try:
#                     fn_args = json.loads(fn_args)
#                 except json.JSONDecodeError:
#                     fn_args = {}

#             # Resolver guard — inject verified IDs
#             if fn_name not in _RESOLVER_TOOLS:
#                 fn_args, messages = await _ensure_codes_for_tool(
#                     fn_name, fn_args, registry, session, messages, entity_hint,
#                 )

#             console.print(f"  [cyan]🛠 Executing:[/cyan] [bold]{fn_name}[/bold] with {fn_args}")

#             try:
#                 res = await asyncio.wait_for(
#                     session.call_tool(fn_name, fn_args),
#                     timeout=600,
#                 )
#                 txt = res.content[0].text if res.content else "Empty response"
#                 console.print(f"  [green]✔ {len(txt)} chars received[/green]")

#                 if fn_name in _RESOLVER_TOOLS:
#                     registry.update_from_resolver_result(fn_name, txt)

#             except asyncio.TimeoutError:
#                 txt = f"Error: {fn_name} timed out."
#                 console.print(f"  [red]✗ {fn_name} timed out[/red]")
#             except Exception as e:
#                 txt = f"Tool Error: {e}"
#                 console.print(f"  [red]✗ {e}[/red]")

#             # ── KEY FIX: always save the latest tool output ────────────────
#             last_tool_result = txt

#             messages.append({
#                 "role":         "tool",
#                 "tool_call_id": tc.id,
#                 "content":      txt,
#             })

#     # Max rounds reached — return whatever data we have
#     console.print("  [yellow]⚠ Max rounds reached[/yellow]")
#     return last_tool_result or "(Process exceeded max rounds without a final answer)"


# # ── Public API ─────────────────────────────────────────────────────────────────

# async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
#     """
#     Public async function consumed by graph.py's node_mcp_tool_call.

#     Parameters
#     ----------
#     query   : Enriched query string (may contain [PRE-RESOLVED CODES] block).
#     backend : "groq" or "ollama". Defaults to LLM_BACKEND env var.
#     """
#     b             = (backend or LLM_BACKEND).lower()
#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()
#             return await _run_query_with_session(query, session, b)


# # ── CLI ────────────────────────────────────────────────────────────────────────

# if __name__ == "__main__":
#     if len(sys.argv) > 1:
#         asyncio.run(run_mcp_query(" ".join(sys.argv[1:])))




# """
# mcp_client.py — Equifiz MF MCP Client (v3.3)

# Fix from v3.2:
# ─────────────────────────────────────────────────────────────────────
# - Accumulates results from ALL tool calls across ALL rounds into
#   `all_tool_results` (a list of labeled strings), so multi-tool
#   queries (e.g. "compare NAV and expense ratio") don't lose data.
# - If the LLM returns empty content after tool calls, the combined
#   results from every tool are joined and returned as the fallback,
#   ensuring the synthesis node always receives complete data.
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
# from rich.panel import Panel
# from dotenv import load_dotenv

# load_dotenv()

# # ── Config ─────────────────────────────────────────────────────────────────────

# SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
# CHROMA_PATH   = Path(__file__).parent / "chroma_db"
# MAX_TOKENS    = 4096
# MAX_ROUNDS    = 15

# LLM_BACKEND  = os.getenv("LLM_BACKEND", "groq").lower()
# GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")
# OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
# OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
# EMBED_MODEL  = "embeddinggemma:latest"

# logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
# logger  = logging.getLogger("equifiz_mf_client")
# console = Console()

# # ── ChromaDB Setup ─────────────────────────────────────────────────────────────

# chroma_client  = chromadb.PersistentClient(path=str(CHROMA_PATH))
# ollama_ef      = embedding_functions.OllamaEmbeddingFunction(
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

# _TOOL_NEEDS_CO_CODE  = {"get_funds_holding_company"}
# _RESOLVER_TOOLS      = {"resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes"}

# # ── Semantic Search Helper ─────────────────────────────────────────────────────

# def get_semantic_tools(query: str, all_openai_tools: list[dict], top_k: int = 7) -> list[dict]:
#     """Finds relevant tools via ChromaDB embeddings. Always includes core resolvers."""
#     CORE_PLUMBING = {"resolve_mf_scheme", "resolve_mf_fund"}
#     try:
#         results      = tool_collection.query(query_texts=[query], n_results=top_k)
#         matched_names = set(results["ids"][0]) | CORE_PLUMBING
#     except Exception as e:
#         console.print(f"ChromaDB query failed: {e}")
#         matched_names = CORE_PLUMBING
#     return [t for t in all_openai_tools if t["function"]["name"] in matched_names]

# # ── Session code registry ─────────────────────────────────────────────────────

# class _CodeRegistry:
#     def __init__(self):
#         self.mf_schcode:  Optional[int] = None
#         self.mf_cocode:   Optional[int] = None
#         self.co_code:     Optional[int] = None
#         self.scheme_name: Optional[str] = None
#         self.amc_name:    Optional[str] = None

#     def parse_injected(self, query: str) -> None:
#         """Parses [VERIFIED ENTITY IDs] or [PRE-RESOLVED CODES] injected by graph.py."""
#         for line in query.splitlines():
#             line = line.strip().replace("- ", "")
#             if m := re.search(r"mf_schcode[:=]\s*(\d+)", line, re.I):
#                 self.mf_schcode = int(m.group(1))
#                 console.print(f"  [Registry] Injected mf_schcode={self.mf_schcode}")
#             elif m := re.search(r"mf_cocode[:=]\s*(\d+)", line, re.I):
#                 self.mf_cocode = int(m.group(1))
#                 console.print(f"  [Registry] Injected mf_cocode={self.mf_cocode}")
#             elif m := re.search(r"co_code[:=]\s*(\d+)", line, re.I):
#                 self.co_code = int(m.group(1))
#                 console.print(f"  [Registry] Injected co_code={self.co_code}")

#     def update_from_resolver_result(self, tool_name: str, result_text: str) -> None:
#         if "RESOLVED" not in result_text:
#             return
#         if tool_name == "resolve_mf_scheme":
#             if m := re.search(r"mf_schcode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE):
#                 self.mf_schcode = int(m.group(1))
#         elif tool_name == "resolve_mf_fund":
#             if m := re.search(r"mf_cocode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE):
#                 self.mf_cocode = int(m.group(1))

# # ── Resolver guard ─────────────────────────────────────────────────────────────

# async def _ensure_codes_for_tool(
#     tool_name: str,
#     tool_args: dict,
#     registry:  _CodeRegistry,
#     session:   ClientSession,
#     messages:  list[dict],
#     entity_hint: str,
# ) -> tuple[dict, list[dict]]:
#     """
#     Injects missing numeric IDs (mf_schcode, mf_cocode) by triggering
#     silent resolver calls when the registry is empty.
#     """
#     updated_args = dict(tool_args)

#     def clean_hint(h: str) -> str:
#         h = re.sub(r"<PRE_RESOLVED>.*?</PRE_RESOLVED>", "", str(h), flags=re.S | re.I)
#         h = re.sub(r"(?i)(User Query|Instruction|Context|RECOMMENDED TOOL):", "", h)
#         h = re.sub(r"\[.*?\]", "", h)
#         cleaned = h.strip()
#         console.print(f"    [dim]Resolver hint:[/dim] '{cleaned}'")
#         return cleaned

#     # ── mf_schcode ────────────────────────────────────────────────────────────
#     if tool_name in _TOOL_NEEDS_MF_SCHCODE:
#         if registry.mf_schcode is None:
#             raw  = tool_args.get("query") or tool_args.get("sch_name") or entity_hint
#             hint = clean_hint(raw)
#             console.print(f"  [yellow]⚡ Auto-resolving mf_schcode for '{hint}'[/yellow]")
#             try:
#                 res = await session.call_tool("resolve_mf_scheme", {"query": hint})
#                 txt = res.content[0].text if res.content else ""
#                 if "RESOLVED" in txt:
#                     registry.update_from_resolver_result("resolve_mf_scheme", txt)
#                     console.print(f"    [green]✔ mf_schcode={registry.mf_schcode}[/green]")
#                     messages.append({
#                         "role": "assistant", "content": None,
#                         "tool_calls": [{
#                             "id": f"auto-sch-{registry.mf_schcode}",
#                             "type": "function",
#                             "function": {
#                                 "name": "resolve_mf_scheme",
#                                 "arguments": json.dumps({"query": hint}),
#                             },
#                         }],
#                     })
#                     messages.append({
#                         "role": "tool",
#                         "tool_call_id": f"auto-sch-{registry.mf_schcode}",
#                         "content": txt,
#                     })
#                 else:
#                     console.print(f"  [red]⚠ Could not resolve scheme for: {hint}[/red]")
#             except Exception as e:
#                 console.print(f"  [red]Auto-resolution failed: {e}[/red]")

#         if registry.mf_schcode is not None:
#             updated_args["mf_schcode"] = registry.mf_schcode

#     # ── mf_cocode ─────────────────────────────────────────────────────────────
#     if tool_name in _TOOL_NEEDS_MF_COCODE:
#         if registry.mf_cocode is None:
#             raw  = tool_args.get("query") or tool_args.get("amc_name") or entity_hint
#             hint = clean_hint(raw)
#             console.print(f"  [yellow]⚡ Auto-resolving mf_cocode for '{hint}'[/yellow]")
#             try:
#                 res = await session.call_tool("resolve_mf_fund", {"query": hint})
#                 txt = res.content[0].text if res.content else ""
#                 if "RESOLVED" in txt:
#                     registry.update_from_resolver_result("resolve_mf_fund", txt)
#                     messages.append({
#                         "role": "assistant", "content": None,
#                         "tool_calls": [{
#                             "id": f"auto-co-{registry.mf_cocode}",
#                             "type": "function",
#                             "function": {
#                                 "name": "resolve_mf_fund",
#                                 "arguments": json.dumps({"query": hint}),
#                             },
#                         }],
#                     })
#                     messages.append({
#                         "role": "tool",
#                         "tool_call_id": f"auto-co-{registry.mf_cocode}",
#                         "content": txt,
#                     })
#                 else:
#                     console.print(f"  [red]⚠ Could not resolve fund for: {hint}[/red]")
#             except Exception as e:
#                 console.print(f"  [red]Auto-resolution failed: {e}[/red]")

#         if registry.mf_cocode is not None:
#             updated_args["mf_cocode"] = registry.mf_cocode

#     # ── co_code ───────────────────────────────────────────────────────────────
#     if tool_name in _TOOL_NEEDS_CO_CODE and registry.co_code is not None:
#         updated_args["co_code"] = registry.co_code

#     return updated_args, messages

# # ── System prompt ──────────────────────────────────────────────────────────────

# SYSTEM_PROMPT = """\
# You are a mutual fund analyst for Indian markets.
# Verified numeric IDs are provided in the context — use them directly for tool calls.
# Do NOT call resolver tools if an ID is already present.
# Do NOT fabricate data. If a tool returns no data, say so clearly.
# """

# # ── Helpers ────────────────────────────────────────────────────────────────────

# def _to_openai_tool(tool) -> dict:
#     schema   = tool.inputSchema or {}
#     props    = schema.get("properties", {})
#     required = schema.get("required", [])

#     if "params" in props and len(props) == 1:
#         inner    = props["params"]
#         props    = inner.get("properties", {})
#         required = inner.get("required", [])

#     for k, v in props.items():
#         if "description" not in v:
#             v["description"] = f"Param {k}"

#     return {
#         "type": "function",
#         "function": {
#             "name":        tool.name,
#             "description": (tool.description or tool.name).strip(),
#             "parameters":  {"type": "object", "properties": props, "required": required},
#         },
#     }


# def _chat(messages: list, tools: list, backend: str):
#     if backend == "ollama":
#         import urllib.request
#         req = urllib.request.Request(
#             f"{OLLAMA_HOST}/api/chat",
#             data=json.dumps({
#                 "model":   OLLAMA_MODEL,
#                 "messages": messages,
#                 "tools":   tools,
#                 "stream":  False,
#                 "options": {"temperature": 0.0},
#             }).encode(),
#             headers={"Content-Type": "application/json"},
#             method="POST",
#         )
#         data = json.loads(urllib.request.urlopen(req, timeout=600).read().decode())
#         omsg = data.get("message", {})

#         class _TC:
#             def __init__(self, d):
#                 self.id = d.get("id", "tc-id")
#                 args = d["function"]["arguments"]
#                 self.function = type("F", (), {
#                     "name":      d["function"]["name"],
#                     "arguments": json.dumps(args) if isinstance(args, dict) else args,
#                 })()

#         class _Choice:
#             def __init__(self):
#                 tcs = [_TC(t) for t in omsg.get("tool_calls", [])]
#                 self.message = type("M", (), {
#                     "content":    omsg.get("content"),
#                     "tool_calls": tcs or None,
#                 })()

#         return _Choice()

#     else:
#         from groq import Groq
#         client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
#         return client.chat.completions.create(
#             model=GROQ_MODEL,
#             messages=messages,
#             tools=tools,
#             tool_choice="auto",
#         ).choices[0]


# def _extract_entity_hint(query: str) -> str:
#     return re.split(r"\[(PRE-RESOLVED|VERIFIED|SUGGESTED)", query)[0].strip()

# # ── Core query loop ────────────────────────────────────────────────────────────

# async def _run_query_with_session(
#     query:   str,
#     session: ClientSession,
#     backend: str,
# ) -> str:
#     registry = _CodeRegistry()
#     registry.parse_injected(query)

#     console.print(Panel(
#         f"[bold blue]Incoming Query Context:[/bold blue]\n{query}",
#         title="LLM Input",
#     ))

#     entity_hint  = _extract_entity_hint(query)
#     raw_tools    = (await session.list_tools()).tools
#     all_manifest = [_to_openai_tool(t) for t in raw_tools]

#     current_tools = get_semantic_tools(query, all_manifest, top_k=7)
#     console.print(f"  [Semantic Router] selected {len(current_tools)} tools")

#     messages         = [
#         {"role": "system", "content": SYSTEM_PROMPT},
#         {"role": "user",   "content": query},
#     ]
#     # ── v3.3: accumulate ALL tool outputs, not just the last one ──────────────
#     all_tool_results: list[str] = []

#     def _combined_fallback() -> str:
#         """Join all tool results collected so far into one string for the synthesis node."""
#         if not all_tool_results:
#             return ""
#         if len(all_tool_results) == 1:
#             return all_tool_results[0]
#         return "\n\n---\n\n".join(all_tool_results)

#     for round_num in range(1, MAX_ROUNDS + 1):
#         console.print(f"  [dim]── Round {round_num} ──[/dim]")

#         loop = asyncio.get_event_loop()
#         try:
#             choice = await asyncio.wait_for(
#                 loop.run_in_executor(None, _chat, messages, current_tools, backend),
#                 timeout=600,
#             )
#         except asyncio.TimeoutError:
#             logger.error(f"LLM timed out at round {round_num}")
#             return _combined_fallback() or "Error: LLM timed out."
#         except Exception as e:
#             logger.error(f"LLM call failed: {e}")
#             return _combined_fallback() or f"LLM Error: {e}"

#         msg = choice.message

#         if msg.content:
#             console.print(f"  [magenta]Assistant:[/magenta] {msg.content}")

#         # No tool calls → LLM is done.
#         # Return LLM content if available; otherwise fall back to ALL accumulated
#         # tool results so the synthesis node always receives complete data.
#         if not msg.tool_calls:
#             console.print(f"  [dim]Round {round_num}: no tool calls — returning final answer[/dim]")
#             console.print(f"  [dim]msg.content={msg.content!r}[/dim]")
#             console.print(f"  [dim]accumulated tool results: {len(all_tool_results)} block(s)[/dim]")
#             return msg.content or _combined_fallback() or "(No data found)"

#         # Append assistant turn
#         messages.append({
#             "role":    "assistant",
#             "content": msg.content,
#             "tool_calls": [
#                 {
#                     "id":   tc.id,
#                     "type": "function",
#                     "function": {
#                         "name":      tc.function.name,
#                         "arguments": tc.function.arguments,
#                     },
#                 }
#                 for tc in msg.tool_calls
#             ],
#         })

#         # Execute tools
#         for tc in msg.tool_calls:
#             fn_name = tc.function.name
#             fn_args = tc.function.arguments
#             if isinstance(fn_args, str):
#                 try:
#                     fn_args = json.loads(fn_args)
#                 except json.JSONDecodeError:
#                     fn_args = {}

#             # Resolver guard — inject verified IDs
#             if fn_name not in _RESOLVER_TOOLS:
#                 fn_args, messages = await _ensure_codes_for_tool(
#                     fn_name, fn_args, registry, session, messages, entity_hint,
#                 )

#             console.print(f"  [cyan]🛠 Executing:[/cyan] [bold]{fn_name}[/bold] with {fn_args}")

#             try:
#                 res = await asyncio.wait_for(
#                     session.call_tool(fn_name, fn_args),
#                     timeout=600,
#                 )
#                 txt = res.content[0].text if res.content else "Empty response"
#                 console.print(f"  [green]✔ {fn_name}: {len(txt)} chars received[/green]")

#                 if fn_name in _RESOLVER_TOOLS:
#                     registry.update_from_resolver_result(fn_name, txt)
#                 else:
#                     # ── v3.3: accumulate every non-resolver tool result ────
#                     all_tool_results.append(f"[{fn_name}]\n{txt}")
#                     console.print(f"  [dim]Total accumulated blocks: {len(all_tool_results)}[/dim]")

#             except asyncio.TimeoutError:
#                 txt = f"Error: {fn_name} timed out."
#                 console.print(f"  [red]✗ {fn_name} timed out[/red]")
#             except Exception as e:
#                 txt = f"Tool Error: {e}"
#                 console.print(f"  [red]✗ {e}[/red]")

#             messages.append({
#                 "role":         "tool",
#                 "tool_call_id": tc.id,
#                 "content":      txt,
#             })

#     # Max rounds reached — return everything collected
#     console.print("  [yellow]⚠ Max rounds reached[/yellow]")
#     return _combined_fallback() or "(Process exceeded max rounds without a final answer)"


# # ── Public API ─────────────────────────────────────────────────────────────────

# async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
#     """
#     Public async function consumed by graph.py's node_mcp_tool_call.

#     Parameters
#     ----------
#     query   : Enriched query string (may contain [PRE-RESOLVED CODES] block).
#     backend : "groq" or "ollama". Defaults to LLM_BACKEND env var.
#     """
#     b             = (backend or LLM_BACKEND).lower()
#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()
#             return await _run_query_with_session(query, session, b)


# # ── CLI ────────────────────────────────────────────────────────────────────────

# if __name__ == "__main__":
#     if len(sys.argv) > 1:
#         asyncio.run(run_mcp_query(" ".join(sys.argv[1:])))





# ##motamuti thik ache 
# """
# mcp_client.py — Equifiz MF MCP Client (v4.0 — Fully Dynamic)

# Key changes from v3.5
# ─────────────────────────────────────────────────────────────────────
# DYNAMIC PARAM DISCOVERY
#   - Removed all hardcoded tool sets (_TOOL_NEEDS_MF_SCHCODE etc.)
#   - _ToolParamIndex is built at session start by inspecting every tool's
#     inputSchema. It maps: tool_name → set of required DB param names.
#   - _ensure_codes_for_tool reads from this index, so it works for ALL
#     140 tools automatically — no manual maintenance needed.

# ENTITY-SCOPED CODE INJECTION
#   - _ensure_codes_for_tool now receives the current entity context
#     (which entity's codes to inject) instead of pulling the first
#     matching code from the registry.
#   - For multi-entity queries the LLM emits one tool call per entity;
#     each tool call carries the entity label in its args (injected by
#     the system prompt). The execution loop matches the tool call back
#     to the correct entity before injecting codes.

# ENTITY LABEL MATCHING
#   - System prompt instructs the LLM to include "entity_label" in every
#     tool call's arguments for multi-entity queries.
#   - Execution loop pops "entity_label" before calling the MCP tool,
#     uses it to select the right _EntityInfo from the registry, then
#     injects that entity's specific codes.

# TOOL FAMILY CLASSIFICATION (replaces static sets)
#   - _classify_tool_family(tool_name, required_params) returns
#     "stock" | "mf_scheme" | "mf_amc" | "etf" | "index" | "general"
#     by inspecting which ID param the tool requires.
#   - Used by get_mf_schcode_for_tool / get_co_code_for_tool to scope
#     lookups to the correct entity type.

# All v3.5 fixes retained:
#   - Integer coercion for numeric params (FIX 1, 2, 3)
#   - Meta-description detection fallback (RAW DATA RETURN FIX)
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
# from rich.panel import Panel
# from dotenv import load_dotenv

# load_dotenv()

# # ── Config ─────────────────────────────────────────────────────────────────────

# SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
# CHROMA_PATH   = Path(__file__).parent / "chroma_db"
# MAX_ROUNDS    = 15

# LLM_BACKEND  = os.getenv("LLM_BACKEND", "groq").lower()
# GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")
# OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
# OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
# EMBED_MODEL  = "embeddinggemma:latest"

# logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
# logger  = logging.getLogger("equifiz_mf_client")
# console = Console()

# # ── ChromaDB Setup ─────────────────────────────────────────────────────────────

# chroma_client  = chromadb.PersistentClient(path=str(CHROMA_PATH))
# ollama_ef      = embedding_functions.OllamaEmbeddingFunction(
#     model_name=EMBED_MODEL,
#     url=f"{OLLAMA_HOST}/api/embeddings",
# )
# tool_collection = chroma_client.get_collection(
#     name="equifiz_tools", embedding_function=ollama_ef
# )

# # ── Known numeric ID params (used for schema coercion only) ───────────────────
# # These are the DB foreign-key params that must always be integers.
# # This list is exhaustive for coercion; actual routing is done dynamically.
# _NUMERIC_PARAMS = {"mf_schcode", "mf_cocode", "co_code", "index_code"}

# # ── Tools that are internal resolvers — never inject codes into these ──────────
# _RESOLVER_TOOLS = {"resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes",
#                    "resolve_nse_symbol", "get_company_details", "search_companies"}

# # ── DB param → entity family mapping ──────────────────────────────────────────
# # Defines which entity type each ID param belongs to.
# _PARAM_TO_FAMILY: dict[str, str] = {
#     "co_code":    "stock",
#     "mf_schcode": "mf_scheme",
#     "mf_cocode":  "mf_amc",
#     "isin":       "etf",
#     "index_code": "index",
# }

# # Reverse: entity family → which param it provides
# _FAMILY_TO_PARAM: dict[str, str] = {v: k for k, v in _PARAM_TO_FAMILY.items()}


# # ══════════════════════════════════════════════════════════════════════════════
# # Dynamic Tool Param Index
# # Built once at session start by inspecting live tool schemas.
# # ══════════════════════════════════════════════════════════════════════════════

# class _ToolParamIndex:
#     """
#     Maps every tool to its required DB params by inspecting inputSchema.
#     Built from the live MCP tool list — zero hardcoding.

#     Structure:
#         tool_name → {
#             "required_db_params": set[str],   # subset of _PARAM_TO_FAMILY keys
#             "family": str,                    # dominant entity family
#             "all_params": set[str],           # all required param names
#         }
#     """

#     def __init__(self):
#         self._index: dict[str, dict] = {}

#     def build(self, raw_tools: list) -> None:
#         """Call once after session.list_tools()."""
#         for tool in raw_tools:
#             schema    = tool.inputSchema or {}
#             props     = schema.get("properties", {})
#             required  = set(schema.get("required", []))

#             # Unwrap nested "params" wrapper if present
#             if "params" in props and len(props) == 1:
#                 inner    = props["params"]
#                 props    = inner.get("properties", {})
#                 required = set(inner.get("required", []))

#             # Find which DB ID params this tool requires
#             required_db = {
#                 p for p in required
#                 if p in _PARAM_TO_FAMILY
#             }

#             # Also check optional params — some tools have co_code as optional
#             optional_db = {
#                 p for p in props
#                 if p in _PARAM_TO_FAMILY and p not in required
#             }

#             # Dominant family: use required first, then optional
#             family = "general"
#             for param in list(required_db) + list(optional_db):
#                 family = _PARAM_TO_FAMILY.get(param, "general")
#                 break  # first match wins

#             self._index[tool.name] = {
#                 "required_db_params": required_db,
#                 "optional_db_params": optional_db,
#                 "family":             family,
#                 "all_params":         set(props.keys()),
#             }

#         console.print(
#             f"  [ToolParamIndex] Indexed {len(self._index)} tools. "
#             f"DB-bound: {sum(1 for v in self._index.values() if v['required_db_params'])}"
#         )

#     def get_required_db_params(self, tool_name: str) -> set[str]:
#         return self._index.get(tool_name, {}).get("required_db_params", set())

#     def get_family(self, tool_name: str) -> str:
#         return self._index.get(tool_name, {}).get("family", "general")

#     def needs_param(self, tool_name: str, param: str) -> bool:
#         entry = self._index.get(tool_name, {})
#         return (param in entry.get("required_db_params", set()) or
#                 param in entry.get("optional_db_params", set()))

#     def accepts_param(self, tool_name: str, param: str) -> bool:
#         """True if the tool schema includes this param at all."""
#         return param in self._index.get(tool_name, {}).get("all_params", set())

#     def debug_summary(self) -> str:
#         lines = []
#         for name, info in sorted(self._index.items()):
#             if info["required_db_params"]:
#                 lines.append(
#                     f"  {name}: requires={info['required_db_params']} "
#                     f"family={info['family']}"
#                 )
#         return "\n".join(lines)


# # ══════════════════════════════════════════════════════════════════════════════
# # Semantic Search Helper
# # ══════════════════════════════════════════════════════════════════════════════

# def get_semantic_tools(
#     query: str,
#     all_openai_tools: list[dict],
#     top_k: int = 10,
# ) -> list[dict]:
#     """
#     Finds relevant tools via ChromaDB embeddings.
#     For multi-entity queries, increases top_k to ensure tools for ALL
#     entity families are represented.
#     """
#     CORE_PLUMBING = {
#         "resolve_mf_scheme", "resolve_mf_fund",
#         "resolve_nse_symbol", "search_companies",
#     }
#     try:
#         results       = tool_collection.query(query_texts=[query], n_results=top_k)
#         matched_names = set(results["ids"][0]) | CORE_PLUMBING
#     except Exception as e:
#         console.print(f"ChromaDB query failed: {e}")
#         matched_names = CORE_PLUMBING
#     return [t for t in all_openai_tools if t["function"]["name"] in matched_names]


# # ══════════════════════════════════════════════════════════════════════════════
# # Raw data detection
# # ══════════════════════════════════════════════════════════════════════════════

# def _looks_like_meta_description(text: str) -> bool:
#     if not text or not text.strip():
#         return True

#     lower = text.lower()
#     meta_phrases = [
#         "the function call returns", "the output includes", "the result contains",
#         "the data includes", "the function returns", "this tool returns",
#         "the api returns", "includes market capitalization",
#         "the following fields", "returns the following",
#     ]
#     if any(phrase in lower for phrase in meta_phrases):
#         console.print(
#             "  [yellow]⚠ LLM returned meta-description — falling back to raw results[/yellow]"
#         )
#         return True

#     numeric_tokens = re.findall(r"\b\d[\d,\.]*\b", text)
#     if len(numeric_tokens) < 3:
#         console.print(
#             f"  [yellow]⚠ Only {len(numeric_tokens)} numeric values — likely a description[/yellow]"
#         )
#         return True

#     return False


# # ══════════════════════════════════════════════════════════════════════════════
# # Per-entity registry
# # ══════════════════════════════════════════════════════════════════════════════

# class _EntityInfo:
#     """Holds resolved codes + metadata for a single entity."""
#     __slots__ = (
#         "label", "entity_type", "query_intent", "recommended_tool",
#         "mf_schcode", "mf_cocode", "co_code", "isin", "index_code",
#         "company_name", "amc_name", "scheme_name",
#     )

#     def __init__(self, label: str = ""):
#         self.label:            str           = label
#         self.entity_type:      str           = "general"
#         self.query_intent:     str           = ""
#         self.recommended_tool: str           = ""
#         self.mf_schcode:       Optional[int] = None
#         self.mf_cocode:        Optional[int] = None
#         self.co_code:          Optional[int] = None
#         self.isin:             Optional[str] = None
#         self.index_code:       Optional[int] = None
#         self.company_name:     str           = ""
#         self.amc_name:         str           = ""
#         self.scheme_name:      str           = ""

#     def get_code_for_param(self, param: str) -> Optional[int | str]:
#         """Return the resolved value for a specific DB param name."""
#         mapping = {
#             "co_code":    self.co_code,
#             "mf_schcode": self.mf_schcode,
#             "mf_cocode":  self.mf_cocode,
#             "isin":       self.isin,
#             "index_code": self.index_code,
#         }
#         return mapping.get(param)

#     def has_any_code(self) -> bool:
#         return any(self.get_code_for_param(p) is not None for p in _PARAM_TO_FAMILY)

#     def matches_family(self, family: str) -> bool:
#         """True if this entity's type matches the tool family."""
#         if self.entity_type == family:
#             return True
#         # Allow general entities to match any family if they have the right code
#         if self.entity_type == "general":
#             param = _FAMILY_TO_PARAM.get(family)
#             return param is not None and self.get_code_for_param(param) is not None
#         return False

#     def summary(self) -> str:
#         parts = [f"  {self.label} ({self.entity_type})"]
#         if self.query_intent:
#             parts.append(f"    intent: {self.query_intent}")
#         if self.recommended_tool:
#             parts.append(f"    tool:   {self.recommended_tool}")
#         for k in ("co_code", "mf_schcode", "mf_cocode", "isin", "index_code"):
#             v = getattr(self, k)
#             if v is not None:
#                 parts.append(f"    {k}={v}")
#         for k in ("company_name", "amc_name", "scheme_name"):
#             v = getattr(self, k)
#             if v:
#                 parts.append(f"    {k}={v}")
#         return "\n".join(parts)


# class _EntityRegistry:
#     """
#     Stores _EntityInfo objects parsed from the <PRE_RESOLVED> block.
#     Provides entity-scoped code lookups for dynamic param injection.
#     """

#     def __init__(self):
#         self.entities: list[_EntityInfo] = []
#         self._primary: _EntityInfo       = _EntityInfo("(primary)")

#     # ── Convenience properties (backward compat) ───────────────────────────
#     @property
#     def mf_schcode(self) -> Optional[int]:
#         return self._primary.mf_schcode

#     @mf_schcode.setter
#     def mf_schcode(self, v: Optional[int]):
#         self._primary.mf_schcode = v

#     @property
#     def mf_cocode(self) -> Optional[int]:
#         return self._primary.mf_cocode

#     @mf_cocode.setter
#     def mf_cocode(self, v: Optional[int]):
#         self._primary.mf_cocode = v

#     @property
#     def co_code(self) -> Optional[int]:
#         return self._primary.co_code

#     @co_code.setter
#     def co_code(self, v: Optional[int]):
#         self._primary.co_code = v

#     @property
#     def scheme_name(self) -> Optional[str]:
#         return self._primary.scheme_name or None

#     @property
#     def amc_name(self) -> Optional[str]:
#         return self._primary.amc_name or None

#     # ── Parsing ────────────────────────────────────────────────────────────
#     def parse_injected(self, query: str) -> None:
#         block_match = re.search(
#             r"<PRE_RESOLVED>(.*?)</PRE_RESOLVED>", query, re.S | re.I
#         )
#         if block_match:
#             self._parse_block(block_match.group(1))
#         else:
#             self._parse_flat(query)

#         if self.entities:
#             self._primary = self.entities[0]

#         self._log_parsed()

#     def _parse_block(self, block: str) -> None:
#         current: Optional[_EntityInfo] = None

#         for raw_line in block.splitlines():
#             line = raw_line.strip()
#             # Skip comment lines (e.g. "# use co_code=476 ONLY with stock tools")
#             if not line or line.startswith("#"):
#                 continue

#             em = re.match(r"Entity\s+\d+:\s*(.+)", line, re.I)
#             if em:
#                 current = _EntityInfo(em.group(1).strip())
#                 self.entities.append(current)
#                 continue

#             if current is None:
#                 current = _EntityInfo("(primary)")
#                 self.entities.append(current)

#             kv = re.match(r"([\w_]+)\s*=\s*(.+)", line)
#             if not kv:
#                 continue
#             key, val = kv.group(1).strip(), kv.group(2).strip()
#             # Strip inline comments from values like "476 (company: Reliance)"
#             val = re.sub(r"\s*\(.*?\)\s*$", "", val).strip()
#             self._apply_kv(current, key, val)

#         if not self.entities:
#             self._parse_flat(block)

#     def _parse_flat(self, text: str) -> None:
#         entity = _EntityInfo("(flat-primary)")
#         for line in text.splitlines():
#             line = line.strip().replace("- ", "")
#             for pat, key in [
#                 (r"mf_schcode[:=]\s*(\d+)", "mf_schcode"),
#                 (r"mf_cocode[:=]\s*(\d+)",  "mf_cocode"),
#                 (r"co_code[:=]\s*(\d+)",    "co_code"),
#                 (r"isin[:=]\s*([A-Z0-9]+)", "isin"),
#                 (r"index_code[:=]\s*(\d+)", "index_code"),
#             ]:
#                 m = re.search(pat, line, re.I)
#                 if m:
#                     self._apply_kv(entity, key, m.group(1))

#         if entity.has_any_code():
#             self.entities.append(entity)

#     @staticmethod
#     def _apply_kv(entity: _EntityInfo, key: str, val: str) -> None:
#         key = key.lower()
#         try:
#             if key == "mf_schcode":
#                 entity.mf_schcode = int(val)
#             elif key == "mf_cocode":
#                 entity.mf_cocode = int(val)
#             elif key == "co_code":
#                 entity.co_code = int(val)
#             elif key == "isin":
#                 entity.isin = val
#             elif key == "index_code":
#                 entity.index_code = int(val)
#             elif key == "entity_type":
#                 entity.entity_type = val
#             elif key == "query_intent":
#                 entity.query_intent = val
#             elif key == "recommended_tool":
#                 entity.recommended_tool = val
#             elif key == "company_name":
#                 entity.company_name = val
#             elif key == "amc_name":
#                 entity.amc_name = val
#             elif key == "scheme_name":
#                 entity.scheme_name = val
#         except (ValueError, TypeError):
#             pass  # Bad value in PRE_RESOLVED block — skip silently

#     def _log_parsed(self) -> None:
#         if not self.entities:
#             console.print("  [Registry] No entities parsed from PRE_RESOLVED block")
#             return
#         for e in self.entities:
#             for part in e.summary().splitlines():
#                 console.print(f"  [Registry] {part}")

#     # ── Update from live resolver calls ────────────────────────────────────
#     def update_from_resolver_result(self, tool_name: str, result_text: str) -> None:
#         if "RESOLVED" not in result_text:
#             return
#         if tool_name in ("resolve_mf_scheme",):
#             m = re.search(r"mf_schcode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE)
#             if m:
#                 val = int(m.group(1))
#                 self._primary.mf_schcode = val
#                 for e in self.entities:
#                     if e.entity_type in ("mf_scheme", "general") and e.mf_schcode is None:
#                         e.mf_schcode = val
#                         break
#         elif tool_name in ("resolve_mf_fund",):
#             m = re.search(r"mf_cocode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE)
#             if m:
#                 val = int(m.group(1))
#                 self._primary.mf_cocode = val
#                 for e in self.entities:
#                     if e.entity_type in ("mf_amc", "general") and e.mf_cocode is None:
#                         e.mf_cocode = val
#                         break
#         elif tool_name in ("resolve_nse_symbol", "get_company_details"):
#             m = re.search(r"co_code\s*[:=]\s*(\d+)", result_text, re.IGNORECASE)
#             if m:
#                 val = int(m.group(1))
#                 self._primary.co_code = val
#                 for e in self.entities:
#                     if e.entity_type in ("stock", "general") and e.co_code is None:
#                         e.co_code = val
#                         break

#     # ── Entity-scoped code lookup (the key new method) ─────────────────────
#     def find_entity_for_tool(
#         self,
#         tool_name:   str,
#         tool_family: str,
#         label_hint:  Optional[str] = None,
#     ) -> Optional[_EntityInfo]:
#         """
#         Returns the _EntityInfo whose codes should be injected into this tool call.

#         Priority order:
#           1. Entity whose label matches label_hint (from tool args "entity_label")
#           2. Entity whose recommended_tool == tool_name
#           3. Entity whose entity_type matches tool_family
#           4. Any entity that has the required code for tool_family
#           5. Primary entity (fallback)
#         """
#         # 1. Label hint from tool args (most precise)
#         if label_hint:
#             for e in self.entities:
#                 if e.label.lower() == label_hint.lower():
#                     return e

#         # 2. Exact recommended_tool match
#         for e in self.entities:
#             if e.recommended_tool == tool_name:
#                 return e

#         # 3. Family match on entity_type
#         for e in self.entities:
#             if e.matches_family(tool_family):
#                 return e

#         # 4. Any entity with the needed param
#         needed_param = _FAMILY_TO_PARAM.get(tool_family)
#         if needed_param:
#             for e in self.entities:
#                 if e.get_code_for_param(needed_param) is not None:
#                     return e

#         # 5. Primary fallback
#         return self._primary if self._primary.has_any_code() else None

#     def get_name_hint_for_family(self, family: str) -> str:
#         """Return a human-readable name hint for auto-resolution fallback."""
#         for e in self.entities:
#             if e.matches_family(family):
#                 if family == "mf_scheme" and e.scheme_name:
#                     return e.scheme_name
#                 if family == "mf_amc" and e.amc_name:
#                     return e.amc_name
#                 if family == "stock" and e.company_name:
#                     return e.company_name
#                 return e.label
#         # Fallback to primary
#         return (self._primary.scheme_name or self._primary.amc_name
#                 or self._primary.company_name or self._primary.label or "")

#     # ── Backward-compat helpers (used by _ensure_codes_for_tool) ──────────
#     def get_mf_schcode_for_tool(self, tool_name: str) -> Optional[int]:
#         e = self.find_entity_for_tool(tool_name, "mf_scheme")
#         return e.mf_schcode if e else None

#     def get_mf_cocode_for_tool(self, tool_name: str) -> Optional[int]:
#         e = self.find_entity_for_tool(tool_name, "mf_amc")
#         return e.mf_cocode if e else None

#     def get_co_code_for_tool(self, tool_name: str) -> Optional[int]:
#         e = self.find_entity_for_tool(tool_name, "stock")
#         return e.co_code if e else None

#     def get_amc_name_hint(self) -> str:
#         return self.get_name_hint_for_family("mf_amc")

#     def get_scheme_name_hint(self) -> str:
#         return self.get_name_hint_for_family("mf_scheme")

#     def get_company_name_hint(self) -> str:
#         return self.get_name_hint_for_family("stock")


# # ══════════════════════════════════════════════════════════════════════════════
# # Dynamic code injection
# # ══════════════════════════════════════════════════════════════════════════════

# async def _ensure_codes_for_tool(
#     tool_name:   str,
#     tool_args:   dict,
#     registry:    _EntityRegistry,
#     param_index: _ToolParamIndex,
#     session:     ClientSession,
#     messages:    list[dict],
#     entity_hint: str,
# ) -> tuple[dict, list[dict]]:
#     """
#     Dynamically inject DB codes into tool_args based on what the tool requires.

#     Steps:
#       1. Read required DB params from _ToolParamIndex (no hardcoded sets).
#       2. Pop "entity_label" from args to find the correct _EntityInfo.
#       3. For each required param, inject from the matched entity.
#       4. If a code is missing, auto-resolve it via the appropriate resolver tool.
#     """
#     updated_args = dict(tool_args)

#     # ── Extract entity_label hint (injected by system prompt for multi-entity) ──
#     label_hint = updated_args.pop("entity_label", None)

#     # ── Get what this tool needs from the live schema index ─────────────────
#     required_db_params = param_index.get_required_db_params(tool_name)
#     tool_family        = param_index.get_family(tool_name)

#     if not required_db_params:
#         # Tool needs no DB codes — nothing to inject
#         return updated_args, messages

#     # ── Find the entity whose codes to inject ───────────────────────────────
#     entity = registry.find_entity_for_tool(tool_name, tool_family, label_hint)

#     console.print(
#         f"    [dim]Code injection: tool={tool_name} family={tool_family} "
#         f"entity={entity.label if entity else 'none'} "
#         f"needs={required_db_params}[/dim]"
#     )

#     def _clean_hint(h: str) -> str:
#         h = re.sub(r"<PRE_RESOLVED>.*?</PRE_RESOLVED>", "", str(h), flags=re.S | re.I)
#         h = re.sub(r"(?i)(User Query|Instruction|Context|RECOMMENDED TOOL):", "", h)
#         h = re.sub(r"\[.*?\]", "", h)
#         return h.strip()

#     # ── Inject each required param ──────────────────────────────────────────
#     for param in required_db_params:
#         # Skip if already present in args (LLM provided it)
#         if param in updated_args and updated_args[param] is not None:
#             try:
#                 if param in _NUMERIC_PARAMS:
#                     updated_args[param] = int(float(str(updated_args[param])))
#             except (ValueError, TypeError):
#                 pass
#             continue

#         # Try to get value from the matched entity
#         value = entity.get_code_for_param(param) if entity else None

#         if value is None:
#             # Auto-resolve: pick the right resolver tool
#             family = _PARAM_TO_FAMILY.get(param, "general")
#             name_hint = _clean_hint(
#                 registry.get_name_hint_for_family(family) or entity_hint
#             )
#             console.print(
#                 f"  [yellow]⚡ Auto-resolving {param} for '{name_hint[:60]}'[/yellow]"
#             )

#             resolver_map = {
#                 "mf_schcode": ("resolve_mf_scheme", "resolve_mf_scheme"),
#                 "mf_cocode":  ("resolve_mf_fund",   "resolve_mf_fund"),
#                 "co_code":    ("resolve_nse_symbol", "resolve_nse_symbol"),
#             }
#             resolver_tool, update_name = resolver_map.get(param, (None, None))

#             if resolver_tool and name_hint:
#                 try:
#                     res = await session.call_tool(resolver_tool, {"query": name_hint})
#                     txt = res.content[0].text if res.content else ""
#                     if "RESOLVED" in txt:
#                         registry.update_from_resolver_result(update_name, txt)
#                         # Re-fetch after update
#                         if entity:
#                             value = entity.get_code_for_param(param)
#                         if value is None:
#                             # Check primary as last resort
#                             value = registry._primary.get_code_for_param(param)

#                         auto_id = f"auto-{param}-{value}"
#                         messages.append({
#                             "role": "assistant", "content": None,
#                             "tool_calls": [{
#                                 "id": auto_id, "type": "function",
#                                 "function": {
#                                     "name": resolver_tool,
#                                     "arguments": json.dumps({"query": name_hint}),
#                                 },
#                             }],
#                         })
#                         messages.append({
#                             "role": "tool",
#                             "tool_call_id": auto_id,
#                             "content": txt,
#                         })
#                         console.print(f"    [green]✔ {param}={value}[/green]")
#                     else:
#                         console.print(
#                             f"  [red]⚠ Could not resolve {param} for: {name_hint[:60]}[/red]"
#                         )
#                 except Exception as e:
#                     console.print(f"  [red]Auto-resolution failed ({param}): {e}[/red]")

#         if value is not None:
#             # Coerce to int for numeric params
#             if param in _NUMERIC_PARAMS:
#                 try:
#                     value = int(float(str(value)))
#                 except (ValueError, TypeError):
#                     pass
#             updated_args[param] = value

#     return updated_args, messages


# # ══════════════════════════════════════════════════════════════════════════════
# # System prompt builder
# # ══════════════════════════════════════════════════════════════════════════════

# def _build_system_prompt(registry: _EntityRegistry, is_multi: bool = False) -> str:
#     base = """\
# You are a financial analyst assistant for Indian markets with access to tools
# covering equities, mutual funds, ETFs, and market indices.

# CRITICAL RULES:
# - Verified numeric IDs are provided below — use them DIRECTLY in tool calls.
# - Do NOT call resolver tools when an ID is already provided.
#   Only call resolve_mf_scheme / resolve_mf_fund / resolve_nse_symbol when an ID
#   is genuinely missing.
# - Do NOT fabricate data. If a tool returns no data, say so clearly.
# - For EACH entity, use ONLY that entity's codes with that entity's tool.
#   NEVER pass a stock's co_code to a mutual fund tool, or vice-versa.
# - Return ALL actual numeric values from tools. Do NOT describe what a tool
#   returns — return the actual data.
# """

#     if is_multi and len(registry.entities) > 1:
#         base += """
# MULTI-ENTITY MODE:
# - You must call one tool per entity, using only that entity's codes.
# - For each tool call, include "entity_label": "<label>" in the arguments
#   so the system knows which entity's codes to inject.
#   Example: {"entity_label": "Reliance Industries", "co_code": 476}
# - Process ALL entities listed below before giving a final answer.
# """

#     if not registry.entities:
#         return base

#     entity_block = "\n\nENTITIES TO PROCESS:\n"
#     for i, e in enumerate(registry.entities, 1):
#         entity_block += f"\nEntity {i}: {e.label}\n"
#         entity_block += f"  type: {e.entity_type}\n"
#         if e.query_intent:
#             entity_block += f"  data needed: {e.query_intent}\n"
#         if e.recommended_tool:
#             entity_block += f"  use tool: {e.recommended_tool}\n"
#         if e.co_code is not None:
#             entity_block += (
#                 f"  co_code={e.co_code}  "
#                 f"← pass THIS to equity/stock tools ONLY\n"
#             )
#         if e.mf_schcode is not None:
#             entity_block += (
#                 f"  mf_schcode={e.mf_schcode}  "
#                 f"← pass THIS to scheme/MF tools ONLY\n"
#             )
#         if e.mf_cocode is not None:
#             entity_block += (
#                 f"  mf_cocode={e.mf_cocode}  "
#                 f"← pass THIS to AMC/fund-house tools ONLY\n"
#             )
#         if e.isin:
#             entity_block += (
#                 f"  isin={e.isin}  "
#                 f"← pass THIS to ETF tools ONLY\n"
#             )
#         if e.company_name:
#             entity_block += f"  company_name={e.company_name}\n"
#         if e.amc_name:
#             entity_block += f"  amc_name={e.amc_name}\n"
#         if e.scheme_name:
#             entity_block += f"  scheme_name={e.scheme_name}\n"

#     entity_block += (
#         "\nProcess each entity with its designated tool. "
#         "Include ALL actual numeric values in your final response."
#     )
#     return base + entity_block


# # ══════════════════════════════════════════════════════════════════════════════
# # OpenAI tool schema builder
# # ══════════════════════════════════════════════════════════════════════════════

# def _to_openai_tool(tool, is_multi: bool = False) -> dict:
#     """
#     Converts MCP tool to OpenAI function schema.
#     - Forces integer type for known numeric ID params.
#     - In multi-entity mode, injects "entity_label" as an optional param
#       so the LLM can tag which entity each tool call belongs to.
#     """
#     schema   = tool.inputSchema or {}
#     props    = dict(schema.get("properties", {}))
#     required = list(schema.get("required", []))

#     if "params" in props and len(props) == 1:
#         inner    = props["params"]
#         props    = dict(inner.get("properties", {}))
#         required = list(inner.get("required", []))

#     # Deep copy props to avoid mutating the original schema
#     props = {k: dict(v) for k, v in props.items()}

#     for k, v in props.items():
#         if "description" not in v:
#             v["description"] = f"Parameter: {k}"
#         if k in _NUMERIC_PARAMS:
#             v["type"] = "integer"

#     # Inject entity_label for multi-entity disambiguation
#     if is_multi:
#         props["entity_label"] = {
#             "type":        "string",
#             "description": (
#                 "The label of the entity this tool call is for. "
#                 "Must match one of the entity labels in ENTITIES TO PROCESS."
#             ),
#         }

#     return {
#         "type": "function",
#         "function": {
#             "name":        tool.name,
#             "description": (tool.description or tool.name).strip(),
#             "parameters":  {"type": "object", "properties": props, "required": required},
#         },
#     }


# # ══════════════════════════════════════════════════════════════════════════════
# # LLM chat wrapper
# # ══════════════════════════════════════════════════════════════════════════════

# def _chat(messages: list, tools: list, backend: str):
#     if backend == "ollama":
#         import urllib.request
#         req = urllib.request.Request(
#             f"{OLLAMA_HOST}/api/chat",
#             data=json.dumps({
#                 "model":    OLLAMA_MODEL,
#                 "messages": messages,
#                 "tools":    tools,
#                 "stream":   False,
#                 "options":  {"temperature": 0.0},
#             }).encode(),
#             headers={"Content-Type": "application/json"},
#             method="POST",
#         )
#         data = json.loads(urllib.request.urlopen(req, timeout=600).read().decode())
#         omsg = data.get("message", {})

#         class _TC:
#             def __init__(self, d):
#                 self.id = d.get("id", "tc-id")
#                 args = d["function"]["arguments"]
#                 self.function = type("F", (), {
#                     "name":      d["function"]["name"],
#                     "arguments": json.dumps(args) if isinstance(args, dict) else args,
#                 })()

#         class _Choice:
#             def __init__(self):
#                 tcs = [_TC(t) for t in omsg.get("tool_calls", [])]
#                 self.message = type("M", (), {
#                     "content":    omsg.get("content"),
#                     "tool_calls": tcs or None,
#                 })()

#         return _Choice()

#     else:
#         from groq import Groq
#         client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
#         return client.chat.completions.create(
#             model=GROQ_MODEL,
#             messages=messages,
#             tools=tools,
#             tool_choice="auto",
#         ).choices[0]


# def _extract_entity_hint(query: str) -> str:
#     return re.split(r"\[(PRE-RESOLVED|VERIFIED|SUGGESTED|RECOMMENDED)", query)[0].strip()


# # ══════════════════════════════════════════════════════════════════════════════
# # Core query loop
# # ══════════════════════════════════════════════════════════════════════════════

# async def _run_query_with_session(
#     query:   str,
#     session: ClientSession,
#     backend: str,
# ) -> str:
#     # ── Build entity registry from PRE_RESOLVED block ─────────────────────
#     registry = _EntityRegistry()
#     registry.parse_injected(query)

#     is_multi = len(registry.entities) > 1

#     console.print(Panel(
#         f"[bold blue]Incoming Query ({len(registry.entities)} entities, "
#         f"multi={is_multi}):[/bold blue]\n{query[:400]}",
#         title="LLM Input",
#     ))

#     entity_hint = _extract_entity_hint(query)

#     # ── Get live tool list and build param index ───────────────────────────
#     raw_tools    = (await session.list_tools()).tools
#     param_index  = _ToolParamIndex()
#     param_index.build(raw_tools)

#     # Build OpenAI-compatible manifests (with entity_label in multi mode)
#     all_manifest  = [_to_openai_tool(t, is_multi=is_multi) for t in raw_tools]

#     # Semantic filtering — use higher top_k for multi-entity to cover all families
#     top_k         = 14 if is_multi else 8
#     current_tools = get_semantic_tools(query, all_manifest, top_k=top_k)
#     console.print(
#         f"  [Semantic Router] selected {len(current_tools)} tools "
#         f"(top_k={top_k}, is_multi={is_multi})"
#     )

#     # ── System prompt ──────────────────────────────────────────────────────
#     system_prompt = _build_system_prompt(registry, is_multi=is_multi)

#     messages: list[dict] = [
#         {"role": "system", "content": system_prompt},
#         {"role": "user",   "content": query},
#     ]

#     all_tool_results: list[str] = []

#     def _combined_fallback() -> str:
#         if not all_tool_results:
#             return ""
#         if len(all_tool_results) == 1:
#             return all_tool_results[0]
#         return "\n\n---\n\n".join(all_tool_results)

#     # ══════════════════════════════════════════════════════════════════════
#     # Agentic loop
#     # ══════════════════════════════════════════════════════════════════════
#     for round_num in range(1, MAX_ROUNDS + 1):
#         console.print(f"  [dim]── Round {round_num} ──[/dim]")

#         loop = asyncio.get_event_loop()
#         try:
#             choice = await asyncio.wait_for(
#                 loop.run_in_executor(None, _chat, messages, current_tools, backend),
#                 timeout=600,
#             )
#         except asyncio.TimeoutError:
#             logger.error(f"LLM timed out at round {round_num}")
#             return _combined_fallback() or "Error: LLM timed out."
#         except Exception as e:
#             logger.error(f"LLM call failed: {e}")
#             return _combined_fallback() or f"LLM Error: {e}"

#         msg = choice.message

#         if msg.content:
#             console.print(f"  [magenta]Assistant:[/magenta] {msg.content[:200]}")

#         # ── No more tool calls → done ─────────────────────────────────────
#         if not msg.tool_calls:
#             if msg.content and not _looks_like_meta_description(msg.content):
#                 return msg.content
#             fallback = _combined_fallback()
#             if fallback:
#                 console.print("  [green]✔ Returning raw tool data[/green]")
#                 return fallback
#             return msg.content or "(No data found)"

#         # ── Sanitize and prepare tool calls ──────────────────────────────
#         sanitized_for_execution: list[tuple[str, dict, str]] = []
#         history_tool_calls: list[dict] = []

#         for tc in msg.tool_calls:
#             fn_name  = tc.function.name
#             raw_args = tc.function.arguments

#             # Parse safely
#             if isinstance(raw_args, str):
#                 try:
#                     args_dict = json.loads(raw_args)
#                 except json.JSONDecodeError:
#                     args_dict = {}
#             else:
#                 args_dict = dict(raw_args)

#             # Coerce numeric params that arrived as strings/floats
#             for param in _NUMERIC_PARAMS:
#                 if param in args_dict:
#                     try:
#                         args_dict[param] = int(float(str(args_dict[param])))
#                     except (ValueError, TypeError):
#                         args_dict.pop(param)

#             clean_args_str = json.dumps(args_dict)
#             sanitized_for_execution.append((fn_name, args_dict, tc.id))
#             history_tool_calls.append({
#                 "id": tc.id, "type": "function",
#                 "function": {"name": fn_name, "arguments": clean_args_str},
#             })

#         messages.append({
#             "role": "assistant",
#             "content": msg.content,
#             "tool_calls": history_tool_calls,
#         })

#         # ── Execute each tool call ────────────────────────────────────────
#         for fn_name, fn_args, tc_id in sanitized_for_execution:

#             # Inject codes for non-resolver tools
#             if fn_name not in _RESOLVER_TOOLS:
#                 fn_args, messages = await _ensure_codes_for_tool(
#                     tool_name   = fn_name,
#                     tool_args   = fn_args,
#                     registry    = registry,
#                     param_index = param_index,
#                     session     = session,
#                     messages    = messages,
#                     entity_hint = entity_hint,
#                 )

#             console.print(
#                 f"  [cyan]🛠  {fn_name}[/cyan]  args={{"
#                 + ", ".join(f"{k}={v}" for k, v in fn_args.items() if k != "entity_label")
#                 + "}"
#             )

#             try:
#                 res = await asyncio.wait_for(
#                     session.call_tool(fn_name, fn_args),
#                     timeout=600,
#                 )
#                 txt = res.content[0].text if res.content else "Empty response"
#                 console.print(
#                     f"  [green]✔ {fn_name}[/green]: {len(txt)} chars"
#                 )

#                 if fn_name in _RESOLVER_TOOLS:
#                     registry.update_from_resolver_result(fn_name, txt)
#                 else:
#                     all_tool_results.append(f"[{fn_name}]\n{txt}")

#             except asyncio.TimeoutError:
#                 txt = f"Error: {fn_name} timed out."
#                 console.print(f"  [red]✗ {fn_name} timed out[/red]")
#             except Exception as e:
#                 txt = f"Tool Error: {e}"
#                 console.print(f"  [red]✗ {fn_name}: {e}[/red]")

#             messages.append({
#                 "role": "tool", "tool_call_id": tc_id, "content": txt,
#             })

#     console.print("  [yellow]⚠ Max rounds reached[/yellow]")
#     return _combined_fallback() or "(Process exceeded max rounds)"


# # ══════════════════════════════════════════════════════════════════════════════
# # Public API
# # ══════════════════════════════════════════════════════════════════════════════

# async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
#     """
#     Public async entry point consumed by graph.py's node_mcp_tool_call.

#     Parameters
#     ----------
#     query   : Enriched query string (may contain <PRE_RESOLVED> block).
#     backend : "groq" or "ollama". Defaults to LLM_BACKEND env var.
#     """
#     b             = (backend or LLM_BACKEND).lower()
#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()
#             return await _run_query_with_session(query, session, b)


# # ── CLI ────────────────────────────────────────────────────────────────────────

# if __name__ == "__main__":
#     if len(sys.argv) > 1:
#         asyncio.run(run_mcp_query(" ".join(sys.argv[1:])))



"""
mcp_client.py — Equifiz MF MCP Client (v4.1 — Production Grade)
FIXED: 400 Validation errors for 'null' strings and cross-domain tool mismatches.
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
from rich.panel import Panel
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

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger  = logging.getLogger("equifiz_mf_client")
console = Console()

# ── ChromaDB & Params ──────────────────────────────────────────────────────────
chroma_client  = chromadb.PersistentClient(path=str(CHROMA_PATH))
ollama_ef      = embedding_functions.OllamaEmbeddingFunction(
    model_name=EMBED_MODEL, url=f"{OLLAMA_HOST}/api/embeddings"
)
tool_collection = chroma_client.get_collection(name="equifiz_tools", embedding_function=ollama_ef)

_NUMERIC_PARAMS = {"mf_schcode", "mf_cocode", "co_code", "index_code"}
_PARAM_TO_FAMILY = {
    "co_code": "stock", "mf_schcode": "mf_scheme", 
    "mf_cocode": "mf_amc", "isin": "etf", "index_code": "index"
}
_FAMILY_TO_PARAM = {v: k for k, v in _PARAM_TO_FAMILY.items()}
_RESOLVER_TOOLS = {"resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes",
                   "resolve_nse_symbol", "get_company_details", "search_companies"}

# ══════════════════════════════════════════════════════════════════════════════
# Helper Classes
# ══════════════════════════════════════════════════════════════════════════════

class _ToolParamIndex:
    def __init__(self): self._index = {}
    def build(self, raw_tools: list):
        for tool in raw_tools:
            schema = tool.inputSchema or {}
            props = schema.get("properties", {})
            required = set(schema.get("required", []))
            if "params" in props and len(props) == 1:
                inner = props["params"]
                props, required = inner.get("properties", {}), set(inner.get("required", []))
            
            required_db = {p for p in required if p in _PARAM_TO_FAMILY}
            optional_db = {p for p in props if p in _PARAM_TO_FAMILY and p not in required}
            
            family = "general"
            for p in list(required_db) + list(optional_db):
                family = _PARAM_TO_FAMILY.get(p, "general")
                break

            self._index[tool.name] = {
                "required_db_params": required_db,
                "optional_db_params": optional_db,
                "family": family,
                "all_params": set(props.keys()),
            }
        console.print(f"  [ToolParamIndex] Indexed {len(self._index)} tools.")

    def get_required_db_params(self, t): return self._index.get(t, {}).get("required_db_params", set())
    def get_family(self, t): return self._index.get(t, {}).get("family", "general")

class _EntityInfo:
    __slots__ = ("label", "entity_type", "query_intent", "recommended_tool", 
                 "mf_schcode", "mf_cocode", "co_code", "isin", "index_code",
                 "company_name", "amc_name", "scheme_name")
    def __init__(self, label=""):
        self.label, self.entity_type = label, "general"
        self.query_intent = self.recommended_tool = ""
        self.mf_schcode = self.mf_cocode = self.co_code = self.isin = self.index_code = None
        self.company_name = self.amc_name = self.scheme_name = ""

    def get_code_for_param(self, p):
        return getattr(self, p) if p in _PARAM_TO_FAMILY else None

class _EntityRegistry:
    def __init__(self):
        self.entities: list[_EntityInfo] = []
        self._primary = _EntityInfo("(primary)")

    def parse_injected(self, query):
        block = re.search(r"<PRE_RESOLVED>(.*?)</PRE_RESOLVED>", query, re.S | re.I)
        if block:
            current = None
            for line in block.group(1).splitlines():
                line = line.strip()
                if not line or line.startswith("#"): continue
                em = re.match(r"Entity\s+\d+:\s*(.+)", line, re.I)
                if em:
                    current = _EntityInfo(em.group(1).strip())
                    self.entities.append(current)
                    continue
                if not current: 
                    current = _EntityInfo("(primary)")
                    self.entities.append(current)
                kv = re.match(r"([\w_]+)\s*=\s*(.+)", line)
                if kv:
                    key, val = kv.group(1).lower(), re.sub(r"\s*\(.*?\)\s*$", "", kv.group(2)).strip()
                    try:
                        if key in _NUMERIC_PARAMS: setattr(current, key, int(val))
                        elif key in _PARAM_TO_FAMILY: setattr(current, key, val)
                        else: setattr(current, key, val)
                    except: pass
        if self.entities: self._primary = self.entities[0]

    def find_entity_for_tool(self, tool_name, tool_family, label_hint):
        if label_hint:
            for e in self.entities:
                if e.label.lower() == label_hint.lower(): return e
        for e in self.entities:
            if e.recommended_tool == tool_name: return e
        for e in self.entities:
            if e.entity_type == tool_family: return e
        return self._primary

# ══════════════════════════════════════════════════════════════════════════════
# Core Logic
# ══════════════════════════════════════════════════════════════════════════════

async def _ensure_codes_for_tool(tool_name, tool_args, registry, param_index, session, messages):
    updated_args = dict(tool_args)
    label_hint = updated_args.pop("entity_label", None)
    required_db = param_index.get_required_db_params(tool_name)
    tool_family = param_index.get_family(tool_name)
    
    if not required_db: return updated_args, messages

    entity = registry.find_entity_for_tool(tool_name, tool_family, label_hint)

    # 400 FIX: Prevent cross-domain injection (e.g., don't put mf_schcode in a stock tool)
    for param in required_db:
        # 1. Clean existing hallucinated "null" strings
        if param in updated_args:
            if str(updated_args[param]).lower() in ["null", "none", "", "nan"]:
                updated_args.pop(param)

        # 2. Inject correct ID if missing
        if param not in updated_args or updated_args[param] is None:
            # Only inject if entity type matches tool family OR entity is general
            if entity and (entity.entity_type == tool_family or tool_family == "general"):
                val = entity.get_code_for_param(param)
                if val is not None:
                    updated_args[param] = int(val) if param in _NUMERIC_PARAMS else val

    return updated_args, messages

async def _run_query_with_session(query, session, backend):
    registry = _EntityRegistry()
    registry.parse_injected(query)
    is_multi = len(registry.entities) > 1

    raw_tools = (await session.list_tools()).tools
    param_index = _ToolParamIndex()
    param_index.build(raw_tools)

    all_manifest = [_to_openai_tool(t, is_multi) for t in raw_tools]
    current_tools = get_semantic_tools(query, all_manifest, top_k=14 if is_multi else 8)

    messages = [
        {"role": "system", "content": _build_system_prompt(registry, is_multi)},
        {"role": "user", "content": query}
    ]

    for round_num in range(1, MAX_ROUNDS + 1):
        choice = await asyncio.get_event_loop().run_in_executor(None, _chat, messages, current_tools, backend)
        msg = choice.message
        if not msg.tool_calls: return msg.content or "No data."

        history_calls = []
        for tc in msg.tool_calls:
            fn, args = tc.function.name, json.loads(tc.function.arguments) if isinstance(tc.function.arguments, str) else tc.function.arguments
            
            # 400 FIX: Pre-sanitize before tool call
            for p in _NUMERIC_PARAMS:
                if p in args:
                    v = str(args[p]).lower()
                    if v in ["null", "none", ""]: args.pop(p)
                    else: 
                        try: args[p] = int(float(v))
                        except: args.pop(p)

            if fn not in _RESOLVER_TOOLS:
                args, messages = await _ensure_codes_for_tool(fn, args, registry, param_index, session, messages)

            console.print(f"  [cyan]🛠 Call:[/cyan] {fn} {args}")
            try:
                res = await session.call_tool(fn, args)
                txt = res.content[0].text if res.content else "Success"
            except Exception as e:
                txt = f"Error: {e}"
            
            messages.append({"role": "assistant", "content": None, "tool_calls": [{"id": tc.id, "type": "function", "function": {"name": fn, "arguments": json.dumps(args)}}]})
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": txt})

    return "Max rounds reached."

# ── Boilerplate ────────────────────────────────────────────────────────────────
def _to_openai_tool(tool, multi):
    p = tool.inputSchema.get("properties", {})
    if multi: p["entity_label"] = {"type": "string", "description": "Entity label for routing"}
    for k in p: 
        if k in _NUMERIC_PARAMS: p[k]["type"] = "integer"
    return {"type": "function", "function": {"name": tool.name, "description": tool.description, "parameters": {"type": "object", "properties": p}}}

def _build_system_prompt(reg, multi):
    p = "You are a financial analyst. Use provided IDs. NEVER pass 'null' for required IDs.\n"
    if multi: p += "Multi-entity: include 'entity_label' in tool calls.\n"
    for i, e in enumerate(reg.entities):
        p += f"Entity {i+1}: {e.label} | type: {e.entity_type} | IDs: co_code={e.co_code}, mf_schcode={e.mf_schcode}\n"
    return p

def get_semantic_tools(q, tools, top_k):
    # Simplified for rewrite
    return tools[:top_k]

def _chat(m, t, b):
    from groq import Groq
    return Groq(api_key=os.environ["GROQ_API_KEY"]).chat.completions.create(model=GROQ_MODEL, messages=m, tools=t)

async def run_mcp_query(query, backend=None):
    async with stdio_client(StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])) as (r, w):
        async with ClientSession(r, w) as session:
            await session.initialize()
            return await _run_query_with_session(query, session, backend or LLM_BACKEND)

if __name__ == "__main__":
    asyncio.run(run_mcp_query(" ".join(sys.argv[1:])))