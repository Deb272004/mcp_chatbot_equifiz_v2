
# """
# mcp_client.py — Equifiz MF MCP Client (v3.1)
# Optimized for LangGraph Integration and High-Precision ID Injection.
# """

# from __future__ import annotations

# import asyncio
# import json
# import logging
# import os
# import re
# import sys
# from pathlib import Path
# from typing import Optional, Any

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

# def get_semantic_tools(query: str, all_openai_tools: list[dict], top_k: int = 7) -> list[dict]:
#     """Finds relevant tools using ChromaDB embeddings. Always includes core resolvers."""
#     CORE_PLUMBING = {"resolve_mf_scheme", "resolve_mf_fund"}
    
#     try:
#         results = tool_collection.query(query_texts=[query], n_results=top_k)
#         matched_names = set(results['ids'][0]) | CORE_PLUMBING
#     except Exception as e:
#         console.print(f"ChromaDB Query Failed: {e}")
#         matched_names = CORE_PLUMBING # Safety fallback

#     return [t for t in all_openai_tools if t["function"]["name"] in matched_names]

# # ── Session code registry ─────────────────────────────────────────────────────

# class _CodeRegistry:
#     def __init__(self):
#         self.mf_schcode: Optional[int]  = None
#         self.mf_cocode:  Optional[int]  = None
#         self.co_code:    Optional[int]  = None
#         self.scheme_name: Optional[str] = None
#         self.amc_name:    Optional[str] = None

#     def parse_injected(self, query: str) -> None:
#         """Parses [VERIFIED ENTITY IDs] or [PRE-RESOLVED CODES] from graph.py."""
#         for line in query.splitlines():
#             line = line.strip().replace("- ", "") # Handle bullet points
#             if m := re.search(r"mf_schcode[:=]\s*(\d+)", line, re.I):
#                 self.mf_schcode = int(m.group(1))
#                 console.print(f"Registry: Injected mf_schcode={self.mf_schcode}")
#             elif m := re.search(r"mf_cocode[:=]\s*(\d+)", line, re.I):
#                 self.mf_cocode = int(m.group(1))
#             elif m := re.search(r"co_code[:=]\s*(\d+)", line, re.I):
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

# # async def _ensure_codes_for_tool(
# #     tool_name: str, tool_args: dict, registry: _CodeRegistry,
# #     session: ClientSession, messages: list[dict], entity_hint: str
# # ) -> tuple[dict, list[dict]]:
    
# #     updated_args = dict(tool_args)

# #     # 1. MF_SCHCODE Logic
# #     if tool_name in _TOOL_NEEDS_MF_SCHCODE:
# #         if registry.mf_schcode is None:
# #             hint = tool_args.get("query") or entity_hint
# #             console.print(f"  [yellow]⚡ Auto-resolving mf_schcode for '{hint}'[/yellow]")
# #             res = await session.call_tool("resolve_mf_scheme", {"query": hint})
# #             txt = res.content[0].text if res.content else ""
# #             registry.update_from_resolver_result("resolve_mf_scheme", txt)
# #             messages.append({
# #                 "role": "assistant", "content": None,
# #                 "tool_calls": [{"id": "auto-sch", "type": "function", "function": {"name": "resolve_mf_scheme", "arguments": json.dumps({"query": hint})}}]
# #             })
# #             messages.append({"role": "tool", "tool_call_id": "auto-sch", "content": txt})
        
# #         # FORCE INJECTION: Overwrite arg with verified registry value
# #         if registry.mf_schcode is not None:
# #             updated_args["mf_schcode"] = registry.mf_schcode

# #     # 2. MF_COCODE Logic
# #     if tool_name in _TOOL_NEEDS_MF_COCODE:
# #         if registry.mf_cocode is None:
# #             hint = tool_args.get("query") or entity_hint
# #             console.print(f"  [yellow]⚡ Auto-resolving mf_cocode for '{hint}'[/yellow]")
# #             res = await session.call_tool("resolve_mf_fund", {"query": hint})
# #             txt = res.content[0].text if res.content else ""
# #             registry.update_from_resolver_result("resolve_mf_fund", txt)
# #             messages.append({
# #                 "role": "assistant", "content": None,
# #                 "tool_calls": [{"id": "auto-co", "type": "function", "function": {"name": "resolve_mf_fund", "arguments": json.dumps({"query": hint})}}]
# #             })
# #             messages.append({"role": "tool", "tool_call_id": "auto-co", "content": txt})

# #         if registry.mf_cocode is not None:
# #             updated_args["mf_cocode"] = registry.mf_cocode

# #     # 3. CO_CODE Logic
# #     if tool_name in _TOOL_NEEDS_CO_CODE and registry.co_code is not None:
# #         updated_args["co_code"] = registry.co_code

# #     return updated_args, messages


# # async def _ensure_codes_for_tool(
# #     tool_name: str, tool_args: dict, registry: _CodeRegistry,
# #     session: ClientSession, messages: list[dict], entity_hint: str
# # ) -> tuple[dict, list[dict]]:
# #     """
# #     Intercepts tool calls to inject missing numeric IDs (mf_schcode, mf_cocode) 
# #     by triggering silent resolver calls if the registry is empty.
# #     """
# #     updated_args = dict(tool_args)

# #     # Helper to clean hints (removes common noise that confuses fuzzy search)
# #     def clean_hint(h):
# #         return re.sub(r"(mf_schcode|mf_cocode|co_code)[:=]\s*", "", str(h), flags=re.I).strip()

# #     # 1. MF_SCHCODE Logic (Scheme Level)
# #     if tool_name in _TOOL_NEEDS_MF_SCHCODE:
# #         if registry.mf_schcode is None:
# #             # Try to find a hint in current args, otherwise use global entity_hint
# #             raw_hint = tool_args.get("query") or tool_args.get("sch_name") or entity_hint
# #             hint = clean_hint(raw_hint)
            
# #             console.print(f"  [yellow]⚡ Auto-resolving mf_schcode for '{hint}'[/yellow]")
            
# #             try:
# #                 res = await session.call_tool("resolve_mf_scheme", {"query": hint})
# #                 txt = res.content[0].text if res.content else ""
                
# #                 # Update registry with the newly found ID
# #                 registry.update_from_resolver_result("resolve_mf_scheme", txt)
                
# #                 # Update conversation history so LLM "sees" the resolution happened
# #                 messages.append({
# #                     "role": "assistant", "content": None,
# #                     "tool_calls": [{
# #                         "id": f"auto-sch-{registry.mf_schcode}", 
# #                         "type": "function", 
# #                         "function": {"name": "resolve_mf_scheme", "arguments": json.dumps({"query": hint})}
# #                     }]
# #                 })
# #                 messages.append({"role": "tool", "tool_call_id": f"auto-sch-{registry.mf_schcode}", "content": txt})
# #             except Exception as e:
# #                 logger.error(f"Auto-resolution failed: {e}")

# #         # FORCE INJECTION: Overwrite the argument with the verified registry value
# #         if registry.mf_schcode is not None:
# #             updated_args["mf_schcode"] = registry.mf_schcode

# #     # 2. MF_COCODE Logic (AMC Level)
# #     if tool_name in _TOOL_NEEDS_MF_COCODE:
# #         if registry.mf_cocode is None:
# #             raw_hint = tool_args.get("query") or tool_args.get("amc_name") or entity_hint
# #             hint = clean_hint(raw_hint)
            
# #             console.print(f"  [yellow]⚡ Auto-resolving mf_cocode for '{hint}'[/yellow]")
            
# #             try:
# #                 res = await session.call_tool("resolve_mf_fund", {"query": hint})
# #                 txt = res.content[0].text if res.content else ""
                
# #                 registry.update_from_resolver_result("resolve_mf_fund", txt)
                
# #                 messages.append({
# #                     "role": "assistant", "content": None,
# #                     "tool_calls": [{
# #                         "id": f"auto-co-{registry.mf_cocode}", 
# #                         "type": "function", 
# #                         "function": {"name": "resolve_mf_fund", "arguments": json.dumps({"query": hint})}
# #                     }]
# #                 })
# #                 messages.append({"role": "tool", "tool_call_id": f"auto-co-{registry.mf_cocode}", "content": txt})
# #             except Exception as e:
# #                 logger.error(f"Auto-resolution failed: {e}")

# #         if registry.mf_cocode is not None:
# #             updated_args["mf_cocode"] = registry.mf_cocode

# #     # 3. CO_CODE Logic (Corporate/Company Level)
# #     if tool_name in _TOOL_NEEDS_CO_CODE and registry.co_code is not None:
# #         updated_args["co_code"] = registry.co_code

# #     return updated_args, messages

# async def _ensure_codes_for_tool(
#     tool_name: str, tool_args: dict, registry: _CodeRegistry,
#     session: ClientSession, messages: list[dict], entity_hint: str
# ) -> tuple[dict, list[dict]]:
#     """
#     Intercepts tool calls to inject missing numeric IDs.
#     Enhanced with 'Prompt-Noise' filtering to prevent 400 Bad Request errors.
#     """
#     updated_args = dict(tool_args)

#     # def clean_hint(h):
#     #     """
#     #     Strips out LangGraph boilerplate and ID labels so the resolver 
#     #     sees only the fund name (e.g., 'DSP Bond Fund').
#     #     """
#     #     h_str = str(h)
#     #     # 1. Remove LangGraph/System headers: Context, Instruction, User Query, etc.
#     #     # This fixes the issue where 'Instruction:...' was being sent to the fuzzy search.
#     #     h_str = re.sub(r"(?i)(Context|Instruction|User Query|RECOMMENDED TOOL):.*?\n", "", h_str, flags=re.S)
        
#     #     # 2. Strip numeric labels like 'mf_schcode: 770' or 'mf_cocode='
#     #     h_str = re.sub(r"(?i)(mf_schcode|mf_cocode|co_code)[:=]\s*", "", h_str).strip()
        
#     #     # 3. Final polish: remove lingering brackets and extra whitespace
#     #     h_str = h_str.replace("[", "").replace("]", "").strip()
#     #     return h_str

#     # Inside mcp_client.py -> _ensure_codes_for_tool
    
#     def clean_hint(h):
#         h_str = str(h)
        
#         # 1. Remove everything inside <PRE_RESOLVED> tags
#         h_str = re.sub(r"<PRE_RESOLVED>.*?</PRE_RESOLVED>", "", h_str, flags=re.S | re.I)
        
#         # 2. Remove "User Query:" or "Instruction:" prefixes
#         h_str = re.sub(r"(?i)(User Query|Instruction|Context|RECOMMENDED TOOL):", "", h_str)
        
#         # 3. Strip any remaining bracketed metadata
#         h_str = re.sub(r"\[.*?\]", "", h_str)

#         cleaned = h_str.strip()
#         console.print(f"    [dim]Cleaned hint for resolver:[/dim] [italic]'{cleaned}'[/italic]")
#         return cleaned

#     # 1. MF_SCHCODE Logic (Scheme Level)
#     if tool_name in _TOOL_NEEDS_MF_SCHCODE:
#         if registry.mf_schcode is None:
#             # Check current args first, fallback to entity_hint
#             raw_hint = tool_args.get("query") or tool_args.get("sch_name") or entity_hint
#             hint = clean_hint(raw_hint)
            
#             console.print(f"  [yellow]⚡ Auto-resolving mf_schcode for clean hint: '{hint}'[/yellow]")
            
#             try:
#                 res = await session.call_tool("resolve_mf_scheme", {"query": hint})
#                 txt = res.content[0].text if res.content else ""
                
#                 # We only update if we actually found a match
#                 if "RESOLVED" in txt:
#                     registry.update_from_resolver_result("resolve_mf_scheme", txt)
#                     console.print(f"    [green]✔ Success:[/green] Resolved to mf_schcode: [bold]{registry.mf_schcode}[/bold]")
#                     messages.append({
#                         "role": "assistant", "content": None,
#                         "tool_calls": [{
#                             "id": f"auto-sch-{registry.mf_schcode}", 
#                             "type": "function", 
#                             "function": {"name": "resolve_mf_scheme", "arguments": json.dumps({"query": hint})}
#                         }]
#                     })
#                     messages.append({"role": "tool", "tool_call_id": f"auto-sch-{registry.mf_schcode}", "content": txt})
#                 else:
#                     console.print(f"  [red]⚠ Could not resolve scheme for: {hint}[/red]")
#             except Exception as e:
#                 console.print(f"Auto-resolution failed: {e}")

#         # FORCE INJECTION: Overwrite arg with verified registry value
#         if registry.mf_schcode is not None:
#             updated_args["mf_schcode"] = registry.mf_schcode

#     # 2. MF_COCODE Logic (AMC Level)
#     if tool_name in _TOOL_NEEDS_MF_COCODE:
#         if registry.mf_cocode is None:
#             raw_hint = tool_args.get("query") or tool_args.get("amc_name") or entity_hint
#             hint = clean_hint(raw_hint)
            
#             console.print(f"  [yellow]⚡ Auto-resolving mf_cocode for clean hint: '{hint}'[/yellow]")
            
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
#                             "function": {"name": "resolve_mf_fund", "arguments": json.dumps({"query": hint})}
#                         }]
#                     })
#                     messages.append({"role": "tool", "tool_call_id": f"auto-co-{registry.mf_cocode}", "content": txt})
#             except Exception as e:
#                 console.print(f"Auto-resolution failed: {e}")

#         if registry.mf_cocode is not None:
#             updated_args["mf_cocode"] = registry.mf_cocode

#     # 3. CO_CODE Logic (Corporate/Company Level)
#     if tool_name in _TOOL_NEEDS_CO_CODE and registry.co_code is not None:
#         updated_args["co_code"] = registry.co_code

#     return updated_args, messages

# # ── Core system prompt ─────────────────────────────────────────────────────────

# SYSTEM_PROMPT = """You are a mutual fund analyst. 
# Verified IDs are provided in context. You MUST use these numeric IDs for tool calls.
# Do not call resolution tools if an ID is already present in the context."""

# # ── Helpers ────────────────────────────────────────────────────────────────────

# def _to_openai_tool(tool) -> dict:
#     schema = tool.inputSchema or {}
#     props = schema.get("properties", {})
#     required = schema.get("required", [])
    
#     # Safe flattening for FastMCP 'params' wrapper
#     if "params" in props and len(props) == 1:
#         inner = props["params"]
#         props = inner.get("properties", {})
#         required = inner.get("required", [])
    
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
#             data=json.dumps({
#                 "model": OLLAMA_MODEL, 
#                 "messages": messages, 
#                 "tools": tools, 
#                 "stream": False, 
#                 "options": {"temperature": 0.0} # Low temp for precision
#             }).encode(),
#             headers={"Content-Type": "application/json"}, method="POST"
#         )
#         data = json.loads(urllib.request.urlopen(req, timeout=600).read().decode())
#         omsg = data.get("message", {})
        
#         class _TC:
#             def __init__(self, d):
#                 self.id = d.get("id", "tc-id")
#                 # Normalize arguments to string for the Registry parser
#                 args = d["function"]["arguments"]
#                 self.function = type("F", (), {
#                     "name": d["function"]["name"], 
#                     "arguments": json.dumps(args) if isinstance(args, dict) else args
#                 })()
        
#         class _Choice:
#             def __init__(self):
#                 self.message = type("M", (), {
#                     "content": omsg.get("content"), 
#                     "tool_calls": [_TC(t) for t in omsg.get("tool_calls", [])] or None
#                 })()
#         return _Choice()
#     else:
#         from groq import Groq
#         client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
#         return client.chat.completions.create(model=GROQ_MODEL, messages=messages, tools=tools, tool_choice="auto").choices[0]

# def _extract_entity_hint(query: str) -> str:
#     # Strip all metadata tags for the resolver hint
#     clean = re.split(r"\[(PRE-RESOLVED|VERIFIED|SUGGESTED)", query)[0].strip()
#     return clean

# # ── Main Query Loop ───────────────────────────────────────────────────────────

# async def _run_query_with_session(query: str, session: ClientSession, backend: str) -> str:
#     registry = _CodeRegistry()
#     registry.parse_injected(query)
    
#     console.print(Panel(f"[bold blue]Incoming Query Context:[/bold blue]\n{query}", title="LLM Input"))

#     entity_hint = _extract_entity_hint(query)
#     raw_tools = (await session.list_tools()).tools
#     all_manifest = [_to_openai_tool(t) for t in raw_tools]

#     # SEMANTIC FILTERING
#     current_tools = get_semantic_tools(query, all_manifest, top_k=7)
#     console.print(f"  [Semantic Router] selected {len(current_tools)} tools")

#     messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": query}]

#     for round_num in range(1, MAX_ROUNDS + 1):
#         loop = asyncio.get_event_loop()
#         try:
#             choice = await asyncio.wait_for(
#                 loop.run_in_executor(None, _chat, messages, current_tools, backend),
#                 timeout=600
#             )
#         except Exception as e: return f"LLM Error: {e}"

#         msg = choice.message
#         if msg.content:
#             console.print(f"\n[bold magenta]Assistant:[/bold magenta] {msg.content}")

#         if not msg.tool_calls: 
#             return msg.content or "(No data found)"

#         # Append Assistant Turn (Fixed for Ollama dictionary arguments)
#         messages.append({
#             "role": "assistant", "content": msg.content,
#             "tool_calls": [
#                 {
#                     "id": tc.id, "type": "function", 
#                     "function": {"name": tc.function.name, "arguments": tc.function.arguments}
#                 } for tc in msg.tool_calls
#             ]
#         })

#         # Execute Tools
#         for tc in msg.tool_calls:
#             fn_name = tc.function.name
#             # Argument Hygiene: Handle string or dict
#             fn_args = tc.function.arguments
#             if isinstance(fn_args, str):
#                 try: fn_args = json.loads(fn_args)
#                 except: fn_args = {}

#             # RESOLVER GUARD: Force Verified IDs into arguments
#             if fn_name not in _RESOLVER_TOOLS:
#                 fn_args, messages = await _ensure_codes_for_tool(fn_name, fn_args, registry, session, messages, entity_hint)

#             console.print(f"  [cyan]🛠 Executing:[/cyan] [bold]{fn_name}[/bold] with {fn_args}")
#             console.print(f"  [cyan]🛠 Final Tool Call:[/cyan] [bold]{fn_name}[/bold]([italic]{json.dumps(fn_args)}[/italic])")
#             try:
#                 res = await asyncio.wait_for(session.call_tool(fn_name, fn_args), timeout=600)
#                 txt = res.content[0].text if res.content else "Empty response"
#                 if fn_name in _RESOLVER_TOOLS: registry.update_from_resolver_result(fn_name, txt)
#             except Exception as e: txt = f"Tool Error: {e}"

#             messages.append({"role": "tool", "tool_call_id": tc.id, "content": txt})

#     return "(Process exceeded max rounds)"

# async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
#     b = (backend or LLM_BACKEND).lower()
#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()
#             return await _run_query_with_session(query, session, b)

# if __name__ == "__main__":
#     if len(sys.argv) > 1:
#         asyncio.run(run_mcp_query(" ".join(sys.argv[1:])))



"""
mcp_client.py — Equifiz MF MCP Client (v3.2)

Fix from v3.1:
─────────────────────────────────────────────────────────────────────
- Tracks `last_tool_result` across rounds so that if the LLM returns
  empty content in Round 2 (after seeing tool output), the raw tool
  data is returned instead of "(No data found)".
- Final fallback in `run_mcp_query` also returns `last_tool_result`
  instead of the generic max-rounds message.
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
MAX_TOKENS    = 4096
MAX_ROUNDS    = 15

LLM_BACKEND  = os.getenv("LLM_BACKEND", "groq").lower()
GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")
OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
EMBED_MODEL  = "embeddinggemma:latest"

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger  = logging.getLogger("equifiz_mf_client")
console = Console()

# ── ChromaDB Setup ─────────────────────────────────────────────────────────────

chroma_client  = chromadb.PersistentClient(path=str(CHROMA_PATH))
ollama_ef      = embedding_functions.OllamaEmbeddingFunction(
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

_TOOL_NEEDS_CO_CODE  = {"get_funds_holding_company"}
_RESOLVER_TOOLS      = {"resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes"}

# ── Semantic Search Helper ─────────────────────────────────────────────────────

def get_semantic_tools(query: str, all_openai_tools: list[dict], top_k: int = 7) -> list[dict]:
    """Finds relevant tools via ChromaDB embeddings. Always includes core resolvers."""
    CORE_PLUMBING = {"resolve_mf_scheme", "resolve_mf_fund"}
    try:
        results      = tool_collection.query(query_texts=[query], n_results=top_k)
        matched_names = set(results["ids"][0]) | CORE_PLUMBING
    except Exception as e:
        console.print(f"ChromaDB query failed: {e}")
        matched_names = CORE_PLUMBING
    return [t for t in all_openai_tools if t["function"]["name"] in matched_names]

# ── Session code registry ─────────────────────────────────────────────────────

class _CodeRegistry:
    def __init__(self):
        self.mf_schcode:  Optional[int] = None
        self.mf_cocode:   Optional[int] = None
        self.co_code:     Optional[int] = None
        self.scheme_name: Optional[str] = None
        self.amc_name:    Optional[str] = None

    def parse_injected(self, query: str) -> None:
        """Parses [VERIFIED ENTITY IDs] or [PRE-RESOLVED CODES] injected by graph.py."""
        for line in query.splitlines():
            line = line.strip().replace("- ", "")
            if m := re.search(r"mf_schcode[:=]\s*(\d+)", line, re.I):
                self.mf_schcode = int(m.group(1))
                console.print(f"  [Registry] Injected mf_schcode={self.mf_schcode}")
            elif m := re.search(r"mf_cocode[:=]\s*(\d+)", line, re.I):
                self.mf_cocode = int(m.group(1))
                console.print(f"  [Registry] Injected mf_cocode={self.mf_cocode}")
            elif m := re.search(r"co_code[:=]\s*(\d+)", line, re.I):
                self.co_code = int(m.group(1))
                console.print(f"  [Registry] Injected co_code={self.co_code}")

    def update_from_resolver_result(self, tool_name: str, result_text: str) -> None:
        if "RESOLVED" not in result_text:
            return
        if tool_name == "resolve_mf_scheme":
            if m := re.search(r"mf_schcode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE):
                self.mf_schcode = int(m.group(1))
        elif tool_name == "resolve_mf_fund":
            if m := re.search(r"mf_cocode\s*[:=]\s*(\d+)", result_text, re.IGNORECASE):
                self.mf_cocode = int(m.group(1))

# ── Resolver guard ─────────────────────────────────────────────────────────────

async def _ensure_codes_for_tool(
    tool_name: str,
    tool_args: dict,
    registry:  _CodeRegistry,
    session:   ClientSession,
    messages:  list[dict],
    entity_hint: str,
) -> tuple[dict, list[dict]]:
    """
    Injects missing numeric IDs (mf_schcode, mf_cocode) by triggering
    silent resolver calls when the registry is empty.
    """
    updated_args = dict(tool_args)

    def clean_hint(h: str) -> str:
        h = re.sub(r"<PRE_RESOLVED>.*?</PRE_RESOLVED>", "", str(h), flags=re.S | re.I)
        h = re.sub(r"(?i)(User Query|Instruction|Context|RECOMMENDED TOOL):", "", h)
        h = re.sub(r"\[.*?\]", "", h)
        cleaned = h.strip()
        console.print(f"    [dim]Resolver hint:[/dim] '{cleaned}'")
        return cleaned

    # ── mf_schcode ────────────────────────────────────────────────────────────
    if tool_name in _TOOL_NEEDS_MF_SCHCODE:
        if registry.mf_schcode is None:
            raw  = tool_args.get("query") or tool_args.get("sch_name") or entity_hint
            hint = clean_hint(raw)
            console.print(f"  [yellow]⚡ Auto-resolving mf_schcode for '{hint}'[/yellow]")
            try:
                res = await session.call_tool("resolve_mf_scheme", {"query": hint})
                txt = res.content[0].text if res.content else ""
                if "RESOLVED" in txt:
                    registry.update_from_resolver_result("resolve_mf_scheme", txt)
                    console.print(f"    [green]✔ mf_schcode={registry.mf_schcode}[/green]")
                    messages.append({
                        "role": "assistant", "content": None,
                        "tool_calls": [{
                            "id": f"auto-sch-{registry.mf_schcode}",
                            "type": "function",
                            "function": {
                                "name": "resolve_mf_scheme",
                                "arguments": json.dumps({"query": hint}),
                            },
                        }],
                    })
                    messages.append({
                        "role": "tool",
                        "tool_call_id": f"auto-sch-{registry.mf_schcode}",
                        "content": txt,
                    })
                else:
                    console.print(f"  [red]⚠ Could not resolve scheme for: {hint}[/red]")
            except Exception as e:
                console.print(f"  [red]Auto-resolution failed: {e}[/red]")

        if registry.mf_schcode is not None:
            updated_args["mf_schcode"] = registry.mf_schcode

    # ── mf_cocode ─────────────────────────────────────────────────────────────
    if tool_name in _TOOL_NEEDS_MF_COCODE:
        if registry.mf_cocode is None:
            raw  = tool_args.get("query") or tool_args.get("amc_name") or entity_hint
            hint = clean_hint(raw)
            console.print(f"  [yellow]⚡ Auto-resolving mf_cocode for '{hint}'[/yellow]")
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
                            "function": {
                                "name": "resolve_mf_fund",
                                "arguments": json.dumps({"query": hint}),
                            },
                        }],
                    })
                    messages.append({
                        "role": "tool",
                        "tool_call_id": f"auto-co-{registry.mf_cocode}",
                        "content": txt,
                    })
                else:
                    console.print(f"  [red]⚠ Could not resolve fund for: {hint}[/red]")
            except Exception as e:
                console.print(f"  [red]Auto-resolution failed: {e}[/red]")

        if registry.mf_cocode is not None:
            updated_args["mf_cocode"] = registry.mf_cocode

    # ── co_code ───────────────────────────────────────────────────────────────
    if tool_name in _TOOL_NEEDS_CO_CODE and registry.co_code is not None:
        updated_args["co_code"] = registry.co_code

    return updated_args, messages

# ── System prompt ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a mutual fund analyst for Indian markets.
Verified numeric IDs are provided in the context — use them directly for tool calls.
Do NOT call resolver tools if an ID is already present.
Do NOT fabricate data. If a tool returns no data, say so clearly.
"""

# ── Helpers ────────────────────────────────────────────────────────────────────

def _to_openai_tool(tool) -> dict:
    schema   = tool.inputSchema or {}
    props    = schema.get("properties", {})
    required = schema.get("required", [])

    if "params" in props and len(props) == 1:
        inner    = props["params"]
        props    = inner.get("properties", {})
        required = inner.get("required", [])

    for k, v in props.items():
        if "description" not in v:
            v["description"] = f"Param {k}"

    return {
        "type": "function",
        "function": {
            "name":        tool.name,
            "description": (tool.description or tool.name).strip(),
            "parameters":  {"type": "object", "properties": props, "required": required},
        },
    }


def _chat(messages: list, tools: list, backend: str):
    if backend == "ollama":
        import urllib.request
        req = urllib.request.Request(
            f"{OLLAMA_HOST}/api/chat",
            data=json.dumps({
                "model":   OLLAMA_MODEL,
                "messages": messages,
                "tools":   tools,
                "stream":  False,
                "options": {"temperature": 0.0},
            }).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        data = json.loads(urllib.request.urlopen(req, timeout=600).read().decode())
        omsg = data.get("message", {})

        class _TC:
            def __init__(self, d):
                self.id = d.get("id", "tc-id")
                args = d["function"]["arguments"]
                self.function = type("F", (), {
                    "name":      d["function"]["name"],
                    "arguments": json.dumps(args) if isinstance(args, dict) else args,
                })()

        class _Choice:
            def __init__(self):
                tcs = [_TC(t) for t in omsg.get("tool_calls", [])]
                self.message = type("M", (), {
                    "content":    omsg.get("content"),
                    "tool_calls": tcs or None,
                })()

        return _Choice()

    else:
        from groq import Groq
        client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
        return client.chat.completions.create(
            model=GROQ_MODEL,
            messages=messages,
            tools=tools,
            tool_choice="auto",
        ).choices[0]


def _extract_entity_hint(query: str) -> str:
    return re.split(r"\[(PRE-RESOLVED|VERIFIED|SUGGESTED)", query)[0].strip()

# ── Core query loop ────────────────────────────────────────────────────────────

async def _run_query_with_session(
    query:   str,
    session: ClientSession,
    backend: str,
) -> str:
    registry = _CodeRegistry()
    registry.parse_injected(query)

    console.print(Panel(
        f"[bold blue]Incoming Query Context:[/bold blue]\n{query}",
        title="LLM Input",
    ))

    entity_hint  = _extract_entity_hint(query)
    raw_tools    = (await session.list_tools()).tools
    all_manifest = [_to_openai_tool(t) for t in raw_tools]

    current_tools = get_semantic_tools(query, all_manifest, top_k=7)
    console.print(f"  [Semantic Router] selected {len(current_tools)} tools")

    messages         = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": query},
    ]
    last_tool_result = ""  # ← tracks the most recent tool output

    for round_num in range(1, MAX_ROUNDS + 1):
        console.print(f"  [dim]── Round {round_num} ──[/dim]")

        loop = asyncio.get_event_loop()
        try:
            choice = await asyncio.wait_for(
                loop.run_in_executor(None, _chat, messages, current_tools, backend),
                timeout=600,
            )
        except asyncio.TimeoutError:
            logger.error(f"LLM timed out at round {round_num}")
            return last_tool_result or "Error: LLM timed out."
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return last_tool_result or f"LLM Error: {e}"

        msg = choice.message

        if msg.content:
            console.print(f"  [magenta]Assistant:[/magenta] {msg.content}")

        # ── FIX: No tool calls → LLM is done. Return content if available,
        #         otherwise fall back to the last raw tool result so the
        #         synthesis node always receives real data.
        if not msg.tool_calls:
            console.print(f"  [dim]Round {round_num}: no tool calls — returning final answer[/dim]")
            console.print(f"  [dim]msg.content={msg.content!r}[/dim]")
            console.print(f"  [dim]last_tool_result length={len(last_tool_result)}[/dim]")
            return msg.content or last_tool_result or "(No data found)"

        # Append assistant turn
        messages.append({
            "role":    "assistant",
            "content": msg.content,
            "tool_calls": [
                {
                    "id":   tc.id,
                    "type": "function",
                    "function": {
                        "name":      tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in msg.tool_calls
            ],
        })

        # Execute tools
        for tc in msg.tool_calls:
            fn_name = tc.function.name
            fn_args = tc.function.arguments
            if isinstance(fn_args, str):
                try:
                    fn_args = json.loads(fn_args)
                except json.JSONDecodeError:
                    fn_args = {}

            # Resolver guard — inject verified IDs
            if fn_name not in _RESOLVER_TOOLS:
                fn_args, messages = await _ensure_codes_for_tool(
                    fn_name, fn_args, registry, session, messages, entity_hint,
                )

            console.print(f"  [cyan]🛠 Executing:[/cyan] [bold]{fn_name}[/bold] with {fn_args}")

            try:
                res = await asyncio.wait_for(
                    session.call_tool(fn_name, fn_args),
                    timeout=600,
                )
                txt = res.content[0].text if res.content else "Empty response"
                console.print(f"  [green]✔ {len(txt)} chars received[/green]")

                if fn_name in _RESOLVER_TOOLS:
                    registry.update_from_resolver_result(fn_name, txt)

            except asyncio.TimeoutError:
                txt = f"Error: {fn_name} timed out."
                console.print(f"  [red]✗ {fn_name} timed out[/red]")
            except Exception as e:
                txt = f"Tool Error: {e}"
                console.print(f"  [red]✗ {e}[/red]")

            # ── KEY FIX: always save the latest tool output ────────────────
            last_tool_result = txt

            messages.append({
                "role":         "tool",
                "tool_call_id": tc.id,
                "content":      txt,
            })

    # Max rounds reached — return whatever data we have
    console.print("  [yellow]⚠ Max rounds reached[/yellow]")
    return last_tool_result or "(Process exceeded max rounds without a final answer)"


# ── Public API ─────────────────────────────────────────────────────────────────

async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
    """
    Public async function consumed by graph.py's node_mcp_tool_call.

    Parameters
    ----------
    query   : Enriched query string (may contain [PRE-RESOLVED CODES] block).
    backend : "groq" or "ollama". Defaults to LLM_BACKEND env var.
    """
    b             = (backend or LLM_BACKEND).lower()
    server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            return await _run_query_with_session(query, session, b)


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1:
        asyncio.run(run_mcp_query(" ".join(sys.argv[1:])))