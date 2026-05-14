
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

# try:
#     from chroma_singleton import get_chroma_collection
#     tool_collection = get_chroma_collection()
# except Exception as _mcp_chroma_err:
#     console.print(f"  [mcp_client] Chroma init failed: {_mcp_chroma_err}")
#     tool_collection = None

    
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
# # Multi-intent single-session runner  (v10.3)
# #
# # All intents are passed as a combined query. The system prompt enumerates
# # every entity + its intent so the LLM can call tools for all of them in
# # one agentic loop. Results are tagged by entity_label so we can split
# # them back after the loop.
# # ══════════════════════════════════════════════════════════════════════════════

# MAX_CHARS_PER_INTENT = 3000   # hard cap per intent result before synthesis
# MAX_TOTAL_CHARS      = 10000  # hard cap for combined result passed to synthesis


# def _build_multi_intent_query(intents: list[dict]) -> str:
#     """
#     Build a single enriched query string containing all intent blocks.
#     Each block is a <PRE_RESOLVED> section tagged with Entity N so the
#     registry parser picks them up, plus a plain-language instruction.
#     """
#     lines = ["<PRE_RESOLVED>"]
#     for i, intent in enumerate(intents, 1):
#         codes = intent.get("resolved_codes") or {}
#         et    = intent.get("entity_type", "general")
#         name  = intent.get("entity", f"entity_{i}")
#         hint  = intent.get("tool_hint", "")
#         desc  = intent.get("intent_description", "")

#         lines.append(f"Entity {i}: {name}")
#         lines.append(f"  entity_type={et}")
#         if desc:
#             lines.append(f"  query_intent={desc}")
#         if hint:
#             lines.append(f"  recommended_tool={hint}")
#         for param, val in codes.items():
#             lines.append(f"  {param}={val}")

#         scheme = intent.get("scheme_name") or ""
#         amc    = intent.get("amc_name") or ""
#         if scheme:
#             lines.append(f"  scheme_name={scheme}")
#         if amc:
#             lines.append(f"  amc_name={amc}")

#     lines.append("</PRE_RESOLVED>")
#     lines.append("")
#     lines.append("User Query: Answer ALL of the following sub-questions:")
#     for i, intent in enumerate(intents, 1):
#         name = intent.get("entity", f"entity_{i}")
#         desc = intent.get("intent_description", "")
#         lines.append(f"  {i}. [{name}] {desc}")

#     return "\n".join(lines)


# def _trim_results_to_budget(
#     intent_results: list[dict],
#     max_per_intent: int = MAX_CHARS_PER_INTENT,
#     max_total:      int = MAX_TOTAL_CHARS,
# ) -> list[dict]:
#     """
#     Trim mcp_result strings so the combined payload fed to synthesis
#     stays within token budget. Trims longest result first.
#     """
#     results = [dict(r) for r in intent_results]

#     # Per-intent cap first
#     for r in results:
#         if len(r.get("mcp_result", "")) > max_per_intent:
#             r["mcp_result"] = (
#                 r["mcp_result"][:max_per_intent]
#                 + f"\n... [trimmed to {max_per_intent} chars]"
#             )

#     # Total cap — trim the longest until we're under budget
#     while sum(len(r.get("mcp_result", "")) for r in results) > max_total:
#         longest = max(results, key=lambda r: len(r.get("mcp_result", "")))
#         current = longest["mcp_result"]
#         if len(current) <= 200:
#             break  # nothing useful left to trim
#         longest["mcp_result"] = current[: len(current) - 500] + "\n... [trimmed]"

#     return results


# async def _run_all_intents_with_session(
#     intents: list[dict],
#     session: ClientSession,
#     backend: str,
# ) -> list[dict]:
#     """
#     Run ALL intents in a single MCP session.

#     Returns the same list with mcp_result populated per intent.
#     Results are tagged [entity_label / intent_description] in the raw
#     tool output, and we split them back by searching for those tags.
#     """
#     combined_query = _build_multi_intent_query(intents)

#     # Reuse the existing session runner — it already handles multi-entity
#     # via _EntityRegistry (parses all Entity N: blocks) and entity_label
#     # tagging in tool calls.
#     raw_result = await _run_query_with_session(combined_query, session, backend)

#     # ── Partition results back to each intent ────────────────────────────
#     # Strategy: look for per-entity tags the LLM or tool output may include.
#     # If we can't split cleanly, assign the full result to all intents
#     # (synthesis prompt already handles duplication gracefully).

#     updated = [dict(i) for i in intents]

#     # Try to find entity-specific sections in the raw result
#     # The LLM is prompted to label its responses; tool results are tagged
#     # with [tool_name] blocks from all_tool_results in _run_query_with_session.
#     for idx, intent in enumerate(updated):
#         name = intent.get("entity", "")
#         # Look for a section that mentions this entity
#         pattern = re.compile(
#             rf"(?:^|\n)(?:\[{re.escape(name)}[^\]]*\]|\*\*{re.escape(name)}"
#             rf"|\b{re.escape(name)}\b.{{0,60}}:)(.*?)(?=\n\[|\n\*\*|\Z)",
#             re.S | re.I,
#         )
#         m = pattern.search(raw_result)
#         if m and m.group(1).strip():
#             intent["mcp_result"] = m.group(1).strip()
#         else:
#             # Fallback: give every intent the full result;
#             # synthesis will extract what's relevant
#             intent["mcp_result"] = raw_result

#     return updated


# async def run_mcp_query_multi(
#     intents: list[dict],
#     backend: Optional[str] = None,
# ) -> list[dict]:
#     """
#     Public async entry point for multi-intent single-session calls.
#     Called by graph.py's node_mcp_tool_call_intents when len(intents) > 0.

#     Parameters
#     ----------
#     intents : List of IntentItem dicts (with resolved_codes already filled).
#     backend : "groq" or "ollama". Defaults to LLM_BACKEND env var.

#     Returns
#     -------
#     The same list with mcp_result populated on each item.
#     """
#     b             = (backend or LLM_BACKEND).lower()
#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])

#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()
#             results = await _run_all_intents_with_session(intents, session, b)

#     return _trim_results_to_budget(results)

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




# """
# mcp_client.py — Equifiz MF MCP Client (v4.1 — Fixed)

# Changes from v4.0
# ─────────────────────────────────────────────────────────────────────
# FIX 1 — 400 Bad Request (Groq) / Ollama history corruption
#   - All assistant messages with tool_calls now use content="" instead of
#     content=None/null. Both Groq and Ollama reject null here.
#   - Fixed in 3 places: main agentic loop, _ensure_codes_for_tool
#     auto-resolve block, and the Ollama _chat history append.

# FIX 2 — Ollama tool_call ID stability
#   - Ollama sometimes omits "id" on tool_calls. We now generate a
#     stable fallback ID (f"tc-{fn_name}-{i}") so message history
#     round-trips correctly.

# FIX 3 — Ollama response parsing robustness
#   - _chat Ollama branch now handles missing "tool_calls" key gracefully.
#   - _TC.id falls back to a generated ID if Ollama omits it.
#   - _TC.function.arguments always produces a JSON string even if
#     Ollama returns a dict (it usually does).

# FIX 4 — ChromaDB init
#   - Removed silent try/except that hid chroma failures.
#   - get_semantic_tools now guards against tool_collection=None and
#     falls back to returning all tools so the agent can still run.

# FIX 5 — run_in_executor timeout
#   - asyncio.wait_for wrapping run_in_executor now uses a dedicated
#     thread so the timeout actually fires; previous pattern could
#     silently hang on slow Ollama models.

# All v4.0 logic retained unchanged:
#   - Dynamic param discovery via _ToolParamIndex
#   - Entity-scoped code injection via _EntityRegistry
#   - Multi-entity / multi-intent single-session runner
#   - Integer coercion for numeric params
#   - Meta-description detection fallback
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

# # ── Config ─────────────────────────────────────────────────────────────────────

# SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
# CHROMA_PATH   = Path(__file__).parent / "chroma_db"
# MAX_ROUNDS    = 15

# LLM_BACKEND  = os.getenv("LLM_BACKEND", "ollama").lower()
# GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")
# OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
# OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
# EMBED_MODEL  = "embeddinggemma:latest"

# logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
# logger  = logging.getLogger("equifiz_mf_client")
# console = Console()

# # ── Thread pool for blocking LLM calls ────────────────────────────────────────
# # A dedicated executor avoids starving the default loop executor and lets
# # asyncio.wait_for actually cancel the future on timeout.
# _LLM_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="llm_worker")

# # ── ChromaDB Setup ─────────────────────────────────────────────────────────────
# # FIX 4: import directly — fail loudly if chroma_singleton is broken.
# # Indexing is handled by a separate script; this file only reads.
# from chroma_singleton import get_chroma_collection
# tool_collection = get_chroma_collection()

# # ── Known numeric ID params (used for schema coercion only) ───────────────────
# _NUMERIC_PARAMS = {"mf_schcode", "mf_cocode", "co_code", "index_code"}

# # ── Tools that are internal resolvers — never inject codes into these ──────────
# _RESOLVER_TOOLS = {"resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes",
#                    "resolve_nse_symbol", "get_company_details", "search_companies"}

# # ── DB param → entity family mapping ──────────────────────────────────────────
# _PARAM_TO_FAMILY: dict[str, str] = {
#     "co_code":    "stock",
#     "mf_schcode": "mf_scheme",
#     "mf_cocode":  "mf_amc",
#     "isin":       "etf",
#     "index_code": "index",
# }

# _FAMILY_TO_PARAM: dict[str, str] = {v: k for k, v in _PARAM_TO_FAMILY.items()}


# # ══════════════════════════════════════════════════════════════════════════════
# # Dynamic Tool Param Index
# # ══════════════════════════════════════════════════════════════════════════════

# class _ToolParamIndex:
#     """
#     Maps every tool to its required DB params by inspecting inputSchema.
#     Built from the live MCP tool list — zero hardcoding.
#     """

#     def __init__(self):
#         self._index: dict[str, dict] = {}

#     def build(self, raw_tools: list) -> None:
#         for tool in raw_tools:
#             schema   = tool.inputSchema or {}
#             props    = schema.get("properties", {})
#             required = set(schema.get("required", []))

#             if "params" in props and len(props) == 1:
#                 inner    = props["params"]
#                 props    = inner.get("properties", {})
#                 required = set(inner.get("required", []))

#             required_db = {p for p in required if p in _PARAM_TO_FAMILY}
#             optional_db = {p for p in props if p in _PARAM_TO_FAMILY and p not in required}

#             family = "general"
#             for param in list(required_db) + list(optional_db):
#                 family = _PARAM_TO_FAMILY.get(param, "general")
#                 break

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
#     FIX 4: Falls back to returning all tools if tool_collection is unavailable,
#     so the agent can still operate (just with a larger tool list).
#     """
#     CORE_PLUMBING = {
#         "resolve_mf_scheme", "resolve_mf_fund",
#         "resolve_nse_symbol", "search_companies",
#     }

#     # FIX 4: guard against None tool_collection
#     if tool_collection is None:
#         console.print(
#             "  [yellow]⚠ ChromaDB unavailable — returning all tools[/yellow]"
#         )
#         return all_openai_tools

#     try:
#         results       = tool_collection.query(query_texts=[query], n_results=top_k)
#         matched_names = set(results["ids"][0]) | CORE_PLUMBING
#     except Exception as e:
#         console.print(f"  [yellow]ChromaDB query failed: {e} — returning all tools[/yellow]")
#         return all_openai_tools

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
#         if self.entity_type == family:
#             return True
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
#     def __init__(self):
#         self.entities: list[_EntityInfo] = []
#         self._primary: _EntityInfo       = _EntityInfo("(primary)")

#     # ── Convenience properties ─────────────────────────────────────────────
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
#             pass

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

#     # ── Entity-scoped code lookup ──────────────────────────────────────────
#     def find_entity_for_tool(
#         self,
#         tool_name:   str,
#         tool_family: str,
#         label_hint:  Optional[str] = None,
#     ) -> Optional[_EntityInfo]:
#         if label_hint:
#             for e in self.entities:
#                 if e.label.lower() == label_hint.lower():
#                     return e

#         for e in self.entities:
#             if e.recommended_tool == tool_name:
#                 return e

#         for e in self.entities:
#             if e.matches_family(tool_family):
#                 return e

#         needed_param = _FAMILY_TO_PARAM.get(tool_family)
#         if needed_param:
#             for e in self.entities:
#                 if e.get_code_for_param(needed_param) is not None:
#                     return e

#         return self._primary if self._primary.has_any_code() else None

#     def get_name_hint_for_family(self, family: str) -> str:
#         for e in self.entities:
#             if e.matches_family(family):
#                 if family == "mf_scheme" and e.scheme_name:
#                     return e.scheme_name
#                 if family == "mf_amc" and e.amc_name:
#                     return e.amc_name
#                 if family == "stock" and e.company_name:
#                     return e.company_name
#                 return e.label
#         return (self._primary.scheme_name or self._primary.amc_name
#                 or self._primary.company_name or self._primary.label or "")

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
#     updated_args = dict(tool_args)

#     label_hint         = updated_args.pop("entity_label", None)
#     required_db_params = param_index.get_required_db_params(tool_name)
#     tool_family        = param_index.get_family(tool_name)

#     if not required_db_params:
#         return updated_args, messages

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

#     for param in required_db_params:
#         if param in updated_args and updated_args[param] is not None:
#             try:
#                 if param in _NUMERIC_PARAMS:
#                     updated_args[param] = int(float(str(updated_args[param])))
#             except (ValueError, TypeError):
#                 pass
#             continue

#         value = entity.get_code_for_param(param) if entity else None

#         if value is None:
#             family    = _PARAM_TO_FAMILY.get(param, "general")
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
#                         if entity:
#                             value = entity.get_code_for_param(param)
#                         if value is None:
#                             value = registry._primary.get_code_for_param(param)

#                         auto_id = f"auto-{param}-{value}"
#                         # FIX 1: content must be "" not None
#                         messages.append({
#                             "role":    "assistant",
#                             "content": "",
#                             "tool_calls": [{
#                                 "id":   auto_id,
#                                 "type": "function",
#                                 "function": {
#                                     "name":      resolver_tool,
#                                     "arguments": json.dumps({"query": name_hint}),
#                                 },
#                             }],
#                         })
#                         messages.append({
#                             "role":         "tool",
#                             "tool_call_id": auto_id,
#                             "content":      txt,
#                         })
#                         console.print(f"    [green]✔ {param}={value}[/green]")
#                     else:
#                         console.print(
#                             f"  [red]⚠ Could not resolve {param} for: {name_hint[:60]}[/red]"
#                         )
#                 except Exception as e:
#                     console.print(f"  [red]Auto-resolution failed ({param}): {e}[/red]")

#         if value is not None:
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
#             entity_block += f"  co_code={e.co_code}  ← pass THIS to equity/stock tools ONLY\n"
#         if e.mf_schcode is not None:
#             entity_block += f"  mf_schcode={e.mf_schcode}  ← pass THIS to scheme/MF tools ONLY\n"
#         if e.mf_cocode is not None:
#             entity_block += f"  mf_cocode={e.mf_cocode}  ← pass THIS to AMC/fund-house tools ONLY\n"
#         if e.isin:
#             entity_block += f"  isin={e.isin}  ← pass THIS to ETF tools ONLY\n"
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
#     """
#     Synchronous LLM call — runs in a thread via _LLM_EXECUTOR.

#     FIX 2 & 3: Ollama tool_call ID and arguments handling.
#       - Ollama often omits "id" on tool_calls → we generate a stable
#         fallback: f"tc-{fn_name}-{index}".
#       - Ollama returns arguments as a dict, not a JSON string → we
#         always serialise to a JSON string so downstream code can
#         json.loads() safely.
#       - Ollama "content" on assistant messages can be None or "" when
#         only tool_calls are present — we normalise to "" here so
#         callers never see None.
#     """
#     if backend == "ollama":
#         import urllib.request

#         payload = {
#             "model":    OLLAMA_MODEL,
#             "messages": messages,
#             "tools":    tools,
#             "stream":   False,
#             "options":  {"temperature": 0.0},
#         }
#         req = urllib.request.Request(
#             f"{OLLAMA_HOST}/api/chat",
#             data=json.dumps(payload).encode(),
#             headers={"Content-Type": "application/json"},
#             method="POST",
#         )
#         data = json.loads(urllib.request.urlopen(req, timeout=600).read().decode())
#         omsg = data.get("message", {})

#         # FIX 2 & 3: robust tool_call parsing for Ollama
#         class _TC:
#             def __init__(self, d: dict, index: int):
#                 fn_name = d.get("function", {}).get("name", f"unknown_{index}")
#                 raw_args = d.get("function", {}).get("arguments", {})

#                 # Ollama returns arguments as a dict — normalise to JSON string
#                 if isinstance(raw_args, dict):
#                     args_str = json.dumps(raw_args)
#                 elif isinstance(raw_args, str):
#                     args_str = raw_args
#                 else:
#                     args_str = "{}"

#                 # Ollama often omits id — generate a stable one
#                 self.id = d.get("id") or f"tc-{fn_name}-{index}"

#                 self.function = type("F", (), {
#                     "name":      fn_name,
#                     "arguments": args_str,
#                 })()

#         raw_tool_calls = omsg.get("tool_calls") or []

#         class _Choice:
#             def __init__(self):
#                 tcs = [_TC(t, i) for i, t in enumerate(raw_tool_calls)]
#                 self.message = type("M", (), {
#                     # FIX 1: normalise None content to "" for Ollama
#                     "content":    omsg.get("content") or "",
#                     "tool_calls": tcs or None,
#                 })()

#         return _Choice()

#     else:
#         # Groq (OpenAI-compatible)
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
#     registry = _EntityRegistry()
#     registry.parse_injected(query)

#     is_multi    = len(registry.entities) > 1
#     entity_hint = _extract_entity_hint(query)

#     console.print(Panel(
#         f"[bold blue]Incoming Query ({len(registry.entities)} entities, "
#         f"multi={is_multi}):[/bold blue]\n{query[:400]}",
#         title="LLM Input",
#     ))

#     raw_tools   = (await session.list_tools()).tools
#     param_index = _ToolParamIndex()
#     param_index.build(raw_tools)

#     all_manifest  = [_to_openai_tool(t, is_multi=is_multi) for t in raw_tools]
#     top_k         = 14 if is_multi else 8
#     current_tools = get_semantic_tools(query, all_manifest, top_k=top_k)
#     console.print(
#         f"  [Semantic Router] selected {len(current_tools)} tools "
#         f"(top_k={top_k}, is_multi={is_multi})"
#     )

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

#     # ── Agentic loop ──────────────────────────────────────────────────────
#     loop = asyncio.get_event_loop()

#     for round_num in range(1, MAX_ROUNDS + 1):
#         console.print(f"  [dim]── Round {round_num} ──[/dim]")

#         # FIX 5: use dedicated executor so wait_for timeout actually fires
#         try:
#             choice = await asyncio.wait_for(
#                 loop.run_in_executor(
#                     _LLM_EXECUTOR,
#                     _chat, messages, current_tools, backend,
#                 ),
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

#         # ── Prepare tool calls ────────────────────────────────────────────
#         sanitized_for_execution: list[tuple[str, dict, str]] = []
#         history_tool_calls: list[dict] = []

#         for tc in msg.tool_calls:
#             fn_name  = tc.function.name
#             raw_args = tc.function.arguments

#             if isinstance(raw_args, str):
#                 try:
#                     args_dict = json.loads(raw_args)
#                 except json.JSONDecodeError:
#                     args_dict = {}
#             else:
#                 args_dict = dict(raw_args)

#             for param in _NUMERIC_PARAMS:
#                 if param in args_dict:
#                     try:
#                         args_dict[param] = int(float(str(args_dict[param])))
#                     except (ValueError, TypeError):
#                         args_dict.pop(param)

#             clean_args_str = json.dumps(args_dict)
#             sanitized_for_execution.append((fn_name, args_dict, tc.id))
#             history_tool_calls.append({
#                 "id":   tc.id,
#                 "type": "function",
#                 "function": {"name": fn_name, "arguments": clean_args_str},
#             })

#         # FIX 1: content must never be None — use "" for tool-call-only turns
#         messages.append({
#             "role":       "assistant",
#             "content":    msg.content or "",
#             "tool_calls": history_tool_calls,
#         })

#         # ── Execute each tool call ────────────────────────────────────────
#         for fn_name, fn_args, tc_id in sanitized_for_execution:

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
#                 console.print(f"  [green]✔ {fn_name}[/green]: {len(txt)} chars")

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
#                 "role":         "tool",
#                 "tool_call_id": tc_id,
#                 "content":      txt,
#             })

#     console.print("  [yellow]⚠ Max rounds reached[/yellow]")
#     return _combined_fallback() or "(Process exceeded max rounds)"


# # ══════════════════════════════════════════════════════════════════════════════
# # Multi-intent single-session runner  (v10.3)
# # ══════════════════════════════════════════════════════════════════════════════

# MAX_CHARS_PER_INTENT = 3000
# MAX_TOTAL_CHARS      = 10000


# def _build_multi_intent_query(intents: list[dict]) -> str:
#     lines = ["<PRE_RESOLVED>"]
#     for i, intent in enumerate(intents, 1):
#         codes = intent.get("resolved_codes") or {}
#         et    = intent.get("entity_type", "general")
#         name  = intent.get("entity", f"entity_{i}")
#         hint  = intent.get("tool_hint", "")
#         desc  = intent.get("intent_description", "")

#         lines.append(f"Entity {i}: {name}")
#         lines.append(f"  entity_type={et}")
#         if desc:
#             lines.append(f"  query_intent={desc}")
#         if hint:
#             lines.append(f"  recommended_tool={hint}")
#         for param, val in codes.items():
#             lines.append(f"  {param}={val}")

#         scheme = intent.get("scheme_name") or ""
#         amc    = intent.get("amc_name") or ""
#         if scheme:
#             lines.append(f"  scheme_name={scheme}")
#         if amc:
#             lines.append(f"  amc_name={amc}")

#     lines.append("</PRE_RESOLVED>")
#     lines.append("")
#     lines.append("User Query: Answer ALL of the following sub-questions:")
#     for i, intent in enumerate(intents, 1):
#         name = intent.get("entity", f"entity_{i}")
#         desc = intent.get("intent_description", "")
#         lines.append(f"  {i}. [{name}] {desc}")

#     return "\n".join(lines)


# def _trim_results_to_budget(
#     intent_results: list[dict],
#     max_per_intent: int = MAX_CHARS_PER_INTENT,
#     max_total:      int = MAX_TOTAL_CHARS,
# ) -> list[dict]:
#     results = [dict(r) for r in intent_results]

#     for r in results:
#         if len(r.get("mcp_result", "")) > max_per_intent:
#             r["mcp_result"] = (
#                 r["mcp_result"][:max_per_intent]
#                 + f"\n... [trimmed to {max_per_intent} chars]"
#             )

#     while sum(len(r.get("mcp_result", "")) for r in results) > max_total:
#         longest = max(results, key=lambda r: len(r.get("mcp_result", "")))
#         current = longest["mcp_result"]
#         if len(current) <= 200:
#             break
#         longest["mcp_result"] = current[: len(current) - 500] + "\n... [trimmed]"

#     return results


# async def _run_all_intents_with_session(
#     intents: list[dict],
#     session: ClientSession,
#     backend: str,
# ) -> list[dict]:
#     combined_query = _build_multi_intent_query(intents)
#     raw_result     = await _run_query_with_session(combined_query, session, backend)

#     updated = [dict(i) for i in intents]

#     for idx, intent in enumerate(updated):
#         name = intent.get("entity", "")
#         pattern = re.compile(
#             rf"(?:^|\n)(?:\[{re.escape(name)}[^\]]*\]|\*\*{re.escape(name)}"
#             rf"|\b{re.escape(name)}\b.{{0,60}}:)(.*?)(?=\n\[|\n\*\*|\Z)",
#             re.S | re.I,
#         )
#         m = pattern.search(raw_result)
#         if m and m.group(1).strip():
#             intent["mcp_result"] = m.group(1).strip()
#         else:
#             intent["mcp_result"] = raw_result

#     return updated


# # ══════════════════════════════════════════════════════════════════════════════
# # Public API
# # ══════════════════════════════════════════════════════════════════════════════

# async def run_mcp_query_multi(
#     intents: list[dict],
#     backend: Optional[str] = None,
# ) -> list[dict]:
#     """
#     Public async entry point for multi-intent single-session calls.
#     Called by graph.py's node_mcp_tool_call_intents when len(intents) > 0.
#     """
#     b             = (backend or LLM_BACKEND).lower()
#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])

#     async with stdio_client(server_params) as (read, write):
#         async with ClientSession(read, write) as session:
#             await session.initialize()
#             results = await _run_all_intents_with_session(intents, session, b)

#     return _trim_results_to_budget(results)


# async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
#     """
#     Public async entry point consumed by graph.py's node_mcp_tool_call.
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
mcp_client.py — Equifiz MF MCP Client (v5.0 — Ollama-fixed, simplified)

Key fixes from v4.1
───────────────────
FIX A — 400 Bad Request on Round 2 (Ollama)
  Root cause 1: Ollama rejects `tool_call_id` in role="tool" messages.
                We now omit tool_call_id when the backend is Ollama.
  Root cause 2: Ollama rejects `type:"function"` wrapper inside
                tool_calls in replayed assistant messages.
                We now store and replay the raw tool_calls exactly as
                Ollama returned them (already normalised to dicts).
  Root cause 3: Ollama sometimes sends content=None on the final
                (non-tool-call) turn. Normalised to "".

FIX B — Simplified architecture
  - _EntityRegistry and _ToolParamIndex merged into lighter helpers.
  - _ensure_codes_for_tool simplified: one function, no class overhead.
  - Multi-intent: each intent still gets its own tool call in the same
    session; entity_label routing preserved.
  - Removed dead code paths (Groq kept but clearly separated).

All v4.1 logic that still makes sense is preserved:
  - Semantic tool selection via ChromaDB
  - PRE_RESOLVED block parsing
  - Auto-resolution of missing codes via resolver tools
  - Meta-description detection / raw-data fallback
  - MAX_ROUNDS guard
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

# ── Config ─────────────────────────────────────────────────────────────────────

SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
CHROMA_PATH   = Path(__file__).parent / "chroma_db"
MAX_ROUNDS    = 15

LLM_BACKEND  = os.getenv("LLM_BACKEND", "ollama").lower()
OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger  = logging.getLogger("equifiz_mf_client")
console = Console()

_LLM_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="llm_worker")

# ── ChromaDB ───────────────────────────────────────────────────────────────────
from chroma_singleton import get_chroma_collection
tool_collection = get_chroma_collection()

# ── Constants ──────────────────────────────────────────────────────────────────
_NUMERIC_PARAMS = {"mf_schcode", "mf_cocode", "co_code", "index_code"}

_RESOLVER_TOOLS = {
    "resolve_mf_scheme", "resolve_mf_fund", "search_mf_schemes",
    "resolve_nse_symbol", "get_company_details", "search_companies",
}

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
# Message history helpers — Ollama-safe
# ══════════════════════════════════════════════════════════════════════════════

def _assistant_msg(content: str, tool_calls: Optional[list] = None) -> dict:
    """
    Build an assistant history entry that Ollama accepts on replay.
    - content is always a string (never None)
    - tool_calls uses Ollama's native format: list of
      {"function": {"name": ..., "arguments": {...}}}
      NO "id" or "type" wrappers — Ollama rejects those on replay.
    """
    msg: dict = {"role": "assistant", "content": content or ""}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    return msg


def _tool_result_msg(tc_id: str, content: str, backend: str) -> dict:
    """
    Build a tool-result history entry.
    Ollama does NOT accept tool_call_id — omit it for Ollama.
    OpenAI/Groq require tool_call_id.
    """
    if backend == "ollama":
        return {"role": "tool", "content": content}
    return {"role": "tool", "tool_call_id": tc_id, "content": content}


# ══════════════════════════════════════════════════════════════════════════════
# LLM wrapper — Ollama only (Groq path kept for completeness)
# ══════════════════════════════════════════════════════════════════════════════

class _ToolCall:
    """Normalised tool call regardless of backend."""
    __slots__ = ("id", "name", "arguments")

    def __init__(self, tc_id: str, name: str, arguments: dict):
        self.id        = tc_id
        self.name      = name
        self.arguments = arguments  # always a dict


class _LLMResponse:
    __slots__ = ("content", "tool_calls", "raw_tool_calls")

    def __init__(self, content: str, tool_calls: list[_ToolCall], raw_tool_calls: list):
        self.content        = content or ""
        self.tool_calls     = tool_calls       # list[_ToolCall]
        self.raw_tool_calls = raw_tool_calls   # Ollama-native dicts for history replay


def _chat_ollama(messages: list, tools: list) -> _LLMResponse:
    """
    Calls Ollama /api/chat and returns a normalised _LLMResponse.

    History format Ollama expects on replay:
      assistant: {"role":"assistant","content":"","tool_calls":[
                    {"function":{"name":"...","arguments":{...}}}
                  ]}
      tool:      {"role":"tool","content":"..."}   ← no tool_call_id!

    We store raw_tool_calls in that exact Ollama-native format so _assistant_msg
    can replay it verbatim without any OpenAI-style wrappers.
    """
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
    raw = json.loads(urllib.request.urlopen(req, timeout=600).read().decode())
    omsg = raw.get("message", {})

    content        = omsg.get("content") or ""
    raw_tool_calls = omsg.get("tool_calls") or []   # Ollama-native format

    tool_calls: list[_ToolCall] = []
    for i, tc in enumerate(raw_tool_calls):
        fn      = tc.get("function", {})
        name    = fn.get("name", f"unknown_{i}")
        args    = fn.get("arguments", {})
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except json.JSONDecodeError:
                args = {}
        tc_id = tc.get("id") or f"tc-{name}-{i}"
        tool_calls.append(_ToolCall(tc_id, name, args))

    return _LLMResponse(content, tool_calls, raw_tool_calls)


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

    raw_tool_calls = []
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
        tool_calls.append(_ToolCall(tc.id, tc.function.name, args))
        raw_tool_calls.append({
            "id": tc.id,
            "type": "function",
            "function": {"name": tc.function.name, "arguments": tc.function.arguments},
        })

    return _LLMResponse(msg.content or "", tool_calls, raw_tool_calls)


def _chat(messages: list, tools: list, backend: str) -> _LLMResponse:
    if backend == "ollama":
        return _chat_ollama(messages, tools)
    return _chat_groq(messages, tools)


# ══════════════════════════════════════════════════════════════════════════════
# Tool param index — built from live MCP schema
# ══════════════════════════════════════════════════════════════════════════════

class ToolParamIndex:
    def __init__(self):
        self._idx: dict[str, dict] = {}

    def build(self, raw_tools: list) -> None:
        for tool in raw_tools:
            schema   = tool.inputSchema or {}
            props    = schema.get("properties", {})
            required = set(schema.get("required", []))

            # Unwrap nested "params" object if that's the only property
            if "params" in props and len(props) == 1:
                inner    = props["params"]
                props    = inner.get("properties", {})
                required = set(inner.get("required", []))

            req_db = {p for p in required if p in _PARAM_TO_FAMILY}
            opt_db = {p for p in props    if p in _PARAM_TO_FAMILY and p not in required}

            family = "general"
            for p in list(req_db) + list(opt_db):
                family = _PARAM_TO_FAMILY.get(p, "general")
                break

            self._idx[tool.name] = {
                "req_db": req_db,
                "opt_db": opt_db,
                "family": family,
                "all":    set(props.keys()),
            }

        db_count = sum(1 for v in self._idx.values() if v["req_db"])
        console.print(f"  [ToolParamIndex] {len(self._idx)} tools, {db_count} DB-bound")

    def required_db(self, name: str) -> set[str]:
        return self._idx.get(name, {}).get("req_db", set())

    def family(self, name: str) -> str:
        return self._idx.get(name, {}).get("family", "general")


# ══════════════════════════════════════════════════════════════════════════════
# Entity registry — parsed from PRE_RESOLVED block
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
        return any(self.code(p) is not None for p in _PARAM_TO_FAMILY)

    def name_hint(self) -> str:
        return self.scheme_name or self.amc_name or self.company_name or self.label


class EntityRegistry:
    def __init__(self):
        self.entities: list[Entity] = []

    def parse(self, query: str) -> None:
        m = re.search(r"<PRE_RESOLVED>(.*?)</PRE_RESOLVED>", query, re.S | re.I)
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
            self._set(current, key, val)

        if not self.entities:
            # Flat fallback — scan for known params
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
            console.print(f"  [Registry] {e.label} ({e.entity_type})")

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

    def find(self, tool_name: str, family: str, label_hint: Optional[str] = None) -> Optional[Entity]:
        if label_hint:
            for e in self.entities:
                if e.label.lower() == label_hint.lower():
                    return e
        for e in self.entities:
            if e.recommended_tool == tool_name:
                return e
        param = _FAMILY_TO_PARAM.get(family)
        for e in self.entities:
            if e.entity_type == family and e.code(param) is not None:
                return e
        for e in self.entities:
            if param and e.code(param) is not None:
                return e
        return self.entities[0] if self.entities else None

    def update_from_resolver(self, tool_name: str, result: str) -> None:
        if "RESOLVED" not in result:
            return
        mapping = {
            "resolve_mf_scheme":  ("mf_schcode", r"mf_schcode\s*[:=]\s*(\d+)"),
            "resolve_mf_fund":    ("mf_cocode",  r"mf_cocode\s*[:=]\s*(\d+)"),
            "resolve_nse_symbol": ("co_code",    r"co_code\s*[:=]\s*(\d+)"),
            "get_company_details":("co_code",    r"co_code\s*[:=]\s*(\d+)"),
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
                break


# ══════════════════════════════════════════════════════════════════════════════
# Semantic tool selection
# ══════════════════════════════════════════════════════════════════════════════

_CORE_TOOLS = {
    "resolve_mf_scheme", "resolve_mf_fund",
    "resolve_nse_symbol", "search_companies",
}


def get_semantic_tools(query: str, all_tools: list[dict], top_k: int = 10) -> list[dict]:
    if tool_collection is None:
        return all_tools
    try:
        results       = tool_collection.query(query_texts=[query], n_results=top_k)
        matched_names = set(results["ids"][0]) | _CORE_TOOLS
        return [t for t in all_tools if t["function"]["name"] in matched_names]
    except Exception as e:
        console.print(f"  [yellow]ChromaDB error: {e} — using all tools[/yellow]")
        return all_tools


# ══════════════════════════════════════════════════════════════════════════════
# Tool schema builder
# ══════════════════════════════════════════════════════════════════════════════

def _to_openai_tool(tool, is_multi: bool = False) -> dict:
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

    if is_multi:
        props["entity_label"] = {
            "type":        "string",
            "description": "Label of the entity this call is for (from ENTITIES TO PROCESS).",
        }

    return {
        "type": "function",
        "function": {
            "name":        tool.name,
            "description": (tool.description or tool.name).strip(),
            "parameters":  {"type": "object", "properties": props, "required": required},
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# Code injection — resolves missing DB params before tool call
# ══════════════════════════════════════════════════════════════════════════════

async def _inject_codes(
    tool_name:   str,
    tool_args:   dict,
    registry:    EntityRegistry,
    param_index: ToolParamIndex,
    session:     ClientSession,
    messages:    list,
    entity_hint: str,
    backend:     str,
) -> dict:
    """
    Fills missing required DB params into tool_args using registry codes.
    If still missing, auto-calls the appropriate resolver tool.
    Returns the updated args dict.
    """
    label_hint = tool_args.pop("entity_label", None)
    required   = param_index.required_db(tool_name)
    family     = param_index.family(tool_name)

    if not required:
        return tool_args

    entity = registry.find(tool_name, family, label_hint)

    for param in required:
        # Already present and valid
        if param in tool_args and tool_args[param] is not None:
            try:
                if param in _NUMERIC_PARAMS:
                    tool_args[param] = int(float(str(tool_args[param])))
            except (ValueError, TypeError):
                tool_args.pop(param)
            continue

        # Try registry
        value = entity.code(param) if entity else None

        # Auto-resolve if still missing
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
                        value = (entity.code(param) if entity else None) or \
                                (registry.entities[0].code(param) if registry.entities else None)

                        auto_id = f"auto-{param}-{value}"
                        # Append resolver call to history in Ollama-safe format
                        messages.append(_assistant_msg("", [{
                            "function": {"name": resolver, "arguments": {"query": hint}}
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
# System prompt
# ══════════════════════════════════════════════════════════════════════════════

def _system_prompt(registry: EntityRegistry, is_multi: bool) -> str:
    base = """\
You are a financial analyst assistant for Indian markets with tools covering
equities, mutual funds, ETFs, and indices.

RULES:
- Verified numeric IDs are provided. Use them DIRECTLY — do not call resolver
  tools unless an ID is genuinely absent.
- Never fabricate data. If a tool returns nothing, say so.
- For multi-entity queries: call ONE tool per entity with THAT entity's codes.
  Include "entity_label": "<label>" in every tool call so codes are routed correctly.
- Return ALL numeric values from tools verbatim. Do NOT describe what a tool
  returns — return the actual data.
"""
    if not registry.entities:
        return base

    block = "\n\nENTITIES TO PROCESS:\n"
    for i, e in enumerate(registry.entities, 1):
        block += f"\nEntity {i}: {e.label}\n"
        block += f"  type: {e.entity_type}\n"
        if e.query_intent:
            block += f"  data needed: {e.query_intent}\n"
        if e.recommended_tool:
            block += f"  use tool: {e.recommended_tool}\n"
        for attr, label in [
            ("co_code",    "co_code    ← equity/stock tools only"),
            ("mf_schcode", "mf_schcode ← MF scheme tools only"),
            ("mf_cocode",  "mf_cocode  ← AMC/fund-house tools only"),
            ("isin",       "isin       ← ETF tools only"),
            ("index_code", "index_code ← index tools only"),
        ]:
            v = e.code(attr)
            if v is not None:
                block += f"  {label}={v}\n"
    block += "\nProcess every entity. Return ALL actual numeric values."
    return base + block


# ══════════════════════════════════════════════════════════════════════════════
# Meta-description guard
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
    if len(nums) < 3:
        console.print(f"  [yellow]⚠ Only {len(nums)} numeric values — likely meta[/yellow]")
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# Core agentic loop
# ══════════════════════════════════════════════════════════════════════════════

async def _run_query(query: str, session: ClientSession, backend: str) -> str:
    registry = EntityRegistry()
    registry.parse(query)

    is_multi    = len(registry.entities) > 1
    entity_hint = re.split(r"\[(PRE-RESOLVED|VERIFIED|SUGGESTED|RECOMMENDED)", query)[0].strip()

    console.print(Panel(
        f"[bold blue]Query ({len(registry.entities)} entities, multi={is_multi}):[/bold blue]\n"
        f"{query[:400]}",
        title="LLM Input",
    ))

    raw_tools   = (await session.list_tools()).tools
    param_index = ToolParamIndex()
    param_index.build(raw_tools)

    all_schemas  = [_to_openai_tool(t, is_multi=is_multi) for t in raw_tools]
    top_k        = 8 if is_multi else 4
    active_tools = get_semantic_tools(query, all_schemas, top_k=top_k)
    console.print(
        f"  [Semantic Router] {len(active_tools)} tools selected (top_k={top_k})"
    )

    messages: list[dict] = [
        {"role": "system",  "content": _system_prompt(registry, is_multi)},
        {"role": "user",    "content": query},
    ]

    all_results: list[str] = []

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
            console.print(f"  [magenta]Assistant:[/magenta] {response.content[:200]}")

        # ── No tool calls → final answer ──────────────────────────────────
        if not response.tool_calls:
            if response.content and not _is_meta(response.content):
                return response.content
            break

        # ── Append assistant turn (Ollama-native format) ──────────────────
        # raw_tool_calls are already in Ollama's format: [{"function":{...}}]
        # This is what Ollama expects to see when replayed in history.
        messages.append(_assistant_msg(response.content, response.raw_tool_calls))

        # ── Execute each tool call ────────────────────────────────────────
        for tc in response.tool_calls:
            args = dict(tc.arguments)

            # Coerce numeric params
            for param in _NUMERIC_PARAMS:
                if param in args:
                    try:
                        args[param] = int(float(str(args[param])))
                    except (ValueError, TypeError):
                        args.pop(param, None)

            # Inject missing DB codes (skip for resolver tools)
            if tc.name not in _RESOLVER_TOOLS:
                args = await _inject_codes(
                    tc.name, args, registry, param_index,
                    session, messages, entity_hint, backend,
                )

            console.print(
                "  [cyan]🛠  " + tc.name + "[/cyan]  args={"
                + ", ".join(f"{k}={v}" for k, v in args.items() if k != "entity_label")
                + "}"
            )

            try:
                res = await asyncio.wait_for(session.call_tool(tc.name, args), timeout=600)
                txt = res.content[0].text if res.content else "Empty response"
                console.print(f"  [green]✔ {tc.name}[/green]: {len(txt)} chars")

                if tc.name in _RESOLVER_TOOLS:
                    registry.update_from_resolver(tc.name, txt)
                else:
                    all_results.append(f"[{tc.name}]\n{txt}")

            except asyncio.TimeoutError:
                txt = f"Error: {tc.name} timed out."
                console.print(f"  [red]✗ {tc.name} timed out[/red]")
            except Exception as e:
                txt = f"Tool Error: {e}"
                console.print(f"  [red]✗ {tc.name}: {e}[/red]")

            # ── Append tool result (Ollama-safe: no tool_call_id) ─────────
            messages.append(_tool_result_msg(tc.id, txt, backend))

    # ── Fallback: return raw tool data ────────────────────────────────────
    if all_results:
        console.print("  [green]✔ Returning raw tool data[/green]")
        return "\n\n---\n\n".join(all_results)
    return "(No data found)"


# ══════════════════════════════════════════════════════════════════════════════
# Multi-intent builder
# ══════════════════════════════════════════════════════════════════════════════

MAX_CHARS_PER_INTENT = 3000
MAX_TOTAL_CHARS      = 10000


def _build_multi_query(intents: list[dict]) -> str:
    lines = ["<PRE_RESOLVED>"]
    for i, intent in enumerate(intents, 1):
        codes = intent.get("resolved_codes") or {}
        name  = intent.get("entity", f"entity_{i}")
        lines.append(f"Entity {i}: {name}")
        lines.append(f"  entity_type={intent.get('entity_type', 'general')}")
        if desc := intent.get("intent_description", ""):
            lines.append(f"  query_intent={desc}")
        if hint := intent.get("tool_hint", ""):
            lines.append(f"  recommended_tool={hint}")
        for param, val in codes.items():
            lines.append(f"  {param}={val}")
        if s := intent.get("scheme_name", ""):
            lines.append(f"  scheme_name={s}")
        if a := intent.get("amc_name", ""):
            lines.append(f"  amc_name={a}")
    lines.append("</PRE_RESOLVED>")
    lines.append("\nUser Query: Answer ALL of the following sub-questions:")
    for i, intent in enumerate(intents, 1):
        name = intent.get("entity", f"entity_{i}")
        desc = intent.get("intent_description", "")
        lines.append(f"  {i}. [{name}] {desc}")
    return "\n".join(lines)


def _trim(results: list[dict]) -> list[dict]:
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


async def _run_all_intents(
    intents: list[dict], session: ClientSession, backend: str
) -> list[dict]:
    query      = _build_multi_query(intents)
    raw_result = await _run_query(query, session, backend)

    updated = [dict(i) for i in intents]
    for intent in updated:
        name = intent.get("entity", "")
        pat  = re.compile(
            rf"(?:^|\n)(?:\[{re.escape(name)}[^\]]*\]|\*\*{re.escape(name)}"
            rf"|\b{re.escape(name)}\b.{{0,60}}:)(.*?)(?=\n\[|\n\*\*|\Z)",
            re.S | re.I,
        )
        m = pat.search(raw_result)
        intent["mcp_result"] = m.group(1).strip() if (m and m.group(1).strip()) else raw_result

    return updated


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

async def run_mcp_query(query: str, backend: Optional[str] = None) -> str:
    b             = (backend or LLM_BACKEND).lower()
    server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            return await _run_query(query, session, b)


async def run_mcp_query_multi(
    intents: list[dict], backend: Optional[str] = None
) -> list[dict]:
    b             = (backend or LLM_BACKEND).lower()
    server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            results = await _run_all_intents(intents, session, b)
    return _trim(results)


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1:
        asyncio.run(run_mcp_query(" ".join(sys.argv[1:])))