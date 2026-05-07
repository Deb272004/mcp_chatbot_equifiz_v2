import os
import asyncio
import chromadb
from chromadb.utils import embedding_functions
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from pathlib import Path

# --- Configuration ---
# Path to your existing server script
SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
CHROMA_PATH = Path(__file__).parent / "chroma_db"
OLLAMA_MODEL = "embeddinggemma:latest" # or "mxbai-embed-large"

# Initialize ChromaDB Persistent Client
client = chromadb.PersistentClient(path=str(CHROMA_PATH))

# Use Ollama for Embeddings
# Note: Ensure Ollama is running and the model is pulled (ollama pull nomic-embed-text)
ollama_ef = embedding_functions.OllamaEmbeddingFunction(
    model_name=OLLAMA_MODEL,
    url="http://localhost:11434/api/embeddings",
)

collection = client.get_or_create_collection(
    name="equifiz_tools",
    embedding_function=ollama_ef,
    metadata={"hnsw:space": "cosine"} # Cosine similarity is best for text
)

# async def index_tools():
#     print(f"🚀 Starting Indexer...")
#     print(f"🔗 Connecting to server: {SERVER_SCRIPT.name}")

#     server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
    
#     try:
#         async with stdio_client(server_params) as (read, write):
#             async with ClientSession(read, write) as session:
#                 await session.initialize()
                
#                 # Fetch tools from MCP Server
#                 response = await session.list_tools()
#                 tools = response.tools
                
#                 print(f"📦 Found {len(tools)} tools. Generating embeddings...")

#                 ids = []
#                 documents = []
#                 metadatas = []

#                 for tool in tools:
#                     # We create a rich document combining name and description for better search
#                     doc_content = f"Tool Name: {tool.name}\nDescription: {tool.description}"
                    
#                     ids.append(tool.name)
#                     documents.append(doc_content)
#                     metadatas.append({
#                         "name": tool.name,
#                         "description": tool.description[:200] # Truncated for storage
#                     })

#                 # Clear existing and add new
#                 # This ensures your DB stays fresh if you rename tools
#                 existing_ids = collection.get()['ids']
#                 if existing_ids:
#                     collection.delete(ids=existing_ids)

#                 collection.add(
#                     ids=ids,
#                     documents=documents,
#                     metadatas=metadatas
#                 )

#                 print(f"✅ Successfully indexed {len(ids)} tools in {CHROMA_PATH}")

#     except Exception as e:
#         print(f"❌ Indexing failed: {e}")

# if __name__ == "__main__":
#     asyncio.run(index_tools())

async def index_tools():
    print(f"🚀 Starting Indexer...")
    server_params = StdioServerParameters(command="python", args=[str(SERVER_SCRIPT)])
    
    try:
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                response = await session.list_tools()
                tools = response.tools
                
                print(f"📦 Found {len(tools)} tools. Processing parameters...")

                ids = []
                documents = []
                metadatas = []

                for tool in tools:
                    # 1. Extract parameter names from the inputSchema
                    # tool.inputSchema typically contains {'type': 'object', 'properties': {...}, 'required': [...]}
                    properties = tool.inputSchema.get("properties", {})
                    required_params = tool.inputSchema.get("required", [])
                    param_names = list(properties.keys())
                    
                    # 2. Create a rich document for semantic search
                    doc_content = (
                        f"Tool Name: {tool.name}\n"
                        f"Description: {tool.description}\n"
                        f"Required Parameters: {', '.join(required_params)}\n"
                        f"All Parameters: {', '.join(param_names)}"
                    )
                    
                    ids.append(tool.name)
                    documents.append(doc_content)
                    
                    # 3. Store parameters in metadata for the Agent to inspect
                    metadatas.append({
                        "name": tool.name,
                        "description": tool.description[:200],
                        "parameters": ",".join(param_names), # Comma-separated list
                        "required_parameters": ",".join(required_params)
                    })

                # Clear and Update Chroma
                existing_ids = collection.get()['ids']
                if existing_ids:
                    collection.delete(ids=existing_ids)

                collection.add(
                    ids=ids,
                    documents=documents,
                    metadatas=metadatas
                )

                print(f"✅ Indexed {len(ids)} tools with parameter metadata.")

    except Exception as e:
        print(f"❌ Indexing failed: {e}")


if __name__ == "__main__":
    asyncio.run(index_tools())