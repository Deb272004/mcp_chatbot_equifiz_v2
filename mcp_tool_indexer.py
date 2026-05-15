
import os
import asyncio
import chromadb
from fastembed import TextEmbedding
from chromadb.api.types import Documents, Embeddings
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from pathlib import Path
from config.config import EMBEDDING_MODEL

# --- Configuration ---
SERVER_SCRIPT = Path(__file__).parent / "mf_equifiz_server.py"
CHROMA_PATH = Path(__file__).parent / "chroma_store"
embedding_model = EMBEDDING_MODEL

# --- FastEmbed Integration ---
class FastEmbedEmbeddingFunction(chromadb.EmbeddingFunction):
    """Uses FastEmbed for local embeddings."""
    
    def __init__(self, model_name: str = "BAAI/bge-large-en-v1.5"):
        # bge-small is very fast and efficient for tool descriptions
        self.model = TextEmbedding(model_name=model_name,threads=4)
    
    def __call__(self, input: Documents) -> Embeddings:
        # FastEmbed.embed returns a generator
        return [emb.tolist() for emb in self.model.embed(input)]

# Initialize ChromaDB and FastEmbed
client = chromadb.PersistentClient(path=str(CHROMA_PATH))
fastembed_ef = FastEmbedEmbeddingFunction()

collection = client.get_or_create_collection(
    name="equifiz_tools",
    embedding_function=fastembed_ef,
    metadata={"hnsw:space": "cosine"}
)

async def index_tools():
    print(f"🚀 Starting Indexer with FastEmbed...")
    print(f"🔗 Server Script: {SERVER_SCRIPT}")

    if not SERVER_SCRIPT.exists():
        print(f"❌ Error: {SERVER_SCRIPT} not found.")
        return

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
                    # Extract parameter details from JSON schema
                    properties = tool.inputSchema.get("properties", {})
                    required_params = tool.inputSchema.get("required", [])
                    param_names = list(properties.keys())
                    
                    # Construct a semantic-rich document for RAG retrieval
                    doc_content = (
                        f"Tool Name: {tool.name}\n"
                        f"Description: {tool.description}\n"
                        f"Required Params: {', '.join(required_params)}\n"
                        f"All Params: {', '.join(param_names)}"
                    )
                    
                    ids.append(tool.name)
                    documents.append(doc_content)
                    metadatas.append({
                        "name": tool.name,
                        "description": (tool.description or "")[:200],
                        "parameters": ",".join(param_names),
                        "required_parameters": ",".join(required_params)
                    })

                # Refresh the collection
                existing_ids = collection.get()['ids']
                if existing_ids:
                    collection.delete(ids=existing_ids)

                collection.add(
                    ids=ids,
                    documents=documents,
                    metadatas=metadatas
                )

                print(f"✅ Successfully indexed {len(ids)} tools using FastEmbed.")

    except Exception as e:
        print(f"❌ Indexing failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(index_tools())



# import os
# from pathlib import Path
# import chromadb

# # --- Configuration ---
# # Point this to your actual chroma_store folder path
# CHROMA_PATH = Path(__file__).parent / "chroma_store"
# COLLECTION_TO_DELETE = "equifiz_tools"

# def delete_chroma_collection(collection_name: str, db_path: Path):
#     """
#     Safely connects to the persistent ChromaDB store and deletes a specified collection.
#     """
#     print(f"🔍 Checking database at: {db_path.resolve()}")
    
#     if not db_path.exists():
#         print(f"❌ Error: The directory '{db_path}' does not exist.")
#         return

#     try:
#         # Initialize the persistent client pointing to your directory
#         client = chromadb.PersistentClient(path=str(db_path))
        
#         # List all existing collections to verify its existence
#         existing_collections = [col.name for col in client.list_collections()]
#         print(f"📦 Found active collections: {existing_collections}")
        
#         if collection_name in existing_collections:
#             print(f"🗑️ Deleting collection '{collection_name}'...")
            
#             # The official API method to drop a collection completely
#             client.delete_collection(name=collection_name)
            
#             print(f"✅ Successfully deleted '{collection_name}' from the vector store.")
#         else:
#             print(f"ℹ️ Collection '{collection_name}' was not found in this database.")
            
#     except Exception as e:
#         print(f"❌ An error occurred during deletion: {str(e)}")

# if __name__ == "__main__":
#     delete_chroma_collection(COLLECTION_TO_DELETE, CHROMA_PATH)