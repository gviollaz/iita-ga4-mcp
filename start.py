import uvicorn; uvicorn.run(mcp.sse_app(), host="0.0.0.0", port=int(__import__("os").environ.get("PORT", 8080)))
