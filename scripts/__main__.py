import asyncio

from scripts.article_extractor import main

if __name__ == "__main__":
    asyncio.run(main(), debug=True)
