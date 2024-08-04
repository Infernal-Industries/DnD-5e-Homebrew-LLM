import aiohttp
import asyncio
import multiprocessing
import psutil
import time
import json
from tqdm import tqdm

async def fetch_url(session, url):
    try:
        async with session.get(url) as response:
            return await response.text()
    except Exception as e:
        return str(e)

async def test_worker_performance(worker_count):
    url = "https://www.dnd5eapi.co/api/spells/fireball"  # Using fireball spell for testing
    tasks = []
    async with aiohttp.ClientSession() as session:
        for _ in range(10 * worker_count):  # 10 requests per worker
            tasks.append(fetch_url(session, url))
        start_time = time.time()
        await asyncio.gather(*tasks)
        elapsed_time = time.time() - start_time
        cpu_usage = psutil.cpu_percent(interval=1)
        memory_usage = psutil.virtual_memory().percent
        return {
            "worker_count": worker_count,
            "requests_made": 10 * worker_count,
            "errors": sum(1 for task in tasks if isinstance(task, Exception)),
            "elapsed_time": elapsed_time,
            "cpu_usage": cpu_usage,
            "memory_usage": memory_usage
        }

async def run_tests():
    results = []
    worker_counts = [1, 4, 8, 16, 32, 64, 128, 256]
    for worker_count in tqdm(worker_counts, desc="Running performance tests", ncols=100):
        result = await test_worker_performance(worker_count)
        results.append(result)
        print(f"Test result for {worker_count} workers: {result}")
    return results

def main():
    results = asyncio.run(run_tests())
    with open('performance_results.json', 'w') as f:
        json.dump(results, f, indent=4)

if __name__ == "__main__":
    main()
