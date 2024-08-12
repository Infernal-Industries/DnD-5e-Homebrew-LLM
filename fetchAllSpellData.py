import aiohttp
import asyncio
import psutil
import logging
import time
from aiohttp import ClientTimeout
from typing import List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from collections import deque
from colorlog import ColoredFormatter

# Configure logging
log_formatter = ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    datefmt=None,
    reset=True,
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler('dnd_fetcher.log', mode='w')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
file_handler.setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

API_BASE_URL = 'https://www.dnd5eapi.co'
SECTIONS = {
    "ability-scores": "/api/ability-scores",
    "alignments": "/api/alignments",
    "backgrounds": "/api/backgrounds",
    "classes": "/api/classes",
    "conditions": "/api/conditions",
    "damage-types": "/api/damage-types",
    "equipment": "/api/equipment",
    "equipment-categories": "/api/equipment-categories",
    "feats": "/api/feats",
    "features": "/api/features",
    "languages": "/api/languages",
    "magic-items": "/api/magic-items",
    "magic-schools": "/api/magic-schools",
    "monsters": "/api/monsters",
    "proficiencies": "/api/proficiencies",
    "races": "/api/races",
    "rule-sections": "/api/rule-sections",
    "rules": "/api/rules",
    "skills": "/api/skills",
    "spells": "/api/spells",
    "subclasses": "/api/subclasses",
    "subraces": "/api/subraces",
    "traits": "/api/traits",
    "weapon-properties": "/api/weapon-properties"
}
RATE_LIMIT_PER_SECOND = 10000
REQUEST_DELAY = 1.0 / RATE_LIMIT_PER_SECOND
ESTIMATED_DESCRIPTION_SIZE_BYTES = 2000

DEFAULT_WORKERS = 10
MAX_WORKERS = 200
MIN_WORKERS = 1
TARGET_LATENCY = 1.0
LATENCY_TOLERANCE = 0.5
THROUGHPUT_TOLERANCE = 0.5

latency_samples = deque(maxlen=10)
throughput_samples = deque(maxlen=10)

async def get_all_section_items(session: aiohttp.ClientSession, section: str) -> List[Dict]:
    url = f"{API_BASE_URL}{section}"
    try:
        logger.info(f"Fetching all items from section: {section}")
        async with session.get(url, timeout=ClientTimeout(total=10)) as response:
            response.raise_for_status()
            section_data = await response.json()
            logger.debug(f"Retrieved {section} list with {section_data.get('count', 'unknown')} items.")
            return section_data.get('results', [])
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"Error fetching {section} list: {e}", exc_info=True)
        return []

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def get_item_details(session: aiohttp.ClientSession, item_url: str) -> Dict:
    try:
        async with session.get(item_url, timeout=ClientTimeout(total=10)) as response:
            response.raise_for_status()
            item_data = await response.json()

            # Remove unnecessary keys to reduce tokens
            for key in ['index', 'url', 'source']:
                item_data.pop(key, None)

            return item_data
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"Error fetching details for {item_url}: {e}", exc_info=True)
        return {'error': str(e)}

async def fetch_section_data(session: aiohttp.ClientSession, item: Dict, section: str, semaphore: asyncio.Semaphore) -> Dict:
    item_url = f"{API_BASE_URL}{item['url']}"
    logger.debug(f"Fetching data for {section} item: {item['name']}")

    async with semaphore:
        await asyncio.sleep(REQUEST_DELAY)
        item_details = await get_item_details(session, item_url)

    return item_details

async def adjust_workers_dynamically(semaphore: asyncio.Semaphore):
    current_workers = DEFAULT_WORKERS
    while True:
        await asyncio.sleep(2)
        if latency_samples:
            avg_latency = sum(latency_samples) / len(latency_samples)
        else:
            avg_latency = TARGET_LATENCY

        if throughput_samples:
            avg_throughput = sum(throughput_samples) / len(throughput_samples)
        else:
            avg_throughput = 0

        logger.info(f"Avg latency: {avg_latency:.2f}s, Avg throughput: {avg_throughput:.2f}MB/s")

        if avg_latency > TARGET_LATENCY + LATENCY_TOLERANCE:
            if current_workers > MIN_WORKERS:
                current_workers -= 1
                logger.info(f"Reducing workers to {current_workers} to adapt to network conditions.")
        elif avg_latency < TARGET_LATENCY - LATENCY_TOLERANCE and avg_throughput > THROUGHPUT_TOLERANCE:
            if current_workers < MAX_WORKERS:
                current_workers += 1
                logger.info(f"Increasing workers to {current_workers} to maximize throughput.")

        latency_samples.clear()
        throughput_samples.clear()

        semaphore._value = current_workers

def calculate_batch_size() -> int:
    available_memory = psutil.virtual_memory().available
    estimated_memory_per_description = ESTIMATED_DESCRIPTION_SIZE_BYTES * 1.5
    batch_size = int(available_memory / estimated_memory_per_description * 0.1)
    logger.info(f"Calculated optimal batch size: {batch_size}")
    return max(1, batch_size)

async def save_sections_to_files():
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(DEFAULT_WORKERS)

        dynamic_adjustment_task = asyncio.create_task(adjust_workers_dynamically(semaphore))

        for section, path in SECTIONS.items():
            items = await get_all_section_items(session, path)
            batch_size = calculate_batch_size()

            tasks = []
            for item in items:
                tasks.append(fetch_section_data(session, item, section, semaphore))

            logger.info(f"Starting to fetch {section} data with dynamic worker adjustment...")

            results = []
            for i in range(0, len(tasks), batch_size):
                batch_tasks = tasks[i:i + batch_size]
                logger.info(f"Processing batch {i // batch_size + 1}/{(len(tasks) + batch_size - 1) // batch_size} for section: {section}")
                batch_results = await asyncio.gather(*batch_tasks)
                results.extend(batch_results)

                filename = f"{section}_details_compact.txt"
                with open(filename, 'w', encoding='utf-8') as file:
                    for result in results:
                        result_string = ' '.join([f"{k} {v}" for k, v in result.items()])
                        file.write(result_string + '\n')
                logger.info(f"Batch {i // batch_size + 1} for section {section} written to file {filename}.")

        dynamic_adjustment_task.cancel()

    logger.info("All sections have been processed and saved to separate files.")

def main():
    try:
        asyncio.run(save_sections_to_files())
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
