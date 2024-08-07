import aiohttp
import asyncio
import psutil
import logging
import json
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
console_handler.setLevel(logging.INFO)  # Only show INFO and higher levels in console

file_handler = logging.FileHandler('spell_fetcher.log', mode='w')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
file_handler.setLevel(logging.DEBUG)  # Log all levels to file

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set overall logger level
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Constants
RATE_LIMIT_PER_SECOND = 10000  # API limit of 10,000 requests per second
REQUEST_DELAY = 1.0 / RATE_LIMIT_PER_SECOND  # Delay between requests
ESTIMATED_DESCRIPTION_SIZE_BYTES = 2000

# Real-time feedback parameters
DEFAULT_WORKERS = 10  # Start with a reasonable default
MAX_WORKERS = 200  # Maximum workers cap
MIN_WORKERS = 1  # Minimum workers
TARGET_LATENCY = 1.0  # Target latency per request (seconds)
LATENCY_TOLERANCE = 0.5  # Allowable deviation from target latency
THROUGHPUT_TOLERANCE = 0.5  # MB/s threshold for increasing or decreasing workers

# Monitoring queues
latency_samples = deque(maxlen=10)
throughput_samples = deque(maxlen=10)

def log_large_data(data, filename='large_log_data.json'):
    """Save large data to a separate file for better readability."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"Large data saved to {filename}")
    except Exception as e:
        logger.error(f"Error saving large data to {filename}: {e}")

async def get_all_spells(session: aiohttp.ClientSession) -> List[Dict]:
    """Fetch all spells from the API asynchronously."""
    base_url = 'https://www.dnd5eapi.co/api/spells'
    try:
        logger.info("Fetching all spells from the API.")
        async with session.get(base_url, timeout=ClientTimeout(total=10)) as response:
            response.raise_for_status()
            spell_data = await response.json()
            logger.debug(f"Retrieved spell list with {spell_data.get('count', 'unknown')} spells.")
            log_large_data(spell_data, 'spell_list.json')
            return spell_data.get('results', [])
    except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
        logger.error(f"Error fetching spell list: {e}", exc_info=True)
        return []

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def get_spell_details(session: aiohttp.ClientSession, spell_url: str) -> Dict:
    """Fetch spell details from the API asynchronously with retry."""
    try:
        start_time = time.time()
        async with session.get(spell_url, timeout=ClientTimeout(total=10)) as response:
            response.raise_for_status()
            spell_data = await response.json()
            end_time = time.time()
            
            # Calculate and log latency
            latency = end_time - start_time
            latency_samples.append(latency)
            logger.debug(f"Latency for {spell_url.split('/')[-1]}: {latency:.2f}s")

            # Calculate and log throughput
            data_size_mb = len(json.dumps(spell_data)) / (1024 * 1024)
            throughput_samples.append(data_size_mb / latency if latency > 0 else 0)
            logger.debug(f"Throughput for {spell_url.split('/')[-1]}: {data_size_mb:.2f} MB, {data_size_mb / latency if latency > 0 else 0:.2f} MB/s")
            
            return spell_data
    except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
        logger.error(f"Error fetching details for {spell_url}: {e}", exc_info=True)
        return {'error': str(e)}

async def fetch_spell_data(session: aiohttp.ClientSession, spell: Dict, semaphore: asyncio.Semaphore) -> Dict:
    """Fetch spell data asynchronously and return a dictionary with spell information."""
    spell_name = spell['name']
    spell_url = f"https://www.dnd5eapi.co{spell['url']}"
    logger.debug(f"Fetching data for spell: {spell_name}")
    
    # Acquire semaphore and implement delay for rate limiting
    async with semaphore:
        # Implement rate limiting delay
        await asyncio.sleep(REQUEST_DELAY)

        # Fetch the spell details with retry
        spell_details = await get_spell_details(session, spell_url)
    
    return spell_details

async def adjust_workers_dynamically(semaphore: asyncio.Semaphore):
    """Adjust the number of workers dynamically based on real-time metrics."""
    current_workers = DEFAULT_WORKERS
    while True:
        # Sleep for a period to collect enough data for analysis
        await asyncio.sleep(2)

        # Calculate average latency and throughput
        if latency_samples:
            avg_latency = sum(latency_samples) / len(latency_samples)
        else:
            avg_latency = TARGET_LATENCY

        if throughput_samples:
            avg_throughput = sum(throughput_samples) / len(throughput_samples)
        else:
            avg_throughput = 0

        logger.info(f"Avg latency: {avg_latency:.2f}s, Avg throughput: {avg_throughput:.2f}MB/s")

        # Decision to increase or decrease workers
        if avg_latency > TARGET_LATENCY + LATENCY_TOLERANCE:
            if current_workers > MIN_WORKERS:
                current_workers -= 1
                logger.info(f"Reducing workers to {current_workers} to adapt to network conditions.")
        elif avg_latency < TARGET_LATENCY - LATENCY_TOLERANCE and avg_throughput > THROUGHPUT_TOLERANCE:
            if current_workers < MAX_WORKERS:
                current_workers += 1
                logger.info(f"Increasing workers to {current_workers} to maximize throughput.")

        # Clear samples for next evaluation period
        latency_samples.clear()
        throughput_samples.clear()

        # Update semaphore with new worker count
        semaphore._value = current_workers  # Update the semaphore's internal worker count

def calculate_batch_size() -> int:
    """Calculate the optimal batch size based on available memory and estimated description size."""
    available_memory = psutil.virtual_memory().available
    estimated_memory_per_description = ESTIMATED_DESCRIPTION_SIZE_BYTES * 1.5  # Add some buffer
    batch_size = int(available_memory / estimated_memory_per_description * 0.1)  # Use 10% of available memory
    logger.info(f"Calculated optimal batch size: {batch_size}")
    return max(1, batch_size)

async def save_spells_to_text(filename: str):
    """Fetch all spell data asynchronously and save it to a text file in JSON format."""
    async with aiohttp.ClientSession() as session:
        spells = await get_all_spells(session)
        batch_size = calculate_batch_size()

        tasks = []
        semaphore = asyncio.Semaphore(DEFAULT_WORKERS)  # Control concurrency with semaphore

        # Start the dynamic adjustment task
        dynamic_adjustment_task = asyncio.create_task(adjust_workers_dynamically(semaphore))

        for spell in spells:
            tasks.append(fetch_spell_data(session, spell, semaphore))

        logger.info(f"Starting to fetch spell data with dynamic worker adjustment...")

        results = []
        # Process tasks in batches
        for i in range(0, len(tasks), batch_size):
            batch_tasks = tasks[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}/{(len(tasks) + batch_size - 1) // batch_size}")
            batch_results = await asyncio.gather(*batch_tasks)
            results.extend(batch_results)

            # Write batch results to text file as compact JSON
            with open(filename, 'w', encoding='utf-8') as file:
                for result in results:
                    json_string = json.dumps(result, separators=(',', ':'), ensure_ascii=False)
                    file.write(json_string + '\n')  # New line for each JSON object
            logger.info(f"Batch {i // batch_size + 1} written to file.")

        # Cancel the dynamic adjustment task when done
        dynamic_adjustment_task.cancel()

    logger.info(f"Spell descriptions have been saved to {filename}")

def main():
    """Main function to run the async spell fetcher."""
    try:
        asyncio.run(save_spells_to_text('spell_details_compact.txt'))
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
