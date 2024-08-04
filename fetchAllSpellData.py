import aiohttp
import asyncio
import multiprocessing
import psutil
import speedtest
import logging
import json
from aiohttp import ClientTimeout
from typing import List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import cProfile
import pstats
import io

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define rate limit constants
RATE_LIMIT_PER_SECOND = 10000  # API limit of 10,000 requests per second
REQUEST_DELAY = 1.0 / RATE_LIMIT_PER_SECOND  # Delay between requests

# Estimate average description size in bytes
ESTIMATED_DESCRIPTION_SIZE_BYTES = 2000

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
            spell_data = await response.json()
            logger.debug(f"Retrieved spell list with {spell_data.get('count', 'unknown')} spells.")
            log_large_data(spell_data, 'spell_list.json')
            return spell_data.get('results', [])
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"Error fetching spell list: {e}")
        return []

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def get_spell_description(session: aiohttp.ClientSession, spell_url: str) -> str:
    """Fetch spell description from the API asynchronously with retry."""
    try:
        async with session.get(spell_url, timeout=ClientTimeout(total=10)) as response:
            spell_data = await response.json()
            description = spell_data.get('desc', [])
            logger.debug(f"Retrieved description for {spell_url.split('/')[-1]}: {len(description)} lines long.")
            return ' '.join(description)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"Error fetching description for {spell_url}: {e}")
        return 'Description not found or error occurred.'

async def fetch_spell_data(session: aiohttp.ClientSession, spell: Dict, semaphore: asyncio.Semaphore) -> str:
    """Fetch spell data asynchronously and return formatted string."""
    spell_name = spell['name']
    spell_url = f"https://www.dnd5eapi.co{spell['url']}"
    logger.debug(f"Fetching data for spell: {spell_name}")
    
    # Acquire semaphore and implement delay for rate limiting
    async with semaphore:
        # Implement rate limiting delay
        await asyncio.sleep(REQUEST_DELAY)

        # Fetch the spell description with retry
        description = await get_spell_description(session, spell_url)
    
    return f"Description of {spell_name}:\n{description}\n\n" + "-"*80 + "\n\n"

def measure_bandwidth() -> float:
    """Use speedtest to measure the actual download bandwidth in Mbps."""
    try:
        logger.info("Measuring network bandwidth...")
        st = speedtest.Speedtest()
        st.get_best_server()

        # Perform a full download test
        download_speed = st.download()
        
        bandwidth_mbps = download_speed / (1024 * 1024)  # Convert to MB/s
        logger.info(f"Measured bandwidth: {bandwidth_mbps:.2f} MB/s")
        return bandwidth_mbps
    except Exception as e:
        logger.error(f"Error measuring bandwidth with speedtest: {e}")
        return 1.0  # Default to 1 MB/s if there's an error

def calculate_optimal_workers() -> int:
    """Calculate the optimal number of workers based on CPU, network, and memory conditions."""
    cpu_count = multiprocessing.cpu_count()
    bandwidth = measure_bandwidth()
    available_memory = psutil.virtual_memory().available

    # Realistic estimates for each constraint
    base_memory = 30 * 1024 * 1024  # 30 MB minimum memory per worker
    base_bandwidth = 1  # 1 MB/s minimum bandwidth per worker

    # Estimate workers based on constraints
    memory_based_workers = available_memory // max(ESTIMATED_DESCRIPTION_SIZE_BYTES * 5, base_memory)
    network_based_workers = bandwidth // max(ESTIMATED_DESCRIPTION_SIZE_BYTES / (1024 * 1024) * 2, base_bandwidth)

    # Use the minimum of these constraints and CPU count as a limit
    max_workers = int(min(cpu_count * 8, memory_based_workers, network_based_workers))

    logger.info(f"Detected {cpu_count} CPU cores, {bandwidth:.2f} MB/s bandwidth, and {available_memory / (1024 * 1024):.2f} MB available memory.")
    logger.info(f"Calculated optimal number of workers: {max_workers}")

    return max(1, max_workers)  # Set a minimum of 1 worker

def calculate_batch_size() -> int:
    """Calculate the optimal batch size based on available memory and estimated description size."""
    available_memory = psutil.virtual_memory().available
    estimated_memory_per_description = ESTIMATED_DESCRIPTION_SIZE_BYTES * 1.5  # Add some buffer
    batch_size = int(available_memory / estimated_memory_per_description * 0.1)  # Use 10% of available memory
    logger.info(f"Calculated optimal batch size: {batch_size}")
    return max(1, batch_size)

async def save_spells_to_file(filename: str):
    """Fetch all spell data asynchronously and save it to a file."""
    async with aiohttp.ClientSession() as session:
        spells = await get_all_spells(session)
        total_spells = len(spells)
        max_workers = calculate_optimal_workers()
        batch_size = calculate_batch_size()

        tasks = []
        semaphore = asyncio.Semaphore(max_workers)  # Control concurrency with semaphore
        for spell in spells:
            tasks.append(fetch_spell_data(session, spell, semaphore))

        logger.info(f"Starting to fetch spell data using {max_workers} workers...")

        results = []
        # Process tasks in batches
        for i in range(0, len(tasks), batch_size):
            batch_tasks = tasks[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}/{(len(tasks) + batch_size - 1) // batch_size}")
            batch_results = await asyncio.gather(*batch_tasks)
            results.extend(batch_results)

            # Write batch results to file
            with open(filename, 'a', encoding='utf-8') as file:
                file.writelines(batch_results)
            logger.info(f"Batch {i // batch_size + 1} written to file.")

    logger.info(f"Spell descriptions have been saved to {filename}")

def main():
    """Main function to run the async spell fetcher."""
    asyncio.run(save_spells_to_file('spell_descriptions_async.txt'))

if __name__ == "__main__":
    # Create a profiler object
    profiler = cProfile.Profile()

    # Enable the profiler
    profiler.enable()

    # Run the main function
    main()

    # Disable the profiler
    profiler.disable()

    # Create a Stats object and sort it
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')

    # Print the stats
    ps.print_stats()

    # Optionally save the profiling results to a file
    with open("profiling_results.txt", "w") as f:
        f.write(s.getvalue())
