import asyncio
import time
import logging
import random

logger = logging.getLogger(__name__)

async def fetch_loop(
    fetch_fn,
    tasks_params: list[tuple],
    duration_secs: int = 600,
    interval_secs: int = 15,
    max_retries: int = 3,
    backoff_base: float = 2.0,
    jitter: bool = True
):
    """
    Generic fetch loop for batch polling APIs.

    Args:
        fetch_fn: async function to call
        tasks_params: list of tuples of arguments to pass to fetch_fn
        duration_secs: how long to run the fetch loop
        interval_secs: interval between fetches
        max_retries: max retries per fetch
        backoff_base: base for exponential backoff
        jitter: whether to add random jitter to sleep
    """
    end_time = time.time() + duration_secs
    results = []

    while(time.time() < end_time):
        tasks = []

        for params in tasks_params:
            tasks.append(
                fetch_with_retries(fetch_fn, params, max_retries, backoff_base, jitter)
            )

        all_data = await asyncio.gather(*tasks)
        for item in all_data:
            if item:
                results.extend(item if isinstance(item, list) else [item])

        sleep_time = interval_secs + (random.uniform(0, 1) if jitter else 0)
        await asyncio.sleep(sleep_time)

    return results


async def fetch_with_retries(fetch_fn, params, max_retries, backoff_base, jitter):
    for attempt in range(max_retries):
        try:
            return await fetch_fn(*params)
        except Exception as e:
            wait_time = (backoff_base ** attempt) + (random.uniform(0, 1) if jitter else 0)
            logger.warning(
                f"Retry {attempt + 1}/{max_retries} for {fetch_fn.__name__}{params} due to error: {e}. Retrying in {wait_time:.2f}s"
            )
            await asyncio.sleep(wait_time)
    logger.error(f"Failed after {max_retries} retries: {fetch_fn.__name__}{params}")
    return None
