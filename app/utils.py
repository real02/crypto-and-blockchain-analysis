import asyncio
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("crypto-utils")


async def async_retry(
    func,
    retries=3,
    backoff_factor=2,
    initial_wait=1,
    exceptions=(Exception,),
    func_name=None,
):
    """
    Retry decorator for async functions with exponential backoff

    Args:
        func: The async function to execute and retry
        retries: Maximum number of retries
        backoff_factor: Factor to increase wait time between retries
        initial_wait: Initial wait time in seconds
        exceptions: Tuple of exceptions to catch and retry
        func_name: Optional name for logging (useful for lambda functions)

    Returns:
        The result of the async function
    """
    max_retries = retries
    retry_count = 0
    wait_time = initial_wait

    # Get name for logging
    name = func_name or getattr(func, "__name__", "unknown_function")

    while True:
        try:
            result = await func()

            # Add validation that result is a list if needed
            # if not isinstance(result, list) and not isinstance(result, dict):
            #     raise TypeError(f"Expected list or dict result from {name}, got {type(result)}")

            return result

        except exceptions as e:
            retry_count += 1
            if retry_count > max_retries:
                logger.error(
                    f"Maximum retries ({max_retries}) exceeded for {name}: {e}"
                )
                raise  # Re-raise the last exception

            logger.warning(
                f"Retry {retry_count}/{max_retries} for {name} after error: {e}"
            )
            await asyncio.sleep(wait_time)
            wait_time *= backoff_factor  # Exponential backoff
