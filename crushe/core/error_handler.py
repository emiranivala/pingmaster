import asyncio
import logging
import random
from functools import wraps
from typing import Callable, Any, Optional

from pyrogram.errors import FloodWait, RPCError, BadRequest, Unauthorized, Forbidden
from pyrogram.errors.exceptions.flood_420 import FloodWait

logger = logging.getLogger(__name__)

# Configure more detailed logging for error tracking
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s/%(asctime)s] %(name)s: %(message)s',
)

# Set specific loggers to higher levels to reduce noise
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyrogram.session").setLevel(logging.ERROR)
logging.getLogger("pyrogram.connection").setLevel(logging.ERROR)


async def exponential_backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
    """
    Calculate delay with exponential backoff and jitter for retries.
    
    Args:
        attempt: The current attempt number (starting from 1)
        base_delay: The base delay in seconds
        max_delay: Maximum delay in seconds
        
    Returns:
        The calculated delay in seconds
    """
    # Calculate exponential backoff
    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
    # Add jitter (±25%)
    jitter = delay * 0.25
    delay = delay + random.uniform(-jitter, jitter)
    return delay


def retry_with_backoff(max_retries: int = 5, initial_delay: float = 1.0, max_delay: float = 60.0):
    """
    Decorator to retry functions with exponential backoff when FloodWait or connection errors occur.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except FloodWait as e:
                    # Handle Telegram's FloodWait explicitly
                    wait_time = e.value if hasattr(e, 'value') else e.x
                    logger.warning(f"FloodWait error: waiting for {wait_time} seconds")
                    await asyncio.sleep(wait_time)
                    # Don't count FloodWait against retry limit as it's a server instruction
                    continue
                except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Maximum retries ({max_retries}) exceeded: {str(e)}")
                        raise
                    
                    delay = await exponential_backoff(retries, initial_delay, max_delay)
                    logger.info(f"Connection error: {str(e)}. Retrying in {delay:.2f} seconds (attempt {retries}/{max_retries})")
                    await asyncio.sleep(delay)
                except (BadRequest, Unauthorized, Forbidden) as e:
                    # Don't retry client errors
                    logger.error(f"Client error: {str(e)}")
                    raise
                except RPCError as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Maximum retries ({max_retries}) exceeded: {str(e)}")
                        raise
                    
                    delay = await exponential_backoff(retries, initial_delay, max_delay)
                    logger.info(f"RPC error: {str(e)}. Retrying in {delay:.2f} seconds (attempt {retries}/{max_retries})")
                    await asyncio.sleep(delay)
                except Exception as e:
                    # Log unexpected errors but don't retry
                    logger.error(f"Unexpected error: {str(e)}")
                    raise
        return wrapper
    return decorator


async def safe_execute(func: Callable, *args, **kwargs) -> Optional[Any]:
    """
    Safely execute a function with error handling.
    
    Args:
        func: The function to execute
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function or None if an error occurred
    """
    try:
        return await func(*args, **kwargs)
    except FloodWait as e:
        wait_time = e.value if hasattr(e, 'value') else e.x
        logger.warning(f"FloodWait error in {func.__name__}: waiting for {wait_time} seconds")
        await asyncio.sleep(wait_time)
        return await safe_execute(func, *args, **kwargs)
    except Exception as e:
        logger.error(f"Error in {func.__name__}: {str(e)}")
        return None


async def run_with_lock(lock: asyncio.Lock, func: Callable, *args, **kwargs) -> Any:
    """
    Run a function with a lock to prevent concurrent execution.
    
    Args:
        lock: The lock to acquire
        func: The function to execute
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function
    """
    async with lock:
        return await func(*args, **kwargs)


async def run_with_timeout(func: Callable, timeout: float, *args, **kwargs) -> Any:
    """
    Run a function with a timeout.
    
    Args:
        func: The function to execute
        timeout: Timeout in seconds
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function
        
    Raises:
        asyncio.TimeoutError: If the function takes longer than the timeout
    """
    return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)