"""
Декоратори для обробки timeout та retry логіки
"""
import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)

def timeout_handler(timeout_seconds):
    """
    Декоратор для обмеження часу виконання функції
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            import signal
            
            def timeout_signal_handler(signum, frame):
                raise TimeoutError(f"Функція {func.__name__} перевищила ліміт часу {timeout_seconds} секунд")
            
            # Встановлюємо обробник сигналу для timeout (тільки на Unix системах)
            try:
                old_handler = signal.signal(signal.SIGALRM, timeout_signal_handler)
                signal.alarm(timeout_seconds)
                
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    signal.alarm(0)  # Скасовуємо timeout
                    signal.signal(signal.SIGALRM, old_handler)  # Відновлюємо старий обробник
                    
            except AttributeError:
                # На Windows SIGALRM не підтримується, просто виконуємо функцію
                logger.warning("⚠️ Timeout декоратор не підтримується на цій платформі, виконуємо без timeout")
                return func(*args, **kwargs)
                
        return wrapper
    return decorator

def retry_on_failure(max_retries=3, delay=1, exceptions=(Exception,)):
    """
    Декоратор для повт
    орних спроб виконання функції при помилках
    
    Args:
        max_retries: Максимальна кількість повторних спроб
        delay: Затримка між спробами в секундах
        exceptions: Tuple з типами винятків, при яких робити retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"⚠️ Спроба {attempt + 1}/{max_retries + 1} не вдалася: {e}")
                        logger.info(f"⏳ Очікування {delay} секунд перед наступною спробою...")
                        time.sleep(delay)
                    else:
                        logger.error(f"❌ Всі {max_retries + 1} спроб не вдалися")
                        
            # Якщо всі спроби не вдалися, кидаємо останню помилку
            if last_exception:
                raise last_exception
            else:
                raise RuntimeError(f"Функція {func.__name__} не вдалася після {max_retries + 1} спроб")
            
        return wrapper
    return decorator
