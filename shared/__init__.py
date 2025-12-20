"""
Спільні модулі для standalone скриптів.

Містить:
- browser_manager - менеджер браузера Playwright
- promo_service - сервіс для роботи з промокодами (без S3/БД залежностей)
"""

from .browser_manager import BrowserManager, create_browser_manager, cleanup_global_browser
from .promo_service import PromoService
