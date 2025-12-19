"""
BrowserManager - Менеджер браузера для Playwright з підтримкою headed/headless режимів

Портовано з AWS Lambda: bonus_system/bonus_replenish_promo_code/browser_manager.py

Режими запуску:
1. HEADED (за замовчуванням для локального дебагу) - з видимим браузером
2. HEADLESS - для продакшна/Docker

Способи увімкнення HEADLESS режиму:

1. Через змінну середовища:
   export PLAYWRIGHT_HEADED=false
   
2. Через код:
   browser_manager = create_browser_manager(headed_mode=False)

3. Тимчасово в терміналі:
   PLAYWRIGHT_HEADED=false python script.py
"""

import os
import logging
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page

logger = logging.getLogger(__name__)

# Визначаємо чи запущено як subprocess
IS_SUBPROCESS = os.environ.get('REPLENISH_SUBPROCESS', 'false').lower() == 'true'


def _log_info(message):
    """Логування для subprocess та звичайного режиму"""
    if IS_SUBPROCESS:
        print(message, flush=True)
    else:
        _log_info(message)


def _log_error(message):
    """Логування помилок для subprocess та звичайного режиму"""
    if IS_SUBPROCESS:
        print(f"ERROR: {message}", flush=True)
    else:
        _log_error(message)


class BrowserManager:
    """
    Менеджер для роботи з Playwright браузером.
    Оптимізований для Docker/VPS середовища.
    Підтримує як headless, так і headed режими для дебагу.
    """
    
    def __init__(self, headed_mode=None):
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self._initialized = False
        
        # Визначаємо режим запуску
        if headed_mode is not None:
            self.headed_mode = headed_mode
        else:
            # За замовчуванням HEADED режим для локального дебагу
            self.headed_mode = os.getenv('PLAYWRIGHT_HEADED', 'true').lower() in ['true', '1', 'yes']
        
        _log_info(f"🎬 BrowserManager режим: {'HEADED (видимий)' if self.headed_mode else 'HEADLESS (фоновий)'}")
    
    def initialize(self) -> Page:
        """
        Ініціалізує браузер та повертає сторінку для роботи.
        """
        if self._initialized and self.page:
            return self.page
        
        _log_info("🚀 Ініціалізуємо Playwright браузер...")
        
        try:
            # Запуск Playwright
            self.playwright = sync_playwright().start()
            
            # Конфігурація браузера - адаптивна залежно від режиму
            browser_args = []
            
            if self.headed_mode:
                # Режим для локального дебагу - мінімальні аргументи
                _log_info("🖥️ Запуск у HEADED режимі для дебагу...")
                browser_args = [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                ]
            else:
                # Headless режим для продакшна (Docker/VPS)
                _log_info("👻 Запуск у HEADLESS режимі для продакшна...")
                browser_args = [
                    # Основні аргументи для Docker
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-blink-features=AutomationControlled',
                    
                    # Оптимізація для serverless
                    '--single-process',
                    '--no-zygote',
                    '--disable-setuid-sandbox',
                    '--disable-background-timer-throttling',
                    '--disable-renderer-backgrounding',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-ipc-flooding-protection',
                    
                    # Зменшення використання пам'яті
                    '--memory-pressure-off',
                    '--max_old_space_size=512',
                    
                    # Відключення непотрібних функцій
                    '--disable-extensions',
                    '--disable-default-apps',
                    '--disable-sync',
                    '--disable-translate',
                    '--disable-background-networking',
                    '--disable-background-mode',
                    '--disable-client-side-phishing-detection',
                    '--disable-component-update',
                    '--disable-domain-reliability',
                    '--disable-features=TranslateUI,BlinkGenPropertyTrees',
                    '--disable-hang-monitor',
                    '--disable-popup-blocking',
                    '--disable-prompt-on-repost',
                    '--disable-web-resources',
                    '--metrics-recording-only',
                    '--no-first-run',
                    '--safebrowsing-disable-auto-update',
                    '--use-mock-keychain',
                    '--no-default-browser-check',
                ]
            
            self.browser = self.playwright.chromium.launch(
                headless=not self.headed_mode,
                args=browser_args,
                slow_mo=50 if self.headed_mode else 0,
            )
            
            # Створення контексту з оптимізованими налаштуваннями
            context_options = {
                'viewport': {'width': 1280, 'height': 720},
                'user_agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'ignore_https_errors': True,
                'java_script_enabled': True,
            }
            
            # У headed режимі збільшуємо viewport для кращого дебагу
            if self.headed_mode:
                context_options['viewport'] = {'width': 1920, 'height': 1080}
                _log_info("🖥️ Використовується збільшений viewport для дебагу: 1920x1080")
            
            self.context = self.browser.new_context(**context_options)
            
            # Створення сторінки
            self.page = self.context.new_page()
            
            # Встановлення тайм-аутів (більші для headed режиму)
            timeout = 120000 if self.headed_mode else 60000
            self.page.set_default_timeout(timeout)
            self.page.set_default_navigation_timeout(timeout)
            
            if self.headed_mode:
                _log_info("⏰ Збільшені тайм-аути для headed режиму: 120 секунд")
            
            self._initialized = True
            _log_info(f"✅ Playwright браузер успішно ініціалізовано в {'HEADED' if self.headed_mode else 'HEADLESS'} режимі")
            
            return self.page
            
        except Exception as e:
            _log_error(f"❌ Помилка при ініціалізації браузера: {e}")
            self.cleanup()
            raise
    
    def get_page(self) -> Page:
        """
        Повертає активну сторінку, ініціалізуючи браузер при необхідності.
        """
        if not self._initialized or not self.page:
            return self.initialize()
        return self.page
    
    def cleanup(self):
        """
        Очищення ресурсів браузера.
        """
        _log_info("🧹 Очищення ресурсів браузера...")
        
        try:
            if self.page:
                self.page.close()
                self.page = None
                
            if self.context:
                self.context.close()
                self.context = None
                
            if self.browser:
                self.browser.close()
                self.browser = None
                
            if self.playwright:
                self.playwright.stop()
                self.playwright = None
                
            self._initialized = False
            _log_info("✅ Ресурси браузера очищено")
            
        except Exception as e:
            _log_error(f"❌ Помилка при очищенні ресурсів: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self.initialize()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.cleanup()


# Глобальний екземпляр для переuse
_global_browser_manager = None


def create_browser_manager(headed_mode=None) -> BrowserManager:
    """
    Створює або повертає глобальний екземпляр браузер менеджера.
    
    Args:
        headed_mode: True для видимого браузера (дебаг), False для headless, 
                    None для автовизначення через змінну середовища
    """
    global _global_browser_manager
    
    if _global_browser_manager is None:
        _global_browser_manager = BrowserManager(headed_mode=headed_mode)
    
    return _global_browser_manager


def cleanup_global_browser():
    """
    Очищає глобальний браузер менеджер.
    """
    global _global_browser_manager
    
    if _global_browser_manager:
        _global_browser_manager.cleanup()
        _global_browser_manager = None
