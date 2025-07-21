import os
import logging
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page

"""
BrowserManager - Менеджер браузера для Playwright з підтримкою headed/headless режимів

Режими запуску:
1. HEADED (за замовчуванням) - для локального дебагу з видимим браузером
2. HEADLESS - для продакшна/AWS Lambda

Способи увімкнення HEADLESS режиму:

1. Через змінну середовища:
   export PLAYWRIGHT_HEADED=false
   python3 generate_promo_codes.py

2. Через код (у promo_logic.py або скрипті):
   browser_manager = create_browser_manager(headed_mode=False)

3. Тимчасово в терміналі:
   PLAYWRIGHT_HEADED=false python3 generate_promo_codes.py

У HEADED режимі:
- Браузер відкривається видимим
- Збільшений viewport (1920x1080)
- Збільшені тайм-аути (120 сек)
- Slow motion для кращого спостереження
- Мінімальні аргументи браузера
"""

# Налаштування логування
logger = logging.getLogger(__name__)

class BrowserManager:
    """
    Менеджер для роботи з Playwright браузером в AWS Lambda.
    Оптимізований для serverless середовища.
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
            # В AWS Lambda завжди headless режим
            if os.getenv('AWS_LAMBDA_FUNCTION_NAME'):
                self.headed_mode = False
            else:
                # Для локального режиму перевіряємо змінну середовища (за замовчуванням HEADED режим)
                self.headed_mode = os.getenv('PLAYWRIGHT_HEADED', 'true').lower() in ['true', '1', 'yes']
        
        logger.info(f"🎬 BrowserManager режим: {'HEADED (видимий)' if self.headed_mode else 'HEADLESS (фоновий)'}")
    
    def initialize(self) -> Page:
        """
        Ініціалізує браузер та повертає сторінку для роботи.
        """
        if self._initialized and self.page:
            return self.page
        
        logger.info("🚀 Ініціалізуємо Playwright браузер...")
        
        # Діагностика середовища
        playwright_browsers_path = os.getenv('PLAYWRIGHT_BROWSERS_PATH', '/opt/playwright-browsers')
        logger.info(f"🔍 PLAYWRIGHT_BROWSERS_PATH: {playwright_browsers_path}")
        
        # Перевіряємо, чи існує директорія з браузерами
        if os.path.exists(playwright_browsers_path):
            logger.info(f"✅ Директорія браузерів знайдена: {playwright_browsers_path}")
            # Виводимо що є в директорії
            try:
                contents = os.listdir(playwright_browsers_path)
                logger.info(f"📂 Вміст директорії: {contents}")
            except Exception as e:
                logger.warning(f"⚠️ Не вдалося прочитати директорію: {e}")
        else:
            logger.error(f"❌ Директорія браузерів не знайдена: {playwright_browsers_path}")
        
        try:
            # Запуск Playwright
            self.playwright = sync_playwright().start()
            
            # Конфігурація браузера - адаптивна залежно від режиму
            browser_args = []
            
            if self.headed_mode:
                # Режим для локального дебагу - мінімальні аргументи
                logger.info("🖥️ Запуск у HEADED режимі для дебагу...")
                browser_args = [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',  # Для доступу до різних доменів під час тестування
                ]
            else:
                # Headless режим для продакшна (AWS Lambda)
                logger.info("👻 Запуск у HEADLESS режимі для продакшна...")
                browser_args = [
                    # Основні аргументи для AWS Lambda
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
                    '--disable-default-apps',
                    '--disable-domain-reliability',
                    '--disable-features=TranslateUI,BlinkGenPropertyTrees',
                    '--disable-hang-monitor',
                    '--disable-ipc-flooding-protection',
                    '--disable-popup-blocking',
                    '--disable-prompt-on-repost',
                    '--disable-renderer-backgrounding',
                    '--disable-sync',
                    '--disable-web-resources',
                    '--disable-web-security',
                    '--metrics-recording-only',
                    '--no-first-run',
                    '--safebrowsing-disable-auto-update',
                    '--use-mock-keychain',
                    
                    # Додаткові опції для AWS Lambda
                    '--enable-logging',
                    '--log-level=0',
                    '--v=1',
                    '--no-default-browser-check',
                    '--disable-component-extensions-with-background-pages',
                    '--disable-default-apps',
                    '--disable-background-mode',
                    '--disable-background-timer-throttling',
                    '--disable-renderer-backgrounding',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-features=TranslateUI',
                    '--disable-features=BlinkGenPropertyTrees',
                    '--run-all-compositor-stages-before-draw',
                    '--disable-threaded-animation',
                    '--disable-threaded-scrolling',
                    '--disable-checker-imaging',
                    '--disable-new-content-rendering-timeout',
                    '--disable-background-media-suspend',
                    '--disable-partial-raster',
                    '--disable-canvas-aa',
                    '--disable-2d-canvas-clip-aa',
                    '--disable-gl-drawing-for-tests',
                    '--disable-canvas-aa',
                    '--disable-3d-apis',
                    '--disable-accelerated-2d-canvas',
                    '--disable-accelerated-jpeg-decoding',
                    '--disable-accelerated-mjpeg-decode',
                    '--disable-app-list-dismiss-on-blur',
                    '--disable-accelerated-video-decode',
                ]
            
            self.browser = self.playwright.chromium.launch(
                headless=not self.headed_mode,
                args=browser_args,
                slow_mo=50 if self.headed_mode else 0,  # Повільніший режим для дебагу
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
                logger.info("🖥️ Використовується збільшений viewport для дебагу: 1920x1080")
            
            self.context = self.browser.new_context(**context_options)
            
            # Створення сторінки
            self.page = self.context.new_page()
            
            # Встановлення тайм-аутів (більші для headed режиму)
            timeout = 120000 if self.headed_mode else 60000  # 2 хвилини для дебагу, 1 хвилина для продакшна
            self.page.set_default_timeout(timeout)
            self.page.set_default_navigation_timeout(timeout)
            
            if self.headed_mode:
                logger.info("⏰ Збільшені тайм-аути для headed режиму: 120 секунд")
            
            self._initialized = True
            logger.info(f"✅ Playwright браузер успішно ініціалізовано в {'HEADED' if self.headed_mode else 'HEADLESS'} режимі")
            
            return self.page
            
        except Exception as e:
            logger.error(f"❌ Помилка при ініціалізації браузера: {e}")
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
        logger.info("🧹 Очищення ресурсів браузера...")
        
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
            logger.info("✅ Ресурси браузера очищено")
            
        except Exception as e:
            logger.error(f"❌ Помилка при очищенні ресурсів: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self.initialize()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.cleanup()

# Глобальний екземпляр для переuse в Lambda
_global_browser_manager = None

def create_browser_manager(headed_mode=None) -> BrowserManager:
    """
    Створює або повертає глобальний екземпляр браузер менеджера.
    Це оптимізує cold start в AWS Lambda.
    
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
