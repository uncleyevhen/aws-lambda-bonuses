'use strict';

/*
  Централізований менеджер скриптів для GTM інтеграції
  - Єдиний обсервер для всіх сторінок
  - Розумна детекція типу сторінки
  - Модульна архітектура для різних скриптів
  - Мінімальне навантаження на сайт
*/

(function() {
  /******************** ГЛОБАЛЬНА КОНФІГУРАЦІЯ ********************/ 
  var DEBUG_MODE = true;  // УВІМКНУТО для перевірки швидкої загрузки
  var LOG_PREFIX = '[SCRIPT_MANAGER]';
  
  // Таймінги для максимальної швидкості
  var OBSERVER_THROTTLE = 100;  // Повертаємо до 100мс для швидкої реакції
  var PAGE_CHANGE_DEBOUNCE = 200; // Зменшуємо затримку зміни сторінки
  
  // Базова URL для завантаження скриптів
  var SCRIPTS_BASE_URL = 'https://safeyourlove.com/';
  var SCRIPT_CACHE_TTL = 7200000; // 2 години в мс - збільшуємо кеш
  
    // Конфігурація відключених скриптів
  var DISABLED_SCRIPTS = {
    // 'text-replacer': true,        // ВІДКЛЮЧЕНО - заміна тексту тепер на рівні Cloudflare CDN!
    // 'online-payment': true,   // Увімкнено - потрібно на checkout
    // 'sms-notifications': true,   // Тимчасово відключено - не потрібно на checkout
    // 'free-delivery-calculator': false,    // УВІМКНЕНО - новий калькулятор безкоштовної доставки
  };

  /******************** ЛОГУВАННЯ ********************/ 
  function log(msg, data) {
    if (!DEBUG_MODE) return;
    var t = new Date().toISOString();
    if (data !== undefined) {
      console.log(LOG_PREFIX + ' [' + t + '] ' + msg, data);
    } else {
      console.log(LOG_PREFIX + ' [' + t + '] ' + msg);
    }
  }

  function logError(msg, err) {
    var t = new Date().toISOString();
    if (err) {
      console.error(LOG_PREFIX + ' [' + t + '] ERROR: ' + msg, err);
    } else {
      console.error(LOG_PREFIX + ' [' + t + '] ERROR: ' + msg);
    }
  }

  /******************** КЕШ ДЛЯ СКРИПТІВ ********************/ 
  var ScriptCache = {
    cache: new Map(),
    
    set: function(url, moduleFactory, ttl) {
      var expiry = Date.now() + (ttl || SCRIPT_CACHE_TTL);
      this.cache.set(url, {
        moduleFactory: moduleFactory,
        expiry: expiry,
        timestamp: Date.now()
      });
      log('Скрипт додано до кешу', { url: url, expiry: new Date(expiry) });
    },
    
    get: function(url) {
      var cached = this.cache.get(url);
      if (!cached) return null;
      
      if (Date.now() > cached.expiry) {
        this.cache.delete(url);
        log('Скрипт видалено з кешу (застарілий)', { url: url });
        return null;
      }
      
      log('Скрипт знайдено в кеші', { url: url });
      return cached.moduleFactory;
    },
    
    clear: function() {
      this.cache.clear();
      log('Кеш скриптів очищено');
    },
    
    getStats: function() {
      var stats = {
        size: this.cache.size,
        items: []
      };
      
      this.cache.forEach(function(cached, url) {
        stats.items.push({
          url: url,
          age: Date.now() - cached.timestamp,
          expiresIn: cached.expiry - Date.now()
        });
      });
      
      return stats;
    }
  };

  /******************** ЗАВАНТАЖУВАЧ СКРИПТІВ ********************/ 
  var ScriptLoader = {
    loadingPromises: new Map(),
    
    loadScript: function(url) {
      // Перевіряємо кеш
      var cached = ScriptCache.get(url);
      if (cached) {
        return Promise.resolve(cached);
      }
      
      // Перевіряємо чи вже завантажуємо
      if (this.loadingPromises.has(url)) {
        return this.loadingPromises.get(url);
      }
      
      var promise = this._fetchAndParseScript(url);
      this.loadingPromises.set(url, promise);
      
      promise.finally(function() {
        this.loadingPromises.delete(url);
      }.bind(this));
      
      return promise;
    },
    
    _fetchAndParseScript: function(url) {
      log('Швидке завантаження скрипта', { url: url });
      
      return fetch(url, {
        method: 'GET',
        cache: 'force-cache', // Агресивне кешування для швидкості
        credentials: 'omit',
        mode: 'cors',
        priority: 'high' // Високий пріоритет для fetch
      })
      .then(function(response) {
        if (!response.ok) {
          throw new Error('HTTP ' + response.status + ': ' + response.statusText);
        }
        return response.text();
      })
      .then(function(scriptText) {
        return this._parseScriptModule(scriptText, url);
      }.bind(this))
      .then(function(moduleFactory) {
        // Додаємо до кешу з довшим TTL
        ScriptCache.set(url, moduleFactory, SCRIPT_CACHE_TTL);
        log('Скрипт швидко завантажено і розпарсено', { url: url });
        return moduleFactory;
      })
      .catch(function(error) {
        logError('Помилка швидкого завантаження скрипта ' + url, error);
        throw error;
      });
    },
    
    _parseScriptModule: function(scriptText, url) {
      try {
        // Створюємо ізольоване середовище для скрипта
        var moduleScope = this._createModuleScope();
        
        // Виконуємо скрипт у ізольованому середовищі
        var scriptFunction = new Function(
          'window', 'document', 'console', 'setTimeout', 'clearTimeout', 
          'setInterval', 'clearInterval', 'fetch', 'localStorage', 
          'sessionStorage', 'location', 'history', 'navigator',
          'moduleExports', 'moduleScope',
          scriptText + '\n\n// Експорт модуля\nreturn moduleExports;'
        );
        
        var moduleExports = scriptFunction.call(
          moduleScope,
          window, document, console, setTimeout, clearTimeout,
          setInterval, clearInterval, fetch, localStorage,
          sessionStorage, location, history, navigator,
          {}, moduleScope
        );
        
        // Створюємо фабрику модулів
        return function() {
          if (typeof moduleExports === 'function') {
            log('Модуль експортує функцію', { url: url });
            return moduleExports();
          } else if (typeof moduleExports === 'object' && moduleExports !== null) {
            log('Модуль експортує об\'єкт', { url: url, keys: Object.keys(moduleExports) });
            return moduleExports;
          } else {
            // Якщо скрипт не експортує модуль, створюємо базовий
            log('Модуль не експортує нічого, створюємо базовий', { url: url, moduleExports: moduleExports });
            return this._createBasicModule(url);
          }
        }.bind(this);
        
      } catch (error) {
        logError('Помилка парсингу скрипта ' + url, error);
        // Повертаємо базовий модуль у випадку помилки
        return function() {
          return this._createBasicModule(url);
        }.bind(this);
      }
    },
    
    _createModuleScope: function() {
      return {
        // Базові утиліти для модулів
        log: log,
        logError: logError,
        waitForElement: function(selector, timeout) {
          timeout = timeout || 5000;
          return new Promise(function(resolve, reject) {
            var element = document.querySelector(selector);
            if (element) return resolve(element);
            
            var observer = new MutationObserver(function() {
              element = document.querySelector(selector);
              if (element) {
                observer.disconnect();
                resolve(element);
              }
            });
            
            observer.observe(document.body, {
              childList: true,
              subtree: true
            });
            
            setTimeout(function() {
              observer.disconnect();
              element = document.querySelector(selector);
              if (element) {
                resolve(element);
              } else {
                reject(new Error('Element not found: ' + selector));
              }
            }, timeout);
          });
        },
        debounce: function(func, wait) {
          var timeout;
          return function() {
            var context = this, args = arguments;
            clearTimeout(timeout);
            timeout = setTimeout(function() {
              func.apply(context, args);
            }, wait);
          };
        }
      };
    },
    
    _createBasicModule: function(url) {
      return {
        init: function() {
          log('Базова ініціалізація модуля', { url: url });
        },
        handleMutations: function(mutations) {
          // Базовий обробник мутацій
        },
        destroy: function() {
          log('Базове знищення модуля', { url: url });
        }
      };
    }
  };

  /******************** ДЕТЕКЦІЯ ТИПУ СТОРІНКИ ********************/ 
  var PageDetector = {
    rules: {
      'checkout-main': {
        url: /\/checkout\/(?!complete)/,
        elements: ['.j-phone-masked', '.order-details', '.j-coupon-add'],
        exclude: ['/checkout/complete'],
        priority: 1
      },
      'checkout-complete': {
        url: /\/checkout\/complete/,
        elements: ['.invoice', '.order-details__total'],
        priority: 1
      },
      'cart-modal': {
        // Модальний кошик має найвищий пріоритет, бо може з'являтися поверх будь-якої сторінки
        modal: true,
        modalSelectors: ['#modal-overlay #cart.popup.__cart', '#cart.popup.__cart[style*="display: block"]'],
        elements: ['.cart-items', '.cart-content', '.popup-title', '.j-total-sum'],
        contentElements: ['.cart-item', '.j-cart-product', '.cart-summary'],
        priority: 0, // Найвищий пріоритет
        description: 'Модальне вікно кошика'
      },
      'cart': {
        url: /\/cart/,
        elements: ['.cart-item', '.cart-total', '.order-summary'],
        priority: 2
      },
      'product': {
        url: /\/product\//,
        elements: ['.product-title', '.add-to-cart', '.product-info'],
        priority: 3
      },
      'category': {
        url: /\/category\//,
        elements: ['.category-products', '.product-grid', '.catalog'],
        priority: 3
      },
      'home': {
        url: /^\/$|\/home/,
        elements: ['.hero-section', '.main-banner', '.homepage'],
        priority: 4
      }
    },

    detectModalPage: function() {
      // Перевіряємо всі правила модальних сторінок
      for (var pageType in this.rules) {
        var rule = this.rules[pageType];
        
        if (!rule.modal) continue;
        
        // Перевіряємо селектори модального вікна
        var modalVisible = false;
        if (rule.modalSelectors) {
          for (var i = 0; i < rule.modalSelectors.length; i++) {
            var modalElement = document.querySelector(rule.modalSelectors[i]);
            if (modalElement) {
              // Перевіряємо чи модальне вікно дійсно видиме
              var style = window.getComputedStyle(modalElement);
              var isVisible = style.display !== 'none' && 
                             style.visibility !== 'hidden' && 
                             modalElement.offsetWidth > 0 && 
                             modalElement.offsetHeight > 0;
              
              if (isVisible) {
                modalVisible = true;
                break;
              }
            }
          }
        }
        
        if (!modalVisible) {
          continue;
        }
        
        // Якщо модальне вікно видиме, перевіряємо контент
        var contentScore = 0;
        
        // Перевіряємо основні елементи
        if (rule.elements) {
          var elementsFound = rule.elements.filter(function(selector) {
            return document.querySelector(selector) !== null;
          });
          contentScore += elementsFound.length * 5;
        }
        
        // Перевіряємо елементи контенту
        if (rule.contentElements) {
          var contentElementsFound = rule.contentElements.filter(function(selector) {
            return document.querySelector(selector) !== null;
          });
          contentScore += contentElementsFound.length * 3;
        }
        
        // Якщо модальне вікно видиме і має достатньо контенту, повертаємо тип
        if (contentScore >= 10) {
          return pageType;
        }
      }
      
      return null;
    },

    detect: function() {
      var url = window.location.href;
      var pathname = window.location.pathname;
      
      // Спочатку перевіряємо модальні вікна (найвищий пріоритет)
      var modalPageType = this.detectModalPage();
      if (modalPageType) {
        return modalPageType;
      }

      var candidates = [];

      // Збираємо всі потенційні кандидати (виключаючи модальні сторінки)
      for (var pageType in this.rules) {
        var rule = this.rules[pageType];
        
        // Пропускаємо модальні сторінки - вони обробляються окремо
        if (rule.modal) continue;
        var score = 0;
        var matchReason = '';
        
        // Перевірка виключень
        if (rule.exclude && rule.exclude.some(function(ex) { return url.includes(ex); })) {
          continue;
        }
        
        // Перевірка URL
        if (rule.url && rule.url.test(pathname)) {
          score += 10;
          matchReason = 'url';
        }
        
        // Перевірка елементів
        if (rule.elements) {
          var elementsFound = rule.elements.filter(function(selector) {
            return document.querySelector(selector) !== null;
          });
          
          if (elementsFound.length > 0) {
            score += elementsFound.length * 5;
            matchReason += (matchReason ? '+' : '') + 'elements(' + elementsFound.length + ')';
          }
        }
        
        if (score > 0) {
          candidates.push({
            type: pageType,
            score: score,
            priority: rule.priority,
            matchReason: matchReason
          });
        }
      }
      
      if (candidates.length === 0) {
        return 'other';
      }
      
      // Сортуємо кандидатів за очками та пріоритетом
      candidates.sort(function(a, b) {
        if (a.score !== b.score) return b.score - a.score; // Більше очок = краще
        return a.priority - b.priority; // Менший пріоритет = важливіше
      });
      
      var winner = candidates[0];
      
      return winner.type;
    }
  };

  /******************** ЦЕНТРАЛІЗОВАНИЙ ОБСЕРВЕР ********************/ 
  var CentralObserver = {
    observer: null,
    handlers: new Map(),
    throttleTimer: null,
    pendingMutations: [],

    init: function() {
      if (this.observer) return;
      
      this.observer = new MutationObserver(this.handleMutations.bind(this));
      this.observer.observe(document.body, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ['style', 'class', 'data-*']
      });
      
      log('Централізований обсервер ініціалізовано');
    },

    handleMutations: function(mutations) {
      this.pendingMutations = this.pendingMutations.concat(mutations);
      
      if (this.throttleTimer) return;
      
      this.throttleTimer = setTimeout(function() {
        this.processPendingMutations();
        this.throttleTimer = null;
      }.bind(this), OBSERVER_THROTTLE);
    },

    processPendingMutations: function() {
      if (this.pendingMutations.length === 0) return;
      
      var mutations = this.pendingMutations;
      this.pendingMutations = [];
      
      // Групуємо мутації по типах для оптимізації
      var groupedMutations = {
        childList: [],
        attributes: []
      };
      
      mutations.forEach(function(mutation) {
        groupedMutations[mutation.type].push(mutation);
      });
      
      // Викликаємо обробники
      this.handlers.forEach(function(handler, scriptName) {
        try {
          if (typeof handler === 'function') {
            handler(groupedMutations);
          }
        } catch (e) {
          logError('Помилка в обробнику ' + scriptName, e);
        }
      });
    },

    registerHandler: function(scriptName, handler) {
      this.handlers.set(scriptName, handler);
      log('Зареєстровано обробник', { script: scriptName });
    },

    unregisterHandler: function(scriptName) {
      this.handlers.delete(scriptName);
      log('Видалено обробник', { script: scriptName });
    },

    destroy: function() {
      if (this.observer) {
        this.observer.disconnect();
        this.observer = null;
      }
      if (this.throttleTimer) {
        clearTimeout(this.throttleTimer);
        this.throttleTimer = null;
      }
      this.handlers.clear();
      this.pendingMutations = [];
      log('Централізований обсервер знищено');
    }
  };

  /******************** МЕНЕДЖЕР СКРИПТІВ ********************/ 
  var ScriptManager = {
    scripts: new Map(),
    currentPageType: null,
    initialized: false,

    // Реєстр доступних скриптів
    scriptRegistry: {
      // Бонусна система для чекауту (головна сторінка замовлення)
      'bonus-checkout': {
        pages: ['checkout-main'],
        url: SCRIPTS_BASE_URL + 'bonus_checkoutpage_script.js',
        module: null,
        priority: 1,
        description: 'Бонусна система для сторінки оформлення замовлення'
      },
      // Бонусна система для thank you сторінки
      'bonus-thankyou': {
        pages: ['checkout-complete'],
        url: SCRIPTS_BASE_URL + 'bonus_thankspage_script.js',
        module: null,
        priority: 1,
        description: 'Бонусна система для сторінки подяки після замовлення'
      },
      // Перевірка безкоштовної доставки (тимчасово відключено - 404)
      /*'free-delivery': {
        pages: ['checkout-main', 'cart', 'cart-modal'],
        url: SCRIPTS_BASE_URL + 'free_delivery_checker.js',
        module: null,
        priority: 2,
        description: 'Перевірка умов безкоштовної доставки'
      },*/
      // Новий калькулятор безкоштовної доставки (від 3000 грн)
      'free-delivery-calculator': {
        pages: ['checkout-main', 'cart', 'cart-modal'],
        url: SCRIPTS_BASE_URL + 'free_delivery_calculator.js',
        module: null,
        priority: 2,
        description: 'Калькулятор безкоштовної доставки від 3000 грн (укр/рос мови)'
      },
      // SMS функціонал
      'sms-notifications': {
        pages: ['checkout-main'],
        url: SCRIPTS_BASE_URL + 'sms_script.js',
        module: null,
        priority: 3,
        description: 'SMS сповіщення для клієнтів'
      },
      // Онлайн платежі
      'online-payment': {
        pages: ['checkout-complete'],
        url: SCRIPTS_BASE_URL + 'online_payment.js',
        module: null,
        priority: 2,
        description: 'Обробка онлайн платежів'
      },
      // Заміна тексту промокодів на бонуси - ПЕРЕНЕСЕНО НА CLOUDFLARE CDN!
      'text-replacer': {
        pages: ['checkout-main', 'checkout-complete', 'cart', 'cart-modal'],
        url: SCRIPTS_BASE_URL + 'bonus_text_replacer_script.js',
        module: null,
        priority: 0, // Найвищий пріоритет - але ВІДКЛЮЧЕНО через Cloudflare
        description: '⚡ Заміна тексту "Промокод/Сертифікат" на "Бонуси" (ПРАЦЮЄ НА CLOUDFLARE CDN)'
      }
    },

    init: function() {
      if (this.initialized) return;
      
      log('Ініціалізація менеджера скриптів');
      
      // Ініціалізуємо централізований обсервер
      CentralObserver.init();
      
      // Визначаємо тип сторінки
      this.currentPageType = PageDetector.detect();
      
      // ПРЕВЕНТИВНЕ ЗАВАНТАЖЕННЯ критичних скриптів
      this.preloadCriticalScripts();
      
      // Завантажуємо і запускаємо відповідні скрипти
      this.loadPageScripts();
      
      // Слухаємо зміни сторінки (SPA)
      this.setupPageChangeListener();
      
      this.initialized = true;
      log('Менеджер скриптів ініціалізовано', { pageType: this.currentPageType });
    },

    preloadCriticalScripts: function() {
      // Список критичних скриптів які завантажуємо завжди
      var criticalScripts = []; // Заміна тексту тепер на рівні Cloudflare!
      
      if (criticalScripts.length === 0) {
        log('Критичні скрипти не потрібні - заміна тексту на рівні CDN');
        return;
      }
      
      log('Превентивне завантаження критичних скриптів', { scripts: criticalScripts });
      
      criticalScripts.forEach(function(scriptName) {
        var config = this.scriptRegistry[scriptName];
        if (config && !DISABLED_SCRIPTS[scriptName]) {
          // Завантажуємо в кеш (але не запускаємо поки)
          ScriptLoader.loadScript(config.url).catch(function(error) {
            logError('Помилка превентивного завантаження скрипта ' + scriptName, error);
          });
        }
      }.bind(this));
    },

    loadPageScripts: function() {
      var pageType = this.currentPageType;
      var scriptsToLoad = [];
      
      log('Аналіз потрібних скриптів для сторінки', { pageType: pageType });
      
      // Знаходимо скрипти для поточного типу сторінки
      for (var scriptName in this.scriptRegistry) {
        var scriptConfig = this.scriptRegistry[scriptName];
        
        if (scriptConfig.pages.includes(pageType)) {
          // Перевіряємо чи скрипт не конфліктує з іншими
          var shouldLoad = this.shouldLoadScript(scriptName, scriptConfig, pageType);
          
          if (shouldLoad) {
            scriptsToLoad.push({
              name: scriptName,
              config: scriptConfig
            });
            log('Скрипт додано до черги завантаження', { 
              script: scriptName, 
              description: scriptConfig.description 
            });
          } else {
            log('Скрипт пропущено через конфлікт', { script: scriptName });
          }
        }
      }
      
      // Сортуємо за пріоритетом
      scriptsToLoad.sort(function(a, b) {
        return a.config.priority - b.config.priority;
      });
      
      log('Фінальний список скриптів для завантаження', { 
        pageType: pageType, 
        scripts: scriptsToLoad.map(function(s) { 
          return { 
            name: s.name, 
            priority: s.config.priority,
            description: s.config.description
          }; 
        })
      });
      
      if (scriptsToLoad.length === 0) {
        log('Немає скриптів для завантаження на цій сторінці', { pageType: pageType });
        return;
      }
      
      // ШВИДКЕ ПАРАЛЕЛЬНЕ ЗАВАНТАЖЕННЯ - без затримок
      var loadPromises = [];
      
      // Розділяємо скрипти на критичні та звичайні
      var criticalScripts = scriptsToLoad.filter(function(s) { return s.config.priority <= 1; });
      var normalScripts = scriptsToLoad.filter(function(s) { return s.config.priority > 1; });
      
      // Спочатку завантажуємо всі критичні скрипти ОДНОЧАСНО
      criticalScripts.forEach(function(scriptInfo) {
        var promise = this.loadScript(scriptInfo.name, scriptInfo.config)
          .catch(function(error) {
            logError('Помилка завантаження критичного скрипта ' + scriptInfo.name, error);
            return null; // Не блокуємо інші скрипти
          });
        loadPromises.push(promise);
      }.bind(this));
      
      // Звичайні скрипти також завантажуємо одночасно
      normalScripts.forEach(function(scriptInfo) {
        var promise = this.loadScript(scriptInfo.name, scriptInfo.config)
          .catch(function(error) {
            logError('Помилка завантаження скрипта ' + scriptInfo.name, error);
            return null; // Не блокуємо інші скрипти
          });
        loadPromises.push(promise);
      }.bind(this));
      
      // Чекаємо завантаження всіх скриптів
      Promise.all(loadPromises).then(function() {
        log('Всі скрипти для сторінки завантажено', { 
          pageType: pageType,
          loadedCount: scriptsToLoad.length
        });
      }).catch(function(error) {
        logError('Помилка завантаження деяких скриптів', error);
      });
    },

    shouldLoadScript: function(scriptName, scriptConfig, pageType) {
      // Перевіряємо чи скрипт відключений глобально
      if (DISABLED_SCRIPTS[scriptName]) {
        log('Скрипт відключений через конфігурацію', { script: scriptName });
        return false;
      }
      
      // Логіка для визначення чи потрібно завантажувати скрипт
      
      // Для checkout-main - не завантажуємо універсальні версії
      if (pageType === 'checkout-main') {
        if (scriptName === 'bonus-universal' || scriptName === 'bonus-cart-modal') {
          return false; // Використовуємо повнофункціональний bonus-checkout
        }
      }
      
      // Для checkout-complete - не завантажуємо checkout скрипти
      if (pageType === 'checkout-complete') {
        if (scriptName === 'bonus-checkout' || scriptName === 'bonus-universal' || scriptName === 'bonus-cart-modal') {
          return false; // Використовуємо тільки bonus-thankyou
        }
        // SMS для thank you сторінки не потрібні
        if (scriptName === 'sms-notifications') {
          return false;
        }
      }
      
      // Для модального кошика - використовуємо спеціалізований скрипт
      if (pageType === 'cart-modal') {
        if (scriptName === 'bonus-checkout' || scriptName === 'bonus-thankyou' || scriptName === 'bonus-universal') {
          return false; // Використовуємо bonus-cart-modal
        }
        // Для модального кошика не завантажуємо SMS і онлайн платежі
        if (['sms-notifications', 'online-payment'].includes(scriptName)) {
          return false;
        }
        // Тимчасово відключаємо bonus-cart-modal поки файл не існує
        if (scriptName === 'bonus-cart-modal') {
          return false;
        }
      }
      
      // Для звичайної сторінки кошика - не завантажуємо модальну версію
      if (pageType === 'cart') {
        if (scriptName === 'bonus-cart-modal') {
          return false; // Використовуємо bonus-universal для звичайної сторінки кошика
        }
      }
      
      // Для інших сторінок - використовуємо тільки universal версії
      if (['other', 'product', 'category', 'home'].includes(pageType)) {
        if (scriptName === 'bonus-checkout' || scriptName === 'bonus-thankyou' || scriptName === 'bonus-cart-modal') {
          return false; // Використовуємо bonus-universal
        }
        // Не завантажуємо checkout-специфічні скрипти на інших сторінках
        if (['sms-notifications', 'online-payment'].includes(scriptName)) {
          return false;
        }
        // Тимчасово відключаємо bonus-universal поки файл не існує
        if (scriptName === 'bonus-universal') {
          return false;
        }
      }
      
      // text-replacer завантажуємо на всіх сторінках де є бонусна система
      if (scriptName === 'text-replacer') {
        // Завантажуємо тільки на сторінках де є форми з промокодами/бонусами
        var supportedPages = ['checkout-main', 'checkout-complete', 'cart', 'cart-modal'];
        return supportedPages.includes(pageType);
      }
      
      // free-delivery-calculator завантажуємо на сторінках з кошиком та checkout
      if (scriptName === 'free-delivery-calculator') {
        // Завантажуємо на сторінках де показується сума замовлення
        var supportedPages = ['checkout-main', 'cart', 'cart-modal'];
        return supportedPages.includes(pageType);
      }
      
      return true;
    },

    loadScript: function(scriptName, config) {
      if (this.scripts.has(scriptName)) {
        log('Скрипт вже завантажено', { script: scriptName });
        return Promise.resolve();
      }
      
      if (!config || !config.url) {
        logError('Невірна конфігурація скрипта', { scriptName: scriptName, config: config });
        return Promise.reject(new Error('Невірна конфігурація скрипта: ' + scriptName));
      }
      
      // Перевіряємо чи скрипт не відключений
      if (DISABLED_SCRIPTS[scriptName]) {
        log('Скрипт пропущено - відключений', { script: scriptName });
        return Promise.resolve();
      }
      
      log('Завантаження скрипта', { script: scriptName, url: config.url });
      
      return ScriptLoader.loadScript(config.url)
        .then(function(moduleFactory) {
          try {
            var scriptModule = moduleFactory();
            
            if (scriptModule) {
              this.scripts.set(scriptName, {
                module: scriptModule,
                config: config,
                active: true,
                url: config.url
              });
              
              // Ініціалізуємо скрипт
              if (typeof scriptModule.init === 'function') {
                scriptModule.init();
              }
              
              // Спеціальна логіка для калькулятора безкоштовної доставки
              if (scriptName === 'free-delivery-calculator') {
                // Калькулятор має власну автоініціалізацію через window.FreeDeliveryCalculator
                if (window.FreeDeliveryCalculator && typeof window.FreeDeliveryCalculator.init === 'function') {
                  if (!window.FreeDeliveryCalculator.isInitialized()) {
                    window.FreeDeliveryCalculator.init();
                    log('Free delivery calculator ініціалізовано через window API');
                  }
                } else {
                  // Якщо немає window API, можливо скрипт ініціалізується автоматично
                  log('Free delivery calculator завантажено (автоініціалізація)');
                }
              }
              
              // Реєструємо обробник мутацій якщо є
              if (typeof scriptModule.handleMutations === 'function') {
                CentralObserver.registerHandler(scriptName, scriptModule.handleMutations);
              }
              
              log('Скрипт завантажено і ініціалізовано', { script: scriptName });
            } else {
              logError('Модуль скрипта не створився', { script: scriptName });
            }
          } catch (e) {
            logError('Помилка ініціалізації скрипта ' + scriptName, e);
          }
        }.bind(this))
        .catch(function(error) {
          logError('Помилка завантаження скрипта ' + scriptName, error);
        });
    },

    setupPageChangeListener: function() {
      // Слухаємо зміни URL для SPA
      var originalPushState = history.pushState;
      var originalReplaceState = history.replaceState;
      var pageChangeTimeout = null;
      
      var handlePageChange = function() {
        // Дебаунс для зміни сторінки
        if (pageChangeTimeout) {
          clearTimeout(pageChangeTimeout);
        }
        
        pageChangeTimeout = setTimeout(function() {
          var newPageType = PageDetector.detect();
          if (newPageType !== this.currentPageType) {
            log('Зміна типу сторінки', { 
              from: this.currentPageType, 
              to: newPageType 
            });
            this.handlePageChange(newPageType);
          }
          pageChangeTimeout = null;
        }.bind(this), PAGE_CHANGE_DEBOUNCE);
      }.bind(this);
      
      history.pushState = function() {
        originalPushState.apply(history, arguments);
        handlePageChange();
      };
      
      history.replaceState = function() {
        originalReplaceState.apply(history, arguments);
        handlePageChange();
      };
      
      window.addEventListener('popstate', handlePageChange);
      
      // Додаємо обробку модальних вікон
      this.setupModalDetection();
    },

    setupModalDetection: function() {
      var modalCheckTimeout = null;
      var lastModalState = null;
      
      // Спостерігаємо за змінами в DOM для виявлення модальних вікон
      var modalObserver = new MutationObserver(function(mutations) {
        var shouldCheckModal = false;
        
        mutations.forEach(function(mutation) {
          // Перевіряємо зміни атрибутів style (display, visibility)
          if (mutation.type === 'attributes' && 
              (mutation.attributeName === 'style' || mutation.attributeName === 'class')) {
            var target = mutation.target;
            if (target.id === 'modal-overlay' || 
                target.id === 'cart' || 
                target.classList.contains('popup') ||
                target.classList.contains('overlay')) {
              shouldCheckModal = true;
            }
          }
          
          // Перевіряємо додавання/видалення вузлів
          if (mutation.type === 'childList') {
            var addedNodes = Array.from(mutation.addedNodes);
            var removedNodes = Array.from(mutation.removedNodes);
            
            addedNodes.concat(removedNodes).forEach(function(node) {
              if (node.nodeType === 1) { // Element node
                if (node.id === 'modal-overlay' || 
                    node.id === 'cart' ||
                    node.classList.contains('popup') ||
                    node.classList.contains('overlay') ||
                    node.querySelector('#modal-overlay, #cart, .popup, .overlay')) {
                  shouldCheckModal = true;
                }
              }
            });
          }
        });
        
        if (shouldCheckModal) {
          // Дебаунс для модальних вікон
          if (modalCheckTimeout) {
            clearTimeout(modalCheckTimeout);
          }
          
          modalCheckTimeout = setTimeout(function() {
            var newPageType = PageDetector.detect();
            
            // Перевіряємо чи дійсно змінився стан модального вікна
            var currentModalState = newPageType === 'cart-modal';
            if (currentModalState !== lastModalState && newPageType !== this.currentPageType) {
              log('Модальне вікно змінилося', { 
                from: this.currentPageType, 
                to: newPageType,
                modalState: currentModalState
              });
              this.handlePageChange(newPageType);
            }
            lastModalState = currentModalState;
            modalCheckTimeout = null;
          }.bind(this), PAGE_CHANGE_DEBOUNCE);
        }
      }.bind(this));
      
      // Спостерігаємо за всім документом
      modalObserver.observe(document.body, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ['style', 'class']
      });
      
      // Зберігаємо посилання на observer для можливості його видалення
      this.modalObserver = modalObserver;
      
      log('Налаштовано спостереження за модальними вікнами');
    },

    handlePageChange: function(newPageType) {
      // Знищуємо поточні скрипти
      this.destroyCurrentScripts();
      
      // Оновлюємо тип сторінки
      this.currentPageType = newPageType;
      
      // Завантажуємо нові скрипти
      this.loadPageScripts();
    },

    destroyCurrentScripts: function() {
      this.scripts.forEach(function(scriptInfo, scriptName) {
        try {
          // Видаляємо обробник мутацій
          CentralObserver.unregisterHandler(scriptName);
          
          // Викликаємо destroy якщо є
          if (scriptInfo.module && typeof scriptInfo.module.destroy === 'function') {
            scriptInfo.module.destroy();
          }
          
          log('Скрипт знищено', { script: scriptName });
        } catch (e) {
          logError('Помилка знищення скрипта ' + scriptName, e);
        }
      });
      
      this.scripts.clear();
    },

    getActiveScripts: function() {
      var active = [];
      this.scripts.forEach(function(scriptInfo, scriptName) {
        if (scriptInfo.active) {
          active.push(scriptName);
        }
      });
      return active;
    },

    destroy: function() {
      this.destroyCurrentScripts();
      CentralObserver.destroy();
      
      // Знищуємо modal observer
      if (this.modalObserver) {
        this.modalObserver.disconnect();
        this.modalObserver = null;
      }
      
      this.initialized = false;
      log('Менеджер скриптів знищено');
    }
  };

  /******************** ГЛОБАЛЬНИЙ API ********************/ 
  // Експортуємо API в глобальну область для можливості управління ззовні
  window.ScriptManager = {
    getPageType: function() {
      return ScriptManager.currentPageType;
    },
    getActiveScripts: function() {
      return ScriptManager.getActiveScripts();
    },
    enableDebug: function() {
      DEBUG_MODE = true;
      log('Debug режим увімкнено');
    },
    disableDebug: function() {
      DEBUG_MODE = false;
      console.log(LOG_PREFIX + ' Debug режим вимкнено');
    },
    getStats: function() {
      return {
        pageType: ScriptManager.currentPageType,
        activeScripts: ScriptManager.getActiveScripts(),
        observerHandlers: CentralObserver.handlers.size,
        initialized: ScriptManager.initialized,
        cacheStats: ScriptCache.getStats(),
        loadingScripts: ScriptLoader.loadingPromises.size
      };
    },
    getDetailedStats: function() {
      var stats = this.getStats();
      
      // Додаємо інформацію про зареєстровані скрипти
      stats.registeredScripts = {};
      for (var name in ScriptManager.scriptRegistry) {
        var config = ScriptManager.scriptRegistry[name];
        stats.registeredScripts[name] = {
          pages: config.pages,
          priority: config.priority,
          description: config.description,
          url: config.url,
          loaded: ScriptManager.scripts.has(name),
          disabled: !!DISABLED_SCRIPTS[name]
        };
      }
      
      // Інформація про відключені скрипти
      stats.disabledScripts = this.getDisabledScripts();
      
      // Інформація про поточні активні скрипти
      stats.activeScriptDetails = {};
      ScriptManager.scripts.forEach(function(scriptInfo, scriptName) {
        stats.activeScriptDetails[scriptName] = {
          url: scriptInfo.url,
          active: scriptInfo.active,
          hasInit: typeof scriptInfo.module.init === 'function',
          hasMutationHandler: typeof scriptInfo.module.handleMutations === 'function',
          hasDestroy: typeof scriptInfo.module.destroy === 'function'
        };
      });
      
      return stats;
    },
    forceReload: function(scriptName) {
      if (scriptName) {
        // Перезавантажуємо конкретний скрипт
        var scriptInfo = ScriptManager.scripts.get(scriptName);
        if (scriptInfo) {
          ScriptCache.cache.delete(scriptInfo.url);
          ScriptManager.scripts.delete(scriptName);
          CentralObserver.unregisterHandler(scriptName);
          
          if (scriptInfo.module && typeof scriptInfo.module.destroy === 'function') {
            scriptInfo.module.destroy();
          }
          
          ScriptManager.loadScript(scriptName, scriptInfo.config);
          log('Скрипт перезавантажено', { script: scriptName });
        }
      } else {
        // Перезавантажуємо всі скрипти
        ScriptCache.clear();
        ScriptManager.handlePageChange(ScriptManager.currentPageType);
        log('Всі скрипти перезавантажено');
      }
    },
    forcePageRedetection: function() {
      var newPageType = PageDetector.detect();
      if (newPageType !== ScriptManager.currentPageType) {
        log('Примусова зміна типу сторінки', { 
          from: ScriptManager.currentPageType, 
          to: newPageType 
        });
        ScriptManager.handlePageChange(newPageType);
      } else {
        log('Тип сторінки не змінився', { pageType: newPageType });
      }
      return newPageType;
    },
    clearCache: function() {
      ScriptCache.clear();
      log('Кеш скриптів очищено');
    },
    
    // Додаємо метод для ручного управління скриптами
    enableMissingScripts: function() {
      delete DISABLED_SCRIPTS['bonus-universal'];
      delete DISABLED_SCRIPTS['bonus-cart-modal'];
      log('Відключені скрипти увімкнено. Для застосування потрібно перезавантажити сторінку.');
    },
    
    disableMissingScripts: function() {
      DISABLED_SCRIPTS['bonus-universal'] = true;
      DISABLED_SCRIPTS['bonus-cart-modal'] = true;
      log('Відсутні скрипти вимкнено');
    },
    setScriptsBaseUrl: function(url) {
      SCRIPTS_BASE_URL = url.endsWith('/') ? url : url + '/';
      log('Базова URL скриптів оновлена', { url: SCRIPTS_BASE_URL });
      
      // Оновлюємо URL в реєстрі
      for (var scriptName in ScriptManager.scriptRegistry) {
        var config = ScriptManager.scriptRegistry[scriptName];
        var filename = config.url.split('/').pop();
        config.url = SCRIPTS_BASE_URL + filename;
      }
    },
    getScriptsBaseUrl: function() {
      return SCRIPTS_BASE_URL;
    },
    registerCustomScript: function(name, config) {
      if (!config.url || !config.pages || !Array.isArray(config.pages)) {
        logError('Невірна конфігурація скрипта', { name: name, config: config });
        return false;
      }
      
      ScriptManager.scriptRegistry[name] = {
        pages: config.pages,
        url: config.url,
        priority: config.priority || 10,
        description: config.description || 'Користувацький скрипт',
        module: null
      };
      
      log('Користувацький скрипт зареєстровано', { name: name, config: config });
      
      // Якщо скрипт підходить для поточної сторінки, завантажуємо його
      if (config.pages.includes(ScriptManager.currentPageType)) {
        ScriptManager.loadScript(name, ScriptManager.scriptRegistry[name]);
      }
      
      return true;
    },
    unregisterScript: function(name) {
      if (ScriptManager.scriptRegistry[name]) {
        delete ScriptManager.scriptRegistry[name];
        
        // Видаляємо активний скрипт якщо він запущений
        if (ScriptManager.scripts.has(name)) {
          var scriptInfo = ScriptManager.scripts.get(name);
          CentralObserver.unregisterHandler(name);
          
          if (scriptInfo.module && typeof scriptInfo.module.destroy === 'function') {
            scriptInfo.module.destroy();
          }
          
          ScriptManager.scripts.delete(name);
        }
        
        log('Скрипт видалено з реєстру', { name: name });
        return true;
      }
      return false;
    },
    // Керування відключеними скриптами
    disableScript: function(scriptName) {
      DISABLED_SCRIPTS[scriptName] = true;
      log('Скрипт відключено', { script: scriptName });
      
      // Якщо скрипт зараз активний - зупиняємо його
      if (ScriptManager.scripts.has(scriptName)) {
        var scriptInfo = ScriptManager.scripts.get(scriptName);
        CentralObserver.unregisterHandler(scriptName);
        
        if (scriptInfo.module && typeof scriptInfo.module.destroy === 'function') {
          scriptInfo.module.destroy();
        }
        
        ScriptManager.scripts.delete(scriptName);
        log('Активний скрипт зупинено', { script: scriptName });
      }
    },
    enableScript: function(scriptName) {
      if (DISABLED_SCRIPTS[scriptName]) {
        delete DISABLED_SCRIPTS[scriptName];
        log('Скрипт увімкнено', { script: scriptName });
        
        // Якщо скрипт підходить для поточної сторінки - завантажуємо
        var config = ScriptManager.scriptRegistry[scriptName];
        if (config && config.pages.includes(ScriptManager.currentPageType)) {
          ScriptManager.loadScript(scriptName, config);
        }
      }
    },
    getDisabledScripts: function() {
      return Object.keys(DISABLED_SCRIPTS).filter(function(script) {
        return DISABLED_SCRIPTS[script];
      });
    },
    isScriptDisabled: function(scriptName) {
      return !!DISABLED_SCRIPTS[scriptName];
    },
    // Утилітарні методи для діагностики
    diagnose: function() {
      var issues = [];
      var warnings = [];
      
      // Перевіряємо стан
      if (!ScriptManager.initialized) {
        issues.push('ScriptManager не ініціалізований');
      }
      
      if (!CentralObserver.observer) {
        issues.push('CentralObserver не активний');
      }
      
      // Перевіряємо скрипти
      var expectedScripts = [];
      for (var scriptName in ScriptManager.scriptRegistry) {
        var config = ScriptManager.scriptRegistry[scriptName];
        if (config.pages.includes(ScriptManager.currentPageType) && !DISABLED_SCRIPTS[scriptName]) {
          expectedScripts.push(scriptName);
        }
      }
      
      var loadedScripts = ScriptManager.getActiveScripts();
      expectedScripts.forEach(function(scriptName) {
        if (!loadedScripts.includes(scriptName)) {
          warnings.push('Очікуваний скрипт не завантажений: ' + scriptName);
        }
      });
      
      return {
        healthy: issues.length === 0,
        issues: issues,
        warnings: warnings,
        pageType: ScriptManager.currentPageType,
        expectedScripts: expectedScripts,
        loadedScripts: loadedScripts,
        disabledScripts: this.getDisabledScripts()
      };
    }
  };

  /******************** ШВИДКИЙ АВТОЗАПУСК ********************/ 
  // МИТТЄВА ініціалізація менеджера без затримок
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() {
      ScriptManager.init(); // Прибираємо setTimeout - запускаємо миттєво
    });
  } else {
    ScriptManager.init(); // Якщо DOM вже готовий - запускаємо негайно
  }

  // Cleanup при закритті сторінки
  window.addEventListener('beforeunload', function() {
    ScriptManager.destroy();
  });

  log('Централізований менеджер скриптів завантажено (ШВИДКИЙ РЕЖИМ)', { 
    debugMode: DEBUG_MODE,
    observerThrottle: OBSERVER_THROTTLE + 'мс',
    pageChangeDebounce: PAGE_CHANGE_DEBOUNCE + 'мс',
    cacheTime: (SCRIPT_CACHE_TTL / 3600000) + ' годин'
  });

})();
