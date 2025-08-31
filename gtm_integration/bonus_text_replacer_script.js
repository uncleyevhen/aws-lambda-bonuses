(function(){
  'use strict';

  // Детальне логування
  var DEBUG_MODE = true; // Тимчасово увімкнено для дебагу thanks page
  var LOG_PREFIX = '[TEXT_REPLACER]';

  function log(message, data) {
    if (DEBUG_MODE) {
      var timestamp = new Date().toISOString();
      if (data) {
        console.log(LOG_PREFIX + ' [' + timestamp + '] ' + message, data);
      } else {
        console.log(LOG_PREFIX + ' [' + timestamp + '] ' + message);
      }
    }
  }

  function logError(message, error) {
    var timestamp = new Date().toISOString();
    if (error) {
      console.error(LOG_PREFIX + ' [' + timestamp + '] ERROR: ' + message, error);
    } else {
      console.error(LOG_PREFIX + ' [' + timestamp + '] ERROR: ' + message);
    }
  }

  // Конфігурація заміни тексту
  var TEXT_REPLACEMENTS = {
    'uk': {
      'Подарунковий сертифікат': 'Бонуси',
      'подарунковий сертифікат': 'бонуси',
      'Промокод': 'Бонуси',
      'промокод': 'бонуси',
      'Сертифікат': 'Бонуси',
      'сертифікат': 'бонуси'
    },
    'ru': {
      'Подарочный сертификат': 'Бонусы',
      'подарочный сертификат': 'бонусы',
      'Промокод': 'Бонусы',
      'промокод': 'бонусы',
      'Сертификат': 'Бонусы',
      'сертификат': 'бонусы'
    }
  };

  // Селектори де потрібно замінювати текст
  var TARGET_SELECTORS = [
    '.cart-discount-info',
    '.coupon__name', 
    '.order-details__cost-name', 
    '.order-details-h',
    '.invoice__name',
    '.order-details-b',
    '.order-details__cost-value',
    '.j-coupon-add',
    '.coupon-title',
    '.discount-title',
    // Додаткові селектори для різних сторінок
    '.cart-coupon',
    '.promo-code',
    '.discount-info',
    '.order-summary .discount',
    '.checkout-discount',
    // Селектори для input полів з placeholder
    'input[placeholder*="промокод"]',
    'input[placeholder*="сертифікат"]',
    'input[placeholder*="промокод"]',
    'input[placeholder*="сертификат"]',
    'input[placeholder*="Промокод"]',
    'input[placeholder*="Сертифікат"]',
    'input[placeholder*="Промокод"]',
    'input[placeholder*="Сертификат"]'
  ];

  // Селектори промокодів/бонусів
  var CERTIFICATE_SELECTORS = ['.cart-discount-info', '.coupon__name', '.order-details__cost-name', '.order-details-h'];

  var replacerState = {
    initialized: false,
    currentLanguage: 'uk',
    lastReplacementTime: 0,
    replacementCount: 0,
    processedElements: new WeakSet()
  };

  // Функції для перевірки стану промокодів/бонусів
  function isPromoCodeApplied() {
    // Для checkout-complete (thanks page) - перевіряємо наявність елементів з інформацією про знижку
    if (window.location.pathname.includes('/checkout/complete')) {
      var orderDetails = document.querySelectorAll('.order-details-h, .order-details__cost-name, .invoice__name');
      for (var i = 0; i < orderDetails.length; i++) {
        var text = (orderDetails[i].textContent || orderDetails[i].innerText || '').toLowerCase();
        if (text.includes('сертифікат') || text.includes('промокод') || text.includes('знижка') || text.includes('бонус')) {
          return true;
        }
      }
      return false;
    }
    
    // Для інших сторінок - перевіряємо кнопку видалення промокода
    var removeButton = document.querySelector('.j-coupon-remove');
    if (!removeButton) return false;
    
    var style = window.getComputedStyle(removeButton);
    return style.display !== 'none' && style.visibility !== 'hidden' && 
           removeButton.offsetWidth > 0 && removeButton.offsetHeight > 0;
  }

  function isBonusPromoCodeApplied() {
    var promoApplied = isPromoCodeApplied();
    if (!promoApplied) return false;
    
    // Для checkout-complete (thanks page) - перевіряємо що є знижка від промокода/сертифіката
    if (window.location.pathname.includes('/checkout/complete')) {
      var orderDetails = document.querySelectorAll('.order-details-h, .order-details__cost-name, .invoice__name');
      for (var i = 0; i < orderDetails.length; i++) {
        var text = (orderDetails[i].textContent || orderDetails[i].innerText || '').toLowerCase();
        // Якщо знайшли сертифікат/промокод - це точно бонуси на thanks page
        if (text.includes('сертифікат') || text.includes('промокод')) {
          log('Знайдено промокод/сертифікат на thanks page', { text: text });
          return true;
        }
        // Також перевіряємо чи вже замінено на бонуси
        if (text.includes('бонус')) {
          log('Знайдено бонуси на thanks page', { text: text });
          return true;
        }
      }
      // Якщо є знижка, але не знайшли конкретного тексту - все одно вважаємо що це бонуси
      return true;
    }
    
    // Перевіряємо чи це саме бонусний промокод за текстом "Бонуси" в елементах
    var certificateElements = document.querySelectorAll(CERTIFICATE_SELECTORS.join(','));
    for (var i = 0; i < certificateElements.length; i++) {
      var element = certificateElements[i];
      var text = (element.textContent || element.innerText || '').toLowerCase();
      if (text.includes('бонус') || text.includes('бонуc')) {
        return true;
      }
    }
    
    // Також перевіряємо через localStorage (може бути збережений стан від bonus_checkoutpage_script)
    try {
      var bonusState = localStorage.getItem('bonusState');
      if (bonusState) {
        var state = JSON.parse(bonusState);
        return state.promoApplied && state.promoType === 'bonus';
      }
    } catch (e) {
      // Ігноруємо помилки localStorage
    }
    
    return false;
  }

  // Визначення поточної мови
  function getCurrentLanguage() {
    var url = window.location.href;
    return url.includes('/ru/') ? 'ru' : 'uk';
  }

  // Заміна тексту в елементі
  function replaceTextInElement(element, oldText, newText) {
    if (!element || replacerState.processedElements.has(element)) return false;
    
    var replaced = false;
    
    // Обробляємо placeholder для input полів
    if (element.tagName === 'INPUT' && element.placeholder) {
      var oldPlaceholder = element.placeholder;
      var newPlaceholder = oldPlaceholder.replace(new RegExp(oldText, 'g'), newText);
      if (newPlaceholder !== oldPlaceholder) {
        element.placeholder = newPlaceholder;
        replaced = true;
        log('Placeholder замінено', {
          element: element.tagName,
          oldPlaceholder: oldPlaceholder,
          newPlaceholder: newPlaceholder
        });
      }
    }
    
    // Обробляємо текстовий контент
    var walker = document.createTreeWalker(
      element, 
      NodeFilter.SHOW_TEXT, 
      null, 
      false
    );
    
    var textNodes = [];
    var node;
    
    while (node = walker.nextNode()) {
      if (node.nodeValue && node.nodeValue.includes(oldText)) {
        textNodes.push(node);
      }
    }
    
    textNodes.forEach(function(textNode) {
      var newValue = textNode.nodeValue.replace(new RegExp(oldText, 'g'), newText);
      if (newValue !== textNode.nodeValue) {
        textNode.nodeValue = newValue;
        replaced = true;
      }
    });
    
    if (replaced) {
      replacerState.processedElements.add(element);
      replacerState.replacementCount++;
      // Помічаємо елемент як оброблений для CSS
      element.classList.add('text-processed');
      
      log('Текст замінено в елементі', {
        selector: element.tagName + (element.className ? '.' + element.className.split(' ').join('.') : ''),
        oldText: oldText,
        newText: newText
      });
    }
    
    return replaced;
  }

  // Основна функція заміни тексту
  function performTextReplacements() {
    // ВАЖЛИВО: Замінюємо текст тільки якщо застосовані саме БОНУСИ, а не звичайні промокоди
    if (!isBonusPromoCodeApplied()) {
      log('Заміна тексту пропущена - бонуси не застосовані або застосований звичайний промокод');
      return false;
    }

    var language = getCurrentLanguage();
    if (language !== replacerState.currentLanguage) {
      replacerState.currentLanguage = language;
      replacerState.processedElements = new WeakSet(); // Очищаємо кеш при зміні мови
    }
    
    var replacements = TEXT_REPLACEMENTS[language];
    if (!replacements) {
      log('Немає налаштувань заміни для мови: ' + language);
      return false;
    }
    
    var totalReplacements = 0;
    var processedSelectors = [];
    
    // Обробляємо кожен селектор
    TARGET_SELECTORS.forEach(function(selector) {
      var elements = document.querySelectorAll(selector);
      if (elements.length === 0) return;
      
      var selectorReplacements = 0;
      
      elements.forEach(function(element) {
        // Пропускаємо елементи всередині блоку бонусів (щоб не псувати наш власний контент)
        if (element.closest('#bonus-block') || 
            element.closest('[id*="bonus"]') ||
            element.classList.contains('bonus-widget') ||
            element.classList.contains('bonus-block')) {
          return;
        }
        
        // Виконуємо заміни для всіх налаштованих фраз
        for (var oldText in replacements) {
          var newText = replacements[oldText];
          if (replaceTextInElement(element, oldText, newText)) {
            selectorReplacements++;
            totalReplacements++;
          }
        }
        
        // Помічаємо елемент як оброблений навіть якщо заміни не було
        if (!element.classList.contains('text-processed')) {
          element.classList.add('text-processed');
        }
      });
      
      if (selectorReplacements > 0) {
        processedSelectors.push({
          selector: selector,
          elements: elements.length,
          replacements: selectorReplacements
        });
      }
    });
    
    if (totalReplacements > 0) {
      replacerState.lastReplacementTime = Date.now();
      log('Виконано заміну тексту (тільки для бонусів)', {
        language: language,
        totalReplacements: totalReplacements,
        processedSelectors: processedSelectors,
        totalCount: replacerState.replacementCount
      });
    }
    
    return totalReplacements > 0;
  }

  // Оптимізована заміна з debounce
  var debouncedTextReplacement = null;
  
  function scheduleTextReplacement() {
    if (debouncedTextReplacement) {
      clearTimeout(debouncedTextReplacement);
    }
    
    debouncedTextReplacement = setTimeout(function() {
      performTextReplacements();
      debouncedTextReplacement = null;
    }, 50); // Зменшена затримка для швидшої реакції
  }

  // Перевірка чи потрібна заміна
  function needsTextReplacement() {
    // Перевіряємо тільки якщо застосовані бонуси
    if (!isBonusPromoCodeApplied()) {
      return false;
    }
    
    var language = getCurrentLanguage();
    var replacements = TEXT_REPLACEMENTS[language];
    if (!replacements) return false;
    
    // Швидка перевірка - чи є на сторінці елементи з текстом що потребує заміни
    for (var oldText in replacements) {
      if (document.body.textContent.includes(oldText)) {
        return true;
      }
    }
    
    return false;
  }

  // Миттєва заміна при завантаженні
  function performInstantReplacement() {
    log('Виконання миттєвої заміни тексту при ініціалізації');
    if (needsTextReplacement()) {
      performTextReplacements();
    } else {
      log('Миттєва заміна не потрібна - немає цільових текстів');
    }
  }

  // Обробник мутацій для централізованого менеджера
  function handleMutations(groupedMutations) {
    var shouldReplace = false;
    
    // Перевіряємо зміни дочірніх елементів
    if (groupedMutations.childList && groupedMutations.childList.length > 0) {
      for (var i = 0; i < groupedMutations.childList.length; i++) {
        var mutation = groupedMutations.childList[i];
        
        // Перевіряємо додані вузли
        for (var j = 0; j < mutation.addedNodes.length; j++) {
          var node = mutation.addedNodes[j];
          if (node.nodeType === 1) { // Element node
            // Перевіряємо чи додані елементи містять цільові селектори або кнопки промокодів
            var hasTargetElements = TARGET_SELECTORS.some(function(selector) {
              return (node.matches && node.matches(selector)) || 
                     (node.querySelector && node.querySelector(selector));
            });
            
            // Також перевіряємо промокод/бонус елементи
            var hasPromoElements = (node.matches && node.matches('.j-coupon-remove, .coupon__name, .cart-discount-info')) ||
                                 (node.querySelector && node.querySelector('.j-coupon-remove, .coupon__name, .cart-discount-info'));
            
            if (hasTargetElements || hasPromoElements) {
              shouldReplace = true;
              break;
            }
            
            // Також перевіряємо текстовий контент на наявність цільових фраз, але тільки якщо бонуси застосовані
            if (node.textContent && isBonusPromoCodeApplied() && needsTextReplacement()) {
              shouldReplace = true;
              break;
            }
          }
        }
        
        // Перевіряємо видалені вузли (можливо видалили промокод)
        for (var k = 0; k < mutation.removedNodes.length; k++) {
          var removedNode = mutation.removedNodes[k];
          if (removedNode.nodeType === 1) {
            if ((removedNode.matches && removedNode.matches('.j-coupon-remove, .coupon__name, .cart-discount-info')) ||
                (removedNode.querySelector && removedNode.querySelector('.j-coupon-remove, .coupon__name, .cart-discount-info'))) {
              // Промокод було видалено - можливо потрібно відновити оригінальний текст
              shouldReplace = true;
              break;
            }
          }
        }
        
        if (shouldReplace) break;
      }
    }
    
    // Перевіряємо зміни атрибутів (зміна класів, стилів)
    if (!shouldReplace && groupedMutations.attributes && groupedMutations.attributes.length > 0) {
      for (var l = 0; l < groupedMutations.attributes.length; l++) {
        var mutation = groupedMutations.attributes[l];
        var target = mutation.target;
        
        // Якщо змінились атрибути елемента з цільовими селекторами або промокодами
        var isTargetElement = TARGET_SELECTORS.some(function(selector) {
          return target.matches && target.matches(selector);
        });
        
        var isPromoElement = target.matches && target.matches('.j-coupon-remove, .coupon__name, .cart-discount-info');
        
        if ((isTargetElement || isPromoElement) && target.textContent) {
          shouldReplace = true;
          break;
        }
      }
    }
    
    if (shouldReplace) {
      log('Виявлено зміни в промокодах/бонусах, перевіряємо потребу заміни тексту');
      scheduleTextReplacement();
    }
  }

  // Додавання CSS для попереднього приховування
  function addHidingCSS() {
    if (document.getElementById('text-replacer-css')) return;
    
    var css = document.createElement('style');
    css.id = 'text-replacer-css';
    css.textContent = `
      /* Приховуємо елементи під час завантаження заміни тексту */
      body.bonus-text-loading .cart-discount-info:not(.text-processed),
      body.bonus-text-loading .j-coupon-add:not(.text-processed),
      body.bonus-text-loading .coupon__name:not(.text-processed),
      body.bonus-text-loading .discount-label:not(.text-processed),
      body.bonus-text-loading .promo-code-label:not(.text-processed),
      body.bonus-text-loading .gift-certificate-label:not(.text-processed),
      body.bonus-text-loading label[for*="coupon"]:not(.text-processed),
      body.bonus-text-loading label[for*="promo"]:not(.text-processed),
      body.bonus-text-loading label[for*="certificate"]:not(.text-processed),
      body.bonus-text-loading .checkout-form .form-group label:not(.text-processed),
      body.bonus-text-loading .cart-form .form-group label:not(.text-processed),
      body.bonus-text-loading input[placeholder*="промокод"]:not(.text-processed),
      body.bonus-text-loading input[placeholder*="сертифікат"]:not(.text-processed),
      body.bonus-text-loading input[placeholder*="промокод"]:not(.text-processed),
      body.bonus-text-loading input[placeholder*="сертификат"]:not(.text-processed) {
        opacity: 0 !important;
        pointer-events: none;
        transition: opacity 0.1s ease-out;
      }
      
      /* Показуємо після обробки або коли готово */
      body.bonus-text-ready .cart-discount-info,
      body.bonus-text-ready .j-coupon-add,
      body.bonus-text-ready .coupon__name,
      body.bonus-text-ready .discount-label,
      body.bonus-text-ready .promo-code-label,
      body.bonus-text-ready .gift-certificate-label,
      body.bonus-text-ready label[for*="coupon"],
      body.bonus-text-ready label[for*="promo"],
      body.bonus-text-ready label[for*="certificate"],
      body.bonus-text-ready .checkout-form .form-group label,
      body.bonus-text-ready .cart-form .form-group label,
      body.bonus-text-ready input[placeholder*="промокод"],
      body.bonus-text-ready input[placeholder*="сертифікат"],
      body.bonus-text-ready input[placeholder*="промокод"],
      body.bonus-text-ready input[placeholder*="сертификат"],
      .text-processed {
        opacity: 1 !important;
        pointer-events: auto;
        transition: opacity 0.2s ease-in;
      }
      
      /* Початкове приховування до ініціалізації скрипта */
      .cart-discount-info:not(.text-processed),
      .j-coupon-add:not(.text-processed),
      .coupon__name:not(.text-processed) {
        opacity: 0;
        transition: opacity 0.1s ease-out;
      }
    `;
    
    document.head.appendChild(css);
    log('CSS для плавної заміни тексту додано');
  }

  // Швидка заміна тексту (для початкової ініціалізації)
  function performInstantReplacement() {
    // Додаємо клас до body для CSS контролю
    document.body.classList.add('bonus-text-loading');
    
    log('performInstantReplacement викликано', {
      url: window.location.href,
      pathname: window.location.pathname
    });
    
    var bonusApplied = isBonusPromoCodeApplied();
    log('Перевірка бонусів', { bonusApplied: bonusApplied });
    
    if (!bonusApplied) {
      log('Бонуси не застосовані - заміна тексту не потрібна');
      // Все одно помічаємо елементи як оброблені для показу
      markAllElementsAsProcessed();
      document.body.classList.remove('bonus-text-loading');
      document.body.classList.add('bonus-text-ready');
      return false;
    }

    var language = getCurrentLanguage();
    var replacements = TEXT_REPLACEMENTS[language];
    if (!replacements) {
      log('Заміни для мови не знайдено', { language: language });
      markAllElementsAsProcessed();
      document.body.classList.remove('bonus-text-loading');
      document.body.classList.add('bonus-text-ready');
      return false;
    }

    var totalReplacements = 0;
    var processedSelectors = [];

    TARGET_SELECTORS.forEach(function(selector) {
      var elements = Array.from(document.querySelectorAll(selector));
      var selectorReplacements = 0;
      
      elements.forEach(function(element) {
        for (var oldText in replacements) {
          var newText = replacements[oldText];
          if (replaceTextInElement(element, oldText, newText)) {
            selectorReplacements++;
            totalReplacements++;
          }
        }
        
        // Помічаємо елемент як оброблений
        element.classList.add('text-processed');
      });
      
      if (selectorReplacements > 0) {
        processedSelectors.push({
          selector: selector,
          elements: elements.length,
          replacements: selectorReplacements
        });
      }
    });

    // Помічаємо всі інші потенційні елементи як оброблені
    markAllElementsAsProcessed();

    // Знімаємо клас завантаження та додаємо готовності
    document.body.classList.remove('bonus-text-loading');
    document.body.classList.add('bonus-text-ready');

    if (totalReplacements > 0) {
      replacerState.lastReplacementTime = Date.now();
      replacerState.replacementCount += totalReplacements;
      log('Миттєва заміна тексту виконана', {
        language: language,
        totalReplacements: totalReplacements,
        processedSelectors: processedSelectors
      });
    }

    return totalReplacements > 0;
  }

  // Помічення всіх елементів як оброблених (для показу)
  function markAllElementsAsProcessed() {
    TARGET_SELECTORS.forEach(function(selector) {
      var elements = Array.from(document.querySelectorAll(selector));
      elements.forEach(function(element) {
        element.classList.add('text-processed');
      });
    });
  }

  // Ініціалізація модуля
  function initTextReplacer() {
    if (replacerState.initialized) {
      log('Модуль заміни тексту вже ініціалізований');
      return;
    }
    
    log('Ініціалізація модуля заміни тексту', {
      url: window.location.href,
      language: getCurrentLanguage()
    });

    // Додаємо CSS для плавної заміни
    addHidingCSS();
    
    replacerState.initialized = true;
    replacerState.currentLanguage = getCurrentLanguage();
    
    // Виконуємо миттєву заміну
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', function() {
        setTimeout(performInstantReplacement, 50);
      });
    } else {
      // Якщо DOM вже готовий - робимо заміну негайно
      performInstantReplacement();
    }
    
    log('Модуль заміни тексту ініціалізовано');
  }

  // Знищення модуля
  function destroyTextReplacer() {
    log('Знищення модуля заміни тексту');
    
    if (debouncedTextReplacement) {
      clearTimeout(debouncedTextReplacement);
      debouncedTextReplacement = null;
    }
    
    // Видаляємо CSS
    var css = document.getElementById('text-replacer-css');
    if (css) {
      css.remove();
    }
    
    // Видаляємо класи з body
    document.body.classList.remove('bonus-text-loading', 'bonus-text-ready');
    
    // Видаляємо класи text-processed з усіх елементів
    var processedElements = document.querySelectorAll('.text-processed');
    processedElements.forEach(function(element) {
      element.classList.remove('text-processed');
    });
    
    replacerState.initialized = false;
    replacerState.processedElements = new WeakSet();
    
    log('Модуль заміни тексту знищено');
  }

  // Модуль для експорту
  var textReplacerModule = {
    init: initTextReplacer,
    handleMutations: handleMutations,
    destroy: destroyTextReplacer,
    
    // Додаткові методи для ручного управління
    performReplacement: performTextReplacements,
    getStats: function() {
      return {
        initialized: replacerState.initialized,
        language: replacerState.currentLanguage,
        replacementCount: replacerState.replacementCount,
        lastReplacementTime: replacerState.lastReplacementTime ? new Date(replacerState.lastReplacementTime) : null
      };
    }
  };

  // Експортуємо модуль для централізованого менеджера
  if (typeof moduleExports !== 'undefined') {
    Object.assign(moduleExports, textReplacerModule);
  } else if (typeof window.moduleExports === 'undefined') {
    window.moduleExports = textReplacerModule;
  }

  // Якщо скрипт завантажується самостійно (не через менеджер)
  if (typeof window.ScriptManager === 'undefined') {
    log('Самостійний запуск модуля заміни тексту');
    // Додаємо CSS одразу
    addHidingCSS();
    initTextReplacer();
  } else {
    // Навіть якщо через менеджер - додаємо CSS якомога раніше
    addHidingCSS();
  }

  // Повертаємо модуль
  return textReplacerModule;

})();
