(function(){
  'use strict';

  // Детальне логування
  var DEBUG_MODE = true;
  var LOG_PREFIX = '[BONUS_CHECKOUT]';

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

  // Updated endpoints for renamed functions (with -prod suffix)
  // Updated with actual deployed endpoints
  var BONUS_BALANCE_API_URL = "https://s3qqv47ka3.execute-api.eu-north-1.amazonaws.com/prod/check-balance";
  var PROMO_API_URL = "https://o723fjnt6i.execute-api.eu-north-1.amazonaws.com/get-code";
  var CERTIFICATE_SELECTORS = ['.cart-discount-info', '.coupon__name', '.order-details__cost-name', '.order-details-h'];
  var PHONE_SELECTORS = ['.j-phone-masked', 'input[type="tel"]', 'input[name*="phone"]'];

  log('Скрипт ініціалізовано', {
    url: window.location.href,
    userAgent: navigator.userAgent,
    timestamp: new Date().toISOString()
  });

  var bonusState = {
    promoApplied: false,
    promoType: null,
    currentPhone: "",
    phoneValidated: false,
    balance: 0,
    isCheckingBalance: false,
    blockVisible: false,
    formVisible: false,
    inputValue: "",
    blockRemovedIntentionally: false,
    currentPageUrl: window.location.href,
    textReplaced: false,
    elementsDisabled: false,
    disabledOpacity: '1'
  };

  // Змінні для оптимізації збереження стану
  var saveStateTimeout = null;
  var lastSaveTime = 0;
  var stateChanged = false;

  function updateBonusState(updates) {
    log('Оновлення стану бонусів', { old: bonusState, updates: updates });
    Object.assign(bonusState, updates);
    stateChanged = true;
    // Відкладаємо збереження стану
    scheduleSaveState();
  }

  function resetBonusState() {
    log('Скидання стану бонусів', { currentState: bonusState });
    bonusState.promoApplied = false;
    bonusState.promoType = null;
    bonusState.currentPhone = "";
    bonusState.phoneValidated = false;
    bonusState.balance = 0;
    bonusState.isCheckingBalance = false;
    bonusState.blockVisible = false;
    bonusState.formVisible = false;
    bonusState.inputValue = "";
    bonusState.blockRemovedIntentionally = false;
    bonusState.elementsDisabled = false;
    bonusState.disabledOpacity = '1';
    stateChanged = true;
    scheduleSaveState();
    log('Стан бонусів скинуто', { newState: bonusState });
    
    // Переконуємося що поле телефону розблоковано після скидання
    setTimeout(function() {
      log('Додаткова перевірка розблокування телефону після скидання стану');
      enablePhoneInput();
    }, 100);
  }

  // Оптимізована функція збереження стану з throttling
  function saveBonusState() {
    // Перевіряємо чи стан дійсно змінився
    if (!stateChanged) return;
    
    var now = Date.now();
    // Обмежуємо частоту збереження до одного разу в 1000 мс
    if (now - lastSaveTime < 1000) {
      // Якщо ще не заплановано збереження, плануємо його
      if (!saveStateTimeout) {
        saveStateTimeout = setTimeout(function() {
          saveStateTimeout = null;
          saveBonusState();
        }, 1000 - (now - lastSaveTime));
      }
      return;
    }
    
    try {
      // Зберігаємо лише необхідні дані
      var stateToSave = {
        promoApplied: bonusState.promoApplied,
        promoType: bonusState.promoType,
        currentPhone: bonusState.currentPhone,
        phoneValidated: bonusState.phoneValidated,
        balance: bonusState.balance,
        isCheckingBalance: bonusState.isCheckingBalance,
        blockVisible: bonusState.blockVisible,
        formVisible: bonusState.formVisible,
        inputValue: bonusState.inputValue,
        blockRemovedIntentionally: bonusState.blockRemovedIntentionally,
        elementsDisabled: bonusState.elementsDisabled,
        disabledOpacity: bonusState.disabledOpacity
      };
      
      localStorage.setItem('bonusState', JSON.stringify(stateToSave));
      lastSaveTime = Date.now();
      stateChanged = false;
      log('Стан збережено в localStorage', stateToSave);
    } catch(e) {
      logError('Помилка збереження стану в localStorage', e);
    }
  }

  // Функція для планування збереження стану
  function scheduleSaveState() {
    // Скасовуємо попереднє планування, якщо є
    if (saveStateTimeout) {
      clearTimeout(saveStateTimeout);
    }
    
    // Плануємо збереження через 100 мс для групування швидких змін
    saveStateTimeout = setTimeout(function() {
      saveStateTimeout = null;
      saveBonusState();
    }, 100);
  }

  function loadBonusState() {
    try {
      var savedState = localStorage.getItem('bonusState');
      if (savedState) {
        var parsedState = JSON.parse(savedState);
        delete parsedState.currentPageUrl;
        Object.assign(bonusState, parsedState);
        log('Стан завантажено з localStorage', bonusState);
      } else {
        log('Збережений стан не знайдено в localStorage');
      }
    } catch (e) {
      logError('Помилка завантаження стану з localStorage', e);
    }
  }

  // Оптимізована функція debounce з можливістю скасуввання
  function debounce(func, wait) {
    var timeout;
    var debounced = function() {
      var context = this, args = arguments;
      var later = function() {
        timeout = null;
        func.apply(context, args);
      };
      clearTimeout(timeout);
      timeout = setTimeout(later, wait);
    };
    
    // Додаємо метод для скасуввання запланованого виклику
    debounced.cancel = function() {
      if (timeout) {
        clearTimeout(timeout);
        timeout = null;
      }
    };
    
    // Додаємо метод для перевірки, чи функція запланована
    debounced.isPending = function() {
      return timeout !== null;
    };
    
    return debounced;
  }

  function waitForElement(selector, timeout) {
    timeout = timeout || 5000;
    log('Очікування елемента: ' + selector + ' (timeout: ' + timeout + 'ms)');
    return new Promise(function(resolve, reject) {
      var element = document.querySelector(selector);
      if (element) {
        log('Елемент знайдено відразу: ' + selector, element);
        resolve(element);
        return;
      }
      var observer = new MutationObserver(function() {
        var element = document.querySelector(selector);
        if (element) {
          log('Елемент знайдено через observer: ' + selector, element);
          observer.disconnect();
          resolve(element);
        }
      });
      observer.observe(document.body, { childList: true, subtree: true });
      setTimeout(function() {
        observer.disconnect();
        var el = document.querySelector(selector);
        if (el) {
          log('Елемент знайдено в кінці timeout: ' + selector, el);
          resolve(el);
        } else {
          logError('Елемент не знайдено після timeout: ' + selector);
          reject(new Error('Element ' + selector + ' not found'));
        }
      }, timeout);
    });
  }

  function waitForElementWithCondition(selector, condition, timeout) {
    timeout = timeout || 5000;
    return new Promise(function(resolve, reject) {
      function checkElement() {
        var elements = document.querySelectorAll(selector);
        for (var i = 0; i < elements.length; i++) {
          if (condition(elements[i])) {
            resolve(elements[i]);
            return true;
          }
        }
        return false;
      }
      if (checkElement()) return;
      var observer = new MutationObserver(function() {
        if (checkElement()) observer.disconnect();
      });
      observer.observe(document.body, { childList: true, subtree: true });
      setTimeout(function() {
        observer.disconnect();
        reject(new Error('Element with condition not found'));
      }, timeout);
    });
  }

  function replaceTextInElement(element, oldText, newText) {
    if (!element) return false;
    var walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT, null, false);
    var textNodes = [];
    var node;
    while (node = walker.nextNode()) {
      if (node.nodeValue.includes(oldText)) {
        textNodes.push(node);
      }
    }
    var replaced = false;
    textNodes.forEach(function(textNode) {
      textNode.nodeValue = textNode.nodeValue.replace(new RegExp(oldText, 'g'), newText);
      replaced = true;
    });
    return replaced;
  }

  function instantTextReplace() {
    log('Спроба миттєвої заміни тексту');
    var replaced = false;
    var language = getCurrentLanguage();
    var oldText = language === 'ru' ? 'Подарочный сертификат' : 'Подарунковий сертифікат';
    var newText = language === 'ru' ? 'Бонусы' : 'Бонуси';
    
    CERTIFICATE_SELECTORS.forEach(function(selector) {
      var elements = document.querySelectorAll(selector);
      log('Знайдено елементів для селектора ' + selector + ':', elements.length);
      elements.forEach(function(el) {
        if (el.textContent && el.textContent.includes(oldText) && !el.closest('#bonus-block')) {
          log('Заміна тексту в елементі', { element: el, oldText: el.textContent, language: language });
          if (replaceTextInElement(el, oldText, newText)) {
            replaced = true;
            log('Текст успішно замінено', { element: el, newText: el.textContent });
          }
        }
      });
    });
    log('Результат миттєвої заміни тексту:', replaced);
    return replaced;
  }

  function isPromoCodeApplied() {
    // Оптимізована перевірка - виконуємо лише необхідні перевірки
    var removeButton = document.querySelector('.j-coupon-remove');
    if (!removeButton) {
        return false;
    }
    
    // Оптимізована перевірка видимості
    var style = window.getComputedStyle(removeButton);
    return style.display !== 'none' && style.visibility !== 'hidden' && removeButton.offsetWidth > 0 && removeButton.offsetHeight > 0;
  }

  function isBonusPromoCodeApplied() {
    // Оптимізована перевірка - виконуємо лише необхідні перевірки
    var promoApplied = isPromoCodeApplied();
    if (!promoApplied) {
        return false;
    }
    
    // Перевіряємо стан в bonusState
    if (bonusState.promoApplied && bonusState.promoType === 'bonus') {
        return true;
    }
    
    // Перевіряємо за текстом "Бонуси" в елементах
    var certificateElements = document.querySelectorAll(CERTIFICATE_SELECTORS.join(','));
    for (var i = 0; i < certificateElements.length; i++) {
      var el = certificateElements[i];
      if (el.textContent && el.textContent.includes('Бонуси') && !el.closest('#bonus-block')) {
        // Додатково оновлюємо стан, якщо він не був збережений
        if (!bonusState.promoApplied || bonusState.promoType !== 'bonus') {
          updateBonusState({ promoApplied: true, promoType: 'bonus' });
        }
        return true;
      }
    }
    
    return false;
  }

  function isRegularPromoCodeApplied() {
    // Оптимізована перевірка - виконуємо лише необхідні перевірки
    return isPromoCodeApplied() && !isBonusPromoCodeApplied();
  }

  function ensurePhoneLockState() {
    // Оптимізована перевірка - виконуємо лише необхідні перевірки
    var isBonus = isBonusPromoCodeApplied();
    if (isBonus) {
      disablePhoneInput();
      disableProductRemoval();
    } else {
      enablePhoneInput();
      enableProductRemoval();
    }
  }

  function disablePhoneInput() {
    log('Блокування поля телефону');
    var phoneInput = findPhoneInput();
    if (phoneInput) {
        if (!phoneInput.disabled) {
            phoneInput.disabled = true;
            log('Поле телефону заблоковано', phoneInput);
        } else {
            log('Поле телефону вже заблоковано');
        }
    } else {
        log('Поле телефону не знайдено для блокування');
    }
  }

  function disableProductRemoval() {
    log('Блокування кнопок видалення товарів');
    var removeButtons = document.querySelectorAll('.order-i-remove, .j-remove-p');
    removeButtons.forEach(function(button) {
      if (!button.hasAttribute('data-bonus-disabled')) {
        button.style.opacity = '0.3';
        button.style.pointerEvents = 'none';
        button.style.cursor = 'not-allowed';
        button.setAttribute('data-bonus-disabled', 'true');
        button.setAttribute('title', 'Неможливо видалити товар поки застосовані бонуси');
        log('Кнопка видалення товару заблокована', button);
      }
    });
  }

  function enableProductRemoval() {
    log('Розблокування кнопок видалення товарів');
    var removeButtons = document.querySelectorAll('.order-i-remove, .j-remove-p');
    removeButtons.forEach(function(button) {
      if (button.hasAttribute('data-bonus-disabled')) {
        button.style.opacity = '';
        button.style.pointerEvents = '';
        button.style.cursor = '';
        button.removeAttribute('data-bonus-disabled');
        button.removeAttribute('title');
        log('Кнопка видалення товару розблокована', button);
      }
    });
  }

  function enablePhoneInput() {
    log('Розблокування поля телефону');
    var phoneInput = findPhoneInput();
    if (phoneInput) {
        if (phoneInput.disabled) {
            phoneInput.disabled = false;
            log('Поле телефону розблоковано', phoneInput);
        } else {
            log('Поле телефону вже розблоковано');
        }
    } else {
        log('Поле телефону не знайдено для розблокування');
    }
    // Також розблоковуємо видалення товарів
    enableProductRemoval();
  }

  function checkBonusPromoWithRetry(maxAttempts, delay) {
    log('Початок перевірки бонусного промокоду з повторами', { maxAttempts: maxAttempts, delay: delay });
    function attemptCheck(attempt) {
      log('Спроба перевірки #' + attempt);
      if (isBonusPromoCodeApplied()) {
        log('Бонусний промокод знайдено - застосовуємо дії');
        instantTextReplace();
        ensurePhoneLockState();
        disableProductRemoval();
        return;
      }
      log('Бонусний промокод не знайдено на спробі #' + attempt);
      if (attempt < maxAttempts) {
        setTimeout(function() { attemptCheck(attempt + 1); }, delay);
      } else {
        log('Вичерпано всі спроби перевірки бонусного промокоду');
      }
    }
    attemptCheck(1);
  }

  function waitForPromoCodeRemoval(timeout) {
    timeout = timeout || 5000;
    return new Promise(function(resolve) {
      var interval = 100;
      var elapsedTime = 0;
      function check() {
        if (!isPromoCodeApplied()) {
          resolve();
        } else {
          elapsedTime += interval;
          if (elapsedTime < timeout) {
            setTimeout(check, interval);
          } else {
            resolve();
          }
        }
      }
      check();
    });
  }

  function removeAppliedPromoCode() {
    var removeButton = document.querySelector('.j-coupon-remove');
    if (removeButton && isPromoCodeApplied()) {
      removeButton.click();
      return waitForPromoCodeRemoval().then(function() {
        showPromoCode();
        return Promise.resolve();
      });
    }
    return Promise.resolve();
  }

  function setupPromoCodeRemovalListener() {
    waitForElement('.j-coupon-remove', 5000).then(function(removeButton) {
      // Оптимізована перевірка - виконуємо лише необхідні перевірки
      if (removeButton.hasAttribute('data-bonus-listener')) return;
      removeButton.setAttribute('data-bonus-listener', 'true');
      removeButton.addEventListener('click', function() {
        var wasBonusPromo = isBonusPromoCodeApplied();
        waitForPromoCodeRemoval().then(function() {
          if (wasBonusPromo) {
            enablePhoneInput();
            enableProductRemoval();
            resetBonusState();
            initializeCheckoutPage();
          }
        });
      });
    }).catch(function() {
      // Ігноруємо помилки, бо це не критично
    });
  }

  function hidePromoCode() {
    var block = document.querySelector('.order-details__block--discount:not(#bonus-block)') || document.querySelector('.order-details-i:not(#bonus-block)');
    if (block) {
      block.style.display = 'none';
      block.style.visibility = 'hidden';
    }
  }

  function showPromoCode() {
    var block = document.querySelector('.order-details__block--discount:not(#bonus-block)') || document.querySelector('.order-details-i:not(#bonus-block)');
    if (block) {
      block.style.display = '';
      block.style.visibility = '';
    }
  }

  function isMobileDevice() {
    return !!document.querySelector('.order-details__block--discount') || !!document.querySelector('.order-details__cost') || window.innerWidth <= 768;
  }

  function findBonusInsertContainer() {
    var containers = [
      { el: document.querySelector('.order-details__block--discount:not(#bonus-block)'), type: 'mobile' },
      { el: document.querySelector('.order-details__cost'), type: 'mobile', insertMode: 'before' },
      { el: document.querySelector('.order-details-i:not(#bonus-block)'), type: 'desktop' },
      { el: document.querySelector('.order-details'), type: 'fallback', insertMode: 'prepend' }
    ];
    for (var i = 0; i < containers.length; i++) {
      if (containers[i].el) {
        return containers[i];
      }
    }
    return null;
  }

  function getOrderTotal() {
    var totalElement = document.querySelector('.j-total-sum');
    if (totalElement) {
      var text = totalElement.textContent || '';
      var amount = parseFloat(text.replace(/[^\d]/g, ''));
      if (!isNaN(amount) && amount > 0) {
        return amount;
      }
    }
    return 1000;
  }

  function getMaxAllowedBonusAmount(bonusBalance) {
    if (bonusBalance <= 0) return 1;
    var orderTotal = getOrderTotal();
    var maxFromOrder = Math.floor(orderTotal * 0.5);
    return Math.min(bonusBalance, maxFromOrder);
  }

  function createBonusBlock(container, bonusBalance, insertMode, containerType, showPrompt) {
    if (document.querySelector('#bonus-block') || bonusState.blockRemovedIntentionally) {
      log('Блок бонусів не створено', { 
        exists: !!document.querySelector('#bonus-block'), 
        intentionallyRemoved: bonusState.blockRemovedIntentionally 
      });
      return;
    }
    
    log('Створення блоку бонусів', { 
      container: container, 
      bonusBalance: bonusBalance, 
      insertMode: insertMode, 
      containerType: containerType,
      showPrompt: showPrompt
    });
    
    var isMobile = containerType === 'mobile' || isMobileDevice();
    log('Тип пристрою:', { isMobile: isMobile, containerType: containerType });
    
    var bonusBlock = document.createElement("div");
    bonusBlock.id = "bonus-block";
    bonusBlock.style.display = ''; // Показуємо блок одразу
    if (isMobile) {
      bonusBlock.className = "order-details__block order-details__block--discount";
      bonusBlock.innerHTML = createMobileBonusHTML(bonusBalance, showPrompt);
    } else {
      bonusBlock.className = "order-details-i";
      bonusBlock.innerHTML = createDesktopBonusHTML(bonusBalance, showPrompt);
    }
    insertBonusBlock(bonusBlock, container, insertMode);
    log('Блок бонусів створено і вставлено');
    
    // Якщо показуємо підказку про введення номера, блокуємо взаємодію
    if (showPrompt) {
      toggleBonusElements(true, '0.5');
      updateBonusState({ blockVisible: true }); // Позначаємо що блок видимий
    } else {
      // Якщо не показуємо підказку, це означає що ми в процесі перевірки
      // Тому блокуємо елементи поки не отримаємо результат
      toggleBonusElements(true, '0.5');
      updateBonusState({ blockVisible: true });
      attachBonusLogic();
    }
    
    restoreBonusBlockState();
  }

  function restoreBonusBlockState() {
    setTimeout(function() {
      var bonusBlock = document.querySelector('#bonus-block');
      if (!bonusBlock) {
        return;
      }
      var phoneInput = findPhoneInput();
      
      // Блок завжди показуємо, але з різними станами залежно від валідності телефону
      var hasValidPhone = phoneInput && isValidPhone(phoneInput.value);
      var shouldShowBlock = !bonusState.blockRemovedIntentionally;
      
      updateBonusState({ blockVisible: shouldShowBlock });
      bonusBlock.style.display = shouldShowBlock ? '' : 'none';
      
      if (shouldShowBlock) {
        var bonusForm = document.querySelector('#bonus-block #bonus-form');
        var bonusLink = document.querySelector('#bonus-block #bonus-link');
        var bonusInput = document.querySelector('#bonus-block #bonus-amount');
        
        if (bonusState.formVisible && bonusForm && bonusLink) {
          bonusLink.parentElement.style.display = "none";
          bonusForm.style.display = isMobileDevice() ? "flex" : "block";
        }
        if (bonusState.inputValue && bonusInput) {
          bonusInput.value = bonusState.inputValue;
        }

        var bonusLinkText = document.querySelector('#bonus-block #bonus-link-text');
        if (bonusLinkText) {
          if (!hasValidPhone) {
            // Якщо номер невалідний, показуємо підказку про введення номера
            var language = getCurrentLanguage();
            bonusLinkText.innerHTML = getLocalizedText('enter_phone', language);
            toggleBonusElements(true, '0.5');
          } else if (bonusState.balance > 0) {
            updateBonusBlock(bonusState.balance);
          } else if (bonusState.balance === 0 && bonusState.phoneValidated && !bonusState.isCheckingBalance) {
            // Якщо баланс 0 і перевірка завершена, оновлюємо текст відповідно
            bonusLinkText.innerHTML = getLocalizedText('no_bonuses', getCurrentLanguage());
            toggleBonusElements(true, '0.5');
          } else if (bonusState.isCheckingBalance) {
            bonusLinkText.innerHTML = getLocalizedText('checking_bonuses', getCurrentLanguage());
            toggleBonusElements(true, '0.5');
          }
          
          var bonusButton = document.querySelector('#bonus-block #apply-bonus');
          if (bonusButton) {
            bonusButton.disabled = false;
            var btnContent = bonusButton.querySelector('.btn-content') || bonusButton.querySelector('span');
            if (btnContent && btnContent.textContent !== "OK") {
              btnContent.textContent = "OK";
            }
          }
        }
        
        // Відновлюємо стан затемнення елементів
        restoreBonusElementsState();
        
        // Додаємо логіку тільки якщо телефон валідний
        if (hasValidPhone) {
          attachBonusLogic();
        }
      }
    }, 100);
  }

  function getLocalizedText(key, language, amount = -1) {
    var translations = {
      'enter_phone': {
        'uk': '🎁 Введіть номер телефону, щоб перевірити бонуси',
        'ru': '🎁 Введите номер телефона, чтобы проверить бонусы'
      },
      'checking_bonuses': {
        'uk': '🎁 Перевіряємо бонуси...',
        'ru': '🎁 Проверяем бонусы...'
      },
      'bonuses_available': {
        'uk': '🎁 У вас є <span style="color: #28a745; font-weight: bold;">' + amount + '</span> бонусів. Хочете використати?',
        'ru': '🎁 У вас есть <span style="color: #28a745; font-weight: bold;">' + amount + '</span> бонусов. Хотите использовать?'
      },
      'bonuses_unavailable': {
        'uk': '🎁 Бонуси недоступні',
        'ru': '🎁 Бонусы недоступны'
      },
      'no_bonuses': {
        'uk': '🎁 На жаль, у вас немає бонусів',
        'ru': '🎁 К сожалению, у вас нет бонусов'
      },
      'bonus_amount_placeholder': {
        'uk': 'Сума бонусів',
        'ru': 'Сумма бонусов'
      },
      'bonuses_available_placeholder': {
        'uk': amount,
        'ru': amount
      },
      'bonuses_unavailable_placeholder': {
        'uk': 'Бонуси недоступні',
        'ru': 'Бонусы недоступны'
      },
      'invalid_amount': {
        'uk': 'Введіть коректну суму бонусів',
        'ru': 'Введите корректную сумму бонусов'
      },
      'insufficient_bonuses': {
        'uk': 'У вас недостатньо бонусів. Доступно: ' + amount,
        'ru': 'У вас недостаточно бонусов. Доступно: ' + amount
      },
      'max_bonus_limit': {
        'uk': 'Максимально можна використати ' + amount + ' бонусів (50% від суми замовлення)',
        'ru': 'Максимально можно использовать ' + amount + ' бонусов (50% от суммы заказа)'
      },
      'applying_bonuses': {
        'uk': 'Застосовуємо...',
        'ru': 'Применяем...'
      },
      'error_apply_promo': {
        'uk': 'Не вдалося автоматично застосувати промокод.',
        'ru': 'Не удалось автоматически применить промокод.'
      },
      'error_open_form': {
        'uk': 'Не вдалося відкрити форму для вводу промокоду.',
        'ru': 'Не удалось открыть форму для ввода промокода.'
      },
      'max_label': {
        'uk': 'Макс: ',
        'ru': 'Макс: '
      }
    };
    
    return translations[key] && translations[key][language] ? translations[key][language] : translations[key]['uk'];
  }
  
  function getCurrentLanguage() {
    var url = window.location.href;
    return url.includes('/ru/') ? 'ru' : 'uk';
  }
  
  function createMobileBonusHTML(bonusBalance, showPrompt) {
    var language = getCurrentLanguage();
    var text;
    if (showPrompt) {
      text = getLocalizedText('enter_phone', language);
    } else {
      if (bonusBalance > 0) {
        text = getLocalizedText('bonuses_available', language, bonusBalance);
      } else if (bonusBalance === 0 && bonusState.phoneValidated && !bonusState.isCheckingBalance) {
        text = getLocalizedText('no_bonuses', language);
      } else {
        text = getLocalizedText('checking_bonuses', language);
      }
    }
    
    var maxAllowed = getMaxAllowedBonusAmount(bonusBalance);
    var placeholder = bonusBalance > 0 ? 
      getLocalizedText('max_label', language) + maxAllowed : 
      getLocalizedText('bonuses_unavailable_placeholder', language);
    
    return '<div class="coupon bonus-coupon"><div class="coupon__toggle j-bonus-add-container bonus-add-container"><button class="btn btn--small btn--clear btn--block j-bonus-add-mobile bonus-add-mobile" id="bonus-link" data-bonus-button="true"><span id="bonus-link-text">' + text + '</span></button></div><div class="form-item form-item--toolbar j-bonus-add-form bonus-add-form" id="bonus-form" style="display: none;" data-bonus-form="true"><div class="form-item__content"><input class="input input--small input--clear j-bonus-input bonus-input" id="bonus-amount" type="number" placeholder="' + placeholder + '" min="1" max="' + maxAllowed + '" data-bonus-input="true"></div><div class="form-item__clear"><button class="btn btn--small btn--clear btn--icon j-bonus-cancel bonus-cancel" id="bonus-cancel" data-bonus-button="true"><span class="btn__icon"><svg class="icon icon--size-s"><use xlink:href="#icon-cross"></use></svg></span></button></div><div class="form-item__button"><button class="btn btn--small btn--primary j-bonus-submit bonus-submit" id="apply-bonus" data-bonus-button="true">OK</button></div></div><div id="bonus-error" style="color: #e74c3c; font-size: 12px; margin-top: 5px; display: none; width: 100%; flex-basis: 100%;"></div></div>';
  }

  function createDesktopBonusHTML(bonusBalance, showPrompt) {
    var language = getCurrentLanguage();
    var text;
    if (showPrompt) {
      text = getLocalizedText('enter_phone', language);
    } else {
      if (bonusBalance > 0) {
        text = getLocalizedText('bonuses_available', language, bonusBalance);
      } else if (bonusBalance === 0 && bonusState.phoneValidated && !bonusState.isCheckingBalance) {
        text = getLocalizedText('no_bonuses', language);
      } else {
        text = getLocalizedText('checking_bonuses', language);
      }
    }
    
    var maxAllowed = getMaxAllowedBonusAmount(bonusBalance);
    var placeholder = bonusBalance > 0 ? 
      getLocalizedText('max_label', language) + maxAllowed : 
      getLocalizedText('bonuses_unavailable_placeholder', language);
    
    return '<div class="order-details-h bonus-details-h"><div class="cart-discount-info j-bonus-add-container bonus-add-container"><a class="link link--pseudo j-bonus-add bonus-add" href="#" id="bonus-link" data-bonus-button="true"><span class="link__text" id="bonus-link-text">' + text + '</span></a></div><div class="cart-discount-coupon j-bonus-add-form bonus-add-form" id="bonus-form" style="display: none;" data-bonus-form="true"><div class="coupon bonus-coupon"><div class="coupon-input"><input class="coupon-field field j-bonus-input bonus-input" id="bonus-amount" placeholder="' + placeholder + '" type="number" min="1" max="' + maxAllowed + '" data-bonus-input="true"><div class="coupon-cancel j-bonus-cancel bonus-cancel" id="bonus-cancel" data-bonus-button="true"><svg class="icon icon--cross-bold"><use xlink:href="#icon-cross-bold"></use></svg></div></div><div class="coupon-submit"><a class="button add controls_simple btn __small j-bonus-submit bonus-submit" href="#" id="apply-bonus" data-bonus-button="true"><span class="btn-content">OK</span></a></div></div><div id="bonus-error" style="color: #e74c3c; font-size: 12px; margin-top: 5px; display: none;"></div></div></div>';
  }

  function insertBonusBlock(bonusBlock, container, insertMode) {
    switch (insertMode) {
      case 'after': container.parentNode.insertBefore(bonusBlock, container.nextSibling); break;
      case 'before': container.parentNode.insertBefore(bonusBlock, container); break;
      case 'prepend': container.insertBefore(bonusBlock, container.firstChild); break;
      default: container.parentNode.insertBefore(bonusBlock, container.nextSibling);
    }
  }

  function checkBonusBalance(phone) {
    var cleanPhone = phone.replace(/[\s\-\(\)]/g, '');
    log('Перевірка балансу бонусів', { phone: phone, cleanPhone: cleanPhone });
    // isCheckingBalance вже встановлено в handlePhoneChange
    var apiUrl = BONUS_BALANCE_API_URL + "?phone=" + encodeURIComponent(phone);
    log('API запит на перевірку балансу:', apiUrl);
    
    return fetch(apiUrl)
      .then(function(response) { 
        log('Відповідь API:', { status: response.status, ok: response.ok });
        return response.ok ? response.json() : null; 
      })
      .then(function(data) {
        log('Дані від API:', data);
        var balance = (data && data.success && data.bonus_balance !== undefined) ? data.bonus_balance : 0;
        log('Отримано баланс бонусів:', balance);
        updateBonusState({ isCheckingBalance: false, balance: balance, currentPhone: cleanPhone, phoneValidated: true });
        log('Стан оновлено після отримання балансу:', { balance: bonusState.balance, phone: bonusState.currentPhone });
        return balance;
      })
      .catch(function(error) {
        logError('Помилка перевірки балансу бонусів', error);
        updateBonusState({ isCheckingBalance: false });
        return 0;
      });
  }

  function findPhoneInput() {
    log('Пошук поля телефону');
    for (var i = 0; i < PHONE_SELECTORS.length; i++) {
      var input = document.querySelector(PHONE_SELECTORS[i]);
      if (input) {
        log('Знайдено поле телефону:', { selector: PHONE_SELECTORS[i], input: input });
        return input;
      }
    }
    log('Поле телефону не знайдено');
    return null;
  }

  function showBonusBlock() {
    var bonusBlock = document.querySelector('#bonus-block');
    if (bonusBlock) {
      bonusBlock.style.display = '';
      updateBonusState({ blockVisible: true });
    }
  }

  function hideBonusBlock() {
    var bonusBlock = document.querySelector('#bonus-block');
    if (bonusBlock) {
      bonusBlock.style.display = 'none';
      updateBonusState({ blockVisible: false });
    }
  }

  function showBonusBlockWithLoading() {
    var bonusBlock = document.querySelector('#bonus-block');
    if (bonusBlock) {
      bonusBlock.style.display = '';
      updateBonusState({ blockVisible: true });
      var bonusLinkText = document.querySelector('#bonus-block #bonus-link-text');
      if (bonusLinkText) {
        var language = getCurrentLanguage();
        var checkingText = getLocalizedText('checking_bonuses', language);
        log('Встановлення тексту "Перевіряємо бонуси..."', { 
          oldText: bonusLinkText.innerHTML, 
          newText: checkingText 
        });
        bonusLinkText.innerHTML = checkingText;
        // Блокуємо елементи під час перевірки
        toggleBonusElements(true, '0.5');
      }
    }
  }

  function showBonusBlockWithPrompt() {
    var bonusBlock = document.querySelector('#bonus-block');
    if (bonusBlock) {
      bonusBlock.style.display = '';
      updateBonusState({ blockVisible: true });
      var bonusLinkText = document.querySelector('#bonus-block #bonus-link-text');
      if (bonusLinkText) {
        var language = getCurrentLanguage();
        bonusLinkText.innerHTML = getLocalizedText('enter_phone', language);
      }
      // Блокуємо взаємодію до введення номера
      toggleBonusElements(true, '0.5');
      log('Показано блок з підказкою про введення номера');
    }
  }

  function isValidPhone(phone) {
    if (!phone) return false;
    var cleanPhone = phone.replace(/[\s\-\(\)]/g, '');
    return phone.indexOf('_') === -1 && cleanPhone.length >= 10;
  }

  function resetBonusStateForNewPhone() {
    resetBonusState();
    var bonusAmount = document.querySelector('#bonus-block #bonus-amount');
    if (bonusAmount) {
      bonusAmount.value = "";
    }
  }

  function handlePhoneChange(telInput, force) {
    force = force || false;
    var phone = telInput.value.trim();
    var cleanPhone = phone.replace(/[\s\-\(\)]/g, '');

    // Оптимізована перевірка - виконуємо лише необхідні перевірки
    var isBonusApplied = isBonusPromoCodeApplied();
    var isChecking = bonusState.isCheckingBalance;
    
    log('Обробка зміни телефону', { 
      phone: phone, 
      cleanPhone: cleanPhone, 
      force: force,
      currentPhone: bonusState.currentPhone,
      isBonusApplied: isBonusApplied,
      isChecking: isChecking
    });

    if (isBonusApplied || isChecking) {
      log('Пропуск обробки: бонусний промокод застосовано або йде перевірка');
      return;
    }

    var isRegularPromo = isRegularPromoCodeApplied();
    if (isRegularPromo) {
      log('Звичайний промокод застосовано - приховуємо блок бонусів');
      hideBonusBlock();
      return;
    }

    if (isValidPhone(phone)) {
      log('Телефон валідний');
      // Якщо це перший валідний телефон, вмикаємо логіку
      var bonusBlock = document.querySelector('#bonus-block');
      if (bonusBlock && !bonusBlock.querySelector('#bonus-link').hasAttribute('data-bonus-handlers-attached')) {
        enableBonusElements();
        attachBonusLogic();
      }
      
      var phoneChanged = cleanPhone !== bonusState.currentPhone;
      var notValidated = !bonusState.phoneValidated;
      
      if (force || phoneChanged || notValidated) {
        log('Оновлення телефону, примусова перевірка або номер не валідований', {
          force: force,
          phoneChanged: phoneChanged,
          notValidated: notValidated,
          currentPhone: bonusState.currentPhone,
          newPhone: cleanPhone
        });
        resetBonusState();
        log('Стан після resetBonusState:', { 
          isCheckingBalance: bonusState.isCheckingBalance, 
          phoneValidated: bonusState.phoneValidated 
        });
        updateBonusState({ currentPhone: cleanPhone, isCheckingBalance: true });
        log('Стан після updateBonusState:', { 
          isCheckingBalance: bonusState.isCheckingBalance, 
          phoneValidated: bonusState.phoneValidated,
          currentPhone: bonusState.currentPhone
        });
        showBonusBlockWithLoading();
        checkBonusBalance(phone).then(function(bonusBalance) {
          var currentPhone = findPhoneInput();
          var currentCleanPhone = currentPhone ? currentPhone.value.trim().replace(/[\s\-\(\)]/g, '') : '';
          if (currentCleanPhone === cleanPhone) {
            log('Телефон не змінився під час перевірки - оновлюємо блок з балансом:', bonusBalance);
            updateBonusBlock(bonusBalance);
          } else {
            log('Телефон змінився під час перевірки - ігноруємо результат');
          }
        });
      } else {
        log('Телефон не змінився - показуємо існуючий баланс');
        // Якщо телефон не змінився, але у нас є збережений баланс, показуємо його
        if (bonusState.balance !== undefined) {
          updateBonusBlock(bonusState.balance);
        }
      }
    } else {
      log('Телефон невалідний - показуємо блок з підказкою');
      showBonusBlockWithPrompt();
      resetBonusState();
      // Додаткова перевірка через короткий час для відновлення стану
      setTimeout(function() {
        var bonusBlock = document.querySelector('#bonus-block');
        var currentPhoneInput = findPhoneInput();
        if (bonusBlock && (!currentPhoneInput || !isValidPhone(currentPhoneInput.value))) {
          log('Відновлення затемнення для невалідного телефону');
          var bonusLinkText = document.querySelector('#bonus-block #bonus-link-text');
          if (bonusLinkText) {
            var language = getCurrentLanguage();
            bonusLinkText.innerHTML = getLocalizedText('enter_phone', language);
          }
          toggleBonusElements(true, '0.5');
        }
      }, 100);
    }
  }

  function addPhoneEventListeners(telInput) {
    if (!telInput || telInput.hasAttribute('data-bonus-listeners')) {
      log('Слухачі вже додано або немає поля телефону');
      return;
    }
    log('Додавання слухачів подій до поля телефону', telInput);
    telInput.setAttribute('data-bonus-listeners', 'true');
    var debouncedHandler = debounce(function() { handlePhoneChange(telInput, false); }, 300);
    telInput.addEventListener('input', debouncedHandler);
    telInput.addEventListener('blur', function() { handlePhoneChange(telInput, false); });
    log('Слухачі подій додано');
  }

  function updateBonusBlock(bonusBalance) {
    // Оптимізована перевірка - виконуємо лише необхідні перевірки
    var isBonusApplied = isBonusPromoCodeApplied();
    if (isBonusApplied) {
      log('Пропуск оновлення блоку - бонусний промокод застосовано');
      return;
    }
    
    log('Оновлення блоку бонусів з балансом:', bonusBalance);
    log('Поточний стан:', { 
      isCheckingBalance: bonusState.isCheckingBalance, 
      phoneValidated: bonusState.phoneValidated,
      balance: bonusState.balance
    });
    
    showBonusBlock();
    var bonusLinkText = document.querySelector('#bonus-block #bonus-link-text');
    if (bonusLinkText) {
      var language = getCurrentLanguage();
      var newText;
      
      // Спочатку перевіряємо чи йде активна перевірка
      if (bonusState.isCheckingBalance) {
        newText = getLocalizedText('checking_bonuses', language);
      } else if (bonusState.phoneValidated) {
        // Показуємо фінальний результат тільки після завершення перевірки
        if (bonusBalance > 0) {
          newText = getLocalizedText('bonuses_available', language, bonusBalance);
        } else {
          newText = getLocalizedText('no_bonuses', language);
        }
      } else {
        // Якщо телефон ще не валідований, показуємо текст перевірки
        newText = getLocalizedText('checking_bonuses', language);
      }
      
      log('Оновлення тексту в блоці бонусів', { 
        element: bonusLinkText,
        oldText: bonusLinkText.innerText, 
        newText: newText,
        balance: bonusBalance,
        isCheckingBalance: bonusState.isCheckingBalance,
        phoneValidated: bonusState.phoneValidated
      });
      
      // Оновлюємо текст лише якщо він змінився
      if (bonusLinkText.innerHTML !== newText) {
        bonusLinkText.innerHTML = newText;
      }
    } else {
      logError('Елемент #bonus-link-text не знайдено для оновлення тексту');
    }
    
    var bonusAmount = document.querySelector('#bonus-block #bonus-amount');
    if (bonusAmount) {
      var maxAllowed = getMaxAllowedBonusAmount(bonusBalance);
      var language = getCurrentLanguage();
      var newPlaceholder = bonusBalance > 0 ? 
        getLocalizedText('max_label', language) + maxAllowed : 
        getLocalizedText('bonuses_unavailable_placeholder', language);
      var newDisabled = bonusBalance === 0;
      
      // Оновлюємо атрибути лише якщо вони змінилися
      if (bonusAmount.max !== maxAllowed) {
        bonusAmount.max = maxAllowed;
      }
      if (bonusAmount.placeholder !== newPlaceholder) {
        bonusAmount.placeholder = newPlaceholder;
      }
      if (bonusAmount.disabled !== newDisabled) {
        bonusAmount.disabled = newDisabled;
      }
    }
    
    // Оптимізована перевірка для блокування елементів
    if (bonusBalance === 0 && bonusState.phoneValidated && !bonusState.isCheckingBalance) {
      // Блокуємо елементи тільки якщо перевірка завершена і дійсно немає бонусів
      showBonusBlock();
      toggleBonusElements(true, '0.5');
      // Переконуємося що затемнення зберігається після AJAX оновлень
      setTimeout(function() {
        if (bonusState.balance === 0 && bonusState.phoneValidated) {
          log('Відновлення затемнення для 0 бонусів після AJAX');
          toggleBonusElements(true, '0.5');
        }
      }, 200);
    } else if (bonusBalance > 0) {
      enableBonusElements();
    }
  }

  function toggleBonusElements(disabled, opacity) {
    log('Зміна стану елементів бонусів', { disabled: disabled, opacity: opacity });
    
    // Зберігаємо стан в bonusState
    updateBonusState({ 
      elementsDisabled: disabled, 
      disabledOpacity: opacity 
    });
    
    var elements = [
      document.querySelector('#bonus-block #bonus-link'),
      document.querySelector('#bonus-block #bonus-amount'),
      document.querySelector('#bonus-block #apply-bonus')
    ];
    elements.forEach(function(el) {
      if (el) {
        el.disabled = disabled;
        el.style.opacity = opacity;
        el.style.pointerEvents = disabled ? 'none' : 'auto';
        // Додаємо data-атрибут для відновлення стану
        el.setAttribute('data-bonus-disabled', disabled);
        el.setAttribute('data-bonus-opacity', opacity);
      }
    });
  }

  function disableBonusElements() {
    toggleBonusElements(true, '0.5');
    var bonusAmount = document.querySelector('#bonus-block #bonus-amount');
    if (bonusAmount) bonusAmount.value = "";
  }

  function enableBonusElements() {
    log('Увімкнення елементів бонусів');
    toggleBonusElements(false, '1');
    var bonusButton = document.querySelector('#bonus-block #apply-bonus');
    if (bonusButton) {
      var btnContent = bonusButton.querySelector('.btn-content') || bonusButton.querySelector('span');
      if (btnContent && btnContent.textContent !== "OK") {
        btnContent.textContent = "OK";
      }
    }
    
    // Переконуємося що текст відповідає поточному балансу (тільки якщо не йде перевірка)
    var bonusLinkText = document.querySelector('#bonus-block #bonus-link-text');
    if (bonusLinkText && bonusState.balance !== undefined && !bonusState.isCheckingBalance && bonusState.phoneValidated) {
      var language = getCurrentLanguage();
      var correctText = bonusState.balance > 0 ? 
        getLocalizedText('bonuses_available', language, bonusState.balance) : 
        getLocalizedText('no_bonuses', language);
      if (bonusLinkText.innerHTML !== correctText) {
        log('Виправлення тексту при включенні елементів', { 
          current: bonusLinkText.innerHTML, 
          correct: correctText, 
          balance: bonusState.balance 
        });
        bonusLinkText.innerHTML = correctText;
      }
    }
  }

  function restoreBonusElementsState() {
    log('Відновлення стану елементів бонусів після AJAX', { 
      elementsDisabled: bonusState.elementsDisabled, 
      disabledOpacity: bonusState.disabledOpacity 
    });
    
    var bonusBlock = document.querySelector('#bonus-block');
    if (!bonusBlock) return;
    
    var elements = [
      bonusBlock.querySelector('#bonus-link'),
      bonusBlock.querySelector('#bonus-amount'),
      bonusBlock.querySelector('#apply-bonus')
    ];
    
    elements.forEach(function(el) {
      if (el) {
        var shouldBeDisabled = bonusState.elementsDisabled;
        var targetOpacity = bonusState.disabledOpacity;
        
        // Відновлюємо стан тільки якщо він змінився
        if (el.disabled !== shouldBeDisabled || el.style.opacity !== targetOpacity) {
          log('Відновлення стану елемента', { 
            element: el.id, 
            wasDisabled: el.disabled, 
            shouldBeDisabled: shouldBeDisabled,
            wasOpacity: el.style.opacity,
            shouldBeOpacity: targetOpacity
          });
          
          el.disabled = shouldBeDisabled;
          el.style.opacity = targetOpacity;
          el.style.pointerEvents = shouldBeDisabled ? 'none' : 'auto';
          el.setAttribute('data-bonus-disabled', shouldBeDisabled);
          el.setAttribute('data-bonus-opacity', targetOpacity);
        }
      }
    });
  }

  function removeBonusBlock() {
    updateBonusState({ blockRemovedIntentionally: true });
    var bonusBlock = document.querySelector('#bonus-block');
    if (bonusBlock && bonusBlock.parentNode) {
      bonusBlock.parentNode.removeChild(bonusBlock);
    }
  }

  function attachBonusLogic() {
    var bonusBlock = document.querySelector('#bonus-block');
    if (!bonusBlock) return;
    
    var bonusLink = bonusBlock.querySelector('#bonus-link');
    var bonusForm = bonusBlock.querySelector('#bonus-form');
    var input = bonusBlock.querySelector('#bonus-amount');
    var button = bonusBlock.querySelector('#apply-bonus');
    
    // Оптимізована перевірка - виконуємо лише необхідні перевірки
    if (!bonusLink || !bonusForm || !input || !button) {
        return;
    }

    // Перевіряємо чи вже додані обробники
    if (bonusLink.hasAttribute('data-bonus-handlers-attached')) {
        return;
    }
    bonusLink.setAttribute('data-bonus-handlers-attached', 'true');

    var isMobile = isMobileDevice();
    bonusLink.addEventListener("click", function(e) {
      e.preventDefault(); e.stopPropagation();
      bonusLink.parentElement.style.display = "none";
      bonusForm.style.display = isMobile ? "flex" : "block";
      input.focus();
      updateBonusState({ formVisible: true });
    });

    var cancelBtn = bonusBlock.querySelector('#bonus-cancel');
    if (cancelBtn) {
      cancelBtn.addEventListener("click", function(e) {
        e.preventDefault(); e.stopPropagation();
        bonusForm.style.display = "none";
        bonusLink.parentElement.style.display = "block";
        input.value = "";
        hideError();
        updateBonusState({ formVisible: false, inputValue: "" });
      });
    }

    input.addEventListener('input', function() { updateBonusState({ inputValue: input.value }); });

    var errorDiv = bonusBlock.querySelector('#bonus-error');
    function showError(message) {
      if (errorDiv) {
        errorDiv.textContent = message;
        errorDiv.style.display = "block";
      }
    }
    function hideError() {
      if (errorDiv) errorDiv.style.display = "none";
    }

    function validateBonusAmount(amount) {
      // Оптимізована перевірка - виконуємо лише необхідні перевірки
      var language = getCurrentLanguage();
      if (isNaN(amount) || amount <= 0) return getLocalizedText('invalid_amount', language);
      if (amount > bonusState.balance) return getLocalizedText('insufficient_bonuses', language, bonusState.balance);
      var orderTotal = getOrderTotal();
      var maxBonusAmount = Math.floor(orderTotal * 0.5);
      if (amount > maxBonusAmount) return getLocalizedText('max_bonus_limit', language, maxBonusAmount);
      return null;
    }

    button.addEventListener("click", function(e) {
      e.preventDefault(); e.stopPropagation();
      hideError();
      var amount = parseInt(input.value, 10);
      var validationError = validateBonusAmount(amount);
      if (validationError) {
        showError(validationError);
        return;
      }
      button.disabled = true;
      var btnContent = button.querySelector('.btn-content') || button.querySelector('span');
      var language = getCurrentLanguage();
      if (btnContent) btnContent.textContent = getLocalizedText('applying_bonuses', language);
      updateBonusState({ promoApplied: true, promoType: 'bonus', formVisible: false, inputValue: "" });
      removeBonusBlock();
      hidePromoCode();
      requestPromoCode(amount)
        .then(applyPromoCodeToForm)
        .catch(function(errorMessage) {
          restoreAfterError(errorMessage);
        });
    });
  }

  function requestPromoCode(amount) {
    log('Запит промокоду', { amount: amount, url: PROMO_API_URL });
    return fetch(PROMO_API_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json;charset=UTF-8' },
      body: JSON.stringify({ amount: amount })
    })
    .then(function(response) {
      log('Відповідь сервера промокодів', { status: response.status, ok: response.ok });
      if (response.ok) return response.json();
      return Promise.reject("Помилка з'єднання з сервером");
    })
    .then(function(data) {
      log('Дані від сервера промокодів', data);
      if (data.success && data.promo_code) {
        log('Промокод отримано успішно:', data.promo_code);
        return data.promo_code;
      }
      logError('Помилка отримання промокоду', data);
      return Promise.reject(data.message || "Помилка застосування бонусів");
    })
    .catch(function(error) {
      logError('Помилка запиту промокоду', error);
      return Promise.reject(typeof error === 'string' ? error : "Помилка мережі");
    });
  }

  function waitForCertificateTextAndReplace() {
    var language = getCurrentLanguage();
    var certificateText = language === 'ru' ? 'Подарочный сертификат' : 'Подарунковий сертифікат';
    
    waitForElementWithCondition(
      CERTIFICATE_SELECTORS.join(','), 
      function(el) { return el.textContent && el.textContent.includes(certificateText); },
      3000
    ).then(instantTextReplace).catch(function() {
    });
  }

  function applyPromoCodeToForm(promoCode) {
    log('Застосування промокоду до форми', { promoCode: promoCode });
    
    function fillAndSubmit(promoInput) {
      log('Заповнення та відправлення промокоду');
      var promoButton = isMobileDevice() ? 
        document.querySelector('button.j-coupon-submit:not(#bonus-block button.j-coupon-submit)') : 
        document.querySelector('a.j-coupon-submit:not(#bonus-block a.j-coupon-submit)');
      
      log('Елементи форми промокоду', { promoInput: promoInput, promoButton: promoButton });
      
      if (promoInput && promoButton) {
        promoInput.value = promoCode;
        promoInput.dispatchEvent(new Event('input', { bubbles: true }));
        promoInput.dispatchEvent(new Event('change', { bubbles: true }));
        log('Промокод введено, натискаємо кнопку');
        promoButton.click();
        setTimeout(function() {
          log('Налаштування слухача видалення промокоду');
          setupPromoCodeRemovalListener();
          waitForCertificateTextAndReplace();
          checkBonusPromoWithRetry(5, 500);
          
          // Додаткова перевірка через більший час для надійності
          setTimeout(function() {
            log('Додаткова перевірка стану телефону через 2 секунди');
            ensurePhoneLockState();
          }, 2000);
        }, 500);
      } else {
        logError('Не вдалося знайти елементи форми промокоду');
        var language = getCurrentLanguage();
        restoreAfterError(getLocalizedText('error_apply_promo', language));
      }
    }
    
    var couponLink = isMobileDevice() ? 
      document.querySelector('button.j-coupon-add:not(#bonus-block button.j-coupon-add):not(#bonus-block button.j-bonus-add-mobile)') : 
      document.querySelector('a.j-coupon-add:not(#bonus-block a.j-coupon-add):not(#bonus-block a.j-bonus-add)');
    
    log('Посилання купона:', couponLink);
    
    if (couponLink) {
      log('Натискаємо посилання купона');
      couponLink.click();
    }
    
    waitForElement('.j-coupon-input:not(#bonus-block .j-coupon-input):not(#bonus-block .j-bonus-input)', 2000)
      .then(fillAndSubmit)
      .catch(function(error) {
        logError('Помилка відкриття форми промокоду', error);
        var language = getCurrentLanguage();
        restoreAfterError(getLocalizedText('error_open_form', language));
      });
  }

  function restoreAfterError(errorMessage) {
    resetBonusState();
    showPromoCode();
    setTimeout(function() {
      initializeCheckoutPage();
      setTimeout(function() {
        var bonusBlock = document.querySelector('#bonus-block');
        if (bonusBlock) {
          var errorDiv = document.querySelector('#bonus-block #bonus-error');
          if (errorDiv) {
            errorDiv.textContent = errorMessage;
            errorDiv.style.display = "block";
          }
          var bonusForm = document.querySelector('#bonus-block #bonus-form');
          var bonusLink = document.querySelector('#bonus-block #bonus-link');
          if (bonusForm && bonusLink) {
            bonusLink.parentElement.style.display = "none";
            bonusForm.style.display = isMobileDevice() ? "flex" : "block";
          }
          var bonusButton = document.querySelector('#bonus-block #apply-bonus');
          if (bonusButton) {
            bonusButton.disabled = false;
            var btnContent = bonusButton.querySelector('.btn-content') || bonusButton.querySelector('span');
            if (btnContent) btnContent.textContent = "OK";
          }
          var phoneInput = findPhoneInput();
          if (phoneInput && !phoneInput.hasAttribute('data-bonus-listeners')) {
            addPhoneEventListeners(phoneInput);
          }
        }
      }, 200);
    }, 300);
  }

  function initializeCheckoutPage() {
    log('Ініціалізація сторінки оформлення замовлення');
    
    if (isPromoCodeApplied()) {
      log('Промокод вже застосовано - налаштовуємо слухача видалення');
      setupPromoCodeRemovalListener();
    }
    
    Promise.all([
      waitForElement(PHONE_SELECTORS.join(', '), 5000),
      new Promise(function(resolve) { 
        var containerInfo = findBonusInsertContainer();
        log('Інформація про контейнер для вставлення:', containerInfo);
        if (containerInfo) {
          resolve(containerInfo);
        } else {
          log('Контейнер не знайдено, очікуємо 1 секунду');
          setTimeout(function() { 
            var retryContainer = findBonusInsertContainer();
            log('Повторний пошук контейнера:', retryContainer);
            resolve(retryContainer); 
          }, 1000);
        }
      })
    ]).then(function(results) {
      var phoneInput = results[0];
      var insertInfo = results[1];
      
      log('Результати Promise.all:', { phoneInput: phoneInput, insertInfo: insertInfo });
      
      if (!phoneInput) {
        log('Поле телефону не знайдено, повторна спроба через 1 секунду');
        setTimeout(initializeCheckoutPage, 1000);
        return;
      }
      
      phoneInput = findPhoneInput();
      if (!phoneInput) {
        log('Поле телефону не знайдено при повторному пошуку');
        return;
      }

      if (isBonusPromoCodeApplied()) {
        log('Бонусний промокод застосовано - блокуємо телефон та видаляємо блок');
        disablePhoneInput();
        disableProductRemoval();
        if (document.querySelector('#bonus-block')) {
            removeBonusBlock();
        }
        instantTextReplace();
        return;
      }

      log('Додаємо слухачі до поля телефону');
      addPhoneEventListeners(phoneInput);

      if (!insertInfo) {
        log('Інформація про вставлення відсутня');
        return;
      }
      
      var existingBonusBlock = document.querySelector('#bonus-block');
      if (existingBonusBlock) {
        log('Видаляємо існуючий блок бонусів');
        existingBonusBlock.parentNode.removeChild(existingBonusBlock);
      }
      
      updateBonusState({ blockRemovedIntentionally: false });
      log('Створюємо новий блок бонусів');
      
      // Перевіряємо чи є введений телефон
      var currentPhoneInput = findPhoneInput();
      var hasValidPhone = currentPhoneInput && currentPhoneInput.value && currentPhoneInput.value.trim() !== '' && isValidPhone(currentPhoneInput.value);
      
      log('Перевірка наявності валідного телефону при створенні блоку', {
        hasInput: !!currentPhoneInput,
        hasValue: currentPhoneInput && !!currentPhoneInput.value,
        phoneValue: currentPhoneInput ? currentPhoneInput.value : 'немає',
        isValid: hasValidPhone
      });
      
      // Завжди створюємо блок, але з різними повідомленнями
      createBonusBlock(insertInfo.el, bonusState.balance || 0, insertInfo.insertMode || 'after', insertInfo.type, !hasValidPhone);
      
      if (hasValidPhone) {
        log('Є валідний номер телефону, обробляємо зміну');
        setTimeout(function() { 
          if (currentPhoneInput) handlePhoneChange(currentPhoneInput, true); 
        }, 50);
      } else {
        log('Немає валідного номера телефону, показуємо блок з підказкою');
        // Блок вже створений з підказкою, просто показуємо його
        setTimeout(function() {
          showBonusBlockWithPrompt();
        }, 100);
      }
    }).catch(function(error) {
      logError('Помилка ініціалізації сторінки оформлення', error);
    });
  }

  function handleMutations(groupedMutations) {
    var lastBonusBlockCheck = 0;
    var lastElementsStateCheck = 0;
    var lastTextReplace = 0;
    
    var shouldCheckBonusBlock = false;
    var shouldReplaceText = false;
    var shouldCheckElementsState = false;
    var shouldSetupPromoListener = false;
    
    // Обробляємо зміни дочірніх елементів
    if (groupedMutations.childList && groupedMutations.childList.length > 0) {
      for (var i = 0; i < groupedMutations.childList.length; i++) {
        var mutation = groupedMutations.childList[i];
        
        // Перевіряємо видалені вузли
        for (var j = 0; j < mutation.removedNodes.length; j++) {
          var node = mutation.removedNodes[j];
          if (node.nodeType === 1) {
            if ((node.id === 'bonus-block' || (node.querySelector && node.querySelector('#bonus-block'))) && 
                !bonusState.blockRemovedIntentionally) {
              shouldCheckBonusBlock = true;
              break;
            }
          }
        }
        
        // Перевіряємо додані вузли
        if (!shouldCheckBonusBlock || !shouldReplaceText || !shouldCheckElementsState || !shouldSetupPromoListener) {
          for (var k = 0; k < mutation.addedNodes.length; k++) {
            var node = mutation.addedNodes[k];
            if (node.nodeType === 1) {
              // Перевірка для заміни тексту
              if (!shouldReplaceText && node.textContent && node.textContent.includes('Подарунковий сертифікат') && !node.closest('#bonus-block')) {
                shouldReplaceText = true;
              }
              
              // Перевірка для видалення промокоду
              if (!shouldSetupPromoListener && ((node.matches && node.matches('.j-coupon-remove')) || (node.querySelector && node.querySelector('.j-coupon-remove')))) {
                shouldSetupPromoListener = true;
              }
              
              // Перевірка кнопок видалення товарів
              if ((node.matches && (node.matches('.order-i-remove') || node.matches('.j-remove-p'))) || 
                  (node.querySelector && (node.querySelector('.order-i-remove') || node.querySelector('.j-remove-p')))) {
                if (isBonusPromoCodeApplied()) {
                  setTimeout(disableProductRemoval, 100);
                }
              }
              
              // Перевірка для відновлення блоку бонусів
              if (!shouldCheckBonusBlock && (node.id === 'bonus-block' || 
                  (node.matches && (node.matches('.order-details') || node.matches('.order-details__block') || node.matches('.order-details-i'))))) {
                shouldCheckBonusBlock = true;
              }
              
              // Перевірка стану елементів бонусів
              if (!shouldCheckElementsState && (node.id === 'bonus-block' || (node.querySelector && node.querySelector('#bonus-block')))) {
                shouldCheckElementsState = true;
              }
              
              if (shouldReplaceText && shouldCheckElementsState && shouldCheckBonusBlock && shouldSetupPromoListener) break;
            }
          }
        }
      }
    }
    
    // Обробляємо зміни атрибутів
    if (groupedMutations.attributes && groupedMutations.attributes.length > 0) {
      for (var i = 0; i < groupedMutations.attributes.length; i++) {
        var mutation = groupedMutations.attributes[i];
        if (!shouldCheckElementsState && mutation.target && mutation.target.closest && 
            mutation.target.closest('#bonus-block') && 
            (mutation.attributeName === 'style' || mutation.attributeName === 'disabled')) {
          shouldCheckElementsState = true;
          break;
        }
      }
    }
    
    // Виконуємо дії лише якщо є необхідність
    if (shouldReplaceText) {
      var now = Date.now();
      if (now - lastTextReplace > 100) {
        lastTextReplace = now;
        setTimeout(function() {
          log('Заміна тексту через централізований MutationObserver');
          instantTextReplace();
          ensurePhoneLockState();
        }, 10);
      }
    }
    
    if (shouldSetupPromoListener) {
      setTimeout(setupPromoCodeRemovalListener, 50);
    }
    
    if (shouldCheckElementsState) {
      var now = Date.now();
      if (now - lastElementsStateCheck > 300) {
        lastElementsStateCheck = now;
        setTimeout(function() {
          log('Перевірка стану елементів після мутації DOM');
          restoreBonusElementsState();
        }, 50);
      }
    }
    
    if (shouldCheckBonusBlock && window.location.href.includes('/checkout/')) {
      var now = Date.now();
      if (now - lastBonusBlockCheck > 2000) {
        lastBonusBlockCheck = now;
        setTimeout(function() {
          if (!document.querySelector('#bonus-block') && !isBonusPromoCodeApplied() && !bonusState.blockRemovedIntentionally) {
            initializeCheckoutPage();
          }
        }, 300);
      }
    }
  }

  function initBonusSystem() {
    log('Ініціалізація модуля бонусної системи для checkout');
    log('Поточний стан:', bonusState);
    
    // Перевіряємо чи застосований бонусний промокод
    var isBonusApplied = isBonusPromoCodeApplied();
    if (isBonusApplied) {
        log('Бонусний промокод застосовано при ініціалізації');
        instantTextReplace();
        ensurePhoneLockState();
    }

    // Видаляємо ініціалізацію власного observer - тепер використовуємо централізований
    log('Використовуємо централізований MutationObserver');
    
    var isCheckoutPage = window.location.href.includes('/checkout/');
    log('Тип сторінки:', { isCheckoutPage: isCheckoutPage, url: window.location.href });
    
    if (isCheckoutPage) {
      log('Ініціалізація сторінки оформлення замовлення');
      initializeCheckoutPage();
    } else {
      log('Не сторінка оформлення - базова ініціалізація');
      var isPromo = isPromoCodeApplied();
      if (isPromo) {
        log('Налаштування слухача видалення промокоду');
        setupPromoCodeRemovalListener();
      }
      waitForElement(PHONE_SELECTORS.join(', '), 3000)
        .then(function(phoneInput) {
          log('Знайдено поле телефону, додаємо слухачі');
          addPhoneEventListeners(phoneInput);
        })
        .catch(function() {
          log('Поле телефону не знайдено');
        });
    }
  }

  function destroyBonusSystem() {
    log('Знищення модуля бонусної системи для checkout');
    // Очищення ресурсів, обробників подій тощо
    bonusState = {
      promoApplied: false,
      promoType: null,
      currentPhone: "",
      phoneValidated: false,
      balance: 0,
      isCheckingBalance: false,
      blockVisible: false,
      formVisible: false,
      inputValue: "",
      blockRemovedIntentionally: false,
      currentPageUrl: window.location.href,
      textReplaced: false,
      elementsDisabled: false,
      disabledOpacity: '1'
    };
  }

  log('Завантаження збереженого стану');
  loadBonusState();
  log('Запуск системи бонусів');
  initBonusSystem();

})();