(function() {
  'use strict';
  
  var DEBUG_MODE = true;
  var LOG_PREFIX = '[BONUS_THANKYOU]';
  var BONUS_PERCENTAGE = 0.10;
  
  // URL для резервування бонусів через HTTP API
  var BONUS_RESERVE_API_URL = "https://8av2jf7zbe.execute-api.eu-north-1.amazonaws.com/prod/reserve";

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
  
  function getOrderNumber() {
    var invoiceRows = document.querySelectorAll('.invoice__item');
    
    for (var i = 0; i < invoiceRows.length; i++) {
      var nameCell = invoiceRows[i].querySelector('.invoice__name'); 
      var valueCell = invoiceRows[i].querySelector('.invoice__value');
      
      if (nameCell && valueCell) {
        var nameText = nameCell.textContent || nameCell.innerText;
        var valueText = valueCell.textContent || valueCell.innerText;
        
        if (nameText.toLowerCase().includes('замовлення') || nameText.toLowerCase().includes('заказ')) {
          var orderMatch = valueText.match(/(\d+)/);
          if (orderMatch) {
            return orderMatch[1];
          }
        }
      }
    }
    
    var orderHeaders = document.querySelectorAll('.h2');
    
    for (var i = 0; i < orderHeaders.length; i++) {
      var text = orderHeaders[i].textContent || orderHeaders[i].innerText;
      var match = text.match(/Замовлення\s*№(\d+)/i);
      if (match) {
        return match[1];
      }
    }
    
    var orderElements = document.querySelectorAll('[id*="order"], [class*="order"]');
    
    for (var i = 0; i < orderElements.length; i++) {
      var text = orderElements[i].textContent || orderElements[i].innerText;
      var match = text.match(/№(\d+)/);
      if (match) {
        return match[1];
      }
    }
    
    return null;
  }
  
  function getOrderTotal() {
    var mobileTotal = document.querySelector('.order-details__total');
    
    if (mobileTotal) {
      var text = mobileTotal.textContent || mobileTotal.innerText;
      // Видаляємо всі символи крім цифр, коми та крапки (включаючи звичайні та нерозривні пробіли)
      var cleanText = text.replace(/[^\d.,]/g, '').replace(',', '.');
      var amount = parseFloat(cleanText);
      
      if (!isNaN(amount) && amount > 0) {
        return amount;
      }
    }
    
    var totalSelectors = [
      '.order-summary-b',
      '.order-total',
      '.checkout-total',
      '.summary-total'
    ];
    
    for (var i = 0; i < totalSelectors.length; i++) {
      var selector = totalSelectors[i];
      var element = document.querySelector(selector);
      
      if (element) {
        var text = element.textContent || element.innerText;
        // Видаляємо всі символи крім цифр, коми та крапки (включаючи звичайні та нерозривні пробіли)
        var cleanText = text.replace(/[^\d.,]/g, '').replace(',', '.');
        var amount = parseFloat(cleanText);
        
        if (!isNaN(amount) && amount > 0) {
          return amount;
        }
      }
    }
    
    return null;
  }
  
  function getCustomerData() {
    var customerData = {};
    
    var invoiceItems = document.querySelectorAll('.invoice__item');
    
    for (var i = 0; i < invoiceItems.length; i++) {
      var nameElement = invoiceItems[i].querySelector('.invoice__name');
      var valueElement = invoiceItems[i].querySelector('.invoice__value');
      
      if (nameElement && valueElement) {
        var nameText = nameElement.textContent || nameElement.innerText;
        var valueText = valueElement.textContent || valueElement.innerText;
        
        nameText = nameText.toLowerCase();
        if (nameText.includes("ім'я") || nameText.includes("прізвище") || nameText.includes("покупець")) {
          customerData.name = valueText.trim();
        } else if (nameText.includes("е-пошта") || nameText.includes("email") || nameText.includes("пошта")) {
          customerData.email = valueText.trim();
        } else if (nameText.includes("телефон") || nameText.includes("тел.")) {
          customerData.phone = valueText.trim();
        }
      }
    }
    
    if (!customerData.name && !customerData.email && !customerData.phone) {
      var customerBlock = document.querySelector('#order-customer-data');
      
      if (customerBlock) {
        var items = customerBlock.querySelectorAll('dt.check-h');
        
        for (var i = 0; i < items.length; i++) {
          var label = items[i].textContent || items[i].innerText;
          var valueElement = items[i].nextElementSibling;
          
          if (valueElement && valueElement.classList.contains('check-b')) {
            var value = valueElement.textContent || valueElement.innerText;
            
            if (label.includes("Ім'я") || label.includes("прізвище")) {
              customerData.name = value.trim();
            } else if (label.includes("Е-пошта") || label.includes("email")) {
              customerData.email = value.trim();
            } else if (label.includes("Телефон")) {
              customerData.phone = value.trim();
            }
          }
        }
      }
    }
    
    return customerData;
  }
  
  function getUsedBonusAmount() {
    // Селектори для пошуку використаних бонусів
    var bonusSelectors = [
      '.order-details__cost-value', // Мобільна версія
      '.order-details-b'            // Десктопна версія
    ];
    
    for (var i = 0; i < bonusSelectors.length; i++) {
      var elements = document.querySelectorAll(bonusSelectors[i]);
      
      for (var j = 0; j < elements.length; j++) {
        var element = elements[j];
        var text = element.textContent || element.innerText;
        
        // Шукаємо текст що починається з мінуса (знак списання)
        if (text && (text.trim().startsWith('–') || text.trim().startsWith('-'))) {
          // Витягуємо число з тексту, враховуючи пробіли між цифрами
          var bonusMatch = text.match(/[–-]\s*([\d\s,.\u00A0]+)/);
          if (bonusMatch) {
            // Очищуємо число від пробілів та нерозривних пробілів, залишаємо тільки цифри, коми та крапки
            var cleanNumber = bonusMatch[1].replace(/[\s\u00A0]/g, '').replace(',', '.');
            var usedAmount = parseFloat(cleanNumber);
            if (!isNaN(usedAmount) && usedAmount > 0) {
              return usedAmount;
            }
          }
        }
      }
    }
    
    return 0;
  }
  
  
  function reserveUsedBonuses(phone, usedBonusAmount, orderId) {
    if (!phone || !usedBonusAmount || usedBonusAmount <= 0 || !orderId) {
      console.log('Резервування бонусів пропущено: недостатньо даних');
      return Promise.resolve({success: false, reason: 'missing_data'});
    }
    
    console.log('Початок резервування бонусів:', {phone: phone, amount: usedBonusAmount, order: orderId});
    
    var requestData = {
      phone: phone,
      used_bonus_amount: usedBonusAmount,
      order_id: orderId
    };
    
    return fetch(BONUS_RESERVE_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(requestData)
    })
    .then(function(response) {
      return response.json().then(function(data) {
        return {
          ok: response.ok,
          status: response.status,
          data: data
        };
      });
    })
    .then(function(result) {
      if (result.ok && result.data.success) {
        console.log('✅ Бонуси успішно зарезервовані:', result.data);
        return {
          success: true,
          data: result.data
        };
      } else {
        console.error('❌ Помилка резервування бонусів:', result.data);
        return {
          success: false,
          error: result.data.error || 'Невідома помилка',
          data: result.data
        };
      }
    })
    .catch(function(error) {
      console.error('❌ Помилка запиту резервування бонусів:', error);
      return {
        success: false,
        error: 'Помилка мережі: ' + error.message
      };
    });
  }
  
  
  function getLocalizedText(key, language) {
    var translations = {
      'bonus_program': {
        'uk': 'Бонусна програма',
        'ru': 'Бонусная программа'
      },
      'deducted': {
        'uk': 'Списано',
        'ru': 'Списано'
      },
      'accrued': {
        'uk': 'Нараховано',
        'ru': 'Начислено'
      },
      'will_accrue': {
        'uk': 'Буде нараховано',
        'ru': 'Будет начислено'
      },
      'current_balance': {
        'uk': 'Поточний баланс',
        'ru': 'Текущий баланс'
      },
      'total': {
        'uk': 'Загалом',
        'ru': 'Всего'
      },
      'bonuses_will_be_accrued': {
        'uk': 'Бонуси будуть зараховані після виконання замовлення',
        'ru': 'Бонусы будут начислены после выполнения заказа'
      }
    };
    
    return translations[key] && translations[key][language] ? translations[key][language] : translations[key]['uk'];
  }
  
  function getCurrentLanguage() {
    var url = window.location.href;
    return url.includes('/ru/') ? 'ru' : 'uk';
  }
  
  function showBonusNotification(operationData) {
    var bonusAmount = operationData.bonusAmount || 0;
    var usedAmount = operationData.usedBonusAmount || 0;
    
    // Отримуємо початковий баланс з localStorage
    var initialBalance = 0;
    try {
      var savedState = localStorage.getItem('bonusState');
      if (savedState) {
        var parsedState = JSON.parse(savedState);
        initialBalance = parsedState.balance || 0;
      }
    } catch (e) {
      console.error('Помилка отримання початкового балансу з localStorage', e);
    }
    
    // Розраховуємо кінцевий баланс (на thanks page бонуси ще не нараховані, тільки зарезервовані)
    var finalBalance = initialBalance - usedAmount; // Баланс не змінюється на thanks page

    // Визначаємо мову
    var language = getCurrentLanguage();
    
    var notification = document.createElement('div');
    notification.style.cssText = 
      'background: linear-gradient(135deg, #16a085, #1abc9c);' +
      'color: white;' +
      'padding: 15px;' +
      'margin: 15px auto 20px auto;' +
      'border-radius: 10px;' +
      'text-align: center;' +
      'font-size: 16px;' +
      'box-shadow: 0 6px 15px rgba(22, 160, 133, 0.4);' +
      'animation: slideIn 0.5s ease-out;' +
      'max-width: 100%;' +
      'box-sizing: border-box;' +
      'position: relative;' +
      'z-index: 1000;' +
      'width: 100%;';
    
    // Заголовок
    var headerText = '<h3 style="margin: 0 0 15px 0; font-size: 20px; font-weight: bold;">' + getLocalizedText('bonus_program', language) + '</h3>';
    
    // Якщо використали бонуси - показуємо інформацію про резервування
    var detailsText = '';
    if (usedAmount > 0) {
      detailsText = 
        '<div style="display: flex; flex-wrap: wrap; justify-content: space-around; gap: 10px; margin-bottom: 15px; align-items: stretch;">' +
          '<div style="background: rgba(255, 255, 255, 0.15); padding: 12px; border-radius: 8px; flex: 1; min-width: 100px; display: flex; flex-direction: column; justify-content: center;">' +
            '<div style="font-size: 12px; margin-bottom: 5px; opacity: 0.9; line-height: 1.2;">' + getLocalizedText('deducted', language) + '</div>' +
            '<div style="font-size: 18px; font-weight: bold; color: #ffa726; line-height: 1;">' + usedAmount + '</div>' +
          '</div>' +
          '<div style="background: rgba(255, 255, 255, 0.15); padding: 12px; border-radius: 8px; flex: 1; min-width: 100px; display: flex; flex-direction: column; justify-content: center;">' +
            '<div style="font-size: 12px; margin-bottom: 5px; opacity: 0.9; line-height: 1.2;">' + getLocalizedText('will_accrue', language) + '</div>' +
            '<div style="font-size: 18px; font-weight: bold; color: #4cd137; line-height: 1;">+' + bonusAmount + '</div>' +
          '</div>' +
          '<div style="background: rgba(255, 255, 255, 0.15); padding: 12px; border-radius: 8px; flex: 1; min-width: 100px; display: flex; flex-direction: column; justify-content: center;">' +
            '<div style="font-size: 12px; margin-bottom: 5px; opacity: 0.9; line-height: 1.2;">' + getLocalizedText('current_balance', language) + '</div>' +
            '<div style="font-size: 18px; font-weight: bold; line-height: 1;">' + 
              finalBalance + 
            '</div>' +
          '</div>' +
        '</div>';
    } else {
      // Якщо не використали бонуси, показуємо тільки що будемо нараховувати
      detailsText = 
        '<div style="display: flex; flex-wrap: wrap; justify-content: space-around; gap: 10px; margin-bottom: 15px; align-items: stretch;">' +
          '<div style="background: rgba(255, 255, 255, 0.15); padding: 12px; border-radius: 8px; flex: 1; min-width: 120px; display: flex; flex-direction: column; justify-content: center;">' +
            '<div style="font-size: 12px; margin-bottom: 5px; opacity: 0.9; line-height: 1.2;">' + getLocalizedText('will_accrue', language) + '</div>' +
            '<div style="font-size: 18px; font-weight: bold; color: #4cd137; line-height: 1;">+' + bonusAmount + '</div>' +
          '</div>' +
          '<div style="background: rgba(255, 255, 255, 0.15); padding: 12px; border-radius: 8px; flex: 1; min-width: 120px; display: flex; flex-direction: column; justify-content: center;">' +
            '<div style="font-size: 12px; margin-bottom: 5px; opacity: 0.9; line-height: 1.2;">' + getLocalizedText('current_balance', language) + '</div>' +
            '<div style="font-size: 18px; font-weight: bold; line-height: 1;">' + 
              finalBalance + 
            '</div>' +
          '</div>' +
        '</div>';
    }
    
    // Пояснення
    var infoText = 
      '<div style="font-size: 14px; opacity: 0.9; margin-top: 10px;">' +
        getLocalizedText('bonuses_will_be_accrued', language) +
      '</div>';
    
    notification.innerHTML = headerText + detailsText + infoText;
    
    var style = document.createElement('style');
    style.textContent = 
      '@keyframes slideIn {' +
        'from { opacity: 0; transform: translateY(-20px); }' +
        'to { opacity: 1; transform: translateY(0); }' +
      '}' +
      '@media (max-width: 768px) {' +
        'div[style*="background: linear-gradient(135deg, #16a085, #1abc9c)"] {' +
          'margin: 10px 5px 15px 5px !important;' +
          'padding: 12px !important;' +
          'font-size: 14px !important;' +
        '}' +
        'div[style*="display: flex; flex-wrap: wrap; justify-content: space-around;"] {' +
          'gap: 8px !important;' +
        '}' +
        'div[style*="flex: 1; min-width:"] {' +
          'min-width: 90px !important;' +
          'padding: 8px !important;' +
        '}' +
      '}';
    document.head.appendChild(style);
    
    var insertionPoint = null;
    
    // Пошук місця для вставки - для десктопу використовуємо оригінальну логіку
    var containers = [
      '.invoice',
      '#order-customer-data'
    ];
    
    for (var i = 0; i < containers.length; i++) {
      var container = document.querySelector(containers[i]);
      if (container) {
        insertionPoint = container;
        break;
      }
    }
    
    if (insertionPoint) {
      // Вставляємо після знайденого елемента (оригінальна логіка для десктопу)
      insertionPoint.parentNode.insertBefore(notification, insertionPoint);
    } else {
      // Якщо не знайшли підходящий контейнер, вставляємо в кінець body
      document.body.appendChild(notification);
    }
  }
  

  function showBonusWidget() {
    var currentUrl = window.location.href;
    var isCheckoutCompletePage = currentUrl.includes('checkout/complete');
    
    if (!isCheckoutCompletePage) {
      return;
    }
    
    var orderId = getOrderNumber();
    var orderTotal = getOrderTotal();
    var customerData = getCustomerData();
    var usedBonusAmount = getUsedBonusAmount();
    
    if (!orderId) {
      return;
    }
    
    if (!orderTotal) {
      return;
    }
    
    if (!customerData.phone && !customerData.email) {
      return;
    }
    
    var bonusAmount = Math.floor(orderTotal * BONUS_PERCENTAGE);
    
    var hasAccrual = bonusAmount > 0;
    var hasDeduction = usedBonusAmount > 0;
    
    if (!hasAccrual && !hasDeduction) {
      log('Немає бонусів для відображення', { hasAccrual: hasAccrual, hasDeduction: hasDeduction });
      return;
    }
    
    // Створюємо об'єкт з даними про бонуси для відображення
    var notificationData = {
      bonusAmount: hasAccrual ? bonusAmount : 0,
      usedBonusAmount: hasDeduction ? usedBonusAmount : 0
    };
    
    log('Відображення інформації про бонуси', notificationData);
    // Показуємо повідомлення з інформацією про бонуси
    showBonusNotification(notificationData);
  }

  function initThankYouPage() {
    log('Ініціалізація модуля thank you сторінки');
    var currentUrl = window.location.href;
    var isValidPage = currentUrl.includes('checkout/complete');
    
    if (!isValidPage) {
      log('Не thank you сторінка, пропускаємо', { url: currentUrl });
      return;
    }
    
    log('Thank you сторінка підтверджена, запускаємо виджет бонусів');
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', function() {
        showBonusWidget();
      });
    } else {
      showBonusWidget();
    }
  }

  function handleMutations(groupedMutations) {
    // Для thank you сторінки мутації не потрібні, оскільки весь функціонал запускається один раз
    // Але метод потрібен для сумісності з централізованим менеджером
  }

  function destroyThankYouPage() {
    log('Знищення модуля thank you сторінки');
    // Очищення ресурсів якщо потрібно
  }
  
  // Експортуємо модуль для централізованого менеджера
  if (typeof window.moduleExports === 'undefined') {
    window.moduleExports = {
      init: initThankYouPage,
      handleMutations: handleMutations,
      destroy: destroyThankYouPage
    };
  }
  
  // Якщо скрипт завантажується самостійно (не через менеджер)
  if (typeof window.ScriptManager === 'undefined') {
    log('Самостійний запуск модуля thank you сторінки');
    initThankYouPage();
  }
  
})();