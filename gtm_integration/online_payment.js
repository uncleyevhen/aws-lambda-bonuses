(function(){
  var monobankApiKey = "mS18mcoPXZe75Au0pcMJFZA";

  function setLocalStorage(key, value, expirationHours) {
    try {
      var data = {
        value: value,
        timestamp: Date.now()
      };
      if (expirationHours) {
        data.expiration = Date.now() + (expirationHours * 60 * 60 * 1000);
      }
      localStorage.setItem(key, JSON.stringify(data));
    } catch (e) {
      console.error('Помилка збереження в localStorage:', e);
    }
  }

  function getLocalStorage(key) {
    try {
      var item = localStorage.getItem(key);
      if (!item) return null;
      
      var data = JSON.parse(item);
      
      // Перевіряємо чи не протерміновані дані
      if (data.expiration && Date.now() > data.expiration) {
        localStorage.removeItem(key);
        return null;
      }
      
      return data.value;
    } catch (e) {
      console.error('Помилка читання з localStorage:', e);
      return null;
    }
  }

  function removeLocalStorage(key) {
    try {
      localStorage.removeItem(key);
    } catch (e) {
      console.error('Помилка видалення з localStorage:', e);
    }
  }

  function clearOrderData(orderNumber) {
    removeLocalStorage('monobankPaymentUrl-' + orderNumber);
    removeLocalStorage('monobankInvoiceId-' + orderNumber);
    removeLocalStorage('paymentDone-' + orderNumber);
  }

  function getCurrentLanguage() {
    var url = window.location.href;
    return url.includes('/ru/') ? 'ru' : 'uk';
  }

  function getLocalizedText(key, language) {
    var translations = {
      'payment_title': {
        'uk': 'Онлайн оплата',
        'ru': 'Онлайн оплата'
      },
      'payment_instruction': {
        'uk': 'Для оплати замовлення натисніть кнопку нижче:',
        'ru': 'Для оплаты заказа нажмите кнопку ниже:'
      },
      'pay_order': {
        'uk': 'Оплатити замовлення',
        'ru': 'Оплатить заказ'
      },
      'order_prefix': {
        'uk': 'Замовлення #',
        'ru': 'Заказ #'
      },
      'no_order_number': {
        'uk': 'Немає номера замовлення',
        'ru': 'Нет номера заказа'
      },
      'invoice_creation_error': {
        'uk': 'Помилка створення інвойсу: ',
        'ru': 'Ошибка создания инвойса: '
      },
      'status_check_error': {
        'uk': 'Помилка перевірки статусу: ',
        'ru': 'Ошибка проверки статуса: '
      },
      'no_pageurl_invoiceid': {
        'uk': 'Не отримано pageUrl або invoiceId: ',
        'ru': 'Не получены pageUrl или invoiceId: '
      }
    };
    
    return translations[key] && translations[key][language] ? translations[key][language] : translations[key]['uk'];
  }

  function getOrderNumber() {
    var selectors = ['.h2'];
    for (var i = 0; i < selectors.length; i++) {
      var element = document.querySelector(selectors[i]);
      if (element && /(Замовлення|Заказ)\s*№\d+/.test(element.innerText)) {
        var match = element.innerText.match(/\d+/);
        if (match) return match[0];
      }
    }
    var rows = document.querySelectorAll('.invoice__item');
    for (var j = 0; j < rows.length; j++) {
      var nameCell = rows[j].querySelector('.invoice__name');
      var valueCell = rows[j].querySelector('.invoice__value');
      if (nameCell && valueCell) {
        var text = nameCell.innerText.trim().toLowerCase();
        if (text.includes("замовлення") || text.includes("заказ")) {
          return valueCell.innerText.trim();
        }
      }
    }
    return '';
  }

  function getOrderSum() {
    var element = document.querySelector('.order-summary .order-summary-b');
    if (element) {
      var sumText = element.innerText.replace(/\s/g, '');
      var match = sumText.match(/\d+/);
      if (match) return parseInt(match[0], 10);
    }
    var mobileElement = document.querySelector('.order-details__total');
    if (mobileElement) {
      var mobileText = mobileElement.innerText.replace(/\s/g, '');
      var mobileMatch = mobileText.match(/\d+/);
      if (mobileMatch) return parseInt(mobileMatch[0], 10);
    }
    return 100;
  }

  function isPartialPayment() {
    var rows = document.querySelectorAll('.invoice__item');
    for (var i = 0; i < rows.length; i++) {
      var nameCell = rows[i].querySelector('.invoice__name');
      var valueCell = rows[i].querySelector('.invoice__value');
      if (nameCell && valueCell) {
        var text = nameCell.innerText.toLowerCase().trim();
        if (text.includes("спосіб оплати") || text.includes("способ оплаты")) {
          var valueText = valueCell.innerText.toLowerCase().trim();
          if (valueText.includes("часткова") || valueText.includes("частичная")) {
            return true;
          }
        }
      }
    }
    var dtElements = document.querySelectorAll('dt.check-h');
    for (var j = 0; j < dtElements.length; j++) {
      var dtText = dtElements[j].innerText.toLowerCase().trim();
      if (dtText.includes("спосіб оплати") || dtText.includes("способ оплаты")) {
        var ddElement = dtElements[j].nextElementSibling;
        if (ddElement && ddElement.classList.contains('check-b')) {
          var ddText = ddElement.innerText.toLowerCase().trim();
          if (ddText.includes("часткова") || ddText.includes("частичная")) {
            return true;
          }
        }
      }
    }
    return false;
  }

  function getInvoiceUrl(orderNumber) {
    return getLocalStorage('monobankPaymentUrl-' + orderNumber);
  }
  function getInvoiceId(orderNumber) {
    return getLocalStorage('monobankInvoiceId-' + orderNumber);
  }
  function saveInvoice(orderNumber, url, invoiceId) {
    // Зберігаємо дані інвойсу з експірацією 24 години
    setLocalStorage('monobankPaymentUrl-' + orderNumber, url, 24);
    setLocalStorage('monobankInvoiceId-' + orderNumber, invoiceId, 24);
  }

  function isPaidByLocalStorage(orderNumber) {
    return getLocalStorage('paymentDone-' + orderNumber) === 'true';
  }
  function markAsPaid(orderNumber) {
    // Позначаємо як оплачене з експірацією 30 днів
    setLocalStorage('paymentDone-' + orderNumber, 'true', 24 * 30);
  }

  function makeContainer() {
    var language = getCurrentLanguage();
    var container = document.createElement('div');
    container.style.cssText = 
      'background: linear-gradient(135deg, #3498db, #2980b9);' +
      'color: white;' +
      'padding: 20px;' +
      'margin: 15px auto 20px auto;' +
      'border-radius: 10px;' +
      'text-align: center;' +
      'font-size: 16px;' +
      'box-shadow: 0 6px 15px rgba(52, 152, 219, 0.4);' +
      'animation: slideIn 0.5s ease-out;' +
      'max-width: 100%;' +
      'box-sizing: border-box;' +
      'position: relative;' +
      'z-index: 1000;' +
      'width: 100%;';
    
    // Заголовок
    var header = document.createElement('h3');
    header.style.cssText = 'margin: 0 0 15px 0; font-size: 20px; font-weight: bold;';
    header.innerText = '💳 ' + getLocalizedText('payment_title', language);
    container.appendChild(header);
    
    // Опис
    var paragraph = document.createElement('p');
    paragraph.style.cssText = 'margin: 0 0 15px 0; font-size: 14px; opacity: 0.9;';
    paragraph.innerText = getLocalizedText('payment_instruction', language);
    container.appendChild(paragraph);
    
    // Кнопка
    var button = document.createElement('button');
    button.id = 'payment-button';
    button.innerText = getLocalizedText('pay_order', language);
    button.style.cssText = 
      'background: rgba(255, 255, 255, 0.2);' +
      'color: white;' +
      'padding: 12px 24px;' +
      'border: 2px solid rgba(255, 255, 255, 0.3);' +
      'border-radius: 25px;' +
      'cursor: pointer;' +
      'font-size: 16px;' +
      'font-weight: bold;' +
      'transition: all 0.3s ease;' +
      'outline: none;' +
      'margin-top: 5px;';
    
    // Ховер ефекти для кнопки
    button.onmouseover = function() {
      this.style.background = 'rgba(255, 255, 255, 0.3)';
      this.style.transform = 'translateY(-2px)';
      this.style.boxShadow = '0 4px 8px rgba(0, 0, 0, 0.2)';
    };
    button.onmouseout = function() {
      this.style.background = 'rgba(255, 255, 255, 0.2)';
      this.style.transform = 'translateY(0)';
      this.style.boxShadow = 'none';
    };
    button.onclick = onButtonClick;
    
    container.appendChild(button);
    
    // Додаємо CSS анімацію
    if (!document.getElementById('payment-widget-styles')) {
      var style = document.createElement('style');
      style.id = 'payment-widget-styles';
      style.textContent = 
        '@keyframes slideIn {' +
          'from { opacity: 0; transform: translateY(-20px); }' +
          'to { opacity: 1; transform: translateY(0); }' +
        '}' +
        '@media (max-width: 768px) {' +
          'div[style*="background: linear-gradient(135deg, #3498db, #2980b9)"] {' +
            'margin: 10px 5px 15px 5px !important;' +
            'padding: 15px !important;' +
            'font-size: 14px !important;' +
          '}' +
          'h3[style*="font-size: 20px"] {' +
            'font-size: 18px !important;' +
          '}' +
          'button[id="payment-button"] {' +
            'padding: 10px 20px !important;' +
            'font-size: 14px !important;' +
          '}' +
        '}';
      document.head.appendChild(style);
    }
    
    return container;
  }

  function addPaymentButton() {
    if (document.getElementById('payment-button')) {
      return;
    }
    var desktopContainer = document.querySelector('.checkout-complete-info');
    if (desktopContainer) {
      var block = makeContainer();
      desktopContainer.insertAdjacentElement('afterend', block);
      return;
    }
    var mobileContainer = document.querySelector('.checkout-success__body');
    if (mobileContainer) {
      var block2 = makeContainer();
      mobileContainer.insertAdjacentElement('afterend', block2);
      return;
    }
  }

  function removePaymentButton() {
    var button = document.getElementById('payment-button');
    if (button && button.parentNode) {
      // Видаляємо весь контейнер з кнопкою
      var container = button.parentNode;
      if (container.parentNode) {
        container.parentNode.removeChild(container);
      }
    }
  }

  function onButtonClick() {
    var orderNumber = getOrderNumber();
    if (!orderNumber) return;
    var existingUrl = getInvoiceUrl(orderNumber);
    var existingId = getInvoiceId(orderNumber);
    if (existingUrl && existingId) {
      window.location.href = existingUrl;
    } else {
      makeInvoice().then(function(newUrl) {
        window.location.href = newUrl;
      }).catch(function(error) {
      });
    }
  }

  function makeInvoice() {
    var language = getCurrentLanguage();
    var orderNumber = getOrderNumber();
    if (!orderNumber) {
      return Promise.reject(getLocalizedText('no_order_number', language));
    }
    var sum = getOrderSum();
    if (isPartialPayment()) {
      sum = 100;
    }
    var amount = sum * 100;
    var data = {
      invoiceId: orderNumber,
      amount: amount,
      ccy: 980,
      redirectUrl: window.location.href,
      merchantPaymInfo: {
        reference: getLocalizedText('order_prefix', language) + orderNumber,
        destination: 'SafeYourLove'
      }
    };
    return fetch('https://api.monobank.ua/api/merchant/invoice/create', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Token': monobankApiKey
      },
      body: JSON.stringify(data)
    })
    .then(function(response) {
      if (!response.ok) {
        throw new Error(getLocalizedText('invoice_creation_error', language) + response.statusText);
      }
      return response.json();
    })
    .then(function(json) {
      if (json.pageUrl && json.invoiceId) {
        saveInvoice(orderNumber, json.pageUrl, json.invoiceId);
        return json.pageUrl;
      } else {
        throw new Error(getLocalizedText('no_pageurl_invoiceid', language) + JSON.stringify(json));
      }
    });
  }

  function checkPaymentStatus() {
    var language = getCurrentLanguage();
    var orderNumber = getOrderNumber();
    if (!orderNumber) {
      return Promise.resolve(false);
    }
    var invoiceId = getInvoiceId(orderNumber);
    if (!invoiceId) {
      return Promise.resolve(false);
    }
    var url = 'https://api.monobank.ua/api/merchant/invoice/status?invoiceId=' + invoiceId;
    return fetch(url, {
      method: 'GET',
      headers: {
        'X-Token': monobankApiKey
      }
    })
    .then(function(response) {
      if (!response.ok) {
        throw new Error(getLocalizedText('status_check_error', language) + response.statusText);
      }
      return response.json();
    })
    .then(function(json) {
      if (json.status && json.status.toLowerCase() === 'success') {
        markAsPaid(orderNumber);
        removePaymentButton();
        return true;
      } else {
        return false;
      }
    })
    .catch(function(err) {
      return false;
    });
  }

  (function(){
    var orderNumber = getOrderNumber();
    if (!orderNumber) {
      return;
    }
    if (isPaidByLocalStorage(orderNumber)) {
      return;
    }
    var invoiceId = getInvoiceId(orderNumber);
    if (invoiceId) {
      checkPaymentStatus().then(function(paid) {
        if (!paid) {
          addPaymentButton();
        }
      });
    } else {
      addPaymentButton();
    }
  })();

})();