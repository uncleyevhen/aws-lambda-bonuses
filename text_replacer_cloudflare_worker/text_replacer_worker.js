// Оптимізований Cloudflare Worker для швидкої заміни тексту
// Версія 2.1 з перевіркою localStorage для точного визначення застосованих бонусів
// Заміна тексту відбувається тільки коли реально застосовані бонуси (не просто промокод)

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const isDebug = env?.DEBUG_MODE === 'true';
    
    // Швидка перевірка чи потрібна заміна тексту
    if (!isTextReplacementNeeded(url, request)) {
      if (isDebug) {
        console.log(`[CF-FAST-REPLACER] Пропускаємо: ${url.pathname} - не потрібна заміна`);
      }
      return fetch(request);
    }
    
    // Перевіряємо чи застосовані бонуси через URL або Cookie
    const hasBonusApplied = checkBonusApplied(url, request);
    
    if (isDebug) {
      console.log(`[CF-FAST-REPLACER] URL: ${url.pathname}, Bonus Applied: ${hasBonusApplied}`);
      console.log(`[CF-FAST-REPLACER] Cookies: ${request.headers.get('Cookie') || 'немає'}`);
      console.log(`[CF-FAST-REPLACER] Логіка: Заміна тексту тільки якщо promoType=bonus, не просто промокод`);
    }
    
    // Отримуємо оригінальну відповідь
    const response = await fetch(request);
    
    // Перевіряємо тип контенту
    if (!isHTMLResponse(response)) {
      if (isDebug) {
        console.log(`[CF-FAST-REPLACER] Не HTML відповідь: ${response.headers.get('content-type')}`);
      }
      return response;
    }
    
    // Виконуємо заміну тексту тільки якщо застосовані бонуси
    const html = await response.text();
    
    if (isDebug) {
      const hasTargetText = /Подарунковий сертифікат|Подарочный сертификат/i.test(html);
      console.log(`[CF-FAST-REPLACER] Містить цільовий текст: ${hasTargetText}`);
    }
    
    // Інжектуємо JavaScript для перевірки localStorage та встановлення cookie
    const htmlWithStateChecker = injectBonusStateChecker(html, isDebug);
    
    // ВИКОНУЄМО ЗАМІНУ ТІЛЬКИ ЯКЩО БОНУСИ РЕАЛЬНО ЗАСТОСОВАНІ
    const modifiedHtml = hasBonusApplied ? 
      performFastTextReplacement(htmlWithStateChecker, env) : 
      htmlWithStateChecker; // Повертаємо HTML без замін якщо бонуси НЕ застосовані
    
    if (isDebug) {
      if (hasBonusApplied) {
        console.log(`[CF-LOGIC] Бонуси застосовані - виконуємо заміну тексту`);
      } else {
        console.log(`[CF-LOGIC] Бонуси НЕ застосовані - пропускаємо заміну тексту`);
      }
    }
    
    // Повертаємо модифіковану відповідь
    return new Response(modifiedHtml, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers
    });
  },
};

// Перевірка чи застосовані бонуси
function checkBonusApplied(url, request) {
  // Перевіряємо URL параметр
  if (url.searchParams.has('bonus-applied') && url.searchParams.get('bonus-applied') === 'true') {
    return true;
  }
  
  // Перевіряємо Cookie - тільки якщо це саме бонуси, а не просто промокод
  const cookieHeader = request.headers.get('Cookie') || '';
  
  // Шукаємо cookie bonus_promo_applied=true
  const bonusAppliedMatch = cookieHeader.match(/bonus_promo_applied=([^;]+)/);
  const bonusApplied = bonusAppliedMatch && bonusAppliedMatch[1] === 'true';
  
  if (bonusApplied) {
    // Додаткова перевірка - чи є також cookie що вказує на тип промокоду
    const promoTypeMatch = cookieHeader.match(/promo_type=([^;]+)/);
    const promoType = promoTypeMatch ? promoTypeMatch[1] : null;
    
    // Повертаємо true тільки якщо тип промокоду = bonus
    return promoType === 'bonus';
  }
  
  // Перевіряємо спеціальний заголовок (якщо буде додано в майбутньому)
  if (request.headers.get('X-Bonus-Applied') === 'true') {
    return true;
  }
  
  return false;
}

// Селективна заміна тексту - тільки в контексті бонусів
function performSelectiveTextReplacement(html, env) {
  const isDebug = env?.DEBUG_MODE === 'true';
  let modifiedHtml = html;
  let replacementCount = 0;
  
  // Розширена перевірка контексту бонусів
  // 1. Шукаємо елементи які вже містять "Бонуси" поруч з цільовим текстом
  const bonusContextWithExistingBonus = /(<[^>]*>[\s\S]*?(?:Бонуси|Бонусы)[\s\S]*?)(Подарунковий сертифікат|Подарочный сертификат)([\s\S]*?<\/[^>]+>)/gi;
  
  modifiedHtml = modifiedHtml.replace(bonusContextWithExistingBonus, (match, before, target, after) => {
    const replacement = target.includes('Подарунковий') ? 'Бонуси' : 'Бонусы';
    replacementCount++;
    
    if (isDebug) {
      console.log(`[CF-SELECTIVE-REPLACER] Замінено поруч з існуючими бонусами: ${target} -> ${replacement}`);
    }
    
    return before + replacement + after;
  });
  
  // 2. Шукаємо в контексті промокодних елементів (класи та атрибути)
  const promoContext = /(<[^>]*(?:class="[^"]*(?:coupon|discount|promo|bonus|cert)[^"]*"|data-[^=]*="[^"]*(?:coupon|discount|promo|bonus|cert)[^"]*")[^>]*>[\s\S]*?)(Подарунковий сертифікат|Подарочный сертификат)([\s\S]*?<\/[^>]+>)/gi;
  
  modifiedHtml = modifiedHtml.replace(promoContext, (match, before, target, after) => {
    const replacement = target.includes('Подарунковий') ? 'Бонуси' : 'Бонусы';
    replacementCount++;
    
    if (isDebug) {
      console.log(`[CF-SELECTIVE-REPLACER] Замінено в промо-контексті: ${target} -> ${replacement}`);
    }
    
    return before + replacement + after;
  });
  
  // 3. Шукаємо в input елементах з placeholder
  const inputPlaceholderContext = /(<input[^>]*placeholder="[^"]*)(Подарунковий сертифікат|Подарочный сертификат)([^"]*"[^>]*>)/gi;
  
  modifiedHtml = modifiedHtml.replace(inputPlaceholderContext, (match, before, target, after) => {
    const replacement = target.includes('Подарунковий') ? 'Бонуси' : 'Бонусы';
    replacementCount++;
    
    if (isDebug) {
      console.log(`[CF-SELECTIVE-REPLACER] Замінено placeholder: ${target} -> ${replacement}`);
    }
    
    return before + replacement + after;
  });
  
  // 4. Якщо нічого не знайдено, але є ознаки що це checkout сторінка - робимо базову заміну
  if (replacementCount === 0 && /\/checkout\/|\/order\/|cart|basket/i.test(html)) {
    const basicReplacements = [
      [/Подарунковий сертифікат/g, 'Бонуси'],
      [/Подарочный сертификат/g, 'Бонусы']
    ];
    
    for (const [regex, replacement] of basicReplacements) {
      const beforeCount = (modifiedHtml.match(regex) || []).length;
      if (beforeCount > 0) {
        modifiedHtml = modifiedHtml.replace(regex, replacement);
        replacementCount += beforeCount;
        
        if (isDebug) {
          console.log(`[CF-SELECTIVE-REPLACER] Базова заміна на checkout: ${beforeCount} входжень`);
        }
      }
    }
  }
  
  // Додаємо маркер що сторінка оброблена селективно
  if (replacementCount > 0) {
    modifiedHtml = modifiedHtml.replace(
      /<body([^>]*)>/i, 
      `<body$1 data-cf-text-processed="selective-${replacementCount}">`
    );
    
    if (isDebug) {
      console.log(`[CF-SELECTIVE-REPLACER] Селективних замін: ${replacementCount}`);
    }
  } else if (isDebug) {
    console.log(`[CF-SELECTIVE-REPLACER] Жодних замін не зроблено`);
  }
  
  return modifiedHtml;
}

// НАДШВИДКА заміна тексту через String.replace (повна заміна)
function performFastTextReplacement(html, env) {
  const isDebug = env?.DEBUG_MODE === 'true';
  let modifiedHtml = html;
  let replacementCount = 0;
  
  // Швидка перевірка - чи є взагалі щось для заміни
  const hasTargetText = /Подарунковий сертифікат|Подарочный сертификат|Промокод|Gift certificate|Coupon code/i.test(html);
  if (!hasTargetText) {
    if (isDebug) {
      console.log('[CF-FAST-REPLACER] Цільового тексту не знайдено - пропускаємо обробку');
    }
    return html;
  }
  
  // Основні заміни тексту (найшвидші)
  const textReplacements = [
    // Українські заміни
    [/Подарунковий сертифікат/g, 'Бонуси'],
    [/Промокод\/Сертифікат/g, 'Бонуси'],
    [/Промокод\s*\/\s*Сертифікат/g, 'Бонуси'],
    [/Промокод\s+або\s+Сертифікат/g, 'Бонуси'],
    [/Введіть промокод/g, 'Введіть бонуси'],
    [/Застосувати промокод/g, 'Застосувати бонуси'],
    [/Код знижки/g, 'Бонуси'],
    // Російські заміни
    [/Подарочный сертификат/g, 'Бонусы'],
    [/Промокод\/Сертификат/g, 'Бонусы'],
    [/Промокод\s*\/\s*Сертификат/g, 'Бонусы'],
    [/Промокод\s+или\s+Сертификат/g, 'Бонусы'],
    [/Введите промокод/g, 'Введите бонусы'],
    [/Применить промокод/g, 'Применить бонусы'],
    // Англійські заміни
    [/Gift certificate/g, 'Bonus points'],
    [/Discount code/g, 'Bonus points'],
    [/Apply coupon/g, 'Apply bonus'],
    [/Enter coupon/g, 'Enter bonus'],
    [/Coupon code/g, 'Bonus code']
  ];
  
  // Заміни в атрибутах (placeholder, title тощо)
  const attributeReplacements = [
    // Українські атрибути
    [/placeholder="[^"]*Подарунковий сертифікат[^"]*"/g, (match) => match.replace(/Подарунковий сертифікат/g, 'Бонуси')],
    [/title="[^"]*Подарунковий сертифікат[^"]*"/g, (match) => match.replace(/Подарунковий сертифікат/g, 'Бонуси')],
    [/aria-label="[^"]*Подарунковий сертифікат[^"]*"/g, (match) => match.replace(/Подарунковий сертифікат/g, 'Бонуси')],
    [/alt="[^"]*Подарунковий сертифікат[^"]*"/g, (match) => match.replace(/Подарунковий сертифікат/g, 'Бонуси')],
    [/placeholder="[^"]*Промокод[^"]*"/g, (match) => match.replace(/Промокод/g, 'Бонуси')],
    [/title="[^"]*Промокод[^"]*"/g, (match) => match.replace(/Промокод/g, 'Бонуси')],
    [/aria-label="[^"]*Промокод[^"]*"/g, (match) => match.replace(/Промокод/g, 'Бонуси')],
    [/alt="[^"]*Промокод[^"]*"/g, (match) => match.replace(/Промокод/g, 'Бонуси')],
    // Російські атрибути
    [/placeholder="[^"]*Подарочный сертификат[^"]*"/g, (match) => match.replace(/Подарочный сертификат/g, 'Бонусы')],
    [/title="[^"]*Подарочный сертификат[^"]*"/g, (match) => match.replace(/Подарочный сертификат/g, 'Бонусы')],
    [/aria-label="[^"]*Подарочный сертификат[^"]*"/g, (match) => match.replace(/Подарочный сертификат/g, 'Бонусы')],
    [/alt="[^"]*Подарочный сертификат[^"]*"/g, (match) => match.replace(/Подарочный сертификат/g, 'Бонусы')]
  ];
  
  // Швидкі текстові заміни
  for (const [regex, replacement] of textReplacements) {
    const beforeCount = (modifiedHtml.match(regex) || []).length;
    if (beforeCount > 0) {
      modifiedHtml = modifiedHtml.replace(regex, replacement);
      replacementCount += beforeCount;
      
      if (isDebug) {
        console.log(`[CF-FAST-REPLACER] Замінено ${beforeCount} входжень: ${regex} -> ${replacement}`);
      }
    }
  }
  
  // Швидкі заміни атрибутів
  for (const [regex, replacer] of attributeReplacements) {
    const beforeCount = (modifiedHtml.match(regex) || []).length;
    if (beforeCount > 0) {
      modifiedHtml = modifiedHtml.replace(regex, replacer);
      replacementCount += beforeCount;
      
      if (isDebug) {
        console.log(`[CF-FAST-REPLACER] Замінено ${beforeCount} атрибутів`);
      }
    }
  }
  
  // Додаємо маркер що сторінка оброблена
  if (replacementCount > 0) {
    modifiedHtml = modifiedHtml.replace(
      /<body([^>]*)>/i, 
      `<body$1 data-cf-text-processed="${replacementCount}">`
    );
    
    if (isDebug) {
      console.log(`[CF-FAST-REPLACER] Загалом замін: ${replacementCount}`);
    }
  }
  
  return modifiedHtml;
}

// Швидка перевірка чи потрібна заміна тексту
function isTextReplacementNeeded(url, request) {
  const pathname = url.pathname.toLowerCase();
  
  // Пропускаємо статичні ресурси
  if (isStaticResource(pathname)) {
    return false;
  }
  
  // Пропускаємо админку та API
  if (pathname.includes('/admin') || pathname.includes('/api')) {
    return false;
  }
  
  // Обробляємо всі HTML сторінки - Worker буде швидко перевіряти контент
  // і робити заміну тільки там, де знайде потрібний текст
  return true;
}

// Перевірка статичних ресурсів
function isStaticResource(pathname) {
  const staticExtensions = ['.js', '.css', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.woff', '.woff2', '.ttf'];
  return staticExtensions.some(ext => pathname.endsWith(ext));
}

// Перевірка HTML відповіді
function isHTMLResponse(response) {
  const contentType = response.headers.get('content-type') || '';
  return contentType.includes('text/html') && response.ok;
}

// Інжекція JavaScript для перевірки стану бонусів з localStorage
function injectBonusStateChecker(html, isDebug) {
  // JavaScript код для перевірки localStorage та встановлення cookie
  const bonusStateScript = `
<script>
(function() {
  try {
    ${isDebug ? 'console.log("[CF-BONUS-CHECKER] Перевіряємо стан бонусів...");' : ''}
    
    // Перевіряємо localStorage на наявність застосованих бонусів
    var bonusState = localStorage.getItem('bonusState');
    var isBonusApplied = false;
    
    if (bonusState) {
      try {
        var parsedState = JSON.parse(bonusState);
        // Перевіряємо чи застосовані саме бонуси (не просто промокод)
        isBonusApplied = parsedState.promoApplied === true && parsedState.promoType === 'bonus';
        
        ${isDebug ? `console.log("[CF-BONUS-CHECKER] Стан з localStorage:", {
          promoApplied: parsedState.promoApplied,
          promoType: parsedState.promoType,
          isBonusApplied: isBonusApplied
        });` : ''}
      } catch (e) {
        ${isDebug ? 'console.log("[CF-BONUS-CHECKER] Помилка парсингу localStorage:", e);' : ''}
      }
    }
    
    // Перевіряємо поточні cookie
    var cookieValue = document.cookie.split('; ').find(row => row.startsWith('bonus_promo_applied='));
    var promoTypeValue = document.cookie.split('; ').find(row => row.startsWith('promo_type='));
    
    var currentBonusCookie = cookieValue ? cookieValue.split('=')[1] === 'true' : false;
    var currentPromoType = promoTypeValue ? promoTypeValue.split('=')[1] : null;
    
    ${isDebug ? `console.log("[CF-BONUS-CHECKER] Поточні cookie:", {
      bonus_promo_applied: currentBonusCookie,
      promo_type: currentPromoType
    });` : ''}
    
    // Встановлюємо або оновлюємо cookie на основі стану localStorage
    if (isBonusApplied) {
      if (!currentBonusCookie || currentPromoType !== 'bonus') {
        ${isDebug ? 'console.log("[CF-BONUS-CHECKER] Встановлюємо cookie для застосованих бонусів");' : ''}
        document.cookie = 'bonus_promo_applied=true; path=/; max-age=86400; SameSite=Lax';
        document.cookie = 'promo_type=bonus; path=/; max-age=86400; SameSite=Lax';
        
        // Перезавантажуємо сторінку для застосування змін тексту
        setTimeout(function() {
          window.location.reload();
        }, 100);
      }
    } else {
      // Якщо бонуси не застосовані, але cookie каже що застосовані - очищуємо
      if (currentBonusCookie) {
        ${isDebug ? 'console.log("[CF-BONUS-CHECKER] Очищуємо cookie - бонуси не застосовані");' : ''}
        document.cookie = 'bonus_promo_applied=false; path=/; max-age=86400; SameSite=Lax';
        document.cookie = 'promo_type=; path=/; max-age=0; SameSite=Lax';
        
        // Перезавантажуємо сторінку для застосування змін тексту
        setTimeout(function() {
          window.location.reload();
        }, 100);
      }
    }
    
    ${isDebug ? 'console.log("[CF-BONUS-CHECKER] Перевірка завершена, isBonusApplied:", isBonusApplied);' : ''}
    
  } catch (e) {
    ${isDebug ? 'console.error("[CF-BONUS-CHECKER] Загальна помилка:", e);' : ''}
  }
})();
</script>`;
  
  // Вставляємо скрипт перед закриттям </body> або в </head>
  if (html.includes('</body>')) {
    return html.replace('</body>', bonusStateScript + '</body>');
  } else if (html.includes('</head>')) {
    return html.replace('</head>', bonusStateScript + '</head>');
  } else {
    // Якщо немає body або head, додаємо в кінець
    return html + bonusStateScript;
  }
}

// Експорт функцій для тестування
export { performFastTextReplacement, performSelectiveTextReplacement, checkBonusApplied, injectBonusStateChecker };
