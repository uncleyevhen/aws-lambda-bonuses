// Cloudflare Worker для обслуговування бонусного скрипта
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    
    // Перевірка рефера та домену
    const referer = request.headers.get('referer');
    const allowedDomains = ['yourdomain.com', 'staging.yourdomain.com']; // Замініть на ваші домени
    
    if (!referer || !allowedDomains.some(domain => referer.includes(domain))) {
      return new Response('Not Found', { status: 404 });
    }
    
    // Обслуговування основного скрипта
    if (url.pathname === '/bonus.js') {
      const script = await getBonusScript(env);
      
      return new Response(script, {
        headers: {
          'Content-Type': 'application/javascript; charset=utf-8',
          'Cache-Control': 'public, max-age=3600',
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
        },
      });
    }
    
    // API ендпоінти для бонусів
    if (url.pathname === '/api/balance') {
      return handleBalanceCheck(request, env);
    }
    
    if (url.pathname === '/api/promo') {
      return handlePromoGeneration(request, env);
    }
    
    return new Response('Not Found', { status: 404 });
  },
};

// Функція для генерації обфускованого скрипта
async function getBonusScript(env) {
  // Тут можна динамічно генерувати обфускований код
  const timestamp = Date.now();
  const obfuscatedVariables = generateObfuscatedNames();
  
  return `
(function(){
  'use strict';
  
  // Обфусковані змінні
  var ${obfuscatedVariables.debug} = true;
  var ${obfuscatedVariables.prefix} = '[BONUS_SCRIPT]';
  var ${obfuscatedVariables.apiUrl1} = "${env.BONUS_BALANCE_API_URL || 'https://your-api.com/balance'}";
  var ${obfuscatedVariables.apiUrl2} = "${env.PROMO_API_URL || 'https://your-api.com/promo'}";
  
  // Вставляємо обфускований код з оригінального скрипта
  ${await getObfuscatedMainScript()}
  
  // Ініціалізація з перевірками
  if (typeof window !== 'undefined' && window.location) {
    ${obfuscatedVariables.init}();
  }
})();
`;
}

// Генерація випадкових імен змінних
function generateObfuscatedNames() {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const nums = '0123456789';
  
  function randomName() {
    let name = chars[Math.floor(Math.random() * chars.length)];
    for (let i = 0; i < 4 + Math.floor(Math.random() * 4); i++) {
      name += (chars + nums)[Math.floor(Math.random() * (chars + nums).length)];
    }
    return '_' + name;
  }
  
  return {
    debug: randomName(),
    prefix: randomName(),
    apiUrl1: randomName(),
    apiUrl2: randomName(),
    init: randomName(),
    state: randomName(),
    log: randomName(),
  };
}

// Обфускований основний код
async function getObfuscatedMainScript() {
  // Тут вставляємо обфускований код з оригінального скрипта
  // Можна використати Base64 кодування або інші методи обфускації
  return `
  // Обфускований код бонусної системи
  var _0x1a2b=['log','error','querySelector','addEventListener','innerHTML','style','display'];
  var _0x3c4d=function(a,b){return a+b;};
  var _0x5e6f=function(a){console[_0x1a2b[0]](a);};
  
  // Тут буде обфускована версія вашого основного коду...
  // Замініть це на реальний обфускований код
  `;
}

// Обробка перевірки балансу
async function handleBalanceCheck(request, env) {
  const url = new URL(request.url);
  const phone = url.searchParams.get('phone');
  
  if (!phone) {
    return Response.json({ success: false, message: 'Phone required' }, { status: 400 });
  }
  
  try {
    // Виклик вашого API для перевірки балансу
    const response = await fetch(`${env.ACTUAL_BALANCE_API}/check-balance?phone=${encodeURIComponent(phone)}`, {
      headers: {
        'Authorization': `Bearer ${env.API_TOKEN}`,
        'Content-Type': 'application/json',
      },
    });
    
    const data = await response.json();
    return Response.json(data);
  } catch (error) {
    return Response.json({ success: false, message: 'Server error' }, { status: 500 });
  }
}

// Обробка генерації промокоду
async function handlePromoGeneration(request, env) {
  if (request.method !== 'POST') {
    return Response.json({ success: false, message: 'Method not allowed' }, { status: 405 });
  }
  
  try {
    const { amount } = await request.json();
    
    if (!amount || amount <= 0) {
      return Response.json({ success: false, message: 'Invalid amount' }, { status: 400 });
    }
    
    // Виклик вашого API для генерації промокоду
    const response = await fetch(env.ACTUAL_PROMO_API, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${env.API_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ amount }),
    });
    
    const data = await response.json();
    return Response.json(data);
  } catch (error) {
    return Response.json({ success: false, message: 'Server error' }, { status: 500 });
  }
}
