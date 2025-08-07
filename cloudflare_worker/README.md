# Інструкції з розгортання обфускованого бонусного скрипта

## 1. Підготовка Cloudflare Worker

### Встановлення Wrangler CLI
```bash
npm install -g wrangler
wrangler login
```

### Налаштування проекту
```bash
cd cloudflare_worker
wrangler init bonus-worker
# Замініть згенерований код на bonus_worker.js
```

### Розгортання
```bash
# Розгортання на staging
wrangler deploy --env development

# Розгортання на production
wrangler deploy --env production
```

## 2. Налаштування GTM

### Замість оригінального скрипта, додайте в GTM:
```javascript
(function(){
  'use strict';
  var _0x4a2b = ['aHR0cHM6Ly9ib251cy1hcGkueW91cmRvbWFpbi5jb20=', 'c2NyaXB0', 'aGVhZA==', 'c3Jj'];
  var _0x5c3d = function(a, b) { return atob(a); };
  var _0x6e4f = _0x5c3d(_0x4a2b[0]);
  
  var _0x7a5b = window.location.hostname;
  var _0x8c6d = ['yourdomain.com', 'staging.yourdomain.com'];
  
  if (!_0x8c6d.some(function(d) { return _0x7a5b.includes(d); })) return;
  
  function _0x9d7e() {
    var s = document.createElement(_0x5c3d(_0x4a2b[1]));
    s[_0x5c3d(_0x4a2b[3])] = _0x6e4f + '/bonus.js?v=' + Date.now();
    s.async = true;
    s.defer = true;
    document[_0x5c3d(_0x4a2b[2])].appendChild(s);
  }
  
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _0x9d7e);
  } else {
    _0x9d7e();
  }
})();
```

## 3. Обфускація основного скрипта

### Запуск обфускатора
```bash
cd scripts
node obfuscator.js
```

### Інтеграція обфускованого коду в Worker
1. Скопіюйте вміст `obfuscated_bonus_script.js`
2. Замініть плейсхолдер у `getObfuscatedMainScript()` у `bonus_worker.js`
3. Перерозгорніть Worker

## 4. Переваги цього підходу

### Безпека
- Основний код завантажується з Cloudflare Worker
- Рефер-перевірки запобігають несанкціонованому використанню
- API токени зберігаються як environment variables
- Обфускований код важко прочитати

### Продуктивність
- Кешування на рівні Cloudflare Edge
- Швидке завантаження через CDN
- Мінімальний код у GTM

### Гнучкість
- Можна оновлювати логіку без зміни GTM
- A/B тестування через Worker
- Динамічна конфігурація

## 5. Додаткові заходи безпеки

### Domain lock
Додайте перевірку домену в Worker:
```javascript
const allowedDomains = ['yourdomain.com'];
const referer = request.headers.get('referer');
if (!allowedDomains.some(domain => referer?.includes(domain))) {
  return new Response('Forbidden', { status: 403 });
}
```

### Rate limiting
```javascript
// Додайте до Worker для захисту від DDOS
const rateLimiter = new Map();
const clientIP = request.headers.get('CF-Connecting-IP');
// Реалізуйте логіку rate limiting
```

### Timestamp validation
```javascript
// Додайте валідацію часу для запобігання replay атак
const timestamp = url.searchParams.get('t');
const now = Date.now();
if (Math.abs(now - parseInt(timestamp)) > 300000) { // 5 хвилин
  return new Response('Request expired', { status: 400 });
}
```

## 6. Моніторинг

### Cloudflare Analytics
- Переглядайте запити та помилки в Cloudflare Dashboard
- Налаштуйте алерти для критичних помилок

### Логування
```javascript
console.log('Bonus script loaded', { 
  timestamp: new Date().toISOString(),
  referer: request.headers.get('referer'),
  userAgent: request.headers.get('user-agent')
});
```

## 7. Backup план

У випадку проблем з Worker, майте запасний варіант:
- Зберігайте мінімальну версію скрипта у GTM
- Додайте fallback логіку в loader скрипт
