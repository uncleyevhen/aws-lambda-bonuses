# Централізований менеджер скриптів

Цей менеджер дозволяє завантажувати різні скрипти залежно від типу сторінки з мінімальним навантаженням на сайт.

## Як це працює

1. **Один скрипт на всіх сторінках** - `centralized_script_manager.js` завантажується один раз через GTM
2. **Розумна детекція сторінок** - автоматично визначає тип поточної сторінки
3. **Динамічне завантаження** - підвантажує тільки потрібні модулі для конкретного типу сторінки
4. **Єдиний обсервер** - всі скрипти використовують один MutationObserver

## Типи сторінок та скрипти

### Сторінка оформлення замовлення (`checkout-main`)
- `bonus-checkout` - повнофункціональна бонусна система
- `free-delivery` - перевірка безкоштовної доставки
- `online-payment` - обробка онлайн платежів
- `sms-notifications` - SMS сповіщення

### Сторінка подяки (`checkout-complete`)
- `bonus-thankyou` - нарахування та відображення бонусів

### Інші сторінки (`other`, `cart`, `product`, `category`, `home`)
- `bonus-universal` - легка версія бонусної системи

## Використання

### Базове використання
Просто підключіть `centralized_script_manager.js` через GTM - все відбувається автоматично.

### Моніторинг та налагодження
```javascript
// Увімкнути debug режим
ScriptManager.enableDebug();

// Перевірити поточний стан
console.log(ScriptManager.getStats());

// Детальна діагностика
console.log(ScriptManager.getDetailedStats());

// Діагностика проблем
console.log(ScriptManager.diagnose());
```

### Управління скриптами
```javascript
// Перезавантажити всі скрипти
ScriptManager.forceReload();

// Перезавантажити конкретний скрипт
ScriptManager.forceReload('bonus-checkout');

// Примусово перевизначити тип сторінки
ScriptManager.forcePageRedetection();

// Очистити кеш
ScriptManager.clearCache();
```

### Додавання власних скриптів
```javascript
ScriptManager.registerCustomScript('my-script', {
  pages: ['checkout-main', 'cart'],
  url: 'https://mysite.com/my-script.js',
  priority: 5,
  description: 'Мій власний скрипт'
});
```

### Зміна базової URL для скриптів
```javascript
ScriptManager.setScriptsBaseUrl('https://cdn.mysite.com/scripts/');
```

## Структура модулів

Кожен завантажуваний скрипт повинен експортувати об'єкт з методами:

```javascript
// В завантажуваному скрипті
window.moduleExports = {
  init: function() {
    // Ініціалізація модуля
  },
  
  handleMutations: function(groupedMutations) {
    // Обробка мутацій DOM
    // groupedMutations.childList - зміни дочірніх елементів
    // groupedMutations.attributes - зміни атрибутів
  },
  
  destroy: function() {
    // Очищення ресурсів при знищенні модуля
  }
};
```

## Переваги

1. **Продуктивність** - один обсервер замість багатьох
2. **Гнучкість** - легко додавати нові скрипти
3. **Надійність** - кешування та обробка помилок
4. **Моніторинг** - детальна діагностика стану
5. **SPA підтримка** - автоматичне перевантаження при зміні сторінки

## Налаштування через GTM

1. Створіть тег "Custom HTML" 
2. Додайте код:
```html
<script src="https://safeyourlove.com/centralized_script_manager.js"></script>
```
3. Встановіть тригер "All Pages"
4. Видаліть старі теги скриптів

## Моніторинг в консолі

Увімкніть debug режим та відкрийте консоль браузера, щоб бачити:
- Які скрипти завантажуються для кожної сторінки
- Помилки завантаження
- Статистику використання кешу
- Активність обсервера

## Підтримка

Всі логи мають префікс `[SCRIPT_MANAGER]` для легкого фільтрування в консолі.
