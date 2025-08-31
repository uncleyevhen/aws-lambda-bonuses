# Логіка позиціонування блоку безкоштовної доставки

## Схема роботи позиціонування

```
📍 СЕЛЕКТОР ЦІНИ → 📦 КОНТЕЙНЕР → 🎯 МІСЦЕ ВСТАВКИ
```

## 1. КОШИК (CART)

### Десктоп кошик:
```
🔍 Селектор: .cart-footer-b
📦 Контейнер: .cart-footer (closest від елемента)
🎯 Вставка: ПІСЛЯ .cart-summary
```

### Мобільний кошик:
```
🔍 Селектор: .cart__total-price
📦 Контейнер: .cart__summary (в .cart__container)
🎯 Вставка: ПІСЛЯ .cart-summary
```

## 2. CHECKOUT

### Десктоп checkout:
```
🔍 Селектор: .order-summary-b
📦 Контейнер: .checkout-aside
🎯 Вставка: ПІСЛЯ .order-details-i.j-delivery-commission
```

### Мобільний checkout:
```
🔍 Селектор: .order-details__total
📦 Контейнер: .order-details__body
🎯 Вставка: ПІСЛЯ .order-details__cost
```

## 3. FALLBACK стратегія

```
1. ✅ Основна позиція (cart-summary / delivery-commission / order-details__cost)
2. ⚠️  Fallback 1: В кінець .checkout-aside
3. ⚠️  Fallback 2: В кінець .order-details__body  
4. ⚠️  Fallback 3: В кінець контейнера
```

## 4. Детальна схема HTML структури

### КОШИК:
```html
<td class="cart-footer"> <!-- 📦 КОНТЕЙНЕР -->
  <div class="cart-summary">...</div> <!-- 🎯 ПІСЛЯ ЦЬОГО -->
  <div class="free-delivery-block">...</div> <!-- ✨ НАШ БЛОК -->
</td>
```

### CHECKOUT ДЕСКТОП:
```html
<div class="checkout-aside"> <!-- 📦 КОНТЕЙНЕР -->
  <div class="order-details-i j-delivery-commission">...</div> <!-- 🎯 ПІСЛЯ ЦЬОГО -->
  <div class="free-delivery-block">...</div> <!-- ✨ НАШ БЛОК -->
</div>
```

### CHECKOUT МОБІЛЬНИЙ:
```html
<div class="order-details__body"> <!-- 📦 КОНТЕЙНЕР -->
  <div class="order-details__cost">...</div> <!-- 🎯 ПІСЛЯ ЦЬОГО -->
  <div class="free-delivery-block">...</div> <!-- ✨ НАШ БЛОК -->
</div>
```

## 5. Логіка спрацювання

1. **Визначення типу сторінки** (кошик vs checkout)
2. **Визначення пристрою** (десктоп vs мобільний)  
3. **Пошук відповідного селектора** ціни
4. **Знаходження контейнера** для блоку
5. **Пошук елементу для позиціонування** (cart-summary, delivery-commission, etc.)
6. **Вставка блоку** після знайденого елементу
7. **Fallback** якщо основний елемент не знайдено

## 6. Налагодження

Для відстеження в консолі:
- `🛒 Desktop cart` - десктоп кошик
- `📱 Mobile cart` - мобільний кошик  
- `💻 Desktop checkout` - десктоп checkout
- `📱 Mobile checkout` - мобільний checkout
- `✅ Block inserted` - успішна вставка
- `⚠️ Block appended` - fallback вставка
