/**
 * Калькулятор безкоштовної доставки для інтернет-магазину
 * Версія: 1.1
 * Безкоштовна доставка від 3000 грн
 * Підтримка української та російської мов
 */

(function() {
    'use strict';

    // Конфігурація
    const CONFIG = {
        FREE_SHIPPING_THRESHOLD: 3000,
        CURRENCY: 'грн',
        LOGO_URL: '/content/uploads/images/shop1.png',
        DEBUG: true
    };

    // Стан калькулятора
    let state = {
        previousContent: {},
        observers: [],
        bodyObserver: null, // Окремо зберігаємо body observer
        previousCountry: "",
        isInitialized: false,
        updateInProgress: false,
        observersInitialized: false // Флаг для запобігання повторної ініціалізації
    };

    // Логування
    function log(message, data) {
        if (!CONFIG.DEBUG) return;
        console.log('[FREE_DELIVERY_CALC]', message, data || '');
    }

    function logError(message, error) {
        console.error('[FREE_DELIVERY_CALC] ERROR:', message, error || '');
    }

    /**
     * Перевірка чи це сторінка оформлення замовлення
     */
    function isCheckoutPage() {
        return document.querySelector('.checkout-aside') !== null || 
               document.querySelector('.order-details__body') !== null ||
               document.querySelector('input[name="Recipient[delivery_country]"]') !== null ||
               document.querySelector('select[name="Recipient[delivery_country]"]') !== null;
    }

    /**
     * Отримання обраної країни доставки
     */
    function getSelectedCountry() {
        const input = document.querySelector('input[name="Recipient[delivery_country]"]');
        const select = document.querySelector('select[name="Recipient[delivery_country]"]');
        
        if (input && input.value) {
            return input.value.trim();
        } else if (select && select.value) {
            return select.value.trim();
        } else {
            return 'Україна';
        }
    }

    /**
     * Отримання обраного міста доставки
     */
    function getSelectedCity() {
        const input = document.querySelector('input[name="Recipient[delivery_city]"]');
        const select = document.querySelector('select[name="Recipient[delivery_city]"]');
        
        if (input && input.value) {
            return input.value.trim();
        } else if (select && select.value) {
            return select.value.trim();
        } else {
            return '';
        }
    }

    /**
     * Перевірка чи обрана інформація про доставку
     */
    function isDeliveryInfoSelected() {
        if (!isCheckoutPage()) {
            return true;
        }
        
        const country = getSelectedCountry();
        const city = getSelectedCity();
        return country && country.trim() !== '' && city && city.trim() !== '';
    }

    /**
     * Отримання вартості доставки
     */
    function getShippingCost() {
        const shippingElementDesktop = document.querySelector('.order-details-b');
        const shippingElementMobile = document.querySelector('.order-details__cost-value');
        
        if (shippingElementDesktop) {
            return parseNumber(shippingElementDesktop.textContent.trim());
        } else if (shippingElementMobile) {
            return parseNumber(shippingElementMobile.textContent.trim());
        } else {
            return 0;
        }
    }

    /**
     * Отримання мови інтерфейсу (аналогічно іншим скриптам)
     */
    function getCurrentLanguage() {
        var url = window.location.href;
        return url.includes('/ru/') ? 'ru' : 'uk';
    }

    /**
     * Перевірка чи це мобільний пристрій
     */
    function isMobileDevice() {
        // Більш точна перевірка для мобільних пристроїв
        const userAgent = navigator.userAgent || navigator.vendor || window.opera;
        const isMobileUA = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(userAgent);
        const isMobileWidth = window.innerWidth <= 768;
        
        // Додаткова перевірка на touch-пристрої
        const isTouchDevice = 'ontouchstart' in window || navigator.maxTouchPoints > 0;
        
        // Вважаємо пристрій мобільним тільки якщо це справді мобільний UA або маленький екран з touch
        return isMobileUA || (isMobileWidth && isTouchDevice);
    }

    /**
     * Отримання локалізованого тексту
     */
    function getLocalizedText(key, language, remaining = 0) {
        const isMobile = isMobileDevice();
        
        const translations = {
            'free_shipping': {
                'uk': isMobile 
                    ? `До безкоштовної доставки залишилось:`
                    : `До безкоштовної доставки залишилось: ${remaining.toFixed(0)} ${CONFIG.CURRENCY}`,
                'ru': isMobile 
                    ? `До бесплатной доставки осталось:`
                    : `До бесплатной доставки осталось: ${remaining.toFixed(0)} ${CONFIG.CURRENCY}`
            },
            'free_shipping_mobile': {
                'uk': `До безкоштовної доставки залишилось:`,
                'ru': `До бесплатной доставки осталось:`
            },
            'free_shipping_desktop': {
                'uk': `До безкоштовної доставки залишилось: ${remaining.toFixed(0)} ${CONFIG.CURRENCY}`,
                'ru': `До бесплатной доставки осталось: ${remaining.toFixed(0)} ${CONFIG.CURRENCY}`
            },
            'free_shipping_amount': {
                'uk': `${remaining.toFixed(0)} ${CONFIG.CURRENCY}`,
                'ru': `${remaining.toFixed(0)} ${CONFIG.CURRENCY}`
            },
            'free_shipping_achieved': {
                'uk': '🎉 Безкоштовна доставка',
                'ru': '🎉 Бесплатная доставка'
            }
        };
        return translations[key] && translations[key][language] ? translations[key][language] : translations[key]['uk'];
    }

    /**
     * Парсинг числа з тексту
     */
    function parseNumber(text) {
        return parseFloat(text.replace(/[^0-9.]/g, '')) || 0;
    }

    /**
     * Debounce функція для оптимізації
     */
    function debounce(func, wait) {
        let timeout;
        return function (...args) {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), wait);
        };
    }

    /**
     * Функція для діагностики структури існуючого блоку
     */
    function analyzeBlockStructure(block) {
        if (!block) return null;
        
        const topSection = block.querySelector('div');
        const spans = block.querySelectorAll('span');
        const amountElement = block.querySelector('div[style*="font-size: 18px"]');
        const headerRow = block.querySelector('div[style*="margin-bottom: 6px"]');
        
        return {
            hasTopSection: !!topSection,
            topSectionStyle: topSection ? topSection.style.cssText : null,
            flexDirection: topSection ? topSection.style.flexDirection : null,
            spansCount: spans.length,
            hasAmountElement: !!amountElement,
            hasHeaderRow: !!headerRow,
            amountElementText: amountElement ? amountElement.textContent : null,
            firstSpanText: spans[0] ? spans[0].textContent : null
        };
    }

    /**
     * Створення блоку з індикатором безкоштовної доставки
     */
    function createFreeDeliveryBlock(currentPrice, container) {
        // Захист від зациклення
        if (state.updateInProgress) {
            log('Update already in progress, skipping');
            return;
        }
        
        state.updateInProgress = true;
        
        try {
            // На checkout сторінці показуємо калькулятор завжди
            // На інших сторінках показуємо без перевірки доставки
            
            const language = getCurrentLanguage();
            const shippingCost = isCheckoutPage() ? getShippingCost() : 0;
            const adjustedPrice = Math.max(currentPrice - shippingCost, 0);
            const remaining = CONFIG.FREE_SHIPPING_THRESHOLD - adjustedPrice;

        // Розраховуємо прогрес (0-1)
        const progress = Math.min(adjustedPrice / CONFIG.FREE_SHIPPING_THRESHOLD, 1);
        
        // Створюємо динамічний градієнт залежно від прогресу
        const getProgressGradient = (progress) => {
            // Плавний горизонтальний градієнт який змінюється залежно від прогресу
            const intensity = progress * 100; // 0-100%
            const firstColor = '#203fdd'; // Фіолетовий
            const secondColor = '#ac018f'; // Рожевий
            if (progress === 0) {
                // При 0% - повністю рожевий
                return `linear-gradient(135deg, ${firstColor}, ${secondColor})`;
            } else if (progress >= 1) {
                // При 100% - повністю фіолетовий
                return `linear-gradient(135deg, ${firstColor} 0%, ${firstColor} 70%, ${secondColor} 95%, ${secondColor}ff 100%)`;
            } else {
                // Плавний перехід: фіолетовий зліва, поступово переходить у рожевий
                return `linear-gradient(135deg, 
                    ${firstColor} 0%, 
                    ${firstColor} ${intensity * 0.3}%,
                    ${secondColor} ${intensity}%, 
                    ${secondColor} 100%)`;
            }
        };

        log('Creating free delivery block', { 
            currentPrice, 
            adjustedPrice, 
            remaining, 
            progress,
            language,
            shippingCost,
            isFreeShipping: remaining <= 0
        });

        // Шукаємо існуючий блок
        let existingBlock = container.querySelector('.free-delivery-block');
        
        if (existingBlock) {
            // Перевіряємо, чи блок не був видалений під час попереднього оновлення
            if (!existingBlock.parentNode) {
                existingBlock = null;
                log('Existing block was removed, creating new one');
            }
        }
        
        if (existingBlock) {
            // Діагностика поточної структури блоку
            const blockAnalysis = analyzeBlockStructure(existingBlock);
            log('Existing block analysis', blockAnalysis);
            
            // Оновлюємо існуючий блок
            const progressBar = existingBlock.querySelector('.progress-bar');
            const percentage = Math.min((adjustedPrice / CONFIG.FREE_SHIPPING_THRESHOLD) * 100, 100);
            const actualIsMobile = isMobileDevice();
            
            // Знаходимо всі елементи
            const topSection = existingBlock.querySelector('div');
            const headerRow = existingBlock.querySelector('.delivery-header-row');
            const textElement = existingBlock.querySelector('.delivery-text');
            const amountElement = existingBlock.querySelector('.delivery-amount');
            
            // Простіше оновлення без складних анімацій
            if (textElement) {
                // Оновлюємо текст напряму без анімацій
                if (remaining > 0) {
                    // Використовуємо правильний текст для десктопу/мобільного
                    textElement.textContent = actualIsMobile 
                        ? getLocalizedText('free_shipping_mobile', language)
                        : getLocalizedText('free_shipping_desktop', language, remaining);
                } else {
                    textElement.textContent = getLocalizedText('free_shipping_achieved', language);
                }
            }
            
            // Оновлюємо формат відображення
            if (topSection) {
                if (actualIsMobile && remaining > 0) {
                    // Мобільний формат
                    topSection.style.flexDirection = 'column';
                    topSection.style.alignItems = 'center';
                    
                    // Показуємо amount елемент
                    if (amountElement) {
                        amountElement.style.display = 'block';
                        amountElement.textContent = getLocalizedText('free_shipping_amount', language, remaining);
                    }
                    
                    // Центруємо текст в headerRow
                    if (headerRow) {
                        headerRow.style.width = '100%';
                        headerRow.style.marginBottom = '6px';
                    }
                    
                    if (textElement) {
                        textElement.style.textAlign = 'center';
                        textElement.style.whiteSpace = 'normal';
                    }
                } else {
                    // Десктопний формат або безкоштовна доставка
                    topSection.style.flexDirection = 'row';
                    topSection.style.alignItems = 'center';
                    
                    // Приховуємо amount елемент
                    if (amountElement) {
                        amountElement.style.display = 'none';
                    }
                    
                    // Вирівнюємо headerRow
                    if (headerRow) {
                        headerRow.style.width = 'auto';
                        headerRow.style.marginBottom = '0';
                    }
                    
                    if (textElement) {
                        textElement.style.textAlign = 'left';
                        textElement.style.whiteSpace = 'pre-line';
                    }
                }
            }
            
            // Оновлюємо фон та прогрес-бар
            existingBlock.style.background = getProgressGradient(progress);
            if (progressBar) {
                progressBar.style.width = `${percentage}%`;
            }
            
            log('Updated existing free delivery block', {
                isMobile,
                remaining,
                textElementFound: !!textElement,
                topSectionFound: !!topSection
            });
            return;
        }

        // Створюємо основний блок
        const blockStrip = document.createElement('div');
        blockStrip.classList.add('free-delivery-block');
        
        // Створюємо блок повністю прихованим для уникнення дьоргання
        blockStrip.style.cssText = `
            background: ${getProgressGradient(progress)};
            border-radius: 8px;
            padding: 12px 16px;
            margin: 10px auto;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            color: white;
            font-family: Arial, sans-serif;
            transition: background 0.8s ease-in-out, box-shadow 0.3s ease, opacity 0.3s ease, transform 0.3s ease;
            width: 95%;
            opacity: 0;
            transform: translateY(10px);
            pointer-events: none;
        `;

        // Верхня частина з логотипом та текстом
        const topSection = document.createElement('div');
        const isMobile = isMobileDevice();
        
        // Завжди створюємо всі елементи, але приховуємо непотрібні
        topSection.style.cssText = `
            display: flex;
            flex-direction: ${isMobile && remaining > 0 ? 'column' : 'row'};
            align-items: center;
            margin-bottom: 8px;
            transition: all 0.3s ease;
        `;
        
        // Рядок з логотипом та текстом
        const headerRow = document.createElement('div');
        headerRow.classList.add('delivery-header-row');
        headerRow.style.cssText = `
            display: flex;
            align-items: center;
            width: ${isMobile && remaining > 0 ? '100%' : 'auto'};
            margin-bottom: ${isMobile && remaining > 0 ? '6px' : '0'};
            transition: all 0.3s ease;
        `;
        
        // Логотип
        const logo = document.createElement('img');
        logo.src = CONFIG.LOGO_URL;
        logo.style.cssText = `
            width: 24px;
            height: 24px;
            margin-right: 10px;
            border-radius: 4px;
        `;
        logo.onerror = function() {
            this.style.display = 'none';
        };
        
        // Текст
        const textElement = document.createElement('span');
        textElement.classList.add('delivery-text');
        const actualIsMobile = isMobileDevice();
        textElement.style.cssText = `
            font-size: 14px;
            font-weight: 600;
            flex: 1;
            text-align: ${actualIsMobile && remaining > 0 ? 'center' : 'left'};
            white-space: ${actualIsMobile && remaining > 0 ? 'normal' : 'pre-line'};
            line-height: 1.3;
        `;
        
        // Встановлюємо правильний текст одразу
        if (remaining > 0) {
            // Використовуємо окремі ключі для десктопу та мобільного
            textElement.textContent = actualIsMobile 
                ? getLocalizedText('free_shipping_mobile', language)
                : getLocalizedText('free_shipping_desktop', language, remaining);
        } else {
            textElement.textContent = getLocalizedText('free_shipping_achieved', language);
        }
        
        headerRow.appendChild(logo);
        headerRow.appendChild(textElement);
        topSection.appendChild(headerRow);
        
        // Цифра з валютою (створюємо завжди, але показуємо тільки коли потрібно)
        const amountElement = document.createElement('div');
        amountElement.classList.add('delivery-amount');
        amountElement.style.cssText = `
            font-size: 18px;
            font-weight: 700;
            text-align: center;
            color: #fff;
            text-shadow: 0 1px 2px rgba(0,0,0,0.3);
            display: ${isMobile && remaining > 0 ? 'block' : 'none'};
            transition: all 0.3s ease;
        `;
        amountElement.textContent = getLocalizedText('free_shipping_amount', language, remaining);
        topSection.appendChild(amountElement);
        
        blockStrip.appendChild(topSection);

        // Прогрес бар
        const progressContainer = document.createElement('div');
        progressContainer.style.cssText = `
            background: rgba(255,255,255,0.3);
            height: 6px;
            border-radius: 3px;
            overflow: hidden;
        `;

        const progressBar = document.createElement('div');
        const percentage = Math.min((adjustedPrice / CONFIG.FREE_SHIPPING_THRESHOLD) * 100, 100);
        
        // Прогрес бар починається з 0 для анімації
        progressBar.style.cssText = `
            background: rgba(255, 255, 255, 0.9);
            height: 100%;
            width: 0%;
            border-radius: 3px;
            transition: width 0.8s ease-out;
        `;
        progressBar.classList.add('progress-bar');

        progressContainer.appendChild(progressBar);
        blockStrip.appendChild(progressContainer);

        // Вставляємо блок в правильне місце залежно від типу сторінки та пристрою
        const cartSummary = container.querySelector('.cart-summary');
        const deliveryCommission = container.querySelector('.order-details-i.j-delivery-commission');
        const orderDetailsCost = container.querySelector('.order-details__cost');
        
        log('Positioning elements found:', {
            container: container.className || container.tagName,
            cartSummary: !!cartSummary,
            deliveryCommission: !!deliveryCommission,
            orderDetailsCost: !!orderDetailsCost,
            isMobile: isMobileDevice(),
            isCheckout: isCheckoutPage()
        });
        
        if (cartSummary) {
            // Кошик - вставляємо після cart-summary
            cartSummary.insertAdjacentElement('afterend', blockStrip);
            log('✅ Block inserted after cart-summary in cart');
        } else if (deliveryCommission) {
            // Десктоп checkout - вставляємо після order-details-i j-delivery-commission
            deliveryCommission.insertAdjacentElement('afterend', blockStrip);
            log('✅ Block inserted after delivery commission (desktop checkout)');
        } else if (orderDetailsCost) {
            // Мобільний checkout - вставляємо після order-details__cost
            orderDetailsCost.insertAdjacentElement('afterend', blockStrip);
            log('✅ Block inserted after order-details__cost (mobile checkout)');
        } else if (container.classList.contains('checkout-aside')) {
            // Fallback для checkout-aside - вставляємо в кінець
            container.appendChild(blockStrip);
            log('⚠️ Block appended to checkout-aside (fallback)');
        } else if (container.classList.contains('order-details__body')) {
            // Fallback для order-details__body - вставляємо в кінець
            container.appendChild(blockStrip);
            log('⚠️ Block appended to order-details__body (fallback)');
        } else {
            // Загальний випадок - додаємо в кінець контейнера
            container.appendChild(blockStrip);
            log('⚠️ Block appended to container (general fallback)', { containerClass: container.className });
        }
        
        // Плавна поява блоку після вставки в DOM
        requestAnimationFrame(() => {
            // Активуємо блок
            blockStrip.style.opacity = '1';
            blockStrip.style.transform = 'translateY(0)';
            blockStrip.style.pointerEvents = 'auto';
            
            // Запускаємо анімацію прогрес-бару після появи блоку
            requestAnimationFrame(() => {
                progressBar.style.width = `${percentage}%`;
            });
        });
        
        log('Free delivery block created successfully');
        
        } catch (error) {
            logError('Error in createFreeDeliveryBlock', error);
        } finally {
            // Скидаємо флаг наприкінці
            state.updateInProgress = false;
        }
    }

    /**
     * Оновлення блоку при зміні ціни
     */
    function updateFreeDeliveryDisplay(targetElement, selector) {
        if (!targetElement) {
            log(`Element not found: ${selector}`);
            return;
        }

        const currentContent = targetElement.textContent.trim();
        const currentCountry = getSelectedCountry();
        
        log('Update attempt', { 
            selector, 
            currentContent, 
            currentCountry, 
            isCheckoutPage: isCheckoutPage(),
            isDeliverySelected: isDeliveryInfoSelected()
        });
        
        // Перевіряємо чи змінилась ціна або країна
        if (currentContent === state.previousContent[selector] && currentCountry === state.previousCountry) {
            log('No changes detected, skipping update');
            return;
        }

        state.previousContent[selector] = currentContent;
        state.previousCountry = currentCountry;
        
        const currentPrice = parseNumber(currentContent);
        log('Price updated', { selector, currentPrice, currentContent });

        // Визначаємо контейнер для блоку
        let container = null;
        
        if (selector === '.cart-footer-b' && document.querySelector('.cart-footer-b')) {
            // КОШИК ДЕСКТОП: шукаємо td.cart-footer як контейнер для блоку
            container = targetElement.closest('.cart-footer');
            log('🛒 Desktop cart: Using .cart-footer container');
        } else if (selector === '.cart__total-price' && document.querySelector('.cart__total-price')) {
            // КОШИК МОБІЛЬНИЙ: використовуємо .cart__summary в .cart__container
            const cartBlock = targetElement.closest('.cart__container');
            if (cartBlock) container = cartBlock.querySelector('.cart__summary');
            log('📱 Mobile cart: Using .cart__summary container');
        } else if (selector === '.order-summary-b' && document.querySelector('.order-summary-b')) {
            // CHECKOUT ДЕСКТОП: використовуємо checkout-aside як контейнер
            const checkoutAside = document.querySelector('.checkout-aside');
            container = checkoutAside || targetElement.closest('.checkout-aside');
            log('💻 Desktop checkout: Using .checkout-aside container');
        } else if (selector.includes('order-details__total') && document.querySelector(selector)) {
            // CHECKOUT МОБІЛЬНИЙ: використовуємо order-details__body як контейнер
            const orderDetails = document.querySelector('.order-details__body');
            container = orderDetails || targetElement.closest('.order-details__body');
            log('📱 Mobile checkout: Using .order-details__body container');
        }

        if (container && currentPrice >= 0) {
            log('Container found for block creation', { selector, container: container.className });
            createFreeDeliveryBlock(currentPrice, container);
        } else {
            log('No suitable container found', { selector, currentPrice, container });
        }
    }

    /**
     * Debounced версія функції оновлення
     */
    const debouncedUpdate = debounce((targetElement, selector) => {
        updateFreeDeliveryDisplay(targetElement, selector);
    }, 150);

    /**
     * Спостереження за змінами цін
     */
    function observePriceChanges() {
        // Перевіряємо чи вже ініціалізовані спостерігачі
        if (state.observersInitialized) {
            log('Price observers already initialized, skipping');
            return;
        }

        // Відключаємо попередніх спостерігачів
        state.observers.forEach(observer => observer.disconnect());
        state.observers = [];

        const priceElements = [
            { selector: '.cart-footer-b' },
            { selector: '.cart__total-price' },
            { selector: '.order-summary-b' },
            { selector: '.order-details__total' }
        ];

        priceElements.forEach(({ selector }) => {
            const targetElement = document.querySelector(selector);
            if (targetElement) {
                log(`Setting up observer for: ${selector}`);
                
                const observer = new MutationObserver(() => {
                    debouncedUpdate(targetElement, selector);
                });
                
                state.observers.push(observer);
                observer.observe(targetElement, { 
                    characterData: true, 
                    subtree: true, 
                    childList: true 
                });
                
                // Ініціальне оновлення
                updateFreeDeliveryDisplay(targetElement, selector);
            } else {
                log(`Element not found on initial load: ${selector}`);
            }
        });

        state.observersInitialized = true;
    }

    /**
     * Спостереження за змінами країни та міста доставки
     */
    function observeDeliveryChanges() {
        const deliveryInputs = [
            'input[name="Recipient[delivery_country]"]',
            'select[name="Recipient[delivery_country]"]',
            'input[name="Recipient[delivery_city]"]',
            'select[name="Recipient[delivery_city]"]'
        ];

        deliveryInputs.forEach(selector => {
            const element = document.querySelector(selector);
            if (element) {
                log(`Setting up delivery observer for: ${selector}`);
                
                const observer = new MutationObserver(() => {
                    updateAllPriceElements();
                });
                observer.observe(element, { 
                    attributes: true, 
                    attributeFilter: ['value'] 
                });
                
                ['input', 'change'].forEach(event => {
                    element.addEventListener(event, updateAllPriceElements);
                });
            }
        });

        // Спостереження за вартістю доставки
        const shippingElements = [
            '.order-details-b',
            '.order-details__cost-value'
        ];

        shippingElements.forEach(selector => {
            const element = document.querySelector(selector);
            if (element) {
                log(`Setting up shipping observer for: ${selector}`);
                
                const observer = new MutationObserver(() => {
                    updateAllPriceElements();
                });
                observer.observe(element, { 
                    characterData: true, 
                    subtree: true, 
                    childList: true 
                });
            }
        });
    }

    /**
     * Спостереження за динамічними змінами DOM
     */
    function observeDOMChanges() {
        // Відключаємо попередній body observer якщо він існує
        if (state.bodyObserver) {
            state.bodyObserver.disconnect();
        }

        const checkForNewElements = debounce(() => {
            if (!state.isInitialized) return;
            
            // Перевіряємо чи з'явились нові елементи для спостереження
            const hasNewElements = ['.cart-footer-b', '.cart__total-price', '.order-summary-b', '.order-details__total']
                .some(selector => {
                    const element = document.querySelector(selector);
                    return element && !state.observers.some(obs => obs._targetSelector === selector);
                });

            if (hasNewElements) {
                log('New price elements detected, reinitializing observers');
                state.observersInitialized = false; // Дозволяємо переініціалізацію
                observePriceChanges();
            }

            // Спостерігаємо за змінами доставки
            observeDeliveryChanges();
        }, 500); // Більша затримка для оптимізації

        state.bodyObserver = new MutationObserver(checkForNewElements);
        
        state.bodyObserver.observe(document.body, { 
            childList: true, 
            subtree: true 
        });
    }

    /**
     * Оновлення всіх елементів цін
     */
    function updateAllPriceElements() {
        const priceSelectors = [
            '.cart-footer-b', 
            '.cart__total-price', 
            '.order-summary-b', 
            '.order-details__total'
        ];
        
        priceSelectors.forEach(selector => {
            const targetElement = document.querySelector(selector);
            if (targetElement) {
                // Скидаємо попередню країну щоб форсувати оновлення
                state.previousCountry = "";
                debouncedUpdate(targetElement, selector);
            }
        });
    }

    /**
     * Ініціалізація калькулятора
     */
    function init() {
        if (state.isInitialized) {
            log('Free delivery calculator already initialized');
            return;
        }

        log('Initializing free delivery calculator');
        
        state.isInitialized = true;
        
        // Спостерігаємо за цінами
        observePriceChanges();
        
        // Спостерігаємо за доставкою
        observeDeliveryChanges();
        
        // Спостерігаємо за DOM тільки один раз
        observeDOMChanges();

        // Затримане оновлення для гарантії завантаження
        setTimeout(() => {
            updateAllPriceElements();
        }, 500);

        log('Free delivery calculator initialized successfully');
    }

    /**
     * Очищення ресурсів
     */
    function cleanup() {
        // Відключаємо всіх спостерігачів
        state.observers.forEach(observer => observer.disconnect());
        state.observers = [];
        
        if (state.bodyObserver) {
            state.bodyObserver.disconnect();
            state.bodyObserver = null;
        }
        
        state.observersInitialized = false;
        state.isInitialized = false;
    }

    /**
     * Публічний API
     */
    window.FreeDeliveryCalculator = {
        init: init,
        updateAll: updateAllPriceElements,
        getThreshold: () => CONFIG.FREE_SHIPPING_THRESHOLD,
        isInitialized: () => state.isInitialized,
        cleanup: cleanup // Додаємо метод очищення
    };

    // Автоматична ініціалізація при завантаженні DOM
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

})();