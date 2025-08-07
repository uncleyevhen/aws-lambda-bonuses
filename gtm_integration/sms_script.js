(function() {
    'use strict';
    
    // Only run on checkout pages
    if (!window.location.pathname.toLowerCase().includes('/checkout')) {
        return;
    }
    
    // ===============================
    // DYNAMIC STYLES INJECTION
    // ===============================
    
    /**
     * Inject dynamic styles into the page
     */
    function injectStyles() {
        if (document.getElementById('sms-verification-styles')) {
            return; // Styles already injected
        }
        
        const style = document.createElement('style');
        style.id = 'sms-verification-styles';
        style.textContent = `
            /* Utility classes */
            .h { display: none !important; }
            .o50 { opacity: 0.5; }
            .dc { background-color: #f9f9f9; color: #999; }
            .v { border: 1px solid #28a745 !important; }
            .e { border: 1px solid #dc3545 !important; }
            
            /* SMS status container */
            #ssc {
                display: flex;
                align-items: center;
                gap: 8px;
                margin: 4px 0;
            }
            
            /* Resend link styles */
            #srl { 
                text-decoration: underline;
                cursor: pointer;
                color: #007bff;
                font-size: 14px;
            }
            #srl:hover {
                color: #0056b3;
            }
            #srl.ld {
                pointer-events: none;
                color: #999;
                text-decoration: none;
                cursor: default;
            }
            
            /* Enhanced button styles - Purple theme like site buttons */
            #sms-button {
                background: linear-gradient(135deg, #AB028E 0%, #dd03b8 100%);
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 6px;
                font-size: 14px;
                font-weight: 500;
                cursor: pointer;
                transition: all 0.2s ease;
                box-shadow: rgba(0,0,0,0.08) 0 2px 4px, rgba(50,50,93,0.25) 0px 6px 12px -2px, rgba(0,0,0,0.3) 0px 3px 7px -3px;
                white-space: nowrap;
                min-width: 120px;
                text-align: center;
                display: block;
                margin: 0 auto;
            }
            
            #sms-button:hover {
                background: linear-gradient(135deg, #ac018f 0%, #E805C2 100%);
                box-shadow: rgba(50,50,93,0.25) 0px 6px 12px -2px, rgba(0,0,0,0.3) 0px 3px 7px -3px;
                transform: translateY(-1px);
            }
            
            #sms-button:active {
                background: linear-gradient(135deg, #AB028E 0%, #8b0270 100%);
                transform: translateY(0);
                box-shadow: rgba(0,0,0,0.08) 0 2px 4px;
            }
            
            #sms-button:disabled {
                background: linear-gradient(135deg, #6c757d 0%, #5a6268 100%);
                cursor: not-allowed;
                transform: none;
                box-shadow: rgba(0,0,0,0.08) 0 2px 4px;
                opacity: 0.65;
            }
            
            #sms-button:disabled:hover {
                background: linear-gradient(135deg, #6c757d 0%, #5a6268 100%);
                transform: none;
                box-shadow: rgba(0,0,0,0.08) 0 2px 4px;
            }
            
            /* SMS form container adjustments */
            .sms-form-row {
                display: block;
                width: 100%;
                margin-bottom: 4px;
                text-align: left;
            }
            
            .sms-input-wrapper {
                display: block;
                position: relative;
                width: 100%;
                margin-bottom: 10px;
            }
            
            .sms-input-wrapper input {
                width: 100%;
                box-sizing: border-box;
            }
            
            /* Success state styling */
            #sms-status.success {
                color: #28a745;
                font-weight: 500;
            }
            
            /* Error state styling */
            #sms-status.error {
                color: #dc3545;
                font-weight: 500;
            }
            
            /* Loading animation for button */
            @keyframes buttonPulse {
                0% { opacity: 1; }
                50% { opacity: 0.7; }
                100% { opacity: 1; }
            }
            
            #sms-button.loading {
                animation: buttonPulse 1.5s ease-in-out infinite;
            }
        `;
        
        document.head.appendChild(style);
    }
    
    // ===============================
    // STATE AND CONFIGURATION
    // ===============================
    
    // Main state object
    let state = {
        sent: false,
        verifying: false,
        countdownInterval: null,
        lastPhone: null,
        phoneInput: null,
        smsContainer: null,
        retryTimer: null,
        googleTrafficResult: undefined,
        lastCheckTime: 0
    };
    
    // Element cache for performance
    const elementCache = new Map();
    
    // Universal storage object with localStorage fallback to cookies
    const storage = {
        isLocalStorageAvailable: null,
        
        // Check localStorage availability
        checkLocalStorageAvailability: function() {
            if (this.isLocalStorageAvailable !== null) {
                return this.isLocalStorageAvailable;
            }
            
            try {
                const testKey = '__test__';
                window.localStorage.setItem(testKey, testKey);
                window.localStorage.removeItem(testKey);
                this.isLocalStorageAvailable = true;
            } catch (e) {
                this.isLocalStorageAvailable = false;
                console.warn('localStorage недоступний. Використовується cookies як фолбек.', e);
            }
            
            return this.isLocalStorageAvailable;
        },
        
        // Set data
        set: function(name, value, expireSeconds) {
            if (this.checkLocalStorageAvailability()) {
                try {
                    const data = {
                        value: value,
                        expire: expireSeconds ? Date.now() + (expireSeconds * 1000) : null
                    };
                    localStorage.setItem(name, JSON.stringify(data));
                } catch (e) {
                    console.warn('Не вдалося зберегти дані в localStorage, використовується cookies:', e);
                    cookies.set(name, value, expireSeconds);
                }
            } else {
                cookies.set(name, value, expireSeconds);
            }
        },
        
        // Get data
        get: function(name) {
            if (this.checkLocalStorageAvailability()) {
                try {
                    const item = localStorage.getItem(name);
                    if (!item) return null;
                    
                    const data = JSON.parse(item);
                    if (data.expire && Date.now() > data.expire) {
                        this.remove(name);
                        return null;
                    }
                    
                    return data.value;
                } catch (e) {
                    console.warn('Не вдалося отримати дані з localStorage, використовується cookies:', e);
                    return cookies.get(name);
                }
            } else {
                return cookies.get(name);
            }
        },
        
        // Remove data
        remove: function(name) {
            if (this.checkLocalStorageAvailability()) {
                try {
                    localStorage.removeItem(name);
                } catch (e) {
                    console.warn('Не вдалося видалити дані з localStorage, використовується cookies:', e);
                    cookies.remove(name);
                }
            } else {
                cookies.remove(name);
            }
        }
    };
    
    // Cookie utility object (fallback)
    const cookies = {
        set: (name, value, expireSeconds) => {
            let expires = '';
            if (expireSeconds) {
                const date = new Date();
                date.setTime(date.getTime() + expireSeconds * 1000);
                expires = '; expires=' + date.toUTCString();
            }
            document.cookie = name + '=' + (value || '') + expires + '; path=/';
        },
        
        get: (name) => {
            const nameEQ = name + "=";
            const ca = document.cookie.split(";");
            for (let i = 0; i < ca.length; i++) {
                const c = ca[i].trim();
                if (c.indexOf(nameEQ) === 0) {
                    return c.substring(nameEQ.length, c.length);
                }
            }
            return null;
        },
        
        remove: (name) => {
            document.cookie = name + '=; Max-Age=-99999999; path=/';
        }
    };
    
    // ===============================
    // UTILITY FUNCTIONS
    // ===============================
    
    /**
     * Get element by ID or selector with caching
     */
    function getElement(id, selector) {
        if (elementCache.has(id)) {
            const element = elementCache.get(id);
            if (document.contains(element)) {
                return element;
            }
            elementCache.delete(id);
        }
        
        const element = selector 
            ? document.querySelector(selector) 
            : document.getElementById(id);
            
        if (element) {
            elementCache.set(id, element);
        }
        
        return element;
    }
    
    /**
     * Throttle function to prevent excessive calls
     */
    function throttle(func, limit) {
        let inThrottle;
        return function(...args) {
            if (!inThrottle) {
                func.apply(this, args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    }
    
    /**
     * Check if current page is Russian version
     */
    function isRussianLocale() {
        return window.location.pathname.includes('/ru/');
    }
    
    /**
     * Get localized text strings
     */
    function getTexts() {
        const isRussian = isRussianLocale();
        return {
            phoneConfirmation: isRussian ? "Подтверждение телефона" : "Підтвердження телефону",
            enterSmsCode: isRussian ? "Введите SMS-код" : "Введіть SMS-код",
            getConfirmationCode: isRussian ? "Получить код подтверждения" : "Отримати код підтвердження",
            sendAgain: isRussian ? "Отправить повторно" : "Відправити повторно",
            confirm: isRussian ? "Подтвердить" : "Підтвердити",
            sendingCode: isRussian ? "Отправка кода..." : "Відправка коду...",
            codeSent: isRussian ? "Код отправлен!" : "Код відправлено!",
            phoneConfirmed: isRussian ? "Телефон успешно подтвержден!" : "Телефон успішно підтверджено!",
            verifying: isRussian ? "Проверка..." : "Перевірка...",
            waitVerifying: isRussian ? "Подождите, идет проверка кода..." : "Зачекайте, йде перевірка коду...",
            incorrectCode: isRussian ? "Неправильный код" : "Неправильний код",
            error: isRussian ? "Ошибка" : "Помилка",
            parseError: isRussian ? "Ошибка разбора ответа" : "Помилка розбору відповіді",
            retryIn: isRussian ? "Повторно через " : "Повторно через ",
            seconds: isRussian ? "с" : "с"
        };
    }
    
    /**
     * Decode Unicode escape sequences
     */
    function decodeUnicode(str) {
        return str ? str.replace(/\\u[\dA-F]{4}/gi, match => 
            String.fromCharCode(parseInt(match.replace(/\\u/g, ''), 16))
        ) : '';
    }
    
    /**
     * Get phone input element
     */
    function getPhoneInput() {
        if (!state.phoneInput || !document.contains(state.phoneInput)) {
            state.phoneInput = getElement('phone-input', 'input.j-phone') || 
                              getElement('phone-input-tel', 'input[type="tel"]');
        }
        return state.phoneInput;
    }
    
    /**
     * Get SMS container element
     */
    function getSmsContainer() {
        if (!state.smsContainer || !document.contains(state.smsContainer)) {
            state.smsContainer = getElement('sms-code-container');
        }
        return state.smsContainer;
    }
    
    /**
     * Check if traffic comes from Google
     */
    function isGoogleTraffic() {
        if (state.googleTrafficResult !== undefined) {
            return state.googleTrafficResult;
        }
        
        const currentCookie = cookies.get('sbjs_current');
        
        if (!currentCookie) {
            const referrer = document.referrer;
            const search = window.location.search;
            const isGoogle = referrer.includes('google.') || 
                           search.includes('utm_source=google') || 
                           search.includes('gclid=');
            state.googleTrafficResult = isGoogle;
            return isGoogle;
        }
        
        try {
            const decoded = decodeURIComponent(currentCookie);
            const isGoogle = decoded.includes('src=google');
            state.googleTrafficResult = isGoogle;
            return isGoogle;
        } catch (e) {
            state.googleTrafficResult = false;
            return false;
        }
    }
    
    // ===============================
    // SMS FORM CREATION
    // ===============================
    
    /**
     * Create SMS verification form
     */
    function createSmsForm(container) {
        const texts = getTexts();
        const hasFormItemTitle = container.querySelector('.form-item__title');
        
        if (hasFormItemTitle) {
            // Create form-item structure
            const wrapper = document.createElement('div');
            wrapper.className = 'form-item h';
            wrapper.id = 'sms-code-container';
            
            const title = document.createElement('div');
            title.className = 'form-item__title h';
            title.id = 'sms-field-label';
            title.textContent = texts.phoneConfirmation;
            
            const content = document.createElement('div');
            content.className = 'form-item__content';
            content.innerHTML = 
                '<div class="sms-form-row">' +
                    '<div class="sms-input-wrapper">' +
                        '<input type="text" ' +
                               'autocomplete="one-time-code" ' +
                               'tabindex="0" ' +
                               'class="input dc h" ' +
                               'id="sms-code" ' +
                               'name="sms_code" ' +
                               'placeholder="' + texts.enterSmsCode + '" ' +
                               'disabled>' +
                    '</div>' +
                    '<button type="button" id="sms-button" class="h">' +
                        texts.getConfirmationCode +
                    '</button>' +
                '</div>' +
                '<div id="ssc">' +
                    '<span id="sms-status"></span>' +
                    '<a id="srl" href="#" class="h">' + texts.sendAgain + '</a>' +
                '</div>';
            
            wrapper.appendChild(title);
            wrapper.appendChild(content);
            container.parentNode.insertBefore(wrapper, container.nextSibling);
        } else {
            // Create dt/dd structure
            const dt = document.createElement('dt');
            dt.className = 'form-head h';
            dt.id = 'sms-field-label';
            dt.textContent = texts.phoneConfirmation;
            
            const dd = document.createElement('dd');
            dd.className = 'form-item h';
            dd.id = 'sms-code-container';
            dd.innerHTML = 
                '<div class="sms-form-row">' +
                    '<div class="sms-input-wrapper">' +
                        '<input class="field dc h" ' +
                               'autocomplete="one-time-code" ' +
                               'type="text" ' +
                               'id="sms-code" ' +
                               'name="sms_code" ' +
                               'placeholder="' + texts.enterSmsCode + '" ' +
                               'disabled>' +
                    '</div>' +
                    '<button type="button" id="sms-button" class="h">' +
                        texts.getConfirmationCode +
                    '</button>' +
                '</div>' +
                '<div id="ssc">' +
                    '<span id="sms-status"></span>' +
                    '<a id="srl" href="#" class="h">' + texts.sendAgain + '</a>' +
                '</div>';
            
            container.parentNode.insertBefore(dt, container.nextSibling);
            dt.parentNode.insertBefore(dd, dt.nextSibling);
        }
        
        attachEventListeners();
    }
    
    /**
     * Attach event listeners to SMS form elements
     */
    function attachEventListeners() {
        const button = getElement('sms-button');
        const resendLink = getElement('srl');
        
        if (button && !button.hasAttribute('data-listeners-attached')) {
            button.addEventListener('click', handleButtonClick);
            button.addEventListener('touchend', (e) => {
                e.preventDefault();
                handleButtonClick();
            });
            button.setAttribute('data-listeners-attached', 'true');
        }
        
        if (resendLink && !resendLink.hasAttribute('data-listeners-attached')) {
            resendLink.addEventListener('click', handleResendClick);
            resendLink.setAttribute('data-listeners-attached', 'true');
        }
    }
    
    // ===============================
    // EVENT HANDLERS
    // ===============================
    
    /**
     * Handle SMS button click
     */
    function handleButtonClick() {
        if (!state.sent) {
            sendSms();
        } else {
            verifySms();
        }
    }
    
    /**
     * Handle resend link click
     */
    function handleResendClick(e) {
        e.preventDefault();
        const resendLink = e.target;
        resendLink.classList.add('ld');
        sendSms();
        resendLink.classList.add('h');
    }
    
    // ===============================
    // SMS OPERATIONS
    // ===============================
    
    /**
     * Send SMS verification code
     */
    function sendSms() {
        const texts = getTexts();
        const phoneInput = getPhoneInput();
        
        if (!phoneInput) return;
        
        const phoneDigits = phoneInput.value.replace(/\D/g, '');
        const button = getElement('sms-button');
        const status = getElement('sms-status');
        const resendLink = getElement('srl');
        
        button.textContent = texts.sendingCode;
        button.disabled = true;
        button.classList.add('loading');
        
        if (resendLink) {
            resendLink.classList.add('ld');
        }
        
        // TODO: Verify if this endpoint needs to be updated after function renaming
        const xhr = new XMLHttpRequest();
        xhr.open('POST', 'https://6rcy9a367a.execute-api.eu-north-1.amazonaws.com/default/SMS_Autorisation', true);
        xhr.setRequestHeader('Content-Type', 'application/json');
        
        xhr.onreadystatechange = function() {
            if (xhr.readyState === 4) {
                button.disabled = false;
                button.classList.remove('loading');
                handleSmsResponse(xhr, phoneDigits);
            }
        };
        
        xhr.send(JSON.stringify({ phone: phoneDigits }));
    }
    
    /**
     * Handle SMS send response
     */
    function handleSmsResponse(xhr, phoneDigits) {
        const texts = getTexts();
        const button = getElement('sms-button');
        const status = getElement('sms-status');
        const codeInput = getElement('sms-code');
        
        if (xhr.status === 200) {
            try {
                const response = JSON.parse(xhr.responseText || '{}');
                const message = decodeUnicode(response.message);
                
                if (response.message && message.includes('успішно')) {
                    if (status) {
                        status.textContent = texts.codeSent;
                        status.classList.remove('h');
                        status.classList.add('success');
                    }
                    
                    if (codeInput) {
                        codeInput.removeAttribute('disabled');
                        codeInput.classList.remove('dc', 'e', 'h');
                    }
                    
                    button.textContent = texts.confirm;
                    button.classList.remove('h');
                    state.sent = true;
                    
            // Save state to storage (localStorage or cookies)
            storage.set('iss', 'true', 300);
            storage.set('ls', texts.codeSent, 300);
            storage.set('sp', phoneDigits, 300);
                    
                    startCountdown();
                    return;
                } else {
                    if (status) {
                        status.textContent = texts.error + ': ' + message;
                        status.classList.add('error');
                    }
                    button.textContent = texts.getConfirmationCode;
                }
            } catch (e) {
                if (status) {
                    status.textContent = texts.parseError;
                    status.classList.add('error');
                }
                button.textContent = texts.getConfirmationCode;
            }
        } else {
            try {
                const error = JSON.parse(xhr.responseText || '{}');
                const errorMessage = decodeUnicode(error.message);
                if (status) {
                    status.textContent = texts.error + ': ' + errorMessage;
                    status.classList.add('error');
                }
            } catch (e) {
                if (status) {
                    status.textContent = texts.error + ': ' + xhr.responseText;
                    status.classList.add('error');
                }
            }
            button.textContent = texts.getConfirmationCode;
        }
    }
    
    /**
     * Verify SMS code
     */
    function verifySms() {
        const texts = getTexts();
        const codeInput = getElement('sms-code');
        const button = getElement('sms-button');
        const status = getElement('sms-status');
        const resendLink = getElement('srl');
        
        if (!codeInput) return;
        
        state.verifying = true;
        
        if (resendLink) {
            resendLink.classList.add('h');
        }
        
        button.disabled = true;
        button.textContent = texts.verifying;
        button.classList.add('loading');
        
        if (status) {
            status.textContent = texts.waitVerifying;
            status.classList.remove('success', 'error');
        }
        
        const code = codeInput.value;
        
        verifyCode(code, function(xhr) {
            button.disabled = false;
            button.classList.remove('loading');
            state.verifying = false;
            
            if (!xhr) return;
            
            const response = JSON.parse(xhr.responseText || '{}');
            const message = decodeUnicode(response.message);
            
            if (xhr.status === 200) {
                if (status) {
                    status.classList.remove('h', 'error');
                    status.classList.add('success');
                    status.textContent = texts.phoneConfirmed;
                }
                
                codeInput.disabled = true;
                codeInput.classList.remove('e');
                codeInput.classList.add('v');
                
                const phoneInput = getPhoneInput();
                if (phoneInput) {
                    const container = phoneInput.closest('.js-phone-container') || 
                                    phoneInput.closest('.form-item');
                    enableAllElements(container);
                }
                
                button.classList.add('h');
                
                if (resendLink) {
                    resendLink.classList.add('h');
                }
                
                // Save verification state
                storage.set('pv', 'true', 2592000); // 30 days
                storage.set('vp', phoneInput.value.replace(/\D/g, ''), 2592000);
                
                // Clean up temporary storage
                storage.remove('iss');
                storage.remove('ls');
                storage.remove('sp');
                
                codeInput.classList.add('h');
                
                if (state.countdownInterval) {
                    clearInterval(state.countdownInterval);
                    state.countdownInterval = null;
                }
                
                storage.remove('sce');
            } else {
                if (status) {
                    status.classList.remove('success');
                    status.classList.add('error');
                    status.textContent = texts.incorrectCode;
                }
                
                storage.set('ls', texts.incorrectCode, 300);
                button.textContent = texts.confirm;
                codeInput.classList.add('e');
                
                if (resendLink) {
                    resendLink.classList.remove('h');
                }
                
                updateCountdown(true);
            }
        });
    }
    
    /**
     * Send verification request to API
     */
    function verifyCode(code, callback) {
        const phoneInput = getPhoneInput();
        if (!phoneInput) return;
        
        const phoneDigits = phoneInput.value.replace(/\D/g, '');
        
        // TODO: Verify if this endpoint needs to be updated after function renaming
        const xhr = new XMLHttpRequest();
        xhr.open('POST', 'https://6rcy9a367a.execute-api.eu-north-1.amazonaws.com/default/SMS_Verification', true);
        xhr.setRequestHeader('Content-Type', 'application/json');
        
        xhr.onreadystatechange = function() {
            if (xhr.readyState === 4) {
                callback(xhr);
            }
        };
        
        xhr.send(JSON.stringify({ phone: phoneDigits, code: code }));
    }
    
    // ===============================
    // COUNTDOWN TIMER
    // ===============================
    
    /**
     * Start countdown timer
     */
    function startCountdown() {
        if (state.countdownInterval) {
            clearInterval(state.countdownInterval);
        }
        
        const endTime = Date.now() + 30 * 1000;
        storage.set('sce', endTime, 300);
        updateCountdown(true);
    }
    
    /**
     * Update countdown display
     */
    function updateCountdown(force) {
        const texts = getTexts();
        const button = getElement('sms-button');
        const resendLink = getElement('srl');
        
        if (state.countdownInterval) {
            clearInterval(state.countdownInterval);
        }
        
        function tick() {
            const savedEndTime = storage.get('sce');
            
            if (!savedEndTime) {
                clearInterval(state.countdownInterval);
                state.countdownInterval = null;
                return;
            }
            
            const timeLeft = parseInt(savedEndTime, 10) - Date.now();
            
            if (timeLeft > 0) {
                const secondsLeft = Math.ceil(timeLeft / 1000);
                
                if (resendLink) {
                    if (state.verifying) {
                        resendLink.classList.add('h');
                    } else {
                        resendLink.classList.remove('h');
                        resendLink.textContent = texts.retryIn + secondsLeft + texts.seconds;
                        resendLink.classList.add('ld');
                    }
                }
                
                if (!state.sent) {
                    button.disabled = true;
                } else {
                    button.disabled = false;
                    button.textContent = texts.confirm;
                }
            } else {
                clearInterval(state.countdownInterval);
                state.countdownInterval = null;
                storage.remove('sce');
                
                if (resendLink) {
                    resendLink.classList.remove('ld');
                    resendLink.textContent = texts.sendAgain;
                }
                
                if (!state.sent) {
                    button.disabled = false;
                }
            }
        }
        
        if (force) tick();
        state.countdownInterval = setInterval(tick, 1000);
    }
    
    /**
     * Check and restore countdown on page load
     */
    function checkCountdown() {
        const savedEndTime = storage.get('sce');
        
        if (savedEndTime) {
            const timeLeft = parseInt(savedEndTime, 10) - Date.now();
            
            if (timeLeft > 0) {
                updateCountdown(true);
            } else {
                storage.remove('sce');
                const resendLink = getElement('srl');
                if (resendLink) {
                    resendLink.classList.remove('h');
                }
            }
        }
    }
    
    // ===============================
    // STATE MANAGEMENT
    // ===============================
    
    /**
     * Check if phone is already verified
     */
    function checkVerificationState() {
        const texts = getTexts();
        const isVerified = storage.get('pv');
        
        if (isVerified === 'true') {
            const codeInput = getElement('sms-code');
            if (codeInput) codeInput.classList.add('h');
            
            const button = getElement('sms-button');
            if (button) button.classList.add('h');
            
            const resendLink = getElement('srl');
            if (resendLink) resendLink.classList.add('h');
            
            const status = getElement('sms-status');
            if (status) {
                status.textContent = texts.phoneConfirmed;
                status.classList.add('success');
            }
            
            const phoneInput = getPhoneInput();
            if (phoneInput) {
                const container = phoneInput.closest('.js-phone-container') || 
                                phoneInput.closest('.form-item');
                enableAllElements(container);
            }
        }
    }
    
    /**
     * Check if SMS was sent for current phone
     */
    function checkSentState() {
        const texts = getTexts();
        const smsSent = storage.get('iss');
        const savedPhone = storage.get('sp');
        const phoneInput = getPhoneInput();
        
        if (!phoneInput) return;
        
        const currentPhone = phoneInput.value.replace(/\D/g, '');
        
        if (smsSent === 'true' && savedPhone === currentPhone) {
            state.sent = true;
            
            const codeInput = getElement('sms-code');
            if (codeInput) {
                codeInput.removeAttribute('disabled');
                codeInput.classList.remove('dc', 'e', 'h');
            }
            
            const button = getElement('sms-button');
            if (button) {
                button.textContent = texts.confirm;
                button.disabled = false;
                button.classList.remove('h');
            }
            
            const lastStatus = storage.get('ls');
            if (lastStatus) {
                const status = getElement('sms-status');
                if (status) {
                    status.textContent = lastStatus;
                    status.classList.remove('h');
                    if (lastStatus === texts.codeSent) {
                        status.classList.add('success');
                    }
                }
            }
        } else {
            // Clean up if phone changed
            storage.remove('iss');
            storage.remove('ls');
            storage.remove('sp');
        }
    }
    
    // ===============================
    // UI STATE MANAGEMENT
    // ===============================
    
    /**
     * Disable elements after the specified element
     */
    function disableAfterElement(element) {
        const allElements = document.querySelectorAll('.form-item, .form-head');
        let found = false;
        
        for (const el of allElements) {
            if (el === element) {
                found = true;
                continue;
            }
            
            if (found) {
                // Skip coupon/promo code blocks
                const isPromoBlock = el.closest('.order-details__block--discount') || 
                                   el.classList.contains('j-coupon-add-form') ||
                                   el.querySelector('.j-coupon-input') ||
                                   el.querySelector('.j-coupon-add') ||
                                   el.querySelector('.j-coupon-submit');
                
                if (!isPromoBlock) {
                    el.classList.add('o50');
                    el.style.pointerEvents = 'none';
                }
            }
        }
    }
    
    /**
     * Enable elements after the specified element
     */
    function enableAllElements(element) {
        const allElements = document.querySelectorAll('.form-item, .form-head');
        let found = false;
        
        for (const el of allElements) {
            if (el === element) {
                found = true;
                continue;
            }
            
            if (found) {
                // Always remove disabled state, including from promo blocks
                el.classList.remove('o50');
                el.style.pointerEvents = 'auto';
            }
        }
    }
    
    /**
     * Reset SMS form state
     */
    function resetPhoneState() {
            storage.remove('sce');
            storage.remove('iss');
            storage.remove('ls');
            storage.remove('sp');
            storage.remove('pv');
            storage.remove('vp');
        
        if (state.countdownInterval) {
            clearInterval(state.countdownInterval);
            state.countdownInterval = null;
        }
        
        state.sent = false;
        
        const codeInput = getElement('sms-code');
        const button = getElement('sms-button');
        const status = getElement('sms-status');
        const resendLink = getElement('srl');
        
        if (codeInput) {
            codeInput.classList.add('h');
            codeInput.classList.remove('dc', 'e', 'v');
            codeInput.value = '';
        }
        
        if (button) {
            const texts = getTexts();
            button.textContent = texts.getConfirmationCode;
            button.disabled = false;
            button.classList.remove('h');
        }
        
        if (status) {
            status.textContent = '';
            status.classList.add('h');
            status.classList.remove('success', 'error');
        }
        
        if (resendLink) {
            resendLink.classList.add('h');
            resendLink.classList.remove('ld');
        }
    }
    
    // ===============================
    // MAIN INITIALIZATION
    // ===============================
    
    /**
     * Initialize SMS verification for phone input
     */
    function initializePhoneVerification() {
        // Inject styles first
        injectStyles();
        
        const phoneInput = getPhoneInput();
        if (!phoneInput) return;
        
        const container = phoneInput.closest('.js-phone-container') || 
                         phoneInput.closest('.form-item');
        if (!container) return;
        
        const existingContainer = getSmsContainer();
        
        if (existingContainer) {
            attachPhoneListeners(phoneInput);
            checkVerificationState();
            checkSentState();
            checkCountdown();
            return;
        }
        
        disableAfterElement(container);
        createSmsForm(container);
        attachPhoneListeners(phoneInput);
        checkVerificationState();
        checkSentState();
        checkCountdown();
    }
    
    /**
     * Attach listeners to phone input
     */
    function attachPhoneListeners(phoneInput) {
        const texts = getTexts();
        const elements = {};
        
        const handlePhoneChange = throttle(() => {
            // Cache elements for performance
            const label = elements.label || getElement('sms-field-label');
            const smsContainer = elements.container || getSmsContainer();
            const button = elements.button || getElement('sms-button');
            const status = elements.status || getElement('sms-status');
            const codeInput = elements.code || getElement('sms-code');
            
            // Update cache
            if (label) elements.label = label;
            if (smsContainer) elements.container = smsContainer;
            if (button) elements.button = button;
            if (status) elements.status = status;
            if (codeInput) elements.code = codeInput;
            
            const currentPhone = phoneInput.value.replace(/\D/g, '');
            const container = smsContainer;
            
            const isPhoneVerified = (cookies.get('pv') === 'true');
            const verifiedPhone = cookies.get('vp');
            const savedPhone = cookies.get('sp');
            
            const isValidPhone = (phoneInput.value.indexOf('_') === -1 && currentPhone.length >= 10);
            
            // If phone is already verified and matches current input
            if (isPhoneVerified && verifiedPhone === currentPhone) {
                if (smsContainer) smsContainer.classList.remove('h');
                if (label) label.classList.remove('h');
                if (codeInput) codeInput.classList.add('h');
                if (button) button.classList.add('h');
                
                const resendLink = getElement('srl');
                if (resendLink) {
                    resendLink.classList.add('h');
                    resendLink.classList.remove('ld');
                }
                
                if (status) {
                    status.classList.remove('h');
                    status.classList.add('success');
                    status.textContent = texts.phoneConfirmed;
                }
                
                if (container) {
                    enableAllElements(container);
                }
                
                state.lastPhone = currentPhone;
                return;
            }
            
            // If phone changed, reset state
            if (state.lastPhone && state.lastPhone !== currentPhone) {
                resetPhoneState();
                if (smsContainer) smsContainer.classList.add('h');
                if (label) label.classList.add('h');
                if (container) disableAfterElement(container);
            }
            
            state.lastPhone = currentPhone;
            
            // Show SMS form for valid phone numbers
            if (isValidPhone) {
                if (smsContainer) smsContainer.classList.remove('h');
                
                if (button) {
                    button.classList.remove('h');
                }
                
                if (label) label.classList.remove('h');
                
                if (status) {
                    status.textContent = '';
                    status.classList.add('h');
                    status.classList.remove('success', 'error');
                }
                
                if (container) {
                    disableAfterElement(container);
                }
            } else {
                // Hide SMS form for invalid phones
                if (button) button.classList.add('h');
                if (codeInput) codeInput.classList.add('h');
                if (smsContainer) smsContainer.classList.add('h');
                if (label) label.classList.add('h');
                
                if (status) {
                    status.textContent = '';
                    status.classList.add('h');
                    status.classList.remove('success', 'error');
                }
                
                if (container) disableAfterElement(container);
            }
        }, 100);
        
        phoneInput.addEventListener('input', handlePhoneChange, { passive: true });
        phoneInput.addEventListener('change', handlePhoneChange, { passive: true });
        
        phoneInput.addEventListener('blur', function() {
            const currentValue = phoneInput.value.replace(/\D/g, '');
            const previousValue = cookies.get('vp') || '';
            
            if (currentValue !== previousValue && currentValue.length >= 10) {
                resetPhoneState();
            }
            
            setTimeout(handlePhoneChange, 100);
        }, { passive: true });
        
        // Initial call
        handlePhoneChange();
    }
    
    /**
     * Check if SMS verification should be initialized
     */
    function checkInitialization() {
        if (!isGoogleTraffic()) {
            return false;
        }
        
        const phoneInput = getPhoneInput();
        const smsContainer = getSmsContainer();
        
        if (phoneInput && !smsContainer) {
            initializePhoneVerification();
            return true;
        }
        
        return false;
    }
    
    // ===============================
    // STARTUP AND OBSERVERS
    // ===============================
    
    // Initialize if Google traffic detected
    if (isGoogleTraffic()) {
        initializePhoneVerification();
        
        // Set up mutation observer for dynamic content
        if (!state.observer) {
            state.observer = new MutationObserver(throttle((mutations) => {
                const now = Date.now();
                if (now - state.lastCheckTime < 200) return;
                state.lastCheckTime = now;
                
                let needsReinit = false;
                const phoneInput = getPhoneInput();
                const smsContainer = getSmsContainer();
                
                // Check if phone changed while verified
                if (phoneInput) {
                    const currentValue = phoneInput.value.replace(/\D/g, '');
                    const savedValue = cookies.get('vp') || '';
                    
                    if (currentValue !== savedValue && currentValue.length >= 10) {
                        const isVerified = cookies.get('pv');
                        if (isVerified) {
                            needsReinit = true;
                        }
                    }
                }
                
                // Check if phone input exists but SMS container doesn't
                if (phoneInput && !smsContainer) {
                    needsReinit = true;
                } else {
                    // Check for specific DOM changes that require reinit
                    for (const mutation of mutations) {
                        if (mutation.type === 'childList') {
                            for (const node of mutation.addedNodes) {
                                if (node.nodeType === 1 && node.className) {
                                    const className = node.className;
                                    if (className.includes('order-details') || 
                                        className.includes('bonus-block')) {
                                        needsReinit = true;
                                        break;
                                    }
                                }
                            }
                            if (needsReinit) break;
                        }
                    }
                }
                
                if (needsReinit) {
                    if (state.retryTimer) {
                        clearTimeout(state.retryTimer);
                    }
                    
                    state.retryTimer = setTimeout(() => {
                        checkInitialization();
                        state.retryTimer = null;
                    }, 150);
                }
            }, 200));
            
            state.observer.observe(document.body, {
                childList: true,
                subtree: true
            });
            
            // Periodic check every 10 seconds
            setInterval(checkInitialization, 10000);
            
            // Check on window focus
            window.addEventListener('focus', () => {
                setTimeout(checkInitialization, 100);
            }, { passive: true });
        }
    }
    
    // Cleanup on page unload
    window.addEventListener('beforeunload', () => {
        if (state.observer) {
            state.observer.disconnect();
        }
        
        if (state.countdownInterval) {
            clearInterval(state.countdownInterval);
        }
        
        if (state.retryTimer) {
            clearTimeout(state.retryTimer);
        }
    });
    
})();