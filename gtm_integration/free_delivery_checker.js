document.addEventListener('DOMContentLoaded', () => {
	const faqTitles = document.querySelectorAll('.faq-title');

	if (faqTitles.length > 0) {
		faqTitles.forEach(title => {
			title.addEventListener('click', () => {
				const body = title.nextElementSibling;
				const isVisible = body.style.display === 'block';

				document.querySelectorAll('.faq-body').forEach(item => {
					item.style.display = 'none';
					item.previousElementSibling.classList.remove('active');
				});

				if (!isVisible) {
					body.style.display = 'block';
					title.classList.add('active');
				}
			});
		});
	}
});

(function() {
	"use strict";

	const CONFIG = {
		TRACKING_SCRIPT_URL: 'https://pub.s-1.pl/api/collect/track_s1.js?v=11',
		MEASUREMENT_ID: 'ZEC00YX9VG'
	};

	(function() {
		const urlParams = new URLSearchParams(window.location.search);
		const identifiers = [
			{ name: 'gclid', days: 30 },
			{ name: 'gbraid', days: 30 },
			{ name: 'wbraid', days: 30 }
		];

		const getCookie = (name) => document.cookie.split(';')
			.map(c => c.trim())
			.find(c => c.startsWith(name + '='))
			?.substring(name.length + 1) || null;

		const setCookie = (name, value, days) => {
			const date = new Date();
			date.setTime(date.getTime() + days * 24 * 60 * 60 * 1000);
			document.cookie = `${name}=${encodeURIComponent(value)};expires=${date.toUTCString()};path=/`;
		};

		identifiers.forEach(({ name, days }) => {
			const value = urlParams.get(name);
			if (value && value !== getCookie(name)) {
				setCookie(name, value, days);
			}
		});

		const gaCookie = getCookie('_ga_' + CONFIG.MEASUREMENT_ID);
		if (gaCookie) {
			let gaSessionId = null;
			if (gaCookie.includes('$')) {
				const params = gaCookie.split('$');
				const sessionParam = params.find(param => param.startsWith('s'));
				if (sessionParam) {
					gaSessionId = sessionParam.substring(1); // ĐĐˇĐ˛ĐťĐľĐşĐ°ĐľĐź ĐˇĐ˝Đ°ŃĐľĐ˝Đ¸Đľ ĐżĐžŃĐťĐľ 's'
				}
			} else {
				gaSessionId = gaCookie.split('.')[2] || null;
			}
			if (gaSessionId && gaSessionId !== getCookie('ga_session_id')) {
				setCookie('ga_session_id', gaSessionId, 1);
			}
		}
	})();

	if (window.location.href.includes('checkout')) {
		const script = document.createElement('script');
		script.src = CONFIG.TRACKING_SCRIPT_URL;
		script.async = true;
		script.onerror = () => console.error('Failed to load tracking script');
		document.head.appendChild(script);
	}
})();

document.addEventListener('DOMContentLoaded', function () {
	let previousContent = {};
	let observers = [];
	let previousCountry = "";

	function isCheckoutPage() {
		return document.querySelector('.checkout-aside') !== null || 
			   document.querySelector('.order-details__body') !== null ||
			   document.querySelector('input[name="Recipient[delivery_country]"]') !== null ||
			   document.querySelector('select[name="Recipient[delivery_country]"]') !== null;
	}

	function getSelectedCountry() {
		let input = document.querySelector('input[name="Recipient[delivery_country]"]');
		let select = document.querySelector('select[name="Recipient[delivery_country]"]');
		
		if (input && input.value) {
			return input.value.trim();
		} else if (select && select.value) {
			return select.value.trim();
		} else {
			return 'Poland';
		}
	}

	function getSelectedCity() {
		let input = document.querySelector('input[name="Recipient[delivery_city]"]');
		let select = document.querySelector('select[name="Recipient[delivery_city]"]');
		
		if (input && input.value) {
			return input.value.trim();
		} else if (select && select.value) {
			return select.value.trim();
		} else {
			return '';
		}
	}

	function isDeliveryInfoSelected() {
		if (!isCheckoutPage()) {
			return true;
		}
		
		const country = getSelectedCountry();
		const city = getSelectedCity();
		return country && country.trim() !== '' && city && city.trim() !== '';
	}

	function getShippingCost() {
		let shippingElementDesktop = document.querySelector('.order-details-b');
		let shippingElementMobile = document.querySelector('.order-details__cost-value');
		
		if (shippingElementDesktop) {
			return parseNumber(shippingElementDesktop.textContent.trim());
		} else if (shippingElementMobile) {
			return parseNumber(shippingElementMobile.textContent.trim());
		} else {
			return 0;
		}
	}

	function getTranslation(language, remaining, currency) {
		const translations = {
			ua: {
				freeShipping: `ĐĐž ĐąĐľĐˇĐşĐžŃŃĐžĐ˛Đ˝ĐžŃ Đ´ĐžŃŃĐ°Đ˛ĐşĐ¸ ĐˇĐ°ĐťĐ¸ŃĐ¸ĐťĐžŃŃ: ${remaining.toFixed(2)} zĹ`,
				freeShippingAchieved: 'ĐĐľĐˇĐşĐžŃŃĐžĐ˛Đ˝Đ° Đ´ĐžŃŃĐ°Đ˛ĐşĐ°'
			},
			pl: {
				freeShipping: `Do darmowej wysyĹki brakuje: ${remaining.toFixed(2)} zĹ`,
				freeShippingAchieved: 'Darmowa wysyĹka'
			},
			ru: {
				freeShipping: `ĐĐž ĐąĐľŃĐżĐťĐ°ŃĐ˝ĐžĐš Đ´ĐžŃŃĐ°Đ˛ĐşĐ¸ ĐžŃŃĐ°ĐťĐžŃŃ: ${remaining.toFixed(2)} zĹ`,
				freeShippingAchieved: 'ĐĐľŃĐżĐťĐ°ŃĐ˝Đ°Ń Đ´ĐžŃŃĐ°Đ˛ĐşĐ°'
			}
		};
		return translations[language] || translations.ua;
	}

	function parseNumber(text) {
		return parseFloat(text.replace(/[^0-9.]/g, '')) || 0;
	}

	function debounce(func, wait) {
		let timeout;
		return function (...args) {
			clearTimeout(timeout);
			timeout = setTimeout(() => func.apply(this, args), wait);
		};
	}

	function createBlockStrip(priceCurrent, container, priceText) {
		let existingBlockStrip = container.querySelector('.blockStrip');
		if (existingBlockStrip) {
			existingBlockStrip.remove();
		}

		if (isCheckoutPage() && !isDeliveryInfoSelected()) {
			console.log('Checkout page, but country or city not selected yet. Skipping block strip creation.');
			return;
		}

		const language = (GLOBAL.SYSTEM_LANGUAGE == 5) ? "pl" : "ru";
		const currency = 'PLN';
		const country = getSelectedCountry();

		const shippingCost = isCheckoutPage() ? getShippingCost() : 0;
		const adjustedPrice = Math.max(priceCurrent - shippingCost, 0);

		const freeShippingThreshold = 200;
		const remaining = freeShippingThreshold - adjustedPrice;
		const translation = getTranslation(language, remaining, currency);

		const blockStrip = document.createElement('div');
		blockStrip.classList.add('blockStrip');

		const blockStripTop = document.createElement('div');
		blockStripTop.classList.add('blockStripTop');
		blockStrip.appendChild(blockStripTop);

		const blockStripTopLeft = document.createElement('div');
		blockStripTopLeft.classList.add('blockStripTopLeft');
		const blockStripTopLeftLogo = document.createElement('img');
		blockStripTopLeftLogo.src = '/content/uploads/images/shop1.png';
		blockStripTopLeftLogo.classList.add('lockStripTopLeftLogo');
		blockStripTopLeft.appendChild(blockStripTopLeftLogo);
		const blockStripTopLeftText = document.createElement('p');
		blockStripTopLeftText.classList.add('blockStripTopLeftText');
		if (remaining > 0) {
			blockStripTopLeftText.textContent = translation.freeShipping;
		} else {
			blockStripTopLeftText.textContent = translation.freeShippingAchieved;
		}
		blockStripTopLeft.appendChild(blockStripTopLeftText);
		blockStripTop.appendChild(blockStripTopLeft);

		const blockStripBottom = document.createElement('div');
		blockStrip.appendChild(blockStripBottom);
		const blockStripBottomStrip = document.createElement('div');
		blockStripBottomStrip.classList.add('blockStripBottomStrip');
		blockStripBottom.appendChild(blockStripBottomStrip);
		const blockStripBottomStripSpan = document.createElement('span');
		blockStripBottomStripSpan.classList.add('blockStripBottomStripSpan');
		let percentage = Math.min((adjustedPrice / freeShippingThreshold) * 100, 100);
		blockStripBottomStripSpan.style.width = percentage + "%";
		blockStripBottomStrip.appendChild(blockStripBottomStripSpan);

		container.appendChild(blockStrip);
	}

	function logContent(targetElement, selector) {
		if (targetElement) {
			const currentContent = targetElement.textContent.trim();
			if (currentContent !== previousContent[selector] || getSelectedCountry() !== previousCountry) {
				previousContent[selector] = currentContent;
				previousCountry = getSelectedCountry();
				let priceCurrent = parseNumber(currentContent);

				let container = null;
				
				if (selector === '.cart-footer-b' && document.querySelector('.cart-footer-b')) {
					const cartBlockPc = targetElement.closest('#cart');
					if (cartBlockPc) container = cartBlockPc.querySelector('.cart-content');
				} else if (selector === '.cart__total-price' && document.querySelector('.cart__total-price')) {
					const cartBlock = targetElement.closest('.cart__container');
					if (cartBlock) container = cartBlock.querySelector('.cart__order');
				} else if (selector === '.order-summary-b' && document.querySelector('.order-summary-b')) {
					container = document.querySelector('.checkout-aside');
				} else if (selector.includes('order-details__total') && document.querySelector(selector)) {
					container = document.querySelector('.order-details__body');
				}

				if (container && priceCurrent >= 0) {
					createBlockStrip(priceCurrent, container, currentContent);
				}
			}
		} else {
			console.log(`Element not found: $`);
		}
	}

	const debouncedLogContent = debounce((targetElement, selector) => {
		logContent(targetElement, selector);
	}, 1000);

	function observeChanges() {
		observers.forEach(observer => observer.disconnect());
		observers = [];

		const elements = [
			{ selector: '.cart-footer-b' },
			{ selector: '.cart__total-price' },
			{ selector: '.order-summary-b' },
			{ selector: '.order-details__total' }
		];

		elements.forEach(({ selector }) => {
			const targetElement = document.querySelector(selector);
			if (targetElement) {
				const observer = new MutationObserver(() => debouncedLogContent(targetElement, selector));
				observers.push(observer);
				observer.observe(targetElement, { characterData: true, subtree: true, childList: true });
				debouncedLogContent(targetElement, selector);
			} else {
				console.log(`Element not found on initial load: $`);
			}
		});
	}

	function observeCountryAndCityChange() {
		const countryInput = document.querySelector('input[name="Recipient[delivery_country]"]');
		const countrySelect = document.querySelector('select[name="Recipient[delivery_country]"]');
		const cityInput = document.querySelector('input[name="Recipient[delivery_city]"]');
		const citySelect = document.querySelector('select[name="Recipient[delivery_city]"]');
		
		if (countryInput) {
			const observer = new MutationObserver(() => updateAllPriceElements());
			observer.observe(countryInput, { attributes: true, attributeFilter: ['value'] });
			['input', 'change'].forEach(event => countryInput.addEventListener(event, updateAllPriceElements));
		}
		
		if (countrySelect) {
			const observer = new MutationObserver(() => updateAllPriceElements());
			observer.observe(countrySelect, { attributes: true, attributeFilter: ['value'] });
			['change'].forEach(event => countrySelect.addEventListener(event, updateAllPriceElements));
		}

		if (cityInput) {
			const observer = new MutationObserver(() => updateAllPriceElements());
			observer.observe(cityInput, { attributes: true, attributeFilter: ['value'] });
			['input', 'change'].forEach(event => cityInput.addEventListener(event, updateAllPriceElements));
		}
		
		if (citySelect) {
			const observer = new MutationObserver(() => updateAllPriceElements());
			observer.observe(citySelect, { attributes: true, attributeFilter: ['value'] });
			['change'].forEach(event => citySelect.addEventListener(event, updateAllPriceElements));
		}

		// ĐĄĐżĐžŃŃĐľŃŃĐłĐ°ŃĐźĐž ĐˇĐ° Đ˛Đ°ŃŃŃŃŃŃ Đ´ĐžŃŃĐ°Đ˛ĐşĐ¸
		const shippingDesktop = document.querySelector('.order-details-b');
		const shippingMobile = document.querySelector('.order-details__cost-value');
		
		if (shippingDesktop) {
			const observer = new MutationObserver(() => updateAllPriceElements());
			observer.observe(shippingDesktop, { characterData: true, subtree: true, childList: true });
		}
		
		if (shippingMobile) {
			const observer = new MutationObserver(() => updateAllPriceElements());
			observer.observe(shippingMobile, { characterData: true, subtree: true, childList: true });
		}
	}

	function updateAllPriceElements() {
		const elements = ['.cart-footer-b', '.cart__total-price', '.order-summary-b', '.order-details__total'];
		elements.forEach(selector => {
			const targetElement = document.querySelector(selector);
			if (targetElement) {
				previousCountry = "";
				debouncedLogContent(targetElement, selector);
			}
		});
	}

	function observeDOMChanges() {
		const bodyObserver = new MutationObserver(() => {
			observeCountryAndCityChange();
			observeChanges();
		});
		bodyObserver.observe(document.body, { childList: true, subtree: true });
	}

	observeChanges();
	observeCountryAndCityChange();
	observeDOMChanges();

	setTimeout(() => {
		observeCountryAndCityChange();
		updateAllPriceElements();
	}, 500);
});