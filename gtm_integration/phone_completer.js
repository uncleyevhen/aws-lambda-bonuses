(function () {
  var FORM_ID = 'checkout-container';
  var FLAG = '__af_patched';
  var cachedForm = null;
  var isSetupRunning = false;

  function getCleanVal(el) {
    try {
      if (el.inputmask && typeof el.inputmask.unmaskedvalue === 'function') {
        return el.inputmask.unmaskedvalue();
      }
    } catch (e) {}
    return el.value || '';
  }

  function getForm() {
    if (!cachedForm || !document.contains(cachedForm)) {
      cachedForm = document.getElementById(FORM_ID);
    }
    return cachedForm;
  }

  function ensureHiddenAfter(el, backendName) {
    var form = el.form || getForm();
    if (!form) return null;

    var hidden = form.querySelector('input[type="hidden"][name="' + backendName + '"]');
    if (!hidden) {
      hidden = document.createElement('input');
      hidden.type = 'hidden';
      hidden.name = backendName;
      el.parentNode.insertBefore(hidden, el.nextSibling);
    }
    return hidden;
  }

  function patchField(el, opts) {
    if (!el || el[FLAG]) return;
    el[FLAG] = true;

    // Batch set attributes for better performance
    var attributes = {
      autocomplete: opts.autocomplete,
      inputmode: opts.inputmode,
      autocapitalize: opts.autocapitalize,
      autocorrect: opts.autocorrect
    };
    
    for (var attr in attributes) {
      if (attributes[attr]) {
        el.setAttribute(attr, attributes[attr]);
      }
    }
    
    if (opts.spellcheck != null) el.setAttribute('spellcheck', String(opts.spellcheck));
    if (opts.id && !el.id) el.id = opts.id;
    if (opts.type) { 
      try { 
        el.type = opts.type; 
      } catch(e){} 
    }

    // hidden з бекенд-ім'ям
    var hidden = ensureHiddenAfter(el, opts.backendName);

    // перейменовуємо видиме поле на просте ім'я (любить Chrome/Safari)
    el.setAttribute('name', opts.visibleName);

    // дружба з маскою (для телефона)
    if (opts.tuneMask && el.inputmask && el.inputmask.opts) {
      try {
        var maskOpts = el.inputmask.opts;
        maskOpts.paste = true;
        maskOpts.showMaskOnFocus = false;
        maskOpts.showMaskOnHover = false;
        maskOpts.positionCaretOnClick = 'lvp';
      } catch (e) {}
    }

    // синхронізація hidden - use single handler
    function syncHidden() {
      if (hidden) {
        hidden.value = getCleanVal(el);
      }
    }
    
    // Add all listeners at once
    ['input', 'change', 'blur'].forEach(function(event) {
      el.addEventListener(event, syncHidden, false);
    });

    // «тихий» autofill у Safari/Chrome - optimized
    var tries = 0;
    var timer = setInterval(function () {
      tries++;
      syncHidden();
      if (tries > 15) clearInterval(timer); // ~1.5с
    }, 100);

    // остаточний sync - move form operations outside
    var form = getForm();
    if (form && !form.__af_submit_patched) {
      form.addEventListener('submit', syncHidden, false);
      form.setAttribute('autocomplete', 'on');
      form.__af_submit_patched = true;
    }
  }

  // Field configurations
  var fieldConfigs = [
    {
      selector: 'input.input.j-phone.j-phone-masked[name="Recipient[delivery_phone]"]',
      options: {
        visibleName: 'tel',
        backendName: 'Recipient[delivery_phone]',
        autocomplete: 'tel',
        inputmode: 'tel',
        autocapitalize: 'off',
        autocorrect: 'off',
        id: 'checkout-phone',
        tuneMask: true
      }
    },
    {
      selector: 'input.input[name="Recipient[delivery_name]"]',
      options: {
        visibleName: 'name',
        backendName: 'Recipient[delivery_name]',
        autocomplete: 'name',
        autocapitalize: 'words',
        spellcheck: false,
        id: 'checkout-name',
        type: 'text'
      }
    },
    {
      selector: 'input.input[name="Recipient[delivery_email]"]',
      options: {
        visibleName: 'email',
        backendName: 'Recipient[delivery_email]',
        autocomplete: 'email',
        inputmode: 'email',
        autocapitalize: 'off',
        autocorrect: 'off',
        spellcheck: false,
        id: 'checkout-email',
        type: 'email'
      }
    }
  ];

  function setup() {
    if (isSetupRunning) return;
    isSetupRunning = true;
    
    var form = getForm();
    if (!form) {
      isSetupRunning = false;
      return;
    }

    // Process all fields at once
    fieldConfigs.forEach(function(config) {
      var element = form.querySelector(config.selector);
      if (element) {
        patchField(element, config.options);
      }
    });
    
    isSetupRunning = false;
  }

  function tick() { 
    try { 
      setup(); 
    } catch(e) {
      console.warn('Phone completer setup error:', e);
    } 
  }

  // Initialize with throttling
  var setupTimeout;
  function throttledTick() {
    if (setupTimeout) return;
    setupTimeout = setTimeout(function() {
      tick();
      setupTimeout = null;
    }, 100);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', tick);
  } else {
    tick();
  }

  // Optimized mutation observer with throttling
  try {
    var mo = new MutationObserver(throttledTick);
    mo.observe(document.documentElement, { 
      childList: true, 
      subtree: true,
      attributes: false,
      characterData: false
    });
  } catch(e){}
})();