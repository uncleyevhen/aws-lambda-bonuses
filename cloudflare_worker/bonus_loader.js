(function(){
  'use strict';
  
  // Обфускована конфігурація
  var _0x4a2b = ['aHR0cHM6Ly9ib251cy13b3JrZXIueW91ci1kb21haW4uY29t', 'c2NyaXB0', 'aGVhZA==', 'c3Jj'];
  var _0x5c3d = function(a, b) { return atob(a); };
  var _0x6e4f = _0x5c3d(_0x4a2b[0]);
  
  // Перевірка домену та умов
  var _0x7a5b = window.location.hostname;
  var _0x8c6d = ['yourdomain.com', 'staging.yourdomain.com']; // Замініть на ваші домени
  
  if (!_0x8c6d.some(function(d) { return _0x7a5b.includes(d); })) return;
  
  // Функція завантаження
  function _0x9d7e() {
    var s = document.createElement(_0x5c3d(_0x4a2b[1]));
    s[_0x5c3d(_0x4a2b[3])] = _0x6e4f + '/bonus.js?v=' + Date.now();
    s.async = true;
    s.defer = true;
    document[_0x5c3d(_0x4a2b[2])].appendChild(s);
  }
  
  // Запуск після завантаження DOM
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _0x9d7e);
  } else {
    _0x9d7e();
  }
})();
