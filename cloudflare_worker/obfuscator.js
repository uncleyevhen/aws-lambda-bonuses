// Генератор обфускованого скрипта
// Запустіть цей скрипт для створення обфускованої версії

const fs = require('fs');
const path = require('path');

// Читаємо оригінальний скрипт
const originalScript = fs.readFileSync(path.join(__dirname, '../gtm_integration/bonus_checkoutpage_script.js'), 'utf8');

// Функція обфускації
function obfuscateScript(script) {
  // Масив для зберігання рядків
  const strings = [];
  const stringMap = new Map();
  
  // Витягуємо всі рядки
  let stringIndex = 0;
  const processedScript = script.replace(/(["'`])((?:\\.|(?!\1)[^\\])*?)\1/g, (match, quote, content) => {
    const key = `_0x${stringIndex.toString(16).padStart(4, '0')}`;
    strings.push(content);
    stringMap.set(content, key);
    stringIndex++;
    return `_strings[${strings.length - 1}]`;
  });
  
  // Генеруємо випадкові імена змінних
  const variableNames = [
    'DEBUG_MODE', 'LOG_PREFIX', 'BONUS_BALANCE_API_URL', 'PROMO_API_URL',
    'CERTIFICATE_SELECTORS', 'PHONE_SELECTORS', 'bonusState', 'saveStateTimeout',
    'lastSaveTime', 'stateChanged', 'log', 'logError', 'updateBonusState',
    'resetBonusState', 'saveBonusState', 'loadBonusState'
  ];
  
  const obfuscatedNames = new Map();
  variableNames.forEach((name, index) => {
    obfuscatedNames.set(name, `_0x${(index + 100).toString(16)}`);
  });
  
  // Замінюємо імена змінних
  let obfuscatedScript = processedScript;
  obfuscatedNames.forEach((obfName, origName) => {
    const regex = new RegExp(`\\b${origName}\\b`, 'g');
    obfuscatedScript = obfuscatedScript.replace(regex, obfName);
  });
  
  // Мінімізуємо код
  obfuscatedScript = obfuscatedScript
    .replace(/\/\*[\s\S]*?\*\//g, '') // Видаляємо коментарі
    .replace(/\/\/.*$/gm, '') // Видаляємо однолінійні коментарі
    .replace(/\s+/g, ' ') // Замінюємо множинні пробіли
    .replace(/;\s*}/g, ';}') // Прибираємо пробіли перед }
    .replace(/{\s*/g, '{') // Прибираємо пробіли після {
    .replace(/,\s*/g, ',') // Прибираємо пробіли після ком
    .trim();
  
  // Створюємо фінальний обфускований скрипт
  const finalScript = `
(function(){
var _strings=${JSON.stringify(strings)};
${obfuscatedScript}
})();`;
  
  return finalScript;
}

// Генеруємо обфускований скрипт
const obfuscated = obfuscateScript(originalScript);

// Зберігаємо результат
fs.writeFileSync(path.join(__dirname, 'obfuscated_bonus_script.js'), obfuscated);

console.log('Обфускований скрипт створено: obfuscated_bonus_script.js');
console.log(`Розмір оригінального файлу: ${(originalScript.length / 1024).toFixed(2)} KB`);
console.log(`Розмір обфускованого файлу: ${(obfuscated.length / 1024).toFixed(2)} KB`);
