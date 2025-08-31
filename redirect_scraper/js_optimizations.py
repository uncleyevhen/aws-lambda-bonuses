#!/usr/bin/env python3
"""
ОПТИМІЗАЦІЇ JAVASCRIPT ДЛЯ REDIRECT SCRAPER
На основі принципів з promo_generator.py
"""

# Оптимізований JavaScript для витягування всіх даних за один запит
OPTIMIZED_JS_SCRIPTS = {
    'extract_all_data_single_request': """
        () => {
            try {
                // Знаходимо рядки таблиці редиректів (на основі реальної структури)
                let rows = [];
                
                // Спробуємо різні селектори для рядків таблиці редиректів
                const possibleSelectors = [
                    'tr[id^="dataGridRow_"]',           // Основний селектор з HTML структури
                    'tbody tr:not(.no-data)',           // Загальний селектор для рядків tbody
                    'table tr:not(:first-child)',       // Всі рядки крім заголовка
                    '.draggable'                        // Рядки з класом draggable (з HTML)
                ];
                
                for (const selector of possibleSelectors) {
                    rows = Array.from(document.querySelectorAll(selector));
                    if (rows.length > 0) {
                        console.log(`✅ Знайдено ${rows.length} рядків з селектором: ${selector}`);
                        break;
                    }
                }
                
                if (rows.length === 0) {
                    console.warn('❌ Не знайдено рядків таблиці редиректів');
                    return {
                        success: false,
                        error: 'Не знайдено рядків таблиці',
                        selectors_tried: possibleSelectors,
                        debug_info: {
                            all_rows: document.querySelectorAll('tr').length,
                            all_tables: document.querySelectorAll('table').length
                        }
                    };
                }
                
                const extractedData = rows.map((row, index) => {
                    try {
                        const cells = row.querySelectorAll('td');
                        
                        // Витягуємо ID рядка з атрибута
                        const rowId = row.id || `redirect_${index}`;
                        
                        if (cells.length < 3) {
                            // Пропускаємо рядки з недостатньою кількістю колонок
                            return null;
                        }
                        
                        // Обробляємо першу колонку зі старими URL
                        let oldUrls = '';
                        let dataHandler = '';
                        let dataRecord = '';
                        
                        const firstCell = cells[0];
                        if (firstCell) {
                            // Шукаємо div з data-handler та data-record
                            const dataDiv = firstCell.querySelector('[data-handler][data-record]');
                            if (dataDiv) {
                                dataHandler = dataDiv.getAttribute('data-handler') || '';
                                dataRecord = dataDiv.getAttribute('data-record') || '';
                            }
                            
                            // Витягуємо текст старих URL з першої колонки
                            // Можуть бути в різних елементах, тому беремо весь текст
                            oldUrls = firstCell.innerText?.trim() || '';
                            
                            // Якщо є прихований input, витягуємо значення звідти
                            const hiddenInput = firstCell.querySelector('input[type="hidden"]');
                            if (hiddenInput && hiddenInput.value) {
                                const inputValue = hiddenInput.value;
                                // Видаляємо ID з початку значення
                                const cleanValue = inputValue.replace(/^\\d+/, '');
                                if (cleanValue && cleanValue !== inputValue) {
                                    oldUrls = cleanValue;
                                }
                            }
                        }
                        
                        // Витягуємо дані з інших колонок на основі структури HTML
                        const currentUrl = cells[1]?.innerText?.trim() || '';     // Поточне посилання
                        const suffix = cells[2]?.innerText?.trim() || '';         // Суфікс
                        const template = cells[3]?.innerText?.trim() || '';       // Шаблон
                        const recordName = cells[4]?.innerText?.trim() || '';     // Запис (назва)
                        
                        return {
                            redirectId: rowId.replace('dataGridRow_', ''),
                            oldUrls: oldUrls,
                            currentUrl: currentUrl,
                            suffix: suffix,
                            template: template,
                            recordName: recordName,
                            dataHandler: dataHandler,
                            dataRecord: dataRecord,
                            rowIndex: index,
                            extractedAt: new Date().toISOString(),
                            cellsCount: cells.length,
                            
                            // Додаткова діагностична інформація
                            debug: {
                                rowClass: row.className,
                                hasDataDiv: !!firstCell?.querySelector('[data-handler][data-record]'),
                                hasHiddenInput: !!firstCell?.querySelector('input[type="hidden"]')
                            }
                        };
                        
                    } catch (cellError) {
                        console.warn('⚠️ Error processing row:', cellError);
                        return {
                            redirectId: `error_${index}`,
                            error: cellError.message,
                            rowIndex: index,
                            oldUrls: '',
                            currentUrl: '',
                            suffix: '',
                            template: '',
                            recordName: ''
                        };
                    }
                }).filter(Boolean); // Видаляємо null значення
                
                // Інформація про пагінацію (адаптована для редиректів)
                let paginationInfo = {};
                
                // Шукаємо поточну сторінку в прихованому елементі
                const currentPageElement = document.querySelector('#current-page, td[id="current-page"]');
                let currentPage = 1;
                if (currentPageElement) {
                    const pageAttr = currentPageElement.getAttribute('data-page');
                    currentPage = parseInt(pageAttr) || parseInt(currentPageElement.innerText) || 1;
                }
                
                // Шукаємо різні типи пагінаторів
                const paginationSelectors = [
                    '.datagrid-pager .pages',      // Оригінальний селектор
                    '.pagination',                 // Стандартний Bootstrap
                    '[class*="pager"]',           // Будь-який клас з pager
                    '[class*="pagination"]'        // Будь-який клас з pagination
                ];
                
                for (const selector of paginationSelectors) {
                    const paginationElement = document.querySelector(selector);
                    if (paginationElement) {
                        paginationInfo.pagerText = paginationElement.innerText?.trim() || '';
                        break;
                    }
                }
                
                // Пошук кнопки "Далі"
                const nextButtonSelectors = [
                    '.datagrid-pager .fl-l.r.active',  // Оригінальний селектор
                    '.pagination .next:not(.disabled)',
                    '[class*="next"]:not([class*="disabled"])',
                    'button[onclick*="next"]',
                    'a[onclick*="next"]'
                ];
                
                let hasNext = false;
                for (const selector of nextButtonSelectors) {
                    const nextButton = document.querySelector(selector);
                    if (nextButton && !nextButton.classList.contains('disabled')) {
                        hasNext = true;
                        break;
                    }
                }
                
                return {
                    success: true,
                    data: extractedData,
                    pagination: {
                        currentPage: currentPage,
                        hasNext: hasNext,
                        pagerText: paginationInfo.pagerText || ''
                    },
                    totalExtracted: extractedData.length,
                    timestamp: new Date().toISOString(),
                    tableStructure: {
                        rowsFound: rows.length,
                        validRows: extractedData.length,
                        averageCellsPerRow: extractedData.length > 0 ? 
                            extractedData.reduce((sum, row) => sum + (row.cellsCount || 0), 0) / extractedData.length : 0
                    }
                };
                
            } catch (error) {
                return {
                    success: false,
                    error: error.message,
                    stack: error.stack,
                    timestamp: new Date().toISOString()
                };
            }
        }
    """,
    
    'get_pagination_info_fast': """
        () => {
            try {
                // Швидке отримання інформації про пагінацію (як у promo_generator.py)
                const pagerText = document.querySelector('.datagrid-pager .pages')?.innerText?.trim() || '';
                const currentPageInput = document.querySelector('#current-page');
                const currentPage = currentPageInput ? parseInt(currentPageInput.value) || 1 : 1;
                
                // Перевіряємо наявність кнопки "Далі"
                const nextButton = document.querySelector('.datagrid-pager .fl-l.r.active');
                const hasNext = nextButton && !nextButton.classList.contains('disabled');
                
                // Витягуємо загальну кількість записів
                const recordsInfo = document.querySelector('.datagrid-pager .total-records')?.innerText || '';
                const totalRecordsMatch = recordsInfo.match(/(\\d+)/);
                const totalRecords = totalRecordsMatch ? parseInt(totalRecordsMatch[1]) : 0;
                
                return {
                    success: true,
                    currentPage: currentPage,
                    hasNext: hasNext,
                    totalRecords: totalRecords,
                    pagerText: pagerText,
                    timestamp: new Date().toISOString()
                };
                
            } catch (error) {
                return {
                    success: false,
                    error: error.message,
                    timestamp: new Date().toISOString()
                };
            }
        }
    """,
    
    'wait_for_page_load_optimized': """
        () => {
            // Оптимізоване очікування завантаження (як у promo_generator.py)
            return new Promise((resolve) => {
                let attempts = 0;
                const maxAttempts = 100; // 5 секунд по 50мс
                
                const checkLoad = () => {
                    attempts++;
                    
                    // Перевіряємо лоадер
                    const loader = document.querySelector('#datagrid-loader');
                    const isLoading = loader && loader.style.display !== 'none';
                    
                    // Перевіряємо наявність рядків
                    const rows = document.querySelectorAll('tr[id^="dataGridRow_"]');
                    const hasRows = rows.length > 0;
                    
                    if (!isLoading && hasRows) {
                        resolve({
                            success: true,
                            loaded: true,
                            rowsCount: rows.length,
                            attempts: attempts
                        });
                    } else if (attempts >= maxAttempts) {
                        resolve({
                            success: false,
                            loaded: false,
                            timeout: true,
                            attempts: attempts
                        });
                    } else {
                        setTimeout(checkLoad, 50); // Перевірка кожні 50мс
                    }
                };
                
                checkLoad();
            });
        }
    """,
    
    'navigate_and_wait': """
        () => {
            // Комбінована функція навігації та очікування для редиректів
            return new Promise((resolve) => {
                try {
                    // Шукаємо кнопку "Далі" для редиректів
                    const nextButtonSelectors = [
                        '.datagrid-pager .fl-l.r.active',  // Основний селектор
                        '.pagination .next:not(.disabled)',
                        '[class*="next"]:not([class*="disabled"])',
                        'a[onclick*="next"]',
                        'button[onclick*="next"]',
                        'a[href*="page"]'                   // Посилання з page в href
                    ];
                    
                    let nextButton = null;
                    for (const selector of nextButtonSelectors) {
                        nextButton = document.querySelector(selector);
                        if (nextButton && !nextButton.classList.contains('disabled')) {
                            break;
                        }
                        nextButton = null;
                    }
                    
                    if (!nextButton) {
                        resolve({
                            success: false,
                            reason: 'no_next_button',
                            timestamp: new Date().toISOString(),
                            debug: {
                                buttons_found: nextButtonSelectors.map(sel => 
                                    document.querySelector(sel) ? sel : null
                                ).filter(Boolean)
                            }
                        });
                        return;
                    }
                    
                    // Зберігаємо поточну сторінку для порівняння
                    const currentPageElement = document.querySelector('#current-page, td[id="current-page"]');
                    let currentPageBefore = '1';
                    if (currentPageElement) {
                        currentPageBefore = currentPageElement.getAttribute('data-page') || 
                                          currentPageElement.innerText || '1';
                    }
                    
                    // Зберігаємо кількість рядків до навігації
                    const rowsBeforeNavigation = document.querySelectorAll('tr[id^="dataGridRow_"]').length;
                    
                    console.log(`🔄 Клікаємо на кнопку навігації. Поточна сторінка: ${currentPageBefore}`);
                    
                    // Клікаємо на кнопку "Далі"
                    nextButton.click();
                    
                    // Очікуємо зміну сторінки та завантаження нових даних
                    let attempts = 0;
                    const maxAttempts = 200; // 10 секунд (200 * 50ms)
                    
                    const waitForChange = () => {
                        attempts++;
                        
                        // Перевіряємо зміну номера сторінки
                        const currentPageElement = document.querySelector('#current-page, td[id="current-page"]');
                        let currentPageAfter = '1';
                        if (currentPageElement) {
                            currentPageAfter = currentPageElement.getAttribute('data-page') || 
                                             currentPageElement.innerText || '1';
                        }
                        
                        // Перевіряємо чи є лоадер (якщо існує)
                        const loader = document.querySelector('#datagrid-loader, .loader, [class*="loading"]');
                        const isLoading = loader && (
                            loader.style.display !== 'none' || 
                            loader.style.visibility !== 'hidden' ||
                            loader.classList.contains('active')
                        );
                        
                        // Перевіряємо наявність нових рядків
                        const rowsAfterNavigation = document.querySelectorAll('tr[id^="dataGridRow_"]');
                        const hasRows = rowsAfterNavigation.length > 0;
                        
                        // Умови успішної навігації:
                        // 1. Сторінка змінилася АБО кількість рядків змінилася
                        // 2. Немає активного лоадера
                        // 3. Є рядки з даними
                        const pageChanged = currentPageAfter !== currentPageBefore;
                        const rowsChanged = rowsAfterNavigation.length !== rowsBeforeNavigation;
                        const navigationSuccessful = (pageChanged || rowsChanged) && !isLoading && hasRows;
                        
                        if (navigationSuccessful) {
                            console.log(`✅ Навігація успішна. Нова сторінка: ${currentPageAfter}, рядків: ${rowsAfterNavigation.length}`);
                            resolve({
                                success: true,
                                navigated: true,
                                newPage: currentPageAfter,
                                rowsCount: rowsAfterNavigation.length,
                                attempts: attempts,
                                debug: {
                                    pageChanged: pageChanged,
                                    rowsChanged: rowsChanged,
                                    currentPageBefore: currentPageBefore,
                                    currentPageAfter: currentPageAfter,
                                    rowsBeforeNavigation: rowsBeforeNavigation,
                                    rowsAfterNavigation: rowsAfterNavigation.length
                                }
                            });
                            return;
                        }
                        
                        if (attempts >= maxAttempts) {
                            console.warn(`⚠️ Таймаут навігації після ${attempts} спроб`);
                            resolve({
                                success: false,
                                timeout: true,
                                attempts: attempts,
                                debug: {
                                    currentPageBefore: currentPageBefore,
                                    currentPageAfter: currentPageAfter,
                                    rowsBeforeNavigation: rowsBeforeNavigation,
                                    rowsAfterNavigation: rowsAfterNavigation.length,
                                    isLoading: isLoading,
                                    hasRows: hasRows
                                }
                            });
                        } else {
                            setTimeout(waitForChange, 50); // Перевірка кожні 50мс
                        }
                    };
                    
                    // Починаємо очікування через невелику затримку
                    setTimeout(waitForChange, 100);
                    
                } catch (error) {
                    resolve({
                        success: false,
                        error: error.message,
                        stack: error.stack,
                        timestamp: new Date().toISOString()
                    });
                }
            });
        }
    """
}

# Конфігурація оптимізацій (як у promo_generator.py)
SCRAPER_OPTIMIZATIONS = {
    'use_single_request_extraction': True,  # Як evaluate_all в promo_generator
    'combine_navigation_and_wait': True,    # Комбінована навігація
    'minimize_dom_queries': True,           # Мінімізація запитів до DOM
    'cache_selectors': True,                # Кешування селекторів
    'batch_data_processing': True,          # Батчева обробка даних
    'fast_pagination_check': True,          # Швидка перевірка пагінації
}

def get_optimized_extraction_method():
    """Повертає оптимізований метод витягування даних."""
    if SCRAPER_OPTIMIZATIONS['use_single_request_extraction']:
        return OPTIMIZED_JS_SCRIPTS['extract_all_data_single_request']
    else:
        # Fallback до старого методу
        return None

def get_optimized_navigation_method():
    """Повертає оптимізований метод навігації.""" 
    if SCRAPER_OPTIMIZATIONS['combine_navigation_and_wait']:
        return OPTIMIZED_JS_SCRIPTS['navigate_and_wait']
    else:
        # Fallback до старого методу
        return None
