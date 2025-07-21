<?php
/**
 * PHP проксі для KeyCRM API
 * Розмістіть цей файл на сервері safeyourlove.com
 * URL: https://safeyourlove.com/api/keycrm-proxy.php
 */

header('Content-Type: application/json');
header('Access-Control-Allow-Origin: https://safeyourlove.com');
header('Access-Control-Allow-Methods: GET, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');

// Обробка preflight запиту
if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    http_response_code(200);
    exit();
}

// Перевіряємо метод запиту
if ($_SERVER['REQUEST_METHOD'] !== 'GET') {
    http_response_code(405);
    echo json_encode(['error' => 'Метод не дозволено']);
    exit();
}

// Отримуємо номер телефону
$phone = $_GET['phone'] ?? '';

if (empty($phone)) {
    http_response_code(400);
    echo json_encode(['error' => 'Параметр phone є обов\'язковим']);
    exit();
}

// KeyCRM API налаштування
$keycrm_token = 'M2IyOTFjNWM4ODA2OWU0NjU4ZDRkODAxZDVkMTQ4ZGNlMzUzYzc5NQ';
$keycrm_url = 'https://openapi.keycrm.app/v1/buyer?' . http_build_query([
    'filter[buyer_phone]' => $phone,
    'include' => 'loyalty,custom_fields'
]);

// Створюємо запит до KeyCRM
$context = stream_context_create([
    'http' => [
        'header' => [
            'Authorization: Bearer ' . $keycrm_token,
            'Accept: application/json'
        ],
        'timeout' => 10
    ]
]);

$response = @file_get_contents($keycrm_url, false, $context);

if ($response === false) {
    http_response_code(500);
    echo json_encode(['error' => 'Помилка з\'єднання з KeyCRM API']);
    exit();
}

$keycrm_data = json_decode($response, true);

if (json_last_error() !== JSON_ERROR_NONE) {
    http_response_code(500);
    echo json_encode(['error' => 'Помилка парсингу відповіді KeyCRM']);
    exit();
}

// Обробляємо відповідь KeyCRM
$bonus_balance = 0;

if (!empty($keycrm_data['data'])) {
    $buyer = $keycrm_data['data'][0];
    
    // Шукаємо бонуси в кастомних полях
    if (!empty($buyer['custom_fields'])) {
        foreach ($buyer['custom_fields'] as $field) {
            if (!empty($field['name'])) {
                $field_name = mb_strtolower($field['name']);
                if (strpos($field_name, 'бонус') !== false || 
                    strpos($field_name, 'bonus') !== false || 
                    strpos($field_name, 'бали') !== false) {
                    $bonus_balance = intval($field['value'] ?? 0);
                    break;
                }
            }
        }
    }
    
    // Альтернативно в loyalty програмі
    if ($bonus_balance == 0 && !empty($buyer['loyalty'])) {
        $bonus_balance = intval($buyer['loyalty'][0]['amount'] ?? 0);
    }
}

// Повертаємо результат
echo json_encode([
    'success' => true,
    'bonus_balance' => $bonus_balance,
    'phone' => $phone
]);
