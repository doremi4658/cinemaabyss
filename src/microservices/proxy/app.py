from flask import Flask, request, jsonify
import requests
import random
import os
from threading import Lock

app = Flask(__name__)

# Конфигурация
MONOLITH_URL = os.getenv('MONOLITH_URL', 'http://monolith:8080')
MOVIES_SERVICE_URL = os.getenv('MOVIES_SERVICE_URL', 'http://movies-service:8081')
EVENTS_SERVICE_URL = os.getenv('EVENTS_SERVICE_URL', 'http://events-service:8082')
GRADUAL_MIGRATION = os.getenv('GRADUAL_MIGRATION', 'false').lower() == 'true'
MOVIES_MIGRATION_PERCENT = int(os.getenv('MOVIES_MIGRATION_PERCENT', '0'))

# Блокировка для потокобезопасности
lock = Lock()


def get_target_service(path):
    """Определяет целевой сервис на основе фиче-флагов и пути"""
    if path.startswith('/api/events'):
        return EVENTS_SERVICE_URL

    if path.startswith('/api/movies'):
        if GRADUAL_MIGRATION:
            with lock:
                if random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
                    return MOVIES_SERVICE_URL
        return MONOLITH_URL

    # Для всех остальных путей используем монолит
    return MONOLITH_URL


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
def proxy(path):
    """Основной прокси-метод"""
    try:
        target_url = get_target_service('/' + path)
        full_url = f"{target_url}/{path}"

        # Убираем двойные слеши
        full_url = full_url.replace('//', '/').replace(':/', '://')

        # Проксируем запрос
        response = requests.request(
            method=request.method,
            url=full_url,
            headers={key: value for key, value in request.headers if key != 'Host'},
            data=request.get_data(),
            params=request.args,
            cookies=request.cookies,
            allow_redirects=False
        )

        # Возвращаем ответ
        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        headers = [
            (name, value) for name, value in response.raw.headers.items()
            if name.lower() not in excluded_headers
        ]

        return (response.content, response.status_code, headers)

    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Proxy error: {str(e)}'}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'proxy'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)