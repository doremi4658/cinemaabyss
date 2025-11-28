## Изучите [README.md](README.md) файл и структуру проекта.

## Задание 1

1. Спроектируйте to be архитектуру КиноБездны, разделив всю систему на отдельные домены и организовав интеграционное взаимодействие и единую точку вызова сервисов.
Результат представьте в виде контейнерной диаграммы в нотации С4.
Код для https://www.planttext.com/

@startuml
!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Container.puml

Person(user, "Пользователь", "Смотрит фильмы через различные устройства")

System_Boundary(c1, "Онлайн-кинотеатр Кинобездна") {
    Container(web_app, "Веб-приложение", "JavaScript, React", "Предоставляет веб-интерфейс для ноутбуков и ПК")
    Container(mobile_app, "Мобильное приложение", "iOS/Android", "Нативное приложение для мобильных устройств")
    Container(tv_app, "Smart TV приложение", "TV OS", "Приложение для смарт-телевизоров")
    
    Container(api_gateway, "API Gateway", "Go", "Единая точка входа, маршрутизация, аутентификация")
    
    Container(user_service, "User Service", "Go", "Управление пользователями, аутентификация")
    Container(subscription_service, "Subscription Service", "Go", "Управление подписками")
    Container(payment_service, "Payment Service", "Go", "Обработка платежей")
    Container(metadata_service, "Metadata Service", "Go", "Управление метаданными фильмов (MVP)")
    Container(content_service, "Content Service", "Go", "Управление видео-контентом и ссылками")
    Container(discount_service, "Discount Service", "Go", "Управление скидками и промо-акциями")
    Container(analytics_service, "Analytics Service", "Go, Python", "Сбор и анализ пользовательской активности")
    
    ContainerDb(user_db, "User Database", "PostgreSQL", "Хранит данные пользователей")
    ContainerDb(subscription_db, "Subscription Database", "PostgreSQL", "Хранит данные о подписках")
    ContainerDb(payment_db, "Payment Database", "PostgreSQL", "Хранит данные о платежах")
    ContainerDb(metadata_db, "Metadata Database", "PostgreSQL", "Хранит метаданные фильмов")
    ContainerDb(content_db, "Content Database", "PostgreSQL", "Хранит информацию о видео")
    ContainerDb(discount_db, "Discount Database", "PostgreSQL", "Хранит данные о скидках")
    ContainerDb(analytics_db, "Analytics Database", "ClickHouse/PostgreSQL", "Хранит данные для аналитики")
    
    Container(kafka, "Apache Kafka", "Kafka", "Шина событий для асинхронной коммуникации")
    Container(s3_storage, "Object Storage", "Amazon S3/MinIO", "Хранилище для ссылок на стримы, метаданных и статики")
}

System_Ext(recommendation_system, "External Recommendation System", "Внешняя рекомендательная система")
System_Ext(payment_system, "Payment Gateways", "Внешние платежные системы")
System_Ext(loyalty_system, "Loyalty Systems", "Внешние системы лояльности")
System_Ext(external_streaming, "External Streaming Services", "Сторонние стриминговые сервисы")

' Connections from user to applications
Rel(user, web_app, "Использует", "HTTPS")
Rel(user, mobile_app, "Использует", "HTTPS")
Rel(user, tv_app, "Использует", "HTTPS")

' Connections from applications to API Gateway
Rel(web_app, api_gateway, "API вызовы", "REST/HTTPS")
Rel(mobile_app, api_gateway, "API вызовы", "REST/HTTPS")
Rel(tv_app, api_gateway, "API вызовы", "REST/HTTPS")

' Connections from API Gateway to services
Rel(api_gateway, user_service, "API вызовы", "REST/HTTPS")
Rel(api_gateway, subscription_service, "API вызовы", "REST/HTTPS")
Rel(api_gateway, payment_service, "API вызовы", "REST/HTTPS")
Rel(api_gateway, metadata_service, "API вызовы", "REST/HTTPS")
Rel(api_gateway, content_service, "API вызовы", "REST/HTTPS")
Rel(api_gateway, discount_service, "API вызовы", "REST/HTTPS")

' Database connections
Rel(user_service, user_db, "Чтение/запись", "SQL")
Rel(subscription_service, subscription_db, "Чтение/запись", "SQL")
Rel(payment_service, payment_db, "Чтение/запись", "SQL")
Rel(metadata_service, metadata_db, "Чтение/запись", "SQL")
Rel(content_service, content_db, "Чтение/запись", "SQL")
Rel(discount_service, discount_db, "Чтение/запись", "SQL")
Rel(analytics_service, analytics_db, "Чтение/запись", "SQL")

' Kafka connections
Rel(user_service, kafka, "Публикует события пользователей", "Kafka Protocol")
Rel(metadata_service, kafka, "Публикует события оценок", "Kafka Protocol")
Rel(content_service, kafka, "Публикует события просмотров", "Kafka Protocol")
Rel(payment_service, kafka, "Публикует платежные события", "Kafka Protocol")
Rel(kafka, recommendation_system, "Отправляет события для анализа", "Kafka Protocol")
Rel(kafka, analytics_service, "Отправляет события для аналитики", "Kafka Protocol")

' External system connections
Rel(payment_service, payment_system, "Интеграция с платежными шлюзами", "HTTPS")
Rel(discount_service, loyalty_system, "Интеграция с системами лояльности", "HTTPS")

' S3 Storage connections - ИСПРАВЛЕННЫЕ
Rel(content_service, s3_storage, "Получает ссылки на стримы", "S3 API")
Rel(metadata_service, s3_storage, "Хранит постеры, трейлеры", "S3 API")
Rel(analytics_service, s3_storage, "Хранит отчеты и дампы", "S3 API")

Rel(content_service, external_streaming, "Проверяет доступность контента", "HTTPS/API")

' Content Service предоставляет ссылки клиентам через API Gateway
Rel(api_gateway, content_service, "Получает стриминговые ссылки", "REST/HTTPS")

@enduml


## Задание 2
ПРОСЬБА!!!
Попробуйте запустить на своей машине и в случае ошибки подсветить в чем она(((
### 1. Proxy
Команда КиноБездны уже выделила сервис метаданных о фильмах movies и вам необходимо реализовать бесшовный переход с применением паттерна Strangler Fig в части реализации прокси-сервиса (API Gateway), с помощью которого можно будет постепенно переключать траффик, используя фиче-флаг.


Реализуйте сервис на любом языке программирования в ./src/microservices/proxy.
Конфигурация для запуска сервиса через docker-compose уже добавлена
```yaml
  proxy-service:
    build:
      context: ./src/microservices/proxy
      dockerfile: Dockerfile
    container_name: cinemaabyss-proxy-service
    depends_on:
      - monolith
      - movies-service
      - events-service
    ports:
      - "8000:8000"
    environment:
      PORT: 8000
      MONOLITH_URL: http://monolith:8080
      #монолит
      MOVIES_SERVICE_URL: http://movies-service:8081 #сервис movies
      EVENTS_SERVICE_URL: http://events-service:8082 
      GRADUAL_MIGRATION: "true" # вкл/выкл простого фиче-флага
      MOVIES_MIGRATION_PERCENT: "50" # процент миграции
    networks:
      - cinemaabyss-network
```

- После реализации запустите postman тесты - они все должны быть зеленые.
- Отправьте запросы к API Gateway:
   ```bash
   curl http://localhost:8000/api/movies
   ```
- Протестируйте постепенный переход, изменив переменную окружения MOVIES_MIGRATION_PERCENT в файле docker-compose.yml.

### 2. Kafka
 Вам как архитектуру нужно также проверить гипотезу насколько просто реализовать применение Kafka в данной архитектуре.

Для этого нужно сделать MVP сервис events, который будет при вызове API создавать и сам же читать сообщения в топике Kafka.

    - Разработайте сервис на любом языке программирования с consumer'ами и producer'ами.
    - Реализуйте простой API, при вызове которого будут создаваться события User/Payment/Movie и обрабатываться внутри сервиса с записью в лог
    - Добавьте в docker-compose новый сервис, kafka там уже есть

Необходимые тесты для проверки этого API вызываются при запуске npm run test:local из папки tests/postman 
Приложите скриншот тестов и скриншот состояния топиков Kafka http://localhost:8090 


## Задание 3

Команда начала переезд в Kubernetes для лучшего масштабирования и повышения надежности. 
Вам, как архитектору осталось самое сложное:
 - реализовать CI/CD для сборки прокси сервиса
 - реализовать необходимые конфигурационные файлы для переключения трафика.


### CI/CD

 В папке .github/worflows доработайте деплой новых сервисов proxy и events в docker-build-push.yml , чтобы api-tests при сборке отрабатывали корректно при отправке коммита в вашу новую ветку.

Нужно доработать 
```yaml
on:
  push:
    branches: [ main ]
    paths:
      - 'src/**'
      - '.github/workflows/docker-build-push.yml'
  release:
    types: [published]
```
и добавить необходимые шаги в блок
```yaml
jobs:
  build-and-push:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    steps:
      - name: Checkout repository
        uses: actions/checkout@v3

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v2

      - name: Log in to the Container registry
        uses: docker/login-action@v2
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

```
Как только сборка отработает и в github registry появятся ваши образы, можно переходить к блоку настройки Kubernetes
Успешным результатом данного шага является "зеленая" сборка и "зеленые" тесты


### Proxy в Kubernetes

#### Шаг 1
Для деплоя в kubernetes необходимо залогиниться в docker registry Github'а.
1. Создайте Personal Access Token (PAT) https://github.com/settings/tokens . Создавайте class с правом read:packages
2. В src/kubernetes/*.yaml (event-service, monolith, movies-service и proxy-service)  отредактируйте путь до ваших образов 
```bash
 spec:
      containers:
      - name: events-service
        image: ghcr.io/ваш логин/имя репозитория/events-service:latest
```
3. Добавьте в секрет src/kubernetes/dockerconfigsecret.yaml в поле
```bash
 .dockerconfigjson: значение в base64 файла ~/.docker/config.json
```

4. Если в ~/.docker/config.json нет значения для аутентификации
```json
{
        "auths": {
                "ghcr.io": {
                       тут пусто
                }
        }
}
```
то выполните 

и добавьте

```json 
 "auth": "имя пользователя:токен в base64"
```

Чтобы получить значение в base64 можно выполнить команду
```bash
 echo -n ваш_логин:ваш_токен | base64
```

После заполнения config.json, также прогоните содержимое через base64

```bash
cat .docker/config.json | base64
```

и полученное значение добавляем в

```bash
 .dockerconfigjson: значение в base64 файла ~/.docker/config.json
```

#### Шаг 2

  Доработайте src/kubernetes/event-service.yaml и src/kubernetes/proxy-service.yaml

  - Необходимо создать Deployment и Service 
  - Доработайте ingress.yaml, чтобы можно было с помощью тестов проверить создание событий
  - Выполните дальшейшие шаги для поднятия кластера:

  1. Создайте namespace:
  ```bash
  kubectl apply -f src/kubernetes/namespace.yaml
  ```
  2. Создайте секреты и переменные
  ```bash
  kubectl apply -f src/kubernetes/configmap.yaml
  kubectl apply -f src/kubernetes/secret.yaml
  kubectl apply -f src/kubernetes/dockerconfigsecret.yaml
  kubectl apply -f src/kubernetes/postgres-init-configmap.yaml
  ```

  3. Разверните базу данных:
  ```bash
  kubectl apply -f src/kubernetes/postgres.yaml
  ```

  На этом этапе если вызвать команду
  ```bash
  kubectl -n cinemaabyss get pod
  ```
  Вы увидите

  NAME         READY   STATUS    
  postgres-0   1/1     Running   

  4. Разверните Kafka:
  ```bash
  kubectl apply -f src/kubernetes/kafka/kafka.yaml
  ```

  Проверьте, теперь должно быть запущено 3 пода, если что-то не так, то посмотрите логи
  ```bash
  kubectl -n cinemaabyss logs имя_пода (например - kafka-0)
  ```

  5. Разверните монолит:
  ```bash
  kubectl apply -f src/kubernetes/monolith.yaml
  ```
  6. Разверните микросервисы:
  ```bash
  kubectl apply -f src/kubernetes/movies-service.yaml
  kubectl apply -f src/kubernetes/events-service.yaml
  ```
  7. Разверните прокси-сервис:
  ```bash
  kubectl apply -f src/kubernetes/proxy-service.yaml
  ```

  После запуска и поднятия подов вывод команды 
  ```bash
  kubectl -n cinemaabyss get pod
  ```

  Будет наподобие такого

  NAME                              READY   STATUS    

  events-service-7587c6dfd5-6whzx   1/1     Running  

  kafka-0                           1/1     Running   

  monolith-8476598495-wmtmw         1/1     Running  

  movies-service-6d5697c584-4qfqs   1/1     Running  

  postgres-0                        1/1     Running  

  proxy-service-577d6c549b-6qfcv    1/1     Running  

  zookeeper-0                       1/1     Running 

  8. Добавим ingress

  - добавьте аддон
  ```bash
  minikube addons enable ingress
  ```
  ```bash
  kubectl apply -f src/kubernetes/ingress.yaml
  ```
  9. Добавьте в /etc/hosts
  127.0.0.1 cinemaabyss.example.com

  10. Вызовите
  ```bash
  minikube tunnel
  ```
  11. Вызовите https://cinemaabyss.example.com/api/movies
  Вы должны увидеть вывод списка фильмов
  Можно поэкспериментировать со значением   MOVIES_MIGRATION_PERCENT в src/kubernetes/configmap.yaml и убедится, что вызовы movies уходят полностью в новый сервис

  12. Запустите тесты из папки tests/postman
  ```bash
   npm run test:kubernetes
  ```
  Часть тестов с health-чек упадет, но создание событий отработает.
  Откройте логи event-service и сделайте скриншот обработки событий

#### Шаг 3
Добавьте сюда скриншота вывода при вызове https://cinemaabyss.example.com/api/movies и  скриншот вывода event-service после вызова тестов.

## Задание 4
Для простоты дальнейшего обновления и развертывания вам как архитектуру необходимо так же реализовать helm-чарты для прокси-сервиса и проверить работу 

Для этого:
1. Перейдите в директорию helm и отредактируйте файл values.yaml

```yaml
# Proxy service configuration
proxyService:
  enabled: true
  image:
    repository: ghcr.io/db-exp/cinemaabysstest/proxy-service
    tag: latest
    pullPolicy: Always
  replicas: 1
  resources:
    limits:
      cpu: 300m
      memory: 256Mi
    requests:
      cpu: 100m
      memory: 128Mi
  service:
    port: 80
    targetPort: 8000
    type: ClusterIP
```

- Вместо ghcr.io/db-exp/cinemaabysstest/proxy-service напишите свой путь до образа для всех сервисов
- для imagePullSecret проставьте свое значение (скопируйте из конфигурации kubernetes)
  ```yaml
  imagePullSecrets:
      dockerconfigjson: ewoJImF1dGhzIjogewoJCSJnaGNyLmlvIjogewoJCQkiYXV0aCI6ICJaR0l0Wlhod09tZG9jRjl2UTJocVZIa3dhMWhKVDIxWmFVZHJOV2hRUW10aFVXbFZSbTVaTjJRMFNYUjRZMWM9IgoJCX0KCX0sCgkiY3JlZHNTdG9yZSI6ICJkZXNrdG9wIiwKCSJjdXJyZW50Q29udGV4dCI6ICJkZXNrdG9wLWxpbnV4IiwKCSJwbHVnaW5zIjogewoJCSIteC1jbGktaGludHMiOiB7CgkJCSJlbmFibGVkIjogInRydWUiCgkJfQoJfSwKCSJmZWF0dXJlcyI6IHsKCQkiaG9va3MiOiAidHJ1ZSIKCX0KfQ==
  ```

2. В папке ./templates/services заполните шаблоны для proxy-service.yaml и events-service.yaml (опирайтесь на свою kubernetes конфигурацию - смысл helm'а сделать шаблоны для быстрого обновления и установки)

```yaml
template:
    metadata:
      labels:
        app: proxy-service
    spec:
      containers:
       Тут ваша конфигурация
```

3. Проверьте установку
Сначала удалим установку руками

```bash
kubectl delete all --all -n cinemaabyss
kubectl delete  namespace cinemaabyss
```
Запустите 
```bash
helm install cinemaabyss .\src\kubernetes\helm --namespace cinemaabyss --create-namespace
```
Если в процессе будет ошибка
```code
[2025-04-08 21:43:38,780] ERROR Fatal error during KafkaServer startup. Prepare to shutdown (kafka.server.KafkaServer)
kafka.common.InconsistentClusterIdException: The Cluster ID OkOjGPrdRimp8nkFohYkCw doesn't match stored clusterId Some(sbkcoiSiQV2h_mQpwy05zQ) in meta.properties. The broker is trying to join the wrong cluster. Configured zookeeper.connect may be wrong.
```

Проверьте развертывание:
```bash
kubectl get pods -n cinemaabyss
minikube tunnel
```

Потом вызовите 
https://cinemaabyss.example.com/api/movies
и приложите скриншот развертывания helm и вывода https://cinemaabyss.example.com/api/movies
<img width="1562" height="170" alt="задание 4_2" src="https://github.com/user-attachments/assets/37272069-26ee-4afe-b048-6578185be641" />
<img width="951" height="369" alt="задание 4_1" src="https://github.com/user-attachments/assets/1a763941-4693-458f-934d-870d787a0662" />
<img width="1919" height="122" alt="задание 4" src="https://github.com/user-attachments/assets/e08118c1-a936-4bd1-b128-ad13175c064c" />
<img width="939" height="520" alt="задание 4_3" src="https://github.com/user-attachments/assets/c1725abc-877b-45e1-9664-6fef9ad060e9" />


# Задание 5
<img width="906" height="248" alt="задание 5" src="https://github.com/user-attachments/assets/bcf477b9-8e2d-464f-bd89-bbe86aebdb4a" />
<img width="1658" height="605" alt="Задание 5_3" src="https://github.com/user-attachments/assets/5c8a93b3-3bfd-42fa-8058-abb064c6b896" />
<img width="1562" height="152" alt="задание 5_2" src="https://github.com/user-attachments/assets/73d27c91-1b10-461f-a610-a618dcaa033f" />

Компания планирует активно развиваться и для повышения надежности, безопасности, реализации сетевых паттернов типа Circuit Breaker и канареечного деплоя вам как архитектору необходимо развернуть istio и настроить circuit breaker для monolith и movies сервисов.

```bash

helm repo add istio https://istio-release.storage.googleapis.com/charts
helm repo update

helm install istio-base istio/base -n istio-system --set defaultRevision=default --create-namespace
helm install istio-ingressgateway istio/gateway -n istio-system
helm install istiod istio/istiod -n istio-system --wait

helm install cinemaabyss .\src\kubernetes\helm --namespace cinemaabyss --create-namespace

kubectl label namespace cinemaabyss istio-injection=enabled --overwrite

kubectl get namespace -L istio-injection

kubectl apply -f .\src\kubernetes\circuit-breaker-config.yaml -n cinemaabyss

```

Тестирование

# fortio
```bash
kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.25/samples/httpbin/sample-client/fortio-deploy.yaml -n cinemaabyss
```

# Get the fortio pod name
```bash
FORTIO_POD=$(kubectl get pod -n cinemaabyss | grep fortio | awk '{print $1}')

kubectl exec -n cinemaabyss $FORTIO_POD -c fortio -- fortio load -c 50 -qps 0 -n 500 -loglevel Warning http://movies-service:8081/api/movies
```
Например,

```bash
kubectl exec -n cinemaabyss fortio-deploy-b6757cbbb-7c9qg  -c fortio -- fortio load -c 50 -qps 0 -n 500 -loglevel Warning http://movies-service:8081/api/movies
```

Вывод будет типа такого

```bash
IP addresses distribution:
10.106.113.46:8081: 421
Code 200 : 79 (15.8 %)
Code 500 : 22 (4.4 %)
Code 503 : 399 (79.8 %)
```
Можно еще проверить статистику

```bash
kubectl exec -n cinemaabyss fortio-deploy-b6757cbbb-7c9qg -c istio-proxy -- pilot-agent request GET stats | grep movies-service | grep pending
```

И там смотрим 

```bash
cluster.outbound|8081||movies-service.cinemaabyss.svc.cluster.local;.upstream_rq_pending_total: 311 - столько раз срабатывал circuit breaker
You can see 21 for the upstream_rq_pending_overflow value which means 21 calls so far have been flagged for circuit breaking.
```

Приложите скриншот работы circuit breaker'а

Удаляем все
```bash
istioctl uninstall --purge
kubectl delete namespace istio-system
kubectl delete all --all -n cinemaabyss
kubectl delete namespace cinemaabyss
```
