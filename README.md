# راهنمای راه‌اندازی و استفاده از پروژه‌های Payment Analytics

این داک توسط هوش مصنوعی درست شده و درصورت ندانستن برخی پورت اطلاعات را از فایل های application.yaml هر پروژه کامل کنید.
این مستند نحوه‌ی نصب و اجرای سه پروژه‌ی مرتبط به هم را توضیح می‌دهد:

| پروژه                                                                    | نقش                                                                                              |
|--------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| [`UserAnalyze-common`](https://github.com/h-ahmadi68/UserAnalyze-common) | ماژول مشترک (مدل‌های event، DTOها و ...) که دو پروژه‌ی دیگر به آن وابسته‌اند                     |
| [`JobBuilder`](https://github.com/h-ahmadi68/JobBuilder)                 | سرویس Spring Boot که با API خودش، جاب‌های Apache Flink را می‌سازد و روی کلاستر Flink اجرا می‌کند |
| [`user-analyzing`](https://github.com/h-ahmadi68/user-analyzing)         | سرویس Spring Boot که event های پرداخت را دریافت و به Kafka ارسال می‌کند                          |

## معماری کلی جریان داده

```
client  --POST /api/v1/events/add-event-->  user-analyzing  --produce-->  Kafka topic
                                                                              │
                                                                              ▼
client --POST /api/v1/start-total-link-*--> JobBuilder --submit job--> Flink Cluster
                                                                              │
                                                                              ▼
                                                                        (consume از همان topic)
                                                                              │
                                                                              ▼
                                                                            Redis
```

هر جاب Flink که با API پروژه‌ی `JobBuilder` ساخته می‌شود، از همان Kafka topic که پروژه‌ی `user-analyzing` رویدادها را در
آن publish می‌کند مصرف می‌کند و نتیجه‌ی aggregate شده را در Redis می‌نویسد.

---

## پیش‌نیازها

- JDK 17
- Maven
- Docker / Docker Compose
- دسترسی به `mvn`, `docker`, `curl` (یا Postman) از خط فرمان

---

## مرحله ۱ — نصب پروژه‌ی `common`

هر دو پروژه‌ی دیگر به آرتیفکت زیر وابسته‌اند:

```xml

<dependency>
    <groupId>org.example</groupId>
    <artifactId>common</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

پس این پروژه باید **اول** clone و در Maven local repository نصب شود:

```bash
git clone https://github.com/h-ahmadi68/UserAnalyze-common.git
cd UserAnalyze-common
mvn clean install
```

> ⚠️ این دستور آرتیفکت `org.example:common:1.0-SNAPSHOT` را در `~/.m2/repository` نصب می‌کند. بدون این مرحله، build دو
> پروژه‌ی دیگر با خطای `Could not resolve dependency` شکست می‌خورد.

---

## مرحله ۲ — کلون و بررسی وابستگی در پروژه‌های دیگر

```bash
git clone https://github.com/h-ahmadi68/JobBuilder.git
git clone https://github.com/h-ahmadi68/user-analyzing.git
```

وابستگی به `common` از قبل در `pom.xml` هر دو پروژه تعریف شده — نیازی به تغییر دستی نیست، فقط مطمئن شو مرحله‌ی ۱ قبل از
build این دو انجام شده باشد.

---

## مرحله ۳ — بالا آوردن زیرساخت (Kafka، Redis، Flink Cluster)

فایل `docker-compose.yml` (که در ریشه‌ی پروژه‌ی `JobBuilder` قرار دارد) چهار سرویس را بالا می‌آورد:

| سرویس               | پورت روی هاست     | توضیح                                                            |
|---------------------|-------------------|------------------------------------------------------------------|
| `redis`             | `6379`            | ذخیره‌ی نتیجه‌ی aggregate شده‌ی جاب‌ها                           |
| `kafka`             | `9092` (external) | بروکر پیام؛ از بیرون کانتینر با `localhost:9092` قابل دسترسی است |
| `flink-jobmanager`  | `8081`            | Flink Web Dashboard + REST API برای submit جاب                   |
| `flink-taskmanager` | —                 | اجرای واقعی جاب‌ها (۲ اسلات)                                     |

```bash
cd JobBuilder
docker compose up -d
```

بعد از بالا آمدن، سلامت سرویس‌ها را چک کن:

```bash
docker compose ps
curl http://localhost:8081        # باید Flink Web UI برگردونه
docker exec -it redis redis-cli ping   # باید PONG برگردونه
```

> ⚠️ چون `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` تنظیم شده، نیازی به ساخت دستی topic نیست — اولین باری که یک producer
> یا consumer به یک topic جدید وصل شود، خودکار ساخته می‌شود.

---

## مرحله ۴ — بیلد و اجرای `JobBuilder`

این پروژه هم یک Spring Boot API است (برای ساخت/مدیریت جاب) و هم شامل کلاس‌های Flink job است که باید به‌صورت یک **fat jar
جدا** (`classifier=flink-job`) ساخته و روی کلاستر Flink submit شوند. `pom.xml` این پروژه دقیقاً همین کار را با
`maven-shade-plugin` انجام می‌دهد و وابستگی‌های Spring/Jackson/Logback را از آن jar حذف می‌کند تا با runtime کلاستر
Flink تداخل نکند.

```bash
cd JobBuilder
mvn clean package
```

این دستور دو artifact تولید می‌کند:

- `target/job-builder-0.0.1-SNAPSHOT.jar` — jar اصلی (بدون repackage)
- `target/job-builder-0.0.1-SNAPSHOT-boot.jar` — نسخه‌ی قابل اجرای Spring Boot API (به‌خاطر `classifier=boot`)
- `target/job-builder-0.0.1-SNAPSHOT-flink-job.jar` — fat jar مخصوص submit به Flink (به‌خاطر `classifier=flink-job`)

سرویس API را اجرا کن:

```bash
java -jar target/job-builder-0.0.1-SNAPSHOT-boot.jar
```

> ⚠️ **نامشخص/نیاز به بررسی خودت:** پورت پیش‌فرض این Spring Boot app در `application.yml`/`application.properties` پروژه
> مشخص می‌شود که در اختیار من نبود. اگر تنظیم نشده، پیش‌فرض Spring Boot یعنی `8080` است. همچنین باید مطمئن شوی این app
> طوری کانفیگ شده که به `localhost:9092` (Kafka) و به Flink REST API روی `localhost:8081` وصل شود — این مقادیر معمولاً در
`application.yml` یا env vars ست می‌شوند.
>
> **نامشخص:** پیاده‌سازی داخلی `JobLaunchService` که مشخص می‌کند دقیقاً چطور jar با classifier `flink-job` روی کلاستر
> submit می‌شود (مثلاً از طریق Flink REST API با فراخوانی `POST /jars/upload` و بعد `POST /jars/:jarid/run`) در اختیار من
> نبود. اگر جاب submit نشد، این سرویس اولین جایی است که باید لاگ‌هایش را چک کنی.

---

## مرحله ۵ — ساخت جاب از طریق API پروژه‌ی `JobBuilder`

دو endpoint در دسترس است:

### الف) جاب شمارش کل لینک‌های ارسالی

```bash
curl -X POST http://localhost:<PORT>/api/v1/start-total-link-sent \
  -H "Content-Type: application/json" \
  -d '{
    "windowType": "TUMBLING",
    "windowSize": "PT1M",
    "windowSlide": null,
    "sessionGap": null
  }'
```

### ب) جاب شمارش نرخ لینک‌های رد شده

```bash
curl -X POST http://localhost:<PORT>/api/v1/start-total-link-rejected \
  -H "Content-Type: application/json" \
  -d '{
    "windowType": "TUMBLING",
    "windowSize": "PT1M",
    "windowSlide": null,
    "sessionGap": null
  }'
```

پاسخ هر دو endpoint، یک `jobId` (کد `202 Accepted`) است.

> فرمت `WindowSpec` سه نوع پنجره را پشتیبانی می‌کند:
> - `TUMBLING` → نیاز به `windowSize` (مثلاً `PT1M` یعنی ۱ دقیقه، فرمت ISO-8601 Duration)
> - `SLIDING` → نیاز به `windowSize` و `windowSlide`
> - `SESSION` → نیاز به `sessionGap`

جاب ساخته‌شده را می‌توانی در Flink Web Dashboard ببینی:

```
http://localhost:8081
```

---

## مرحله ۶ — بیلد و اجرای `user-analyzing` و ارسال event

```bash
cd user-analyzing
mvn clean package
java -jar target/demo-0.0.1-SNAPSHOT.jar
```

> ⚠️ همین‌جا هم پورت پیش‌فرض این app مشخص نیست (در اختیار من نبود) — پیش‌فرض Spring Boot یعنی `8080` است، اما چون
`JobBuilder` هم احتمالاً روی `8080` بالا می‌آید، **باید حداقل یکی از این دو app را با `--server.port=<PORT_متفاوت>` اجرا
کنی** تا با هم تداخل پورت پیدا نکنند. مثلاً:
> ```bash
> java -jar target/demo-0.0.1-SNAPSHOT.jar --server.port=8082
> ```

بعد از بالا آمدن، برای ارسال یک event پرداخت:

```bash
curl -X POST http://localhost:8082/api/v1/events/add-event \
  -H "Content-Type: application/json" \
  -d '{
    "type": "ASK_FOR_LINK",
    "timestamp": "2026-08-17T08:26:10.910669200Z",
    "userId": "user-123",
    "amount": 1500000.5,
    "bank": "MELLAT"
  }'
```

و برای رد کردن همان لینک:

```bash
curl -X POST http://localhost:8082/api/v1/events/add-event \
  -H "Content-Type: application/json" \
  -d '{
    "type": "REJECT_LINK",
    "timestamp": "2026-08-17T08:26:20.619030100Z",
    "paymentLinkId": "link-123456",
    "userId": "user-123"
  }'
```

هر بار که این endpoint فراخوانی شود، `EventService` رویداد را پردازش کرده و به Kafka topic مربوطه produce می‌کند.

---

## مرحله ۷ — مشاهده‌ی نتیجه در Redis

```bash
docker exec -it redis redis-cli
127.0.0.1:6379> keys *
127.0.0.1:6379> get "TotalRejectedLink-TUMBLING-<window-start>-to-<window-end>"
```

---

## ترتیب کامل اجرا (خلاصه)

1. `git clone` + `mvn clean install` روی `UserAnalyze-common`
2. `git clone` روی `JobBuilder` و `user-analyzing`
3. `docker compose up -d` در ریشه‌ی `JobBuilder` (Redis + Kafka + Flink cluster)
4. `mvn clean package` و اجرای `JobBuilder` (`-boot.jar`)
5. با `curl`/Postman یک یا چند جاب از طریق API `JobBuilder` بساز
6. `mvn clean package` و اجرای `user-analyzing` (با پورت متفاوت از `JobBuilder`)
7. با `curl`/Postman از `user-analyzing` رویداد ارسال کن
8. نتیجه را در Redis (`redis-cli`) یا Flink Dashboard (`localhost:8081`) ببین

---

## نکات و مسائل شناخته‌شده

- اگر پنجره‌ی زمانی هیچ رویداد جدیدی بعد از بسته شدنش دریافت نکند، ممکن است تا رسیدن رویداد بعدی fire نشود (رفتار
  استاندارد event-time watermark در Flink). برای رفع این مورد از `withIdleness` یا `ProcessingTimeTrigger` روی window
  استفاده کن.
- اگر بعد از فراخوانی `add-event`، عدد نهایی در Redis با انتظارت مطابقت نداشت، اول چک کن که رویداد واقعاً به Kafka
  رسیده (با یک consumer دستی روی topic)، بعد لاگ‌های filter/aggregate داخل جاب Flink را بررسی کن.

## موارد باز برای تکمیل توسط تو

- [ ] پورت پیش‌فرض `JobBuilder` (Spring Boot API)
- [ ] پورت پیش‌فرض `user-analyzing` (Spring Boot API)
- [ ] نام دقیق Kafka topic(هایی) که `user-analyzing` به آن publish می‌کند و `JobBuilder` از آن consume می‌کند (باید یکی
  باشند)
- [ ] پیاده‌سازی داخلی `JobLaunchService` (نحوه‌ی submit جار به Flink REST API)
- [ ] فیلدهای کامل DTO ی `PaymentEventDto` (برای پوشش همه‌ی نوع event ها، نه فقط دو نمونه‌ی بالا)