# Házi Feladat Specifikáció

**Data Engineering** - Opcionális házi feladat

---

## 1. Hallgató adatai

|                |                       |
| -------------- | --------------------- |
| **Név**        | Sőrés Barna           |
| **Neptun-kód** | CASI6C                |
| **E-mail**     | sores.barni@gmail.com |

---

## 2. Témaválasztás

|                     |                                          |
| ------------------- | ---------------------------------------- |
| **Választott téma** | Kriptovaluta és részvény adatok elemzése |

**Rövid leírás**
A projekt célja egy automatizált adatfolyam megvalósítása, amely tőzsdei és kriptovaluta piaci adatokat gyűjt össze és rendszerez közös platformra. Az adatokat külső REST API-kból (CoinPaprika, yfinance) és statikus forrásból (Kaggle S&P 500 CSV) nyeri ki. A pipeline feladata a nyers adatok tisztítása, egységesített csillag sémába rendezése, valamint az adatok hatékony kiszolgálása PostgreSQL adattárház és Redis cache segítségével, vizuális megjelenítéssel (Grafana) kiegészítve.

---

## 3. Tervezett pipeline elemei

| Elem                          | Tervezett megoldás / eszköz                                                                             |
| ----------------------------- | ------------------------------------------------------------------------------------------------------- |
| **Adatforrások**              | CoinPaprika REST API, yfinance Python könyvtár, Kaggle S&P 500 vállalatlista CSV                        |
| **Feldolgozási mód**          | Batch: 5 perces napközbeni a kriptó és részvényekre és napi egyszeri ütemezés a S&P 500 céges adatokhoz |
| **Landing zone**              | MinIO (S3 kompatibilis objektumtároló)                                                                  |
| **Adatmodell típusa**         | Csillag séma: 1 központi ténytábla (árak) és 2 dimenziótábla (eszközök, naptár)                         |
| **Adattárház / adatplatform** | PostgreSQL                                                                                              |
| **Transzformáció**            | Pandas                                                                                                  |
| **Orchestration eszköz**      | Prefect (Dockerizált szerver és worker architektúra)                                                    |
| **Infrastruktúra**            | Docker Compose                                                                                          |
| **Adatkiszolgálás**           | Grafana Dashboardok, FastAPI végpontok, Redis Cache                                                     |
