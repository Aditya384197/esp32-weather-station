/*
 * Aditya Weather Station
 * ESP32 firmware - USB/Serial flashing only, no OTA
 */


#include <WiFi.h>
#include <HTTPClient.h>
#include <ESPAsyncWebServer.h>
#include <DNSServer.h>
#include <AsyncTCP.h>
#include <LittleFS.h>
#include <ArduinoJson.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <DHT.h>
#include <Adafruit_BME280.h>
#include <BH1750.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <ESPmDNS.h>
#include <esp_task_wdt.h>
#include <Preferences.h>
#include <time.h>
#include <math.h>
#include <stdarg.h>
#include <ctype.h>
#include <esp_heap_caps.h>
#include <esp_system.h>

#ifndef ESP_ARDUINO_VERSION
  #define ESP_ARDUINO_VERSION 0
#endif
#ifndef ESP_ARDUINO_VERSION_VAL
  #define ESP_ARDUINO_VERSION_VAL(a,b,c) (((a)<<16)|((b)<<8)|(c))
#endif

#define FIRMWARE_VERSION      "1.0"
#define SEA_LEVEL_HPA         1013.25f
#define AUTO_CYCLE_MS         10000
#define SENSOR_READ_MS        5000
#define DISPLAY_REFRESH_MS    250
#define LOG_INTERVAL_MS       300000
#define MAX_LOG_RECORDS       288
#define LOG_MAX_FILE_BYTES    65536
#define NTP_RESYNC_INTERVAL   3600
#define AP_SSID               "WeatherStation-Setup"
#define MDNS_NAME             "weatherstation"
#define WS_CLEANUP_INTERVAL_MS 30000

#define BOOT_CONFIRM_DELAY_MS   20000
#define PREF_BOOT_FAIL_COUNT    "boot_fc"
#define PREF_BOOT_CONFIRMED     "boot_ok"
#define CRASH_LOOP_THRESHOLD    3

#define I2C_RECOVERY_AFTER_FAILS 8
#define I2C_SDA_PIN  21
#define I2C_SCL_PIN  22

#define TASK_HEARTBEAT_TIMEOUT_MS  15000
#define WIFI_HARD_RESET_AFTER_FAILS 10

#define DS18_SAMPLE_COUNT     3
#define BME_RETRY_MAX         3
#define MA_WINDOW             5
#define DHT_TEMP_JUMP_MAX     15.0f
#define DHT_HUM_JUMP_MAX      25.0f
#define DS18_JUMP_MAX         10.0f
#define BME_PRESS_JUMP_MAX     8.0f
#define LUX_JUMP_RATIO         6.0f
#define SENSOR_WARN_FAILS      3
#define SENSOR_ERROR_FAILS     6
#define EVENT_LOG_SIZE         40
#define HEALTH_TASK_PERIOD_MS  2000
#define HEAP_WARN_BYTES        40000
#define HEAP_ERROR_BYTES       20000
#define FRAG_WARN_PCT          50

#define DASH_USER             "admin"
#define DASH_PASS             "weather123"
#define API_KEY               "ws-api-key-v3"
#define LOG_CLEAR_TOKEN       "clear-ok"

#define MAX_BODY_BYTES          4096
#define AUTH_TRACK_SLOTS        16
#define AUTH_FAIL_LOCK_THRESHOLD 5
#define AUTH_FAIL_WINDOW_MS      60000UL
#define AUTH_LOCKOUT_BASE_MS     2000UL
#define AUTH_LOCKOUT_MAX_MS      300000UL

#define DHT_PIN       4
#define DHT_TYPE      DHT22
#define DS18B20_PIN   5
#define BUTTON_PIN    15
#define DEBOUNCE_MS   50
#define SCREEN_W      128
#define SCREEN_H       64
#define OLED_ADDR     0x3C
#define BME_ADDR      0x76
#define BH_ADDR       0x23
#define PRESS_BUF_SIZE  12
#define WDT_TIMEOUT_S   60
#define OVERRIDE_DURATION_MS  60000UL
#define OWM_API_DEFAULT_KEY   ""

#define PREF_NAMESPACE  "ws_cfg"
#define PREF_TZ_OFFSET  "tz_offset"
#define PREF_WIFI_SSID  "wifi_ssid"
#define PREF_WIFI_PASS  "wifi_pass"
#define PREF_OWM_KEY    "owm_key"
#define PREF_TZ_STR     "tz_str"
#define PREF_AP_PASS    "ap_pass"
#define PREF_WX_CITY    "wx_city"
#define PREF_WX_COUNTRY "wx_country"
#define FALLBACK_AP_PASS "weather123"
#define WEATHER_REFRESH_MS (10UL*60UL*1000UL)

Adafruit_SSD1306  display(SCREEN_W, SCREEN_H, &Wire, -1);
DHT               dht(DHT_PIN, DHT_TYPE);
Adafruit_BME280   bme;
BH1750            lightMeter;
OneWire           oneWireBus(DS18B20_PIN);
DallasTemperature ds18b20(&oneWireBus);
AsyncWebServer    server(80);
DNSServer         captiveDns;
AsyncWebSocket    ws("/ws");
Preferences       prefs;

SemaphoreHandle_t dataMutex;
SemaphoreHandle_t displayMutex;
SemaphoreHandle_t overrideMutex;
SemaphoreHandle_t eventMutex;
SemaphoreHandle_t wsMutex;
SemaphoreHandle_t fsMutex;

TaskHandle_t      hSensorTask;
TaskHandle_t      hDisplayTask;
TaskHandle_t      hWifiTask;
TaskHandle_t      hHealthTask;

volatile bool     bootConfirmed    = false;
uint32_t          bootStartMs      = 0;
volatile bool     safeModeActive   = false;
char              safeModeReason[64] = {0};

volatile uint32_t hbSensor=0, hbDisplay=0, hbWifi=0, hbHealth=0;

volatile uint16_t i2cBmeFailStreak = 0, i2cBhFailStreak = 0;
volatile uint32_t i2cRecoveryCount = 0;

volatile uint16_t wifiHardFailStreak = 0;
volatile uint32_t wifiHardResetCount = 0;

volatile bool     fetchWxRunning   = false;

volatile bool oledOK = true;

enum SensorHealth : uint8_t { HEALTH_ONLINE = 0, HEALTH_WARNING = 1, HEALTH_ERROR = 2 };
inline const char* healthStr(SensorHealth h) {
  switch (h) { case HEALTH_ONLINE: return "ONLINE"; case HEALTH_WARNING: return "WARNING"; default: return "ERROR"; }
}

struct SensorData {
  float dhtTemp, dhtHumidity;
  float bmeTemp, bmePressure, bmeHumidity, altitudeM, pressureTrend;
  float lux;
  float ds18Temp[2];
  uint8_t ds18Count;
  float ds18Min, ds18Max;
  float heatIndex, dewPoint, rainProbPct;
  bool dhtOK, bmeOK, bh1750OK, ds18OK;
  bool ds18MinMaxInit;
  uint8_t       dhtConfidence, bmeConfidence, bhConfidence, dsConfidence;
  SensorHealth  dhtHealth, bmeHealth, bhHealth, dsHealth;
  uint16_t      dhtRetries, bmeRetries, bhRetries, dsRetries;
  uint16_t      dhtErrors,  bmeErrors,  bhErrors,  dsErrors;
  uint32_t      snapshotSeq;
  uint32_t      snapshotMs;
};
SensorData sd = {};

float dhtTempSamples[MA_WINDOW]; float dhtHumSamples[MA_WINDOW]; uint8_t dhtMaIdx=0; bool dhtMaFull=false;
float bmeTempSamples[MA_WINDOW]; float bmePressSamples[MA_WINDOW]; float bmeHumSamples[MA_WINDOW]; uint8_t bmeMaIdx=0; bool bmeMaFull=false;
float luxSamples[MA_WINDOW]; uint8_t luxMaIdx=0; bool luxMaFull=false;
float dsSamples[MA_WINDOW]; uint8_t dsMaIdx=0; bool dsMaFull=false;
bool  dhtHasLastGood=false, bmeHasLastGood=false, bhHasLastGood=false, dsHasLastGood=false;
float dhtLastGoodT=0, dhtLastGoodH=0, bmeLastGoodP=0, bhLastGoodLux=0, dsLastGoodT=0;

enum EventLevel : uint8_t { EVT_INFO = 0, EVT_WARN = 1, EVT_ERROR = 2 };
struct EventEntry { uint32_t ms; EventLevel level; char msg[64]; };
EventEntry        eventLog[EVENT_LOG_SIZE];
uint16_t          eventHead = 0, eventCount = 0;
uint32_t          eventSeq  = 0;

bool offlineMode = false;
char apIpStr[20] = "0.0.0.0";

void logEvent(EventLevel level, const char* fmt, ...);

float    pressBuf[PRESS_BUF_SIZE];
uint32_t pressBufTime[PRESS_BUF_SIZE];
uint8_t  pressBufIdx  = 0;
bool     pressBufFull = false;

enum DisplayState : uint8_t { DISP_LOCAL = 0, DISP_OVERRIDE = 1 };

struct OverrideData {
  char  city[48]; char country[8]; char description[48];
  float tempC, feelsLike, humidity, pressure, windSpeed;
  int   clouds;
  char  timeStr[12]; char timezone[40]; char iconCode[8];
  bool  valid;
};
OverrideData overrideData = {};

struct PersistentWeatherData {
  char city[48];
  char country[8];
  char description[48];
  char iconCode[8];
  char timeStr[12];
  char timezone[40];
  float tempC, feelsLike, humidity, pressure, windSpeed;
  int clouds;
  int visibilityM;
  uint32_t updatedMs;
  bool valid;
};
PersistentWeatherData savedWeather = {};

volatile DisplayState displayState  = DISP_LOCAL;
volatile uint32_t     overrideEndMs = 0;

struct SavedConfig {
  int32_t tzOffsetSec; char tzStr[40]; char owmApiKey[48]; char wifiSsid[64]; char wifiPass[64];
  char apPass[32];
  char weatherCity[48];
  char weatherCountry[8];
};
SavedConfig cfg = { 19800, "IST-5:30", "", "", "", FALLBACK_AP_PASS, "", "" };

struct SecurityConfig {
  char dashUser[24];
  char dashPass[40];
  char apiKey[48];
  char logClearToken[32];
  bool usingFactoryDefaults;
};
SecurityConfig sec = { DASH_USER, DASH_PASS, API_KEY, LOG_CLEAR_TOKEN, true };

struct AuthFailEntry { uint32_t ip; uint8_t fails; uint32_t firstFailMs; uint32_t lastFailMs; };
AuthFailEntry authFailTable[AUTH_TRACK_SLOTS] = {};

#define MAX_SESSIONS      4
#define SESSION_TOKEN_LEN 32
#define SESSION_TTL_MS    (30UL*60UL*1000UL)
struct SessionEntry { char token[SESSION_TOKEN_LEN+1]; uint32_t expiresAt; bool active; };
SessionEntry sessions[MAX_SESSIONS] = {};
SemaphoreHandle_t sessionMutex;
SemaphoreHandle_t authTableMutex;

enum Page : uint8_t {
  PAGE_WEATHER = 0, PAGE_TIME = 1, PAGE_CALENDAR = 2, PAGE_EXTRA = 3,
  PAGE_SYSTEM  = 4, PAGE_SENSOR_DIAG = 5, TOTAL_PAGES = 6
};
volatile Page    currentPage    = PAGE_WEATHER;
volatile Page    targetPage     = PAGE_WEATHER;
volatile bool    pageChangeReq  = false;
uint32_t         lastAutoCycleMs = 0;

struct LogRecord { time_t ts; float temp, humidity, pressure, lux, outdoorTemp; };
LogRecord logRing[MAX_LOG_RECORDS];
uint16_t  logHead=0, logCount=0;
uint8_t   activeLog=0;

static const char* const DAY_NAMES[]  = { "Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday" };
static const char* const MONTH_FULL[] = { "","January","February","March","April","May","June","July","August","September","October","November","December" };

void sensorTask(void*); void displayTask(void*); void wifiTask(void*); void systemHealthTask(void*);
void setupWebServer();
void startCaptiveDns();
void showSplash(const char* l1, const char* l2 = "");
void bootProgressStep(const char* label, uint8_t pct);
void showBootSequence();
void nextPage(); void renderCurrentPage();
void drawLocalPageOnly(Page p);
void drawWeatherPage(); void drawTimePage(); void drawCalendarPage(); void drawExtraPage();
bool drawOverridePage();
void drawSystemStatusPage(); void drawSensorDiagPage();
void drawPageDots(Page p); void drawCornerTicks(); void drawTitleBar(const char* title);
void printCentered(const char* text, int16_t y, uint8_t sz = 1);
void slideTransition(Page from, Page to);
float computeHeatIndex(float tempC, float rh);
float computeDewPoint(float tempC, float rh);
float computeRainProbability(float rh, float pressHPa, float trend, float dew, float tempC);
const char* weatherClassification(float prob);
float pressureTrendFromBuffer();
void logRotateIfNeeded(); void appendLogRecord(const LogRecord& rec);
void broadcastWsUpdate();
bool checkApiKey(AsyncWebServerRequest* req); bool checkBasicAuth(AsyncWebServerRequest* req);
void loadConfig(); void saveConfig();
bool fetchWeatherAndTime(const char* city, const char* countryCode);
bool fetchPersistentWeather();
bool fetchWeatherRaw(const char* city, const char* countryCode, PersistentWeatherData* out);
bool connectHiddenWifi(const char* ssid, const char* password);
void formatUtcOffset(long offsetSec, char* out, size_t outLen);
float medianOf(float* arr, uint8_t n);
float movingAverage(float* ring, uint8_t cap, bool full, uint8_t idx);
uint8_t computeConfidence(uint16_t retries, uint16_t consecFails, bool rejectedSpike, bool gotFreshSample);
SensorHealth healthFromFails(uint16_t consecFails, bool hasLastGood, bool gotFreshSample);
void safeRestart(const char* reason);
void wsTextAll(const String& msg);

bool secureCompare(const char* a, const char* b);
uint32_t clientIpToU32(AsyncWebServerRequest* req);
String ipU32ToStr(uint32_t ip);
bool isAuthLocked(uint32_t ip, uint32_t* retryAfterMsOut);
void recordAuthFailure(uint32_t ip);
void recordAuthSuccess(uint32_t ip);
bool checkOriginSameHost(AsyncWebServerRequest* req);
bool rejectIfBodyTooLarge(AsyncWebServerRequest* req, size_t total);
void loadSecurityConfig();
void saveSecurityConfig();
void applySecurityHeaders(AsyncWebServerResponse* resp);

void i2cBusRecover();
void confirmBootIfStable();
void enterSafeMode(const char* reason);
void checkTaskHeartbeats();
void hardResetWifi();
void recordBootAttempt();

void wsTextAll(const String& msg) {
  if (ws.count() == 0) return;
  if (xSemaphoreTake(wsMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    ws.textAll(msg);
    xSemaphoreGive(wsMutex);
  }
}

bool secureCompare(const char* a, const char* b) {
  size_t la = strlen(a), lb = strlen(b);
  uint8_t diff = (la != lb) ? 1 : 0;
  size_t n = (la < lb) ? la : lb;
  for (size_t i = 0; i < n; i++) diff |= (uint8_t)(a[i] ^ b[i]);
  
  for (size_t i = n; i < ((la > lb) ? la : lb); i++) diff |= 1;
  return diff == 0;
}

uint32_t clientIpToU32(AsyncWebServerRequest* req) {
  IPAddress ip = req->client()->remoteIP();
  return ((uint32_t)ip[0] << 24) | ((uint32_t)ip[1] << 16) | ((uint32_t)ip[2] << 8) | (uint32_t)ip[3];
}

bool isAuthLocked(uint32_t ip, uint32_t* retryAfterMsOut) {
  if (retryAfterMsOut) *retryAfterMsOut = 0;
  if (xSemaphoreTake(authTableMutex, pdMS_TO_TICKS(50)) != pdTRUE) return false;
  bool locked = false;
  uint32_t now = millis();
  for (uint8_t i = 0; i < AUTH_TRACK_SLOTS; i++) {
    if (authFailTable[i].ip == ip && authFailTable[i].fails > 0) {
      
      if (now - authFailTable[i].lastFailMs > AUTH_FAIL_WINDOW_MS) { authFailTable[i].fails = 0; break; }
      if (authFailTable[i].fails >= AUTH_FAIL_LOCK_THRESHOLD) {
        uint8_t over = authFailTable[i].fails - AUTH_FAIL_LOCK_THRESHOLD;
        uint32_t lockMs = AUTH_LOCKOUT_BASE_MS << (over > 6 ? 6 : over);
        if (lockMs > AUTH_LOCKOUT_MAX_MS) lockMs = AUTH_LOCKOUT_MAX_MS;
        uint32_t elapsed = now - authFailTable[i].lastFailMs;
        if (elapsed < lockMs) { locked = true; if (retryAfterMsOut) *retryAfterMsOut = lockMs - elapsed; }
      }
      break;
    }
  }
  xSemaphoreGive(authTableMutex);
  return locked;
}

String ipU32ToStr(uint32_t ip) {
  char buf[16];
  snprintf(buf,sizeof(buf),"%u.%u.%u.%u",(unsigned)(ip>>24)&0xFF,(unsigned)(ip>>16)&0xFF,(unsigned)(ip>>8)&0xFF,(unsigned)ip&0xFF);
  return String(buf);
}

void recordAuthFailure(uint32_t ip) {
  if (xSemaphoreTake(authTableMutex, pdMS_TO_TICKS(50)) != pdTRUE) return;
  uint32_t now = millis();
  int8_t slot = -1, oldestIdx = 0; uint32_t oldestMs = 0xFFFFFFFFUL;
  for (uint8_t i = 0; i < AUTH_TRACK_SLOTS; i++) {
    if (authFailTable[i].ip == ip) { slot = i; break; }
    if (authFailTable[i].fails == 0) { if (slot < 0) slot = i; }
    if (authFailTable[i].lastFailMs < oldestMs) { oldestMs = authFailTable[i].lastFailMs; oldestIdx = i; }
  }
  if (slot < 0) slot = oldestIdx;
  if (authFailTable[slot].ip != ip || authFailTable[slot].fails == 0) {
    authFailTable[slot].ip = ip; authFailTable[slot].fails = 0; authFailTable[slot].firstFailMs = now;
  }
  if (authFailTable[slot].fails < 250) authFailTable[slot].fails++;
  authFailTable[slot].lastFailMs = now;
  uint8_t failsNow = authFailTable[slot].fails;
  xSemaphoreGive(authTableMutex);
  
  if (failsNow == 1) logEvent(EVT_WARN, "Auth failure from %s", ipU32ToStr(ip).c_str());
  else if (failsNow == AUTH_FAIL_LOCK_THRESHOLD) logEvent(EVT_ERROR, "Auth lockout engaged for %s (%u failures)", ipU32ToStr(ip).c_str(), (unsigned)failsNow);
}

void recordAuthSuccess(uint32_t ip) {
  if (xSemaphoreTake(authTableMutex, pdMS_TO_TICKS(50)) != pdTRUE) return;
  for (uint8_t i = 0; i < AUTH_TRACK_SLOTS; i++) if (authFailTable[i].ip == ip) { authFailTable[i].fails = 0; break; }
  xSemaphoreGive(authTableMutex);
}

static String extractUrlHost(const String& url) {
  int start = url.indexOf("://");
  start = (start >= 0) ? start + 3 : 0;
  int end = url.length();
  for (int i = start; i < (int)url.length(); i++) {
    char c = url[i];
    if (c == '/' || c == '?' || c == '#') { end = i; break; }
  }
  return url.substring(start, end);
}

bool checkOriginSameHost(AsyncWebServerRequest* req) {
  String host = req->host();
  if (host.length() == 0) return true;
  String origin = req->hasHeader("Origin") ? req->header("Origin") : String();
  String referer = req->hasHeader("Referer") ? req->header("Referer") : String();
  String check = origin.length() ? origin : referer;
  if (check.length() == 0) return true;
  return extractUrlHost(check).equalsIgnoreCase(host);
}

bool rejectIfBodyTooLarge(AsyncWebServerRequest* req, size_t total) {
  if (total > MAX_BODY_BYTES) {
    req->send(413, "application/json", "{\"error\":\"Body too large\"}");
    return true;
  }
  return false;
}

void applySecurityHeaders(AsyncWebServerResponse* resp) {
  resp->addHeader("X-Content-Type-Options", "nosniff");
  resp->addHeader("X-Frame-Options", "DENY");
  resp->addHeader("Cache-Control", "no-store");
}

void loadSecurityConfig() {
  prefs.begin(PREF_NAMESPACE, true);
  String u = prefs.getString("sec_user", DASH_USER);
  String p = prefs.getString("sec_pass", DASH_PASS);
  String k = prefs.getString("sec_apikey", API_KEY);
  String t = prefs.getString("sec_clrtok", LOG_CLEAR_TOKEN);
  prefs.end();
  strlcpy(sec.dashUser, u.c_str(), sizeof(sec.dashUser));
  strlcpy(sec.dashPass, p.c_str(), sizeof(sec.dashPass));
  strlcpy(sec.apiKey,   k.c_str(), sizeof(sec.apiKey));
  strlcpy(sec.logClearToken, t.c_str(), sizeof(sec.logClearToken));
  sec.usingFactoryDefaults = secureCompare(sec.dashPass, DASH_PASS) && secureCompare(sec.apiKey, API_KEY);
}
void saveSecurityConfig() {
  prefs.begin(PREF_NAMESPACE, false);
  prefs.putString("sec_user",   sec.dashUser);
  prefs.putString("sec_pass",   sec.dashPass);
  prefs.putString("sec_apikey", sec.apiKey);
  prefs.putString("sec_clrtok", sec.logClearToken);
  prefs.end();
}

void i2cBusRecover() {
  logEvent(EVT_WARN, "I2C bus recovery triggered");
  i2cRecoveryCount++;
  Wire.end();
  pinMode(I2C_SDA_PIN, INPUT_PULLUP);
  pinMode(I2C_SCL_PIN, OUTPUT);
  digitalWrite(I2C_SCL_PIN, HIGH);
  delayMicroseconds(5);
  
  for (uint8_t i = 0; i < 9 && digitalRead(I2C_SDA_PIN) == LOW; i++) {
    digitalWrite(I2C_SCL_PIN, LOW);  delayMicroseconds(5);
    digitalWrite(I2C_SCL_PIN, HIGH); delayMicroseconds(5);
  }
  
  pinMode(I2C_SDA_PIN, OUTPUT);
  digitalWrite(I2C_SDA_PIN, LOW);  delayMicroseconds(5);
  digitalWrite(I2C_SCL_PIN, HIGH); delayMicroseconds(5);
  digitalWrite(I2C_SDA_PIN, HIGH); delayMicroseconds(5);
  Wire.begin(I2C_SDA_PIN, I2C_SCL_PIN);
  Wire.setClock(400000);
  i2cBmeFailStreak = 0; i2cBhFailStreak = 0;
  logEvent(EVT_INFO, "I2C bus recovery complete (#%lu)", (unsigned long)i2cRecoveryCount);
}

void confirmBootIfStable() {
  if (bootConfirmed) return;
  if (millis() - bootStartMs < BOOT_CONFIRM_DELAY_MS) return;
  prefs.begin(PREF_NAMESPACE, false);
  prefs.putUInt(PREF_BOOT_FAIL_COUNT, 0);
  prefs.putBool(PREF_BOOT_CONFIRMED, true);
  prefs.end();
  bootConfirmed = true;
  logEvent(EVT_INFO, "Boot confirmed stable — crash-loop counter cleared");
}

void recordBootAttempt() {
  prefs.begin(PREF_NAMESPACE, false);
  uint32_t fails = prefs.getUInt(PREF_BOOT_FAIL_COUNT, 0);
  fails++;
  prefs.putUInt(PREF_BOOT_FAIL_COUNT, fails);
  prefs.end();
  if (fails >= CRASH_LOOP_THRESHOLD) {
    snprintf(safeModeReason, sizeof(safeModeReason), "%lu rapid reboots detected", (unsigned long)fails);
    safeModeActive = true;
  }
}

void enterSafeMode(const char* reason) {
  safeModeActive = true;
  strncpy(safeModeReason, reason, sizeof(safeModeReason) - 1);
  Serial.printf("[SAFE MODE] %s\n", reason);
}

void checkTaskHeartbeats() {
  uint32_t now = millis();
  if (hbSensor  && (now - hbSensor)  > TASK_HEARTBEAT_TIMEOUT_MS) logEvent(EVT_ERROR, "Sensor task heartbeat stale (%lus)",  (unsigned long)((now-hbSensor)/1000));
  if (hbDisplay && (now - hbDisplay) > TASK_HEARTBEAT_TIMEOUT_MS) logEvent(EVT_ERROR, "Display task heartbeat stale (%lus)", (unsigned long)((now-hbDisplay)/1000));
  if (hbWifi    && (now - hbWifi)    > TASK_HEARTBEAT_TIMEOUT_MS) logEvent(EVT_ERROR, "WiFi task heartbeat stale (%lus)",    (unsigned long)((now-hbWifi)/1000));
  if (hbHealth  && (now - hbHealth)  > TASK_HEARTBEAT_TIMEOUT_MS) logEvent(EVT_ERROR, "Health task heartbeat stale (%lus)",  (unsigned long)((now-hbHealth)/1000));
}

void hardResetWifi() {
  wifiHardResetCount++;
  logEvent(EVT_WARN, "WiFi hard-reset escalation (#%lu)", (unsigned long)wifiHardResetCount);

  WiFi.disconnect(true);
  delay(200);
  WiFi.mode(WIFI_OFF);
  delay(300);

  WiFi.mode(WIFI_AP_STA);
  WiFi.softAP(AP_SSID, cfg.apPass);
  delay(200);

  strncpy(apIpStr, WiFi.softAPIP().toString().c_str(), sizeof(apIpStr) - 1);
  apIpStr[sizeof(apIpStr) - 1] = '\0';

  startCaptiveDns();

  if (cfg.wifiSsid[0] != '\0') {
    WiFi.begin(cfg.wifiSsid, cfg.wifiPass);
  }

  wifiHardFailStreak = 0;
}

static const char DASHBOARD_HTML[] PROGMEM = R"WSDASH(<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,minimum-scale=0.5">
<title>ADITYA WEATHER STATION</title>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;500;600;700;800;900&family=Share+Tech+Mono&family=Rajdhani:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>

:root{
  --bg:#030b07;--bg1:#040e09;--bg2:#050f0a;
  --c1:#00ffaa;--c2:#00c8ff;--c3:#c084fc;--c4:#fbbf24;--c5:#ff3366;
  --c1a:rgba(0,255,170,.18);--c2a:rgba(0,200,255,.15);
  --bdr:rgba(0,255,170,.22);--bdr2:rgba(0,200,255,.18);
  --txt:#ccfff0;--dim:#3a7055;--dim2:#183328;
  --sh1:0 0 18px rgba(0,255,170,.28),0 0 50px rgba(0,255,170,.08);
  --sh2:0 0 18px rgba(0,200,255,.28),0 0 50px rgba(0,200,255,.08);
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{background:var(--bg);color:var(--txt);
  font-family:'Rajdhani',sans-serif;font-weight:500;
  
  min-height:100vh;
  background-image:
    radial-gradient(ellipse 90% 60% at 10% 40%,rgba(0,255,170,.05) 0%,transparent 65%),
    radial-gradient(ellipse 70% 90% at 88% 15%,rgba(0,200,255,.05) 0%,transparent 65%),
    radial-gradient(ellipse 50% 50% at 50% 95%,rgba(192,132,252,.04) 0%,transparent 65%),
    linear-gradient(rgba(0,255,170,.025) 1px,transparent 1px),
    linear-gradient(90deg,rgba(0,255,170,.025) 1px,transparent 1px);
  background-size:100% 100%,100% 100%,100% 100%,38px 38px,38px 38px}
body::before{content:"";position:fixed;inset:0;z-index:0;pointer-events:none;
  background:repeating-linear-gradient(0deg,transparent 0,transparent 2px,rgba(0,0,0,.07) 2px,rgba(0,0,0,.07) 3px)}
#scan{position:fixed;left:0;right:0;height:2px;z-index:4;pointer-events:none;opacity:.6;
  background:linear-gradient(transparent,rgba(0,255,170,.5),transparent);animation:sc 11s linear infinite}
@keyframes sc{from{top:-2px}to{top:100vh}}
.shell{position:relative;z-index:10;display:grid;
  grid-template-columns:206px 1fr 248px;
  grid-template-rows:54px 1fr 30px;min-height:100vh}

.topbar{grid-column:1/-1;display:flex;align-items:stretch;position:relative;
  background:linear-gradient(180deg,rgba(5,16,11,.99),rgba(3,10,7,.99));
  border-bottom:1px solid var(--bdr);
  box-shadow:0 2px 0 rgba(0,255,170,.08) inset,0 6px 40px rgba(0,0,0,.7)}
.topbar::after{content:"";position:absolute;bottom:0;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,var(--c1),var(--c2),var(--c3),var(--c2),var(--c1),transparent);opacity:.8}
.tb-brand{width:206px;flex-shrink:0;display:flex;align-items:center;gap:10px;padding:0 14px;
  border-right:1px solid var(--bdr)}
.brand-dot{width:9px;height:9px;border-radius:50%;background:var(--c1);
  box-shadow:0 0 12px var(--c1),0 0 24px rgba(0,255,170,.3);animation:bd 2s ease-in-out infinite}
.brand-dot.off{background:var(--c5);box-shadow:0 0 10px var(--c5);animation:none}
@keyframes bd{0%,100%{opacity:1}50%{opacity:.25}}
.brand h1{font-family:'Orbitron',sans-serif;font-size:12px;font-weight:900;letter-spacing:2.5px;
  color:var(--c1);line-height:1.2;
  animation:hg 3s ease-in-out infinite}
@keyframes hg{0%,100%{text-shadow:0 0 12px rgba(0,255,170,.9),0 0 2px rgba(0,255,170,1)}
  50%{text-shadow:0 0 24px rgba(0,255,170,1),0 0 50px rgba(0,255,170,.35),0 0 2px #fff}}
.brand small{font-size:8px;color:var(--c2);letter-spacing:2px}
.tb-feed{padding:0 12px;display:flex;flex-direction:column;justify-content:center;
  border-right:1px solid var(--bdr);min-width:164px}
.tb-feed .fl{font-size:8px;color:var(--dim);letter-spacing:1px;text-transform:uppercase;margin-bottom:3px}
.tb-mid{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px}
.sys-online{font-family:'Orbitron',sans-serif;font-size:17px;font-weight:900;color:var(--c1);
  letter-spacing:3px;text-shadow:0 0 20px rgba(0,255,170,.9)}
.sys-sub{font-size:10px;color:var(--c2);letter-spacing:2px;font-weight:600}
.tb-stats{display:flex;border-left:1px solid var(--bdr)}
.tbs{display:flex;flex-direction:column;justify-content:center;padding:0 12px;
  border-right:1px solid var(--bdr);text-align:center;min-width:88px}
.tbs .l{font-size:8px;color:var(--dim);letter-spacing:1px;text-transform:uppercase}
.tbs .v{font-family:'Orbitron',sans-serif;font-size:12px;font-weight:700;color:var(--c1)}
.tbs .v.c2{color:var(--c2)}.tbs .v.wsl{color:var(--c1)}
.tbs .v.wso{color:var(--c5)}
.tb-clock{display:flex;flex-direction:column;justify-content:center;padding:0 14px;
  text-align:right;border-left:1px solid var(--bdr)}
.tb-clock .dl{font-size:9px;color:var(--dim);letter-spacing:1px}
.tb-clock .dv{font-size:10px;color:var(--txt);font-weight:600;margin:1px 0}
.tb-clock .tv{font-family:'Orbitron',sans-serif;font-size:16px;font-weight:800;color:var(--c2);
  text-shadow:0 0 14px rgba(0,200,255,.8);letter-spacing:2px}
.orb{font-family:'Orbitron',sans-serif}

.sidebar{background:linear-gradient(180deg,rgba(5,16,11,.99),rgba(3,10,7,1));
  border-right:1px solid var(--bdr);display:flex;flex-direction:column;overflow:hidden;
  box-shadow:8px 0 32px rgba(0,0,0,.5)}
.sb-section{padding:10px 12px 5px;font-size:8px;color:var(--dim);letter-spacing:2px;text-transform:uppercase;
  border-bottom:1px solid var(--bdr)}
.sb-nav{flex:1;overflow-y:auto;padding:3px 0}
.sb-nav::-webkit-scrollbar{width:2px}.sb-nav::-webkit-scrollbar-thumb{background:var(--dim2)}
.sb-item{display:flex;align-items:center;gap:8px;padding:8px 12px;cursor:pointer;width:100%;
  border:none;background:none;font-family:'Rajdhani',sans-serif;font-size:11px;font-weight:700;
  color:var(--dim);letter-spacing:.8px;text-transform:uppercase;transition:all .15s;position:relative;text-align:left}
.sb-item:hover{color:var(--txt);background:rgba(0,255,170,.06)}
.sb-item.on{color:var(--c1);background:linear-gradient(90deg,rgba(0,255,170,.1),rgba(0,255,170,.02))}
.sb-item.on::before{content:"";position:absolute;left:0;top:3px;bottom:3px;width:2px;
  background:linear-gradient(180deg,var(--c1),var(--c2));
  box-shadow:0 0 10px var(--c1);border-radius:2px}
.sb-item .ico{font-size:12px;width:15px;text-align:center}
.sb-devstatus{border-top:1px solid var(--bdr);padding:10px 12px;margin-top:auto;
  background:rgba(0,255,170,.02)}
.sb-devstatus .dlbl{font-size:8px;color:var(--dim);letter-spacing:2px;text-transform:uppercase;margin-bottom:6px}
.status-online{font-family:'Orbitron',sans-serif;font-size:11px;font-weight:800;color:var(--c1);
  text-shadow:0 0 12px var(--c1);letter-spacing:1.5px;animation:hg 2.5s ease-in-out infinite}
.sb-ip{font-family:'Share Tech Mono',monospace;font-size:13px;color:var(--c2);margin-top:3px;font-weight:700}
.sb-rssi{font-size:10px;color:var(--dim);margin-top:2px}

.center{overflow-y:auto;background:var(--bg2);padding:8px;display:flex;flex-direction:column;gap:8px}
.center::-webkit-scrollbar{width:5px}
.center::-webkit-scrollbar-track{background:rgba(0,0,0,.2)}
.center::-webkit-scrollbar-thumb{background:linear-gradient(180deg,var(--c1),var(--c2));border-radius:3px}
.pg{display:none;flex-direction:column;gap:8px}.pg.on{display:flex}

@keyframes cardIn{from{opacity:0;transform:translateY(16px) scale(.98)}to{opacity:1;transform:translateY(0) scale(1)}}
.scard,.gpanel,.panel{animation:cardIn .6s cubic-bezier(.16,1,.3,1) both}
.scard:nth-child(1){animation-delay:.02s}.scard:nth-child(2){animation-delay:.07s}
.scard:nth-child(3){animation-delay:.12s}.scard:nth-child(4){animation-delay:.17s}
.scard:nth-child(5){animation-delay:.22s}.scard:nth-child(6){animation-delay:.27s}
.gpanel:nth-child(1){animation-delay:.30s}.gpanel:nth-child(2){animation-delay:.35s}.gpanel:nth-child(3){animation-delay:.40s}
.panel:nth-child(1){animation-delay:.43s}.panel:nth-child(2){animation-delay:.48s}.panel:nth-child(3){animation-delay:.53s}
@media (prefers-reduced-motion:reduce){.scard,.gpanel,.panel{animation:none}}

.panel{background:linear-gradient(145deg,rgba(5,20,13,.95),rgba(3,12,8,.98));
  border:1px solid var(--bdr);border-radius:10px;padding:12px 14px;
  position:relative;overflow:hidden;
  box-shadow:0 4px 32px rgba(0,0,0,.45),inset 0 1px 0 rgba(0,255,170,.07);
  backdrop-filter:blur(6px);transition:border-color .3s,box-shadow .3s}
.panel:hover{border-color:rgba(0,255,170,.38);
  box-shadow:0 4px 32px rgba(0,0,0,.45),var(--sh1),inset 0 1px 0 rgba(0,255,170,.12)}
.panel::before{content:"";position:absolute;top:0;left:8%;right:8%;height:1px;
  background:linear-gradient(90deg,transparent,var(--c1),var(--c2),var(--c1),transparent)}
.panel::after{content:"";position:absolute;inset:0;
  background:linear-gradient(135deg,rgba(0,255,170,.025) 0%,transparent 50%);pointer-events:none}
.ph{font-family:'Orbitron',sans-serif;font-size:9px;font-weight:800;letter-spacing:3px;
  color:var(--c2);margin-bottom:11px;text-transform:uppercase;display:flex;align-items:center;gap:7px;
  text-shadow:0 0 12px rgba(0,200,255,.6)}
.ph::before{content:"";width:3px;height:11px;
  background:linear-gradient(180deg,var(--c1),var(--c2));
  border-radius:2px;box-shadow:0 0 10px var(--c2);flex-shrink:0}

.cards-row{display:grid;grid-template-columns:repeat(6,1fr);gap:8px}
.scard{background:linear-gradient(145deg,rgba(5,22,15,.97),rgba(3,14,9,.97));
  border:1px solid rgba(0,255,170,.2);border-radius:10px;padding:11px 11px 9px;
  position:relative;overflow:hidden;transition:all .22s ease;
  box-shadow:0 2px 20px rgba(0,0,0,.35),inset 0 0 24px rgba(0,0,0,.25)}
.scard::before{content:"";position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,transparent,var(--c1),var(--c2),var(--c1),transparent);opacity:.65}
.scard::after{content:"";position:absolute;inset:0;
  background:linear-gradient(145deg,rgba(0,255,170,.04) 0%,transparent 55%);pointer-events:none}
.scard:hover{border-color:rgba(0,255,170,.55);transform:translateY(-3px);
  box-shadow:0 10px 32px rgba(0,0,0,.45),var(--sh1)}
.scard .ico{font-size:19px;margin-bottom:4px;display:block;filter:drop-shadow(0 0 7px var(--c1))}
.scard .slbl{font-size:8px;color:var(--dim);letter-spacing:1.5px;text-transform:uppercase;font-weight:700}
.scard .sval{font-family:'Orbitron',sans-serif;font-size:23px;font-weight:900;color:var(--c1);
  text-shadow:0 0 16px rgba(0,255,170,.8),0 0 2px rgba(0,255,170,1);
  line-height:1.1;margin:4px 0 2px;letter-spacing:-1px}
.scard .sval.flash{animation:vf .5s ease-out}
@keyframes vf{0%{color:#fff;text-shadow:0 0 40px #fff,0 0 20px var(--c1)}
  70%{color:rgba(180,255,220,.9);text-shadow:0 0 20px rgba(0,255,170,.8)}
  100%{color:var(--c1);text-shadow:0 0 16px rgba(0,255,170,.8)}}
.scard .sval.c2{color:var(--c2);text-shadow:0 0 16px rgba(0,200,255,.8)}
.scard .sval.c3{color:var(--c3);text-shadow:0 0 16px rgba(192,132,252,.8)}
.scard .sval.c4{color:var(--c4);text-shadow:0 0 16px rgba(251,191,36,.8)}
.scard .sunit{font-size:9px;color:var(--dim);font-weight:700}
.scard .trend-lbl{font-size:8px;color:var(--dim);letter-spacing:1px;text-transform:uppercase;font-weight:600;margin-top:5px}
.scard .smeta{font-size:10px;color:var(--dim);margin-top:4px;font-weight:600}
.scard .smeta b{color:var(--txt)}
.scard .sp-wrap{margin:6px 0 3px;height:32px;position:relative;
  background:rgba(0,0,0,.35);border-radius:3px;overflow:hidden;
  border:1px solid rgba(0,212,255,.1)}
.scard canvas.spark{position:absolute;inset:0;width:100%;height:100%}
.scard .sbadge{position:absolute;top:7px;right:7px;font-size:7px;padding:2px 6px;border-radius:10px;
  font-family:'Share Tech Mono',monospace;letter-spacing:.5px;font-weight:700}
.sbadge.ONLINE{color:var(--c1);border:1px solid rgba(0,255,170,.4);background:rgba(0,255,170,.09)}
.sbadge.WARNING{color:var(--c4);border:1px solid rgba(251,191,36,.4)}
.sbadge.ERROR{color:var(--c5);border:1px solid rgba(255,51,102,.4)}

.graphs-row{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}
.gpanel{background:linear-gradient(180deg,rgba(4,16,10,.97),rgba(2,10,7,1));
  border:1px solid var(--bdr);border-radius:10px;padding:10px 12px;
  position:relative;overflow:hidden;
  box-shadow:0 4px 24px rgba(0,0,0,.55),inset 0 0 50px rgba(0,0,0,.15)}
.gpanel::before{content:"";position:absolute;top:0;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,var(--c1),var(--c2),var(--c1),transparent)}
.gphdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:5px}
.gphdr .gtitle{font-family:'Orbitron',sans-serif;font-size:9px;font-weight:800;letter-spacing:2px}
.gphdr .glive{font-size:8px;padding:2px 8px;border-radius:10px;letter-spacing:1px;font-weight:700}
.gphdr .gcur{font-family:'Orbitron',sans-serif;font-size:17px;font-weight:900;letter-spacing:-0.5px}
.gcvs{width:100%;height:125px;display:block;
  background:linear-gradient(180deg,rgba(0,6,3,.9),rgba(0,3,2,1));
  border-radius:5px;border:1px solid rgba(0,200,255,.12)}
.gtime-axis{display:flex;justify-content:space-between;font-size:8px;color:var(--dim);
  margin-top:3px;font-family:'Share Tech Mono',monospace;padding:0 2px}

.telemetry-split{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.telemetry-panel{min-width:0}
.telemetry-panel .mpanel{height:100%}
.telemetry-grid{display:grid;grid-template-columns:1fr 1fr;gap:0 14px}
.telemetry-grid .kv{min-width:0}
.telemetry-grid .k{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.telemetry-grid .v{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.online-weather{margin-top:8px;border-top:1px solid var(--bdr);padding-top:8px}
.location-chip{display:inline-block;padding:3px 7px;border:1px solid var(--bdr2);border-radius:10px;color:var(--c2);font-family:'Share Tech Mono',monospace;font-size:9px}
.data-note{font-size:9px;color:var(--dim);line-height:1.4;margin-top:6px}
@media(max-width:960px){.telemetry-split{grid-template-columns:1fr}}
@media(max-width:620px){.telemetry-grid{grid-template-columns:1fr}}

.lower-row{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}

.sh-row{display:flex;align-items:center;gap:8px;padding:6px 0;
  border-bottom:1px dashed var(--dim2);font-size:11px;font-weight:700}
.sh-row:last-child{border:none}
.sh-ico{font-size:15px;width:19px;flex-shrink:0}
.sh-name{width:64px;font-family:'Orbitron',sans-serif;font-size:8px;color:var(--txt);letter-spacing:.5px}
.sh-dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;animation:sp 2s ease-in-out infinite}
.sh-dot.ONLINE{background:var(--c1);box-shadow:0 0 9px var(--c1)}
.sh-dot.WARNING{background:var(--c4);box-shadow:0 0 9px var(--c4);animation-duration:1.2s}
.sh-dot.ERROR{background:var(--c5);box-shadow:0 0 9px var(--c5);animation-duration:.7s}
@keyframes sp{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.45;transform:scale(1.25)}}
.sh-status{font-family:'Orbitron',sans-serif;font-size:8px;width:46px}
.sh-status.ONLINE{color:var(--c1)}.sh-status.WARNING{color:var(--c4)}.sh-status.ERROR{color:var(--c5)}
.sh-conf{margin-left:auto;text-align:right;font-size:11px;font-weight:700}
.sh-conf .cl{font-size:8px;color:var(--dim)}.sh-conf .cv{color:var(--c1)}

.wx-icon{font-size:50px;filter:drop-shadow(0 0 16px rgba(0,200,255,.6));line-height:1}
.wx-big{display:flex;align-items:center;gap:14px;margin-bottom:4px}
.wx-main .wxcond{font-family:'Orbitron',sans-serif;font-size:17px;font-weight:900;color:var(--txt);margin-bottom:3px}
.wx-main .wxsub{font-size:10px;color:var(--dim);font-weight:600;line-height:1.5}
.wx-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:5px;margin-top:8px}
.wxcell .wl{font-size:8px;color:var(--dim);letter-spacing:1px;text-transform:uppercase;font-weight:700}
.wxcell .wv{font-family:'Orbitron',sans-serif;font-size:12px;font-weight:800;color:var(--txt)}

.gauges-row{display:flex;justify-content:space-around;margin-bottom:10px}
.gauge-item canvas{display:block}
.gauge-item .glbl{font-size:8px;color:var(--dim);letter-spacing:1px;text-transform:uppercase;
  text-align:center;line-height:1.4;font-weight:700;margin-top:4px}
.prog-row{display:flex;align-items:center;gap:8px;margin-bottom:6px;font-size:11px;font-weight:700}
.prog-row:last-child{margin:0}
.prog-lbl{width:70px;color:var(--dim)}
.prog-bar{flex:1;height:5px;background:rgba(0,0,0,.5);border-radius:3px;overflow:hidden;border:1px solid var(--dim2)}
.prog-fill{height:100%;border-radius:3px;transition:width .6s}
.prog-fill.gr{background:linear-gradient(90deg,rgba(0,180,90,.8),var(--c1));box-shadow:0 0 8px rgba(0,255,170,.4)}
.prog-fill.am{background:linear-gradient(90deg,rgba(200,120,0,.8),var(--c4))}
.prog-fill.pu{background:linear-gradient(90deg,rgba(120,0,200,.8),var(--c3))}
.prog-val{width:58px;text-align:right;color:var(--txt);font-family:'Share Tech Mono',monospace;font-size:10px}

.rpanel{background:linear-gradient(180deg,rgba(5,16,11,.99),rgba(3,10,7,1));
  border-left:1px solid var(--bdr);display:flex;flex-direction:column;overflow:hidden;
  box-shadow:-8px 0 32px rgba(0,0,0,.5)}
.rp-section{border-bottom:1px solid rgba(0,255,170,.1);padding:10px 12px;flex-shrink:0}
.rp-title{font-family:'Orbitron',sans-serif;font-size:8px;font-weight:800;letter-spacing:2.5px;
  color:var(--c2);text-transform:uppercase;margin-bottom:7px;display:flex;align-items:center;gap:5px;
  text-shadow:0 0 12px rgba(0,200,255,.5)}
.rp-title::before{content:"";display:block;width:2px;height:11px;
  background:linear-gradient(180deg,var(--c1),var(--c2));border-radius:1px;box-shadow:0 0 8px var(--c2)}
.ev-item{display:flex;gap:7px;align-items:flex-start;padding:4px 0;
  border-bottom:1px dashed rgba(26,64,48,.5);font-size:11px;font-weight:600}
.ev-item:last-child{border:none}
.ev-time{color:var(--dim);white-space:nowrap;font-family:'Share Tech Mono',monospace;
  font-size:9px;min-width:48px;flex-shrink:0}
.ev-msg{color:var(--txt);line-height:1.3}
.ev-msg.WARN{color:var(--c4)}.ev-msg.ERROR{color:var(--c5)}
.evlog-scroll{flex:1;overflow-y:auto;padding:8px 12px}
.evlog-scroll::-webkit-scrollbar{width:2px}.evlog-scroll::-webkit-scrollbar-thumb{background:var(--dim2)}
.ev-full{display:flex;gap:5px;font-size:9px;padding:3px 0;
  border-bottom:1px dashed rgba(26,64,48,.4);font-family:'Share Tech Mono',monospace}
.ev-full:last-child{border:none}
.ev-full .t{color:var(--dim);min-width:44px;flex-shrink:0}
.ev-full .lvl{min-width:36px;flex-shrink:0}.ev-full .lvl.INFO{color:var(--c2)}.ev-full .lvl.WARN{color:var(--c4)}.ev-full .lvl.ERROR{color:var(--c5)}
.ev-full .m{color:var(--txt);word-break:break-word}
.view-btn{display:block;width:100%;background:transparent;border:1px solid var(--bdr);
  color:var(--dim);font-family:'Orbitron',sans-serif;font-size:8px;letter-spacing:1px;
  padding:6px;cursor:pointer;text-transform:uppercase;border-radius:5px;
  margin-top:6px;transition:all .15s}
.view-btn:hover{border-color:var(--c2);color:var(--c2);box-shadow:var(--sh2)}
.qa-grid{display:flex;flex-direction:column;gap:5px}
.qa-btn{background:linear-gradient(135deg,rgba(0,200,255,.05),transparent);
  border:1px solid var(--bdr2);color:var(--dim);font-family:'Orbitron',sans-serif;
  font-size:8px;letter-spacing:1px;padding:9px;cursor:pointer;text-transform:uppercase;
  transition:all .2s;border-radius:6px;text-align:center;font-weight:800}
.qa-btn:hover{border-color:var(--c2);color:var(--c2);box-shadow:var(--sh2);transform:translateY(-1px)}
.qa-btn.green{border-color:rgba(0,255,170,.3);color:var(--c1)}
.qa-btn.green:hover{border-color:var(--c1);box-shadow:var(--sh1)}
.qa-btn.red{border-color:rgba(255,51,102,.3);color:var(--c5)}
.qa-btn.red:hover{box-shadow:0 0 16px rgba(255,51,102,.2)}

.botbar{grid-column:1/-1;display:flex;align-items:center;justify-content:space-between;
  background:linear-gradient(90deg,rgba(5,16,11,.99),rgba(3,10,7,.99),rgba(5,16,11,.99));
  border-top:1px solid var(--bdr);padding:0 16px;font-size:9px;color:var(--dim);
  font-weight:600;letter-spacing:1.5px;box-shadow:0 -4px 24px rgba(0,0,0,.5)}
.botbar b{color:var(--c1)}

.kv{display:flex;justify-content:space-between;font-size:11px;font-weight:600;
  padding:5px 0;border-bottom:1px dashed rgba(26,64,48,.5);transition:background .15s}
.kv:hover{background:rgba(0,255,170,.03);margin:0 -4px;padding:5px 4px}
.kv:last-child{border:none}
.kv .k{color:var(--dim)}.kv .v{font-family:'Share Tech Mono',monospace;color:var(--txt)}
.kv .v.gr{color:var(--c1);text-shadow:0 0 6px rgba(0,255,170,.3)}
.kv .v.bl,.kv .v.cy{color:var(--c2);text-shadow:0 0 6px rgba(0,200,255,.3)}
.kv .v.rd{color:var(--c5)}

.sect{font-family:'Orbitron',sans-serif;font-size:8px;letter-spacing:3px;color:var(--c2);
  text-transform:uppercase;font-weight:800;padding:6px 0 5px;
  border-bottom:1px solid rgba(0,200,255,.22);margin-bottom:8px;
  text-shadow:0 0 10px rgba(0,200,255,.5)}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.g3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px}
.mini-panel{background:rgba(3,14,9,.75);border:1px solid var(--bdr);border-radius:8px;padding:10px}
input[type=text],input[type=password],input[type=number]{
  background:rgba(2,9,5,.95);border:1px solid rgba(0,255,170,.2);color:var(--c1);
  font-family:'Share Tech Mono',monospace;padding:7px 10px;font-size:11px;
  width:100%;border-radius:5px;margin-bottom:6px;transition:border-color .2s,box-shadow .2s}
input:focus{outline:none;border-color:var(--c2);box-shadow:0 0 12px rgba(0,200,255,.2)}
input::placeholder{color:var(--dim)}
.btn{background:linear-gradient(135deg,rgba(0,200,255,.08),transparent);
  border:1px solid rgba(0,200,255,.35);color:var(--c2);font-family:'Orbitron',sans-serif;
  letter-spacing:1px;font-size:8px;padding:8px 14px;cursor:pointer;text-transform:uppercase;
  border-radius:5px;transition:all .2s;margin-right:5px;margin-bottom:4px;font-weight:800}
.btn:hover{background:rgba(0,200,255,.14);box-shadow:var(--sh2);transform:translateY(-1px)}
.btn.sec{border-color:var(--dim);color:var(--dim)}
.smsg{font-size:9px;min-height:13px;margin-top:4px;font-weight:700}
.smsg.ok{color:var(--c1)}.smsg.er{color:var(--c5)}
.page-content{padding:10px}

.mpanel{background:rgba(3,14,9,.75);border:1px solid var(--bdr);border-radius:8px;padding:10px}
.page-wrap{padding:10px}
.fbtn{background:linear-gradient(135deg,rgba(0,200,255,.08),transparent);
  border:1px solid rgba(0,200,255,.35);color:var(--c2);font-family:'Orbitron',sans-serif;
  letter-spacing:1px;font-size:9px;padding:8px 14px;cursor:pointer;text-transform:uppercase;
  border-radius:5px;transition:all .2s;margin-right:5px;margin-bottom:4px;font-weight:800}
.fbtn:hover{background:rgba(0,200,255,.14);box-shadow:var(--sh2);transform:translateY(-1px)}
.fbtn.sec{border-color:var(--dim);color:var(--dim)}
.fld{margin-bottom:8px}
.fld label{font-size:9px;color:var(--dim);letter-spacing:1px;text-transform:uppercase;
  font-weight:700;display:block;margin-bottom:4px}
.evlog-page{height:360px;overflow-y:auto;border:1px solid var(--bdr);
  background:rgba(2,8,5,.9);padding:8px;border-radius:6px;font-family:'Share Tech Mono',monospace;font-size:9px}

#ovBan{display:none;position:fixed;top:54px;left:206px;right:248px;z-index:100;
  background:rgba(251,191,36,.12);border:1px solid rgba(251,191,36,.5);
  color:var(--c4);font-size:9px;padding:5px 14px;text-align:center;
  font-weight:700;letter-spacing:1px;backdrop-filter:blur(4px)}

@media(max-width:1200px){
  .shell{grid-template-columns:190px 1fr 220px}
  .cards-row{grid-template-columns:repeat(3,1fr)}
}
@media(max-width:960px){
  .shell{grid-template-columns:46px 1fr;grid-template-rows:54px 1fr 30px}
  .rpanel{display:none}
  .sb-item .ico{font-size:14px;width:auto}
  .sb-item span:not(.ico){display:none}
  .sb-item{justify-content:center;padding:10px 0}
  .sb-section,.sb-devstatus .dlbl,.sb-ip,.sb-rssi{display:none}
  .sb-devstatus{padding:8px 4px;text-align:center}
  .tb-feed{display:none}
  .cards-row{grid-template-columns:repeat(2,1fr)}
  .graphs-row,.lower-row{grid-template-columns:1fr 1fr}
}
@media(max-width:620px){
  .cards-row{grid-template-columns:1fr 1fr}
  .graphs-row,.lower-row{grid-template-columns:1fr}
  .tb-stats .tbs:nth-child(n+3){display:none}
}
@media(max-width:420px){
  .shell{
    grid-template-columns:1fr;
    grid-template-rows:54px minmax(0,1fr) auto 30px;
    min-height:100vh;
  }
  .topbar{grid-column:1}
  .sidebar{
    display:none;
    position:fixed;left:0;top:54px;bottom:30px;width:220px;
    z-index:250;background:linear-gradient(180deg,rgba(5,16,11,.99),rgba(3,10,7,.99));
    border-right:1px solid var(--bdr);box-shadow:10px 0 35px rgba(0,0,0,.75);
  }
  .sidebar.mob-open{display:flex}
  .sidebar .sb-item{justify-content:flex-start;padding:10px 14px}
  .sidebar .sb-item span:not(.ico){display:inline}
  #mob-menu{display:block !important}
  .center{grid-column:1;grid-row:2;min-width:0;width:100%}
  .rpanel{grid-column:1;grid-row:3;width:100%;max-height:none;border-left:none;border-top:1px solid var(--bdr)}
  .botbar{grid-column:1;grid-row:4;min-width:0}
  .cards-row{grid-template-columns:1fr}
}

</style>
</head>
<body>

<canvas id="cnv-bg" style="position:fixed;inset:0;z-index:0;pointer-events:none;opacity:.5"></canvas>
<button id="mob-menu" onclick="document.querySelector('.sidebar').classList.toggle('mob-open')"
  style="display:none;position:fixed;top:12px;left:12px;z-index:200;background:rgba(0,20,12,.9);
  border:1px solid rgba(0,212,255,.4);color:var(--c2);font-size:18px;padding:4px 10px;cursor:pointer;border-radius:4px">☰</button>
<div id="offline-badge" style="display:none;position:fixed;top:68px;left:210px;right:270px;z-index:100;background:rgba(245,158,11,.15);border:1px solid var(--c4);color:var(--c4);font-size:10px;padding:5px 14px;font-weight:700;letter-spacing:1px;text-align:center"></div>
<div id="scan"></div>
<div class="shell">

<header class="topbar">
  <div class="tb-brand brand">
    <div class="brand-dot" id="wdot"></div>
    <div><h1>ADITYA</h1><small>WEATHER STATION</small></div>
  </div>
  <div class="tb-feed">
    <div class="fl">LIVE DATA FEED</div>
    <canvas id="feedCanvas" width="160" height="34"></canvas>
  </div>
  <div class="tb-mid">
    <div class="sys-online" id="sysTitle">SYSTEM OFFLINE</div>
    <div class="sys-sub">All Systems Operational</div>
  </div>
  <div class="tb-stats">
    <div class="tbs">
      <div class="l">UPTIME</div>
      <div class="v orb" id="t-up">00d 00h 00m 00s</div>
    </div>
    <div class="tbs">
      <div class="l">HEAP</div>
      <div class="v" id="ts-heap">-- KB</div>
    </div>
    <div class="tbs">
      <div class="l">RSSI</div>
      <div class="v" id="ts-rssi">-- dBm</div>
    </div>
    <div class="tbs">
      <div class="l">WS</div>
      <div class="v" id="ts-ws">● --</div>
    </div>
  </div>
  <div class="tb-clock">
    <div class="dl">DATE &amp; TIME</div>
    <div class="dv" id="t-date">-- --- ----</div>
    <div class="tv" id="t-time">--:--:-- --</div>
  </div>
</header>

<nav class="sidebar">
  <div class="sb-section">MAIN MENU</div>
  <div class="sb-nav">
    <button class="sb-item on"  data-pg="dash"      onclick="nav(this,'dash')"><span class="ico">⌂</span>Dashboard</button>
    <button class="sb-item"     data-pg="livedata"  onclick="nav(this,'livedata')"><span class="ico">◎</span>Live Data</button>
    <button class="sb-item"     data-pg="graphs"    onclick="nav(this,'graphs')"><span class="ico">〜</span>Graphs</button>
    <button class="sb-item"     data-pg="sensors"   onclick="nav(this,'sensors')"><span class="ico">◈</span>Sensors</button>
    <button class="sb-item"     data-pg="wxsearch"  onclick="nav(this,'wxsearch')"><span class="ico">⊕</span>Weather Search</button>
    <button class="sb-item"     data-pg="location"  onclick="nav(this,'location')"><span class="ico">⌖</span>Location Config</button>
    <button class="sb-item"     data-pg="wtime"     onclick="nav(this,'wtime')"><span class="ico">◷</span>World Time</button>
    <button class="sb-item"     data-pg="evtlog"    onclick="nav(this,'evtlog')"><span class="ico">≡</span>Event Log</button>
    <button class="sb-item"     data-pg="settings"  onclick="nav(this,'settings')"><span class="ico">⚙</span>Settings</button>
    <button class="sb-item"     data-pg="sysinfo"   onclick="nav(this,'sysinfo')"><span class="ico">ℹ</span>System Info</button>
    <button class="sb-item" style="color:var(--c4)" onclick="doRestart()"><span class="ico">↺</span>Restart</button>
  </div>
  <div class="sb-devstatus">
    <div class="dlbl">DEVICE STATUS</div>
    <div class="status-online" id="sb-onl">● OFFLINE</div>
    <div class="sb-ip" id="sb-ip">---.---.---.---</div>
    <div class="sb-rssi" id="sb-rssi">-- dBm</div>
  </div>
</nav>

<main class="center">

<div id="pg-dash" class="pg on">
  
  <div class="cards-row">
    <div class="scard">
      <div class="sbadge" id="bd-dht">--</div>
      <div class="ico">🌡️</div>
      <div class="slbl">TEMPERATURE <span id="src-temp" style="font-size:8px;opacity:.65;font-weight:400"></span></div>
      <div class="sval" id="v-temp">--.-</div>
      <div class="sunit">°C</div>
      <div class="trend-lbl">Trend</div>
      <div class="sp-wrap"><canvas class="spark" id="sp-temp"></canvas></div>
      <div class="smeta">Feels Like <b id="v-hi">--.-</b> °C</div>
    </div>
    <div class="scard">
      <div class="ico">💧</div>
      <div class="slbl">HUMIDITY <span id="src-hum" style="font-size:8px;opacity:.65;font-weight:400"></span></div>
      <div class="sval" style="color:var(--c3);text-shadow:0 0 12px rgba(168,85,247,.55)" id="v-hum">--</div>
      <div class="sunit">%</div>
      <div class="trend-lbl">Trend</div>
      <div class="sp-wrap"><canvas class="spark" id="sp-hum"></canvas></div>
      <div class="smeta">Dew Point <b id="v-dew">--.-</b> °C</div>
    </div>
    <div class="scard">
      <div class="sbadge" id="bd-bme">--</div>
      <div class="ico">📊</div>
      <div class="slbl">PRESSURE <span id="src-pres" style="font-size:8px;opacity:.65;font-weight:400"></span></div>
      <div class="sval" style="color:var(--c4);text-shadow:0 0 12px rgba(245,158,11,.55)" id="v-pres">----</div>
      <div class="sunit">hPa <span id="v-trend" style="font-size:13px"></span></div>
      <div class="trend-lbl">Trend</div>
      <div class="sp-wrap"><canvas class="spark" id="sp-pres"></canvas></div>
      <div class="smeta">Altitude <b id="v-alt">--</b> m</div>
    </div>
    <div class="scard">
      <div class="sbadge" id="bd-bh">--</div>
      <div class="ico">☀️</div>
      <div class="slbl">LIGHT (LUX)</div>
      <div class="sval" style="color:var(--c4);text-shadow:0 0 12px rgba(245,158,11,.55)" id="v-lux">--</div>
      <div class="sunit">lx</div>
      <div class="trend-lbl">Trend</div>
      <div class="sp-wrap"><canvas class="spark" id="sp-lux"></canvas></div>
      <div class="smeta">Level <b id="v-luxl">--</b></div>
    </div>
    <div class="scard">
      <div class="sbadge" id="bd-ds">--</div>
      <div class="ico">🌡️</div>
      <div class="slbl">DS18B20 TEMP</div>
      <div class="sval" id="v-ds">--.-</div>
      <div class="sunit">°C</div>
      <div class="trend-lbl">Trend</div>
      <div class="sp-wrap"><canvas class="spark" id="sp-ds"></canvas></div>
      <div class="smeta">Sensors <b id="v-dscnt">--</b></div>
    </div>
    <div class="scard">
      <div class="ico">🌧️</div>
      <div class="slbl">RAIN PROBABILITY</div>
      <div class="sval" style="color:var(--c2);text-shadow:0 0 12px rgba(0,212,255,.55)" id="v-rain">--</div>
      <div class="sunit">%</div>
      <div class="trend-lbl">Trend</div>
      <div class="sp-wrap"><canvas class="spark" id="sp-rain"></canvas></div>
      <div class="smeta">Level <b id="v-rainl">--</b></div>
    </div>
  </div>

  <div class="graphs-row">
    <div class="gpanel">
      <div class="gphdr">
        <div>
          <div class="gtitle" style="color:var(--c1)">TEMPERATURE (°C)</div>
        </div>
        <span class="glive" style="color:var(--c1);border:1px solid rgba(0,255,136,.4);background:rgba(0,255,136,.08)">LIVE GRAPH</span>
        <div class="gcur" style="color:var(--c1)" id="gc-t">--.-°C</div>
      </div>
      <canvas class="gcvs" id="gv-t"></canvas>
      <div class="gtime-axis"><span>-24h</span><span>-18h</span><span>-12h</span><span>-6h</span><span>Now</span></div>
    </div>
    <div class="gpanel">
      <div class="gphdr">
        <div>
          <div class="gtitle" style="color:var(--c3)">HUMIDITY (%)</div>
        </div>
        <span class="glive" style="color:var(--c3);border:1px solid rgba(168,85,247,.4);background:rgba(168,85,247,.08)">LIVE GRAPH</span>
        <div class="gcur" style="color:var(--c3)" id="gc-h">--%</div>
      </div>
      <canvas class="gcvs" id="gv-h"></canvas>
      <div class="gtime-axis"><span>-24h</span><span>-18h</span><span>-12h</span><span>-6h</span><span>Now</span></div>
    </div>
    <div class="gpanel">
      <div class="gphdr">
        <div>
          <div class="gtitle" style="color:var(--c4)">PRESSURE (hPa)</div>
        </div>
        <span class="glive" style="color:var(--c4);border:1px solid rgba(245,158,11,.4);background:rgba(245,158,11,.08)">LIVE GRAPH</span>
        <div class="gcur" style="color:var(--c4)" id="gc-p">-- hPa</div>
      </div>
      <canvas class="gcvs" id="gv-p"></canvas>
      <div class="gtime-axis"><span>-24h</span><span>-18h</span><span>-12h</span><span>-6h</span><span>Now</span></div>
    </div>
  </div>

  <div class="telemetry-split" style="margin-top:8px">
    <div class="telemetry-panel panel">
      <div class="ph">REAL SENSOR DATA — NO FAKE VALUES</div>
      <div class="telemetry-grid">
        <div class="kv"><span class="k">DHT22 Temp</span><span class="v gr" id="rt-dht-t">--</span></div>
        <div class="kv"><span class="k">DHT22 Humidity</span><span class="v gr" id="rt-dht-h">--</span></div>
        <div class="kv"><span class="k">BME280 Temp</span><span class="v" id="rt-bme-t">--</span></div>
        <div class="kv"><span class="k">BME280 Humidity</span><span class="v" id="rt-bme-h">--</span></div>
        <div class="kv"><span class="k">Pressure</span><span class="v" id="rt-bme-p">--</span></div>
        <div class="kv"><span class="k">Altitude</span><span class="v" id="rt-alt">--</span></div>
        <div class="kv"><span class="k">BH1750 Lux</span><span class="v" id="rt-lux">--</span></div>
        <div class="kv"><span class="k">DS18B20 #1</span><span class="v" id="rt-ds1">--</span></div>
        <div class="kv"><span class="k">DS18B20 #2</span><span class="v" id="rt-ds2">--</span></div>
        <div class="kv"><span class="k">DS18 Count</span><span class="v" id="rt-dsc">--</span></div>
        <div class="kv"><span class="k">DS Min / Max</span><span class="v" id="rt-dsmm">--</span></div>
        <div class="kv"><span class="k">Heat Index</span><span class="v" id="rt-hi">--</span></div>
        <div class="kv"><span class="k">Dew Point</span><span class="v" id="rt-dew">--</span></div>
        <div class="kv"><span class="k">Pressure Trend</span><span class="v" id="rt-trend">--</span></div>
        <div class="kv"><span class="k">Rain Probability</span><span class="v" id="rt-rain">--</span></div>
        <div class="kv"><span class="k">Snapshot</span><span class="v" id="rt-snap">--</span></div>
      </div>
      <div class="online-weather">
        <div class="sect" style="font-size:9px">SENSOR HEALTH / QUALITY</div>
        <div class="telemetry-grid">
          <div class="kv"><span class="k">DHT22</span><span class="v" id="rt-dht-health">--</span></div>
          <div class="kv"><span class="k">BME280</span><span class="v" id="rt-bme-health">--</span></div>
          <div class="kv"><span class="k">BH1750</span><span class="v" id="rt-bh-health">--</span></div>
          <div class="kv"><span class="k">DS18B20</span><span class="v" id="rt-ds-health">--</span></div>
          <div class="kv"><span class="k">Confidence D/B/BH/DS</span><span class="v" id="rt-conf">--</span></div>
          <div class="kv"><span class="k">Retries D/B/BH/DS</span><span class="v" id="rt-retries">--</span></div>
          <div class="kv"><span class="k">Errors D/B/BH/DS</span><span class="v" id="rt-errs">--</span></div>
        </div>
      </div>
      <div class="data-note">Only successful sensor reads are displayed. Missing hardware stays OFFLINE/--.</div>
    </div>

    <div class="telemetry-panel panel">
      <div class="ph">ONLINE SYSTEM DATA — COMPLETE STATUS</div>
      <div class="telemetry-grid">
        <div class="kv"><span class="k">Internet / WiFi</span><span class="v" id="rt-wifi">--</span></div>
        <div class="kv"><span class="k">SSID</span><span class="v" id="rt-ssid">--</span></div>
        <div class="kv"><span class="k">STA IP</span><span class="v cy" id="rt-ip">--</span></div>
        <div class="kv"><span class="k">RSSI</span><span class="v" id="rt-rssi">--</span></div>
        <div class="kv"><span class="k">AP SSID</span><span class="v" id="rt-apssid">--</span></div>
        <div class="kv"><span class="k">AP IP</span><span class="v cy" id="rt-apip">--</span></div>
        <div class="kv"><span class="k">AP Clients</span><span class="v" id="rt-apsta">--</span></div>
        <div class="kv"><span class="k">Uptime</span><span class="v" id="rt-up">--</span></div>
        <div class="kv"><span class="k">Free Heap</span><span class="v" id="rt-heap">--</span></div>
        <div class="kv"><span class="k">Heap Fragmentation</span><span class="v" id="rt-frag">--</span></div>
        <div class="kv"><span class="k">Flash Used / Total</span><span class="v" id="rt-fs">--</span></div>
        <div class="kv"><span class="k">Firmware</span><span class="v" id="rt-fw">--</span></div>
        <div class="kv"><span class="k">CPU</span><span class="v" id="rt-cpu">--</span></div>
        <div class="kv"><span class="k">Chip Revision</span><span class="v" id="rt-chip">--</span></div>
        <div class="kv"><span class="k">MAC</span><span class="v" id="rt-mac">--</span></div>
        <div class="kv"><span class="k">Min Heap Ever</span><span class="v" id="rt-minheap">--</span></div>
        <div class="kv"><span class="k">OLED</span><span class="v" id="rt-oled">--</span></div>
        <div class="kv"><span class="k">Boot Confirmed</span><span class="v" id="rt-boot">--</span></div>
        <div class="kv"><span class="k">Safe Mode</span><span class="v" id="rt-safe">--</span></div>
        <div class="kv"><span class="k">I2C Recoveries</span><span class="v" id="rt-i2c">--</span></div>
        <div class="kv"><span class="k">WiFi Hard Resets</span><span class="v" id="rt-whr">--</span></div>
        <div class="kv"><span class="k">WebSocket</span><span class="v" id="rt-ws">--</span></div>
        <div class="kv"><span class="k">Clock / NTP</span><span class="v" id="rt-ntp">--</span></div>
      </div>
      <div class="online-weather">
        <div class="sect" style="font-size:9px">SAVED ONLINE WEATHER</div>
        <div><span class="location-chip" id="rt-location">NO LOCATION SET</span></div>
        <div class="telemetry-grid">
          <div class="kv"><span class="k">Condition</span><span class="v" id="rt-wx-cond">--</span></div>
          <div class="kv"><span class="k">Temperature</span><span class="v" id="rt-wx-temp">--</span></div>
          <div class="kv"><span class="k">Feels Like</span><span class="v" id="rt-wx-feel">--</span></div>
          <div class="kv"><span class="k">Humidity</span><span class="v" id="rt-wx-hum">--</span></div>
          <div class="kv"><span class="k">Pressure</span><span class="v" id="rt-wx-pres">--</span></div>
          <div class="kv"><span class="k">Wind</span><span class="v" id="rt-wx-wind">--</span></div>
          <div class="kv"><span class="k">Clouds</span><span class="v" id="rt-wx-cloud">--</span></div>
          <div class="kv"><span class="k">Local Time</span><span class="v" id="rt-wx-time">--</span></div>
          <div class="kv"><span class="k">Timezone</span><span class="v" id="rt-wx-zone">--</span></div>
        </div>
        <div class="data-note" id="rt-wx-updated">No successful API reading yet.</div>
      </div>
    </div>
  </div>

  <div class="lower-row">
    
    <div class="panel">
      <div class="ph">SENSOR HEALTH</div>
      <div class="sh-row"><span class="sh-ico">🌡️</span><span class="sh-name">DHT22</span><div class="sh-dot ERROR" id="sd-dht"></div><span class="sh-status ERROR" id="ss-dht">OFFLINE</span><div class="sh-conf"><div class="cl">Confidence</div><div class="cv" id="sc-dht">--%</div></div></div>
      <div class="sh-row"><span class="sh-ico">📊</span><span class="sh-name">BME280</span><div class="sh-dot ERROR" id="sd-bme"></div><span class="sh-status ERROR" id="ss-bme">OFFLINE</span><div class="sh-conf"><div class="cl">Confidence</div><div class="cv" id="sc-bme">--%</div></div></div>
      <div class="sh-row"><span class="sh-ico">☀️</span><span class="sh-name">BH1750</span><div class="sh-dot ERROR" id="sd-bh"></div><span class="sh-status ERROR" id="ss-bh">OFFLINE</span><div class="sh-conf"><div class="cl">Confidence</div><div class="cv" id="sc-bh">--%</div></div></div>
      <div class="sh-row"><span class="sh-ico">🌡️</span><span class="sh-name">DS18B20</span><div class="sh-dot ERROR" id="sd-ds"></div><span class="sh-status ERROR" id="ss-ds">OFFLINE</span><div class="sh-conf"><div class="cl">Confidence</div><div class="cv" id="sc-ds">--%</div></div></div>
    </div>
    
    <div class="panel">
      <div class="ph">WEATHER OVERVIEW</div>
      <div class="wx-big">
        <div class="wx-icon" id="wx-ico">⛅</div>
        <div class="wx-main">
          <div class="wxcond" id="wx-cond">--</div>
          <div class="wxsub" id="wx-sub">Waiting for data...</div>
        </div>
      </div>
      <div class="wx-grid">
        <div class="wxcell"><div class="wl">TEMP</div><div class="wv" id="wx-t">--°C</div></div>
        <div class="wxcell"><div class="wl">HUMIDITY</div><div class="wv" id="wx-h">--%</div></div>
        <div class="wxcell"><div class="wl">WIND</div><div class="wv" id="wx-w">--</div></div>
        <div class="wxcell"><div class="wl">VISIBILITY</div><div class="wv" id="wx-v">--</div></div>
      </div>
    </div>
    
    <div class="panel">
      <div class="ph">SYSTEM MONITOR</div>
      <div class="gauges-row">
        <div class="gauge-item"><canvas id="g-heap" width="80" height="80"></canvas><div class="glbl">HEAP<br>FREE</div></div>
        <div class="gauge-item"><canvas id="g-frag" width="80" height="80"></canvas><div class="glbl">FLASH<br>USED</div></div>
        <div class="gauge-item"><canvas id="g-wifi" width="80" height="80"></canvas><div class="glbl">WiFi<br>SIGNAL</div></div>
      </div>
      <div class="prog-row"><div class="prog-lbl">Heap Free</div><div class="prog-bar"><div class="prog-fill gr" id="pr-h" style="width:0%"></div></div><div class="prog-val" id="pv-h">-- KB</div></div>
      <div class="prog-row"><div class="prog-lbl">Flash Used</div><div class="prog-bar"><div class="prog-fill am" id="pr-f" style="width:0%"></div></div><div class="prog-val" id="pv-f">--%</div></div>
      <div class="prog-row"><div class="prog-lbl">WiFi Signal</div><div class="prog-bar"><div class="prog-fill pu" id="pr-w" style="width:0%"></div></div><div class="prog-val" id="pv-w">-- dBm</div></div>
      
      <div style="display:none">
        <span id="sh"></span><span id="sf"></span><span id="sr"></span>
        <span id="si"></span><span id="sup"></span><span id="ss"></span>
        <span id="sfs"></span>
      </div>
    </div>
  </div>
</div>

<div id="pg-livedata" class="pg"><div class="page-wrap">
  <div class="sect">LIVE SENSOR DATA</div>
  <div class="g3">
    <div class="mpanel">
      <div class="kv"><span class="k">Temperature</span><span class="v gr" id="ld-t">--°C</span></div>
      <div class="kv"><span class="k">Humidity</span><span class="v" id="ld-h">--%</span></div>
      <div class="kv"><span class="k">Heat Index</span><span class="v" id="ld-hi">--°C</span></div>
      <div class="kv"><span class="k">Dew Point</span><span class="v" id="ld-dp">--°C</span></div>
    </div>
    <div class="mpanel">
      <div class="kv"><span class="k">Pressure</span><span class="v cy" id="ld-p">-- hPa</span></div>
      <div class="kv"><span class="k">Altitude</span><span class="v" id="ld-a">-- m</span></div>
      <div class="kv"><span class="k">Trend</span><span class="v" id="ld-tr">--</span></div>
      <div class="kv"><span class="k">BME Temp</span><span class="v" id="ld-bt">--°C</span></div>
    </div>
    <div class="mpanel">
      <div class="kv"><span class="k">Light</span><span class="v cy" id="ld-l">-- lx</span></div>
      <div class="kv"><span class="k">DS18B20 #1</span><span class="v" id="ld-d1">--°C</span></div>
      <div class="kv"><span class="k">DS18B20 #2</span><span class="v" id="ld-d2">--°C</span></div>
      <div class="kv"><span class="k">Min/Max</span><span class="v" id="ld-mm">--/--</span></div>
    </div>
  </div>
  <div class="sect" style="margin-top:12px">CONDITIONS</div>
  <div class="g3">
    <div class="mpanel"><div class="kv"><span class="k">Rain Probability</span><span class="v gr" id="ld-r">--%</span></div><div class="kv"><span class="k">Weather</span><span class="v" id="ld-w">--</span></div></div>
    <div class="mpanel"><div class="kv"><span class="k">Uptime</span><span class="v cy" id="ld-up">--</span></div><div class="kv"><span class="k">Snapshot</span><span class="v" id="ld-sn">--</span></div></div>
    <div class="mpanel"><div class="kv"><span class="k">Free Heap</span><span class="v gr" id="ld-fh">-- KB</span></div><div class="kv"><span class="k">Frag</span><span class="v" id="ld-fg">--%</span></div></div>
  </div>
</div></div>

<div id="pg-graphs" class="pg"><div class="page-wrap">
  <div class="sect">HISTORICAL GRAPHS</div>
  <div class="g3">
    <div class="mpanel"><div class="sect" style="color:var(--c1)">TEMPERATURE °C</div><canvas id="gg-t" style="width:100%;height:140px;border:1px solid var(--bdr2)"></canvas></div>
    <div class="mpanel"><div class="sect" style="color:var(--c3)">HUMIDITY %</div><canvas id="gg-h" style="width:100%;height:140px;border:1px solid var(--bdr2)"></canvas></div>
    <div class="mpanel"><div class="sect" style="color:var(--c4)">PRESSURE hPa</div><canvas id="gg-p" style="width:100%;height:140px;border:1px solid var(--bdr2)"></canvas></div>
  </div>
  <div class="g3" style="margin-top:10px">
    <div class="mpanel"><div class="sect" style="color:var(--c4)">LIGHT lx</div><canvas id="gg-l" style="width:100%;height:140px;border:1px solid var(--bdr2)"></canvas></div>
    <div class="mpanel"><div class="sect" style="color:var(--c1)">OUTDOOR TEMP °C</div><canvas id="gg-d" style="width:100%;height:140px;border:1px solid var(--bdr2)"></canvas></div>
    <div class="mpanel"><div class="sect" style="color:var(--c2)">RAIN PROB %</div><canvas id="gg-r" style="width:100%;height:140px;border:1px solid var(--bdr2)"></canvas></div>
  </div>
</div></div>

<div id="pg-sensors" class="pg"><div class="page-wrap">
  <div class="sect">SENSOR DIAGNOSTICS</div>
  <div class="g2">
    <div class="mpanel">
      <div class="sect" style="font-size:9px">DHT22 — TEMPERATURE & HUMIDITY</div>
      <div class="kv"><span class="k">Health</span><span class="v gr" id="sg-dh">--</span></div>
      <div class="kv"><span class="k">Confidence</span><span class="v" id="sg-dc">--%</span></div>
      <div class="kv"><span class="k">Retries</span><span class="v" id="sg-dr">--</span></div>
      <div class="kv"><span class="k">Errors</span><span class="v rd" id="sg-de">--</span></div>
    </div>
    <div class="mpanel">
      <div class="sect" style="font-size:9px">BME280 — PRESSURE, TEMP, HUMIDITY</div>
      <div class="kv"><span class="k">Health</span><span class="v gr" id="sg-bh">--</span></div>
      <div class="kv"><span class="k">Confidence</span><span class="v" id="sg-bc">--%</span></div>
      <div class="kv"><span class="k">Retries</span><span class="v" id="sg-br">--</span></div>
      <div class="kv"><span class="k">Errors</span><span class="v rd" id="sg-be">--</span></div>
    </div>
    <div class="mpanel">
      <div class="sect" style="font-size:9px">BH1750 — LIGHT SENSOR</div>
      <div class="kv"><span class="k">Health</span><span class="v gr" id="sg-lh">--</span></div>
      <div class="kv"><span class="k">Confidence</span><span class="v" id="sg-lc">--%</span></div>
      <div class="kv"><span class="k">Retries</span><span class="v" id="sg-lr">--</span></div>
      <div class="kv"><span class="k">Errors</span><span class="v rd" id="sg-le">--</span></div>
    </div>
    <div class="mpanel">
      <div class="sect" style="font-size:9px">DS18B20 — OUTDOOR TEMPERATURE</div>
      <div class="kv"><span class="k">Health</span><span class="v gr" id="sg-dsh">--</span></div>
      <div class="kv"><span class="k">Confidence</span><span class="v" id="sg-dsc">--%</span></div>
      <div class="kv"><span class="k">Retries</span><span class="v" id="sg-dsr">--</span></div>
      <div class="kv"><span class="k">Errors</span><span class="v rd" id="sg-dse">--</span></div>
    </div>
  </div>
</div></div>

<div id="pg-location" class="pg"><div class="page-wrap">
  <div class="sect">PERSISTENT WEATHER LOCATION</div>
  <p style="font-size:12px;color:var(--dim);margin-bottom:12px;font-weight:600">
    Set one location for continuous OpenWeatherMap updates. It remains saved across reboot until you change or clear it.
  </p>
  <div class="g2">
    <div class="mpanel">
      <div class="fld"><label>CITY / LOCATION</label><input type="text" id="loc-city" maxlength="47" placeholder="e.g. Lucknow"></div>
      <div class="fld"><label>COUNTRY CODE (OPTIONAL)</label><input type="text" id="loc-country" maxlength="7" placeholder="e.g. IN"></div>
      <button class="fbtn" onclick="saveLocation()">SAVE & FETCH LOCATION</button>
      <button class="fbtn sec" onclick="clearLocation()">CLEAR SAVED LOCATION</button>
      <div class="smsg" id="loc-msg"></div>
    </div>
    <div class="mpanel">
      <div class="sect" style="font-size:9px">CURRENT SAVED LOCATION</div>
      <div class="kv"><span class="k">Location</span><span class="v cy" id="loc-current">NONE</span></div>
      <div class="kv"><span class="k">API Key</span><span class="v" id="loc-api">NOT SET</span></div>
      <div class="kv"><span class="k">Internet</span><span class="v" id="loc-internet">OFFLINE</span></div>
      <div class="kv"><span class="k">Last Successful Update</span><span class="v" id="loc-updated">--</span></div>
      <div class="data-note">A location is not treated as ONLINE until OpenWeatherMap returns a successful reading.</div>
    </div>
  </div>
</div></div>

<div id="pg-wxsearch" class="pg"><div class="page-wrap">
  <div class="sect">GLOBAL WEATHER SEARCH — OLED OVERRIDE</div>
  <p style="font-size:12px;color:var(--dim);margin-bottom:12px;font-weight:600">Search any city — weather data will display on OLED for 60 seconds.</p>
  <div class="fld"><label>CITY NAME</label><input type="text" id="wx-city" placeholder="e.g. Tokyo, Mumbai, London, Dubai"></div>
  <div class="fld"><label>COUNTRY CODE (OPTIONAL)</label><input type="text" id="wx-cc" placeholder="e.g. JP, IN, GB, AE" style="max-width:220px"></div>
  <button class="fbtn" onclick="doSearch()">SEARCH & SHOW ON OLED</button>
  <div class="smsg" id="wx-msg"></div>
  <div id="wx-res" style="display:none;margin-top:14px">
    <div class="sect">CURRENT OVERRIDE DATA — SHOWING ON OLED</div>
    <div class="g3">
      <div class="mpanel">
        <div class="kv"><span class="k">City</span><span class="v cy" id="ov-c">--</span></div>
        <div class="kv"><span class="k">Temperature</span><span class="v gr" id="ov-t">--</span></div>
        <div class="kv"><span class="k">Feels Like</span><span class="v" id="ov-fl">--</span></div>
        <div class="kv"><span class="k">Humidity</span><span class="v" id="ov-h">--</span></div>
      </div>
      <div class="mpanel">
        <div class="kv"><span class="k">Pressure</span><span class="v" id="ov-p">--</span></div>
        <div class="kv"><span class="k">Wind</span><span class="v" id="ov-w">--</span></div>
        <div class="kv"><span class="k">Clouds</span><span class="v" id="ov-cl">--</span></div>
        <div class="kv"><span class="k">Condition</span><span class="v" id="ov-cd">--</span></div>
      </div>
      <div class="mpanel">
        <div class="kv"><span class="k">Local Time</span><span class="v cy" id="ov-tm">--</span></div>
        <div class="kv"><span class="k">Timezone</span><span class="v" id="ov-tz">--</span></div>
        <div class="kv"><span class="k">OLED Remaining</span><span class="v" style="color:var(--c4)" id="ov-rm">--s</span></div>
      </div>
    </div>
  </div>
</div></div>

<div id="pg-wtime" class="pg"><div class="page-wrap">
  <div class="sect">WORLD TIME LOOKUP</div>
  <p style="font-size:12px;color:var(--dim);margin-bottom:12px;font-weight:600">Search a city to show its local time on the OLED display for 60 seconds.</p>
  <div class="fld"><label>CITY NAME</label><input type="text" id="wt-city" placeholder="e.g. New York, Paris, Tokyo, Sydney"></div>
  <button class="fbtn" onclick="doWorldTime()">SHOW LOCAL TIME ON OLED</button>
  <div class="smsg" id="wt-msg"></div>
  <div style="margin-top:14px" class="mpanel">
    <div class="kv"><span class="k">Override Active</span><span class="v" id="wt-a">NO</span></div>
    <div class="kv"><span class="k">Current City</span><span class="v cy" id="wt-c">--</span></div>
    <div class="kv"><span class="k">Local Time</span><span class="v gr" id="wt-t">--</span></div>
    <div class="kv"><span class="k">Timezone</span><span class="v" id="wt-z">--</span></div>
    <div class="kv"><span class="k">OLED Remaining</span><span class="v" style="color:var(--c4)" id="wt-r">--</span></div>
  </div>
</div></div>

<div id="pg-evtlog" class="pg"><div class="page-wrap">
  <div class="sect">COMPLETE EVENT LOG</div>
  <div class="evlog-page" id="evlog-full"></div>
</div></div>

<div id="pg-settings" class="pg"><div class="page-wrap">
  <div class="g2">
    <div>
      <div class="sect">TIMEZONE &amp; API SETTINGS</div>
      <div class="fld"><label>POSIX TZ STRING</label><input type="text" id="cfg-tz" placeholder="e.g. IST-5:30"></div>
      <div class="fld"><label>UTC OFFSET (SECONDS)</label><input type="number" id="cfg-off" placeholder="e.g. 19800"></div>
      <div class="fld"><label>OPENWEATHERMAP API KEY</label><input type="password" id="cfg-owm" placeholder="Your OWM API Key"></div>
      <button class="fbtn" onclick="saveCfg()">SAVE SETTINGS</button>
      <button class="fbtn sec" onclick="loadCfg()">LOAD CURRENT</button>
      <div class="smsg" id="cfg-msg"></div>
    </div>
    <div>
      <div class="sect">INTERNET / WIFI CONNECTION</div>
      <div class="fld"><label>WIFI NETWORK SSID</label><input type="text" id="cfg-ssid" placeholder="Your WiFi network name"></div>
      <div class="fld"><label>WIFI PASSWORD</label><input type="password" id="cfg-pass" placeholder="WiFi Password"></div>
      <button class="fbtn" onclick="saveWifi()">CONNECT TO INTERNET WIFI</button>
      <div class="smsg" id="wifi-msg"></div>
    </div>
  </div>
</div>

    <div class="g2" style="margin-top:10px">
      <div>
        <div class="sect">ACCESS POINT CONFIGURATION</div>
        <p style="font-size:11px;color:var(--dim);margin-bottom:8px;font-weight:600">Change the offline fallback AP password. When WiFi is unavailable, connect to <b style="color:var(--c2)">WeatherStation-Setup</b> using this password.</p>
        <div class="fld"><label>AP PASSWORD (min 8 chars)</label><input type="password" id="ap-pass" placeholder="Default: weather123"></div>
        <button class="fbtn" onclick="saveApCfg()">SAVE AP CONFIG</button>
        <div class="smsg" id="ap-msg"></div>
      </div>
      <div>
        <div class="sect">CONNECTION MANAGEMENT</div>
        <div class="mpanel" style="margin-bottom:8px">
          <div class="kv"><span class="k">Connection Mode</span><span class="v" id="cm-mode">--</span></div>
          <div class="kv"><span class="k">Network SSID</span><span class="v cy" id="cm-ssid">--</span></div>
          <div class="kv"><span class="k">IP Address</span><span class="v cy" id="cm-ip">--</span></div>
          <div class="kv"><span class="k">Signal</span><span class="v" id="cm-rssi">--</span></div>
          <div class="kv"><span class="k">AP IP (Offline)</span><span class="v" id="cm-apip">192.168.4.1</span></div>
          <div class="kv"><span class="k">AP Clients</span><span class="v" id="cm-sta">--</span></div>
        </div>
        <button class="fbtn sec" onclick="wifiRecon()">RECONNECT WIFI</button>
        <button class="fbtn" style="border-color:var(--c5);color:var(--c5)" onclick="wifiForget()">FORGET SAVED WIFI</button>
        <div class="smsg" id="conn-msg"></div>
      </div>
    </div>
  </div></div>

<div id="pg-sysinfo" class="pg"><div class="page-wrap">
  <div class="sect">DEVICE INFORMATION</div>
  <div class="mpanel">
    <div class="kv"><span class="k">Firmware</span><span class="v cy" id="si-fw">--</span></div>
    <div class="kv"><span class="k">Free Heap</span><span class="v gr" id="si-fh">--</span></div>
    <div class="kv"><span class="k">Min Heap Ever</span><span class="v" id="si-mh">--</span></div>
    <div class="kv"><span class="k">CPU Frequency</span><span class="v" id="si-cpu">--</span></div>
    <div class="kv"><span class="k">MAC Address</span><span class="v cy" id="si-mac">--</span></div>
    <div class="kv"><span class="k">Chip Revision</span><span class="v" id="si-rev">--</span></div>
    <div class="kv"><span class="k">Filesystem</span><span class="v" id="si-fs">--</span></div>
  </div>
  <div class="sect" style="margin-top:12px">RECOVERY &amp; ERROR-HANDLING STATUS</div>
  <div class="mpanel">
    <div class="kv"><span class="k">Safe Mode</span><span class="v" id="si-safe">--</span></div>
    <div class="kv"><span class="k">Boot Confirmed</span><span class="v" id="si-bootok">--</span></div>
    <div class="kv"><span class="k">OLED Display</span><span class="v" id="si-oled">--</span></div>
    <div class="kv"><span class="k">I2C Bus Recoveries</span><span class="v" id="si-i2c">--</span></div>
    <div class="kv"><span class="k">WiFi Hard Resets</span><span class="v" id="si-wifirst">--</span></div>
  </div>
  <button class="fbtn" style="margin-top:10px" onclick="loadSys()">REFRESH SYSTEM INFO</button>
</div></div>

</main>

<aside class="rpanel">
  
  <div class="rp-section">
    <div class="rp-title">UPCOMING EVENTS</div>
    <div id="rp-upcoming">
      <div class="ev-item"><span class="ev-time" id="ev-t1">--:-- --</span><span class="ev-msg">Auto Log Save</span></div>
      <div class="ev-item"><span class="ev-time" id="ev-t2">--:-- --</span><span class="ev-msg">Sensor Read Cycle</span></div>
      <div class="ev-item"><span class="ev-time" id="ev-t3">--:-- --</span><span class="ev-msg">NTP Sync</span></div>
      <div class="ev-item"><span class="ev-time" id="ev-t4">--:-- --</span><span class="ev-msg">Health Check</span></div>
    </div>
  </div>
  
  <div class="evlog-scroll" id="rp-evscroll">
    <div class="rp-title">LATEST EVENTS</div>
    <div id="rp-evlog"></div>
    <button class="view-btn" onclick="nav(document.querySelector('[data-pg=evtlog]'),'evtlog')">VIEW ALL LOGS</button>
  </div>
  
  <div class="rp-section">
    <div class="rp-title">QUICK ACTIONS</div>
    <div class="qa-grid">
      <button class="qa-btn green" onclick="refreshWs()">REFRESH DATA</button>
      <button class="qa-btn" onclick="clearLog()">CLEAR LOGS</button>
      <button class="qa-btn red" onclick="doRestart()">RESTART DEVICE</button>
      <button class="qa-btn red" onclick="if(confirm('FACTORY RESET — all config lost!'))alert('Factory reset via device API')">FACTORY RESET</button>
    </div>
  </div>
</aside>

<footer class="botbar">
  <span><b>ADITYA</b> WEATHER STATION &nbsp;•&nbsp; Cyberpunk Edition &nbsp;•&nbsp; Live Data &nbsp;•&nbsp; Secure &nbsp;•&nbsp; Reliable &nbsp;•&nbsp; 24/7 Monitoring</span>
  <span>All Rights Reserved</span>
</footer>

</div>

<script>

(function(){
  var c=document.getElementById('cnv-bg');
  if(!c)return;
  var ctx=c.getContext('2d');
  var chars='01アイウエオ$#@&%<>XZ+-';
  var drops=[];
  function rs(){c.width=window.innerWidth;c.height=window.innerHeight;drops=Array(Math.floor(c.width/18)).fill(0).map(()=>-Math.random()*60);}
  window.addEventListener('resize',rs);rs();
  setInterval(function(){
    ctx.fillStyle='rgba(3,11,7,.08)';ctx.fillRect(0,0,c.width,c.height);
    ctx.font='13px monospace';
    for(var i=0;i<drops.length;i++){
      ctx.fillStyle=Math.random()>.97?'#aaffdd':'#00ffaa';
      ctx.globalAlpha=Math.random()*.7+.3;
      ctx.fillText(chars[Math.floor(Math.random()*chars.length)],i*18,drops[i]*18);
      ctx.globalAlpha=1;
      if(drops[i]*18>c.height&&Math.random()>.975)drops[i]=0;
      drops[i]++;
    }
  },65);
})();

function nav(el, pgId) {
  document.querySelectorAll('.sb-item').forEach(b => b.classList.remove('on'));
  if (el) el.classList.add('on');
  document.querySelectorAll('.pg').forEach(p => p.classList.remove('on'));
  var pg = document.getElementById('pg-' + pgId);
  if (pg) pg.classList.add('on');
  if (pgId === 'sysinfo') loadSys();
  var sb=document.querySelector('.sidebar');
  if (sb && window.innerWidth<=420) sb.classList.remove('mob-open');
}

var feedData=[];
function pushFeedSample(v){
  if(v===undefined||v===null||isNaN(+v)) return;
  feedData.push(+v); if(feedData.length>80) feedData.shift();
}
(function(){
  var c=document.getElementById('feedCanvas'); if(!c) return;
  var ctx=c.getContext('2d');
  function draw(){
    ctx.clearRect(0,0,c.width,c.height);
    if(feedData.length>1){
      var mn=Math.min.apply(null,feedData), mx=Math.max.apply(null,feedData);
      if(mn===mx){mn-=1;mx+=1;}
      ctx.beginPath();ctx.strokeStyle='#00ff88';ctx.lineWidth=1.5;ctx.shadowBlur=5;ctx.shadowColor='#00ff88';
      feedData.forEach(function(v,i){
        var x=i*(c.width/(feedData.length-1)), y=c.height-2-((v-mn)/(mx-mn))*(c.height-4);
        if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);
      });
      ctx.stroke();ctx.shadowBlur=0;
    }
    requestAnimationFrame(draw);
  }
  draw();
})();

var months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
function updateClock(){
  var n = new Date();
  var h = n.getHours(), m = n.getMinutes(), s = n.getSeconds();
  var ampm = h >= 12 ? 'PM' : 'AM', hh = h % 12 || 12;
  var timeStr = pad(hh)+':'+pad(m)+':'+pad(s)+' '+ampm;
  var dateStr = pad(n.getDate())+' '+months[n.getMonth()]+' '+n.getFullYear();
  setText('t-time', timeStr); setText('t-date', dateStr);
  
  var base = n.getTime();
  [1,5,60,65].forEach(function(min,i){
    var d = new Date(base + min*60000);
    var eh = d.getHours(), em = d.getMinutes(), ea = eh>=12?'PM':'AM', ehh=eh%12||12;
    setText('ev-t'+(i+1), pad(ehh)+':'+pad(em)+' '+ea);
  });
}
setInterval(updateClock, 1000); updateClock();

var BUF = 80;
function mkSpark(id, color) {
  var c = document.getElementById(id); if (!c) return {push:function(){}};
  var ctx = c.getContext('2d'), buf = [];
  function sz(){ c.width = c.offsetWidth * devicePixelRatio || 200; c.height = c.offsetHeight * devicePixelRatio || 32; }
  sz(); window.addEventListener('resize', sz);
  return { push: function(v) {
    if (v===null||v===undefined||isNaN(+v)) return;
    buf.push(+v); if (buf.length > BUF) buf.shift();
    var W=c.width, H=c.height, mn=Math.min(...buf), mx=Math.max(...buf);
    if (mn===mx){mn-=1;mx+=1;}
    ctx.clearRect(0,0,W,H);
    ctx.beginPath();
    buf.forEach((p,i)=>{ var x=i/(BUF-1)*W, y=H-(p-mn)/(mx-mn)*(H-3)-1.5; if(!i) ctx.moveTo(x,y); else ctx.lineTo(x,y); });
    ctx.strokeStyle=color; ctx.lineWidth=2*devicePixelRatio; ctx.shadowBlur=8; ctx.shadowColor=color; ctx.stroke(); ctx.shadowBlur=0;
    
    if(buf.length>1){var lv=buf[buf.length-1],mn2=Math.min(...buf),mx2=Math.max(...buf);if(mn2===mx2){mn2-=1;mx2+=1;}var lx=(BUF-1)/(BUF-1)*W,ly=H-(lv-mn2)/(mx2-mn2)*(H-3)-1.5;ctx.beginPath();ctx.arc(W-2,ly,2.5*devicePixelRatio,0,7);ctx.fillStyle=color;ctx.shadowBlur=6;ctx.shadowColor=color;ctx.fill();ctx.shadowBlur=0;}
  }};
}

function mkChart(id, color) {
  var c = document.getElementById(id); if (!c) return {push:function(){}};
  var ctx = c.getContext('2d'), buf = [];
  function sz(){ c.width = c.clientWidth * devicePixelRatio; c.height = c.clientHeight * devicePixelRatio; }
  sz(); window.addEventListener('resize', sz);
  return { push: function(v) {
    if (v===null||v===undefined||isNaN(+v)) return;
    buf.push(+v); if (buf.length > BUF) buf.shift();
    if (buf.length < 2) return;
    var W=c.width, H=c.height, pd=8, mn=Math.min(...buf), mx=Math.max(...buf);
    if (mn===mx){mn-=1;mx+=1;}
    ctx.clearRect(0,0,W,H);
    
    ctx.strokeStyle='rgba(0,212,255,.08)'; ctx.lineWidth=1;
    for(var g=1;g<5;g++){ var gy=H*g/5; ctx.beginPath(); ctx.moveTo(0,gy); ctx.lineTo(W,gy); ctx.stroke(); }
    
    ctx.beginPath();
    var pts=[];
    buf.forEach((p,i)=>{ var x=i/(buf.length-1)*(W-pd)+pd/2, y=H-pd-(p-mn)/(mx-mn)*(H-2*pd); pts.push({x,y}); if(!i) ctx.moveTo(x,y); else ctx.lineTo(x,y); });
    ctx.lineTo(pts[pts.length-1].x,H); ctx.lineTo(pts[0].x,H); ctx.closePath();
    var grad=ctx.createLinearGradient(0,0,0,H);
    grad.addColorStop(0,color.replace(')',', 0.2)').replace('rgb','rgba'));
    grad.addColorStop(1,color.replace(')',', 0.01)').replace('rgb','rgba'));
    ctx.fillStyle=grad; ctx.fill();
    
    ctx.beginPath();
    buf.forEach((p,i)=>{ var x=i/(buf.length-1)*(W-pd)+pd/2, y=H-pd-(p-mn)/(mx-mn)*(H-2*pd); if(!i) ctx.moveTo(x,y); else ctx.lineTo(x,y); });
    ctx.strokeStyle=color; ctx.lineWidth=5*devicePixelRatio; ctx.globalAlpha=0.15; ctx.shadowBlur=0; ctx.stroke();
    ctx.globalAlpha=1;
    ctx.beginPath();
    buf.forEach((p,i)=>{ var x=i/(buf.length-1)*(W-pd)+pd/2, y=H-pd-(p-mn)/(mx-mn)*(H-2*pd); if(!i) ctx.moveTo(x,y); else ctx.lineTo(x,y); });
    ctx.strokeStyle=color; ctx.lineWidth=2*devicePixelRatio; ctx.shadowBlur=16; ctx.shadowColor=color; ctx.stroke(); ctx.shadowBlur=0;
    
    ctx.fillStyle=color; ctx.font=(8*devicePixelRatio)+'px monospace'; ctx.textAlign='left';
    ctx.fillText(mx.toFixed(1), 2, 10*devicePixelRatio);
    ctx.fillText(mn.toFixed(1), 2, H-2);
  }};
}

function drawGauge(id, pct, color, label, unit) {
  var c = document.getElementById(id); if (!c) return;
  var ctx = c.getContext('2d'), W=c.width, H=c.height, cx=W/2, cy=H/2, r=cx-7;
  ctx.clearRect(0,0,W,H);
  ctx.beginPath(); ctx.arc(cx,cy,r,Math.PI*0.7,Math.PI*2.3);
  ctx.strokeStyle='rgba(0,212,255,.12)'; ctx.lineWidth=8; ctx.stroke();
  var end = Math.PI*0.7 + Math.PI*1.6*(pct/100);
  ctx.beginPath(); ctx.arc(cx,cy,r,Math.PI*0.7,end);
  ctx.strokeStyle=color; ctx.lineWidth=8; ctx.shadowBlur=12; ctx.shadowColor=color; ctx.stroke(); ctx.shadowBlur=0;
  ctx.fillStyle=color; ctx.textAlign='center'; ctx.textBaseline='middle';
  ctx.font='bold '+(13)+'px Orbitron,monospace'; ctx.fillText(label, cx, cy-5);
  ctx.font=(9)+'px Rajdhani,sans-serif'; ctx.fillStyle='#4a9a80'; ctx.fillText(unit||'', cx, cy+9);
}

var spT=mkSpark('sp-temp','#00ff88'), spH=mkSpark('sp-hum','#a855f7'), spP=mkSpark('sp-pres','#f59e0b');
var spL=mkSpark('sp-lux','#f59e0b'), spD=mkSpark('sp-ds','#00ff88'), spR=mkSpark('sp-rain','#00d4ff');
var gcT=mkChart('gv-t','#00ff88'), gcH=mkChart('gv-h','#a855f7'), gcP=mkChart('gv-p','#f59e0b');
var ggT=mkChart('gg-t','#00ff88'), ggH=mkChart('gg-h','#a855f7'), ggP=mkChart('gg-p','#f59e0b');
var ggL=mkChart('gg-l','#f59e0b'), ggD=mkChart('gg-d','#00ff88'), ggR=mkChart('gg-r','#00d4ff');

function setText(id,v){ var e=document.getElementById(id); if(e&&v!==undefined&&v!==null) e.textContent=v; }
function flash(id,v){ var e=document.getElementById(id); if(!e) return; var s=String(v); if(e.textContent===s) return; e.textContent=s; e.classList.remove('flash'); void e.offsetWidth; e.classList.add('flash'); }
function fmt(v,d){ return(v===undefined||v===null||isNaN(+v))?'--':(+v).toFixed(d); }
function pad(n){ return n<10?'0'+n:''+n; }
function fmtUp(s){ s=Math.floor(s); var d=Math.floor(s/86400),h=Math.floor((s%86400)/3600),m=Math.floor((s%3600)/60),sc=s%60; return pad(d)+'d '+pad(h)+'h '+pad(m)+'m '+pad(sc)+'s'; }
function lxLvl(v){ if(v>=10000)return'Bright Sun'; if(v>=1000)return'Daylight'; if(v>=100)return'Moderate'; if(v>=10)return'Twilight'; return'Night'; }
function rainLvl(v){ if(v>=70)return'High'; if(v>=50)return'Moderate'; if(v>=30)return'Low'; return'Low'; }
function wxIco(s){ if(!s) return'⛅'; var c=s.toLowerCase(); if(c.includes('storm'))return'⛈'; if(c.includes('snow'))return'❄'; if(c.includes('rain')||c.includes('drizzle'))return'🌧'; if(c.includes('cloud'))return'☁'; if(c.includes('clear'))return'☀'; return'⛅'; }
function setBadge(id,ok,h){ var e=document.getElementById(id); if(!e) return; var s=h||(ok?'ONLINE':'ERROR'); e.textContent=s; e.className='sbadge '+s; }
function setSH(pfx,h,c){ var dot=document.getElementById('sd-'+pfx); var st=document.getElementById('ss-'+pfx); var cv=document.getElementById('sc-'+pfx); if(dot){dot.className='sh-dot '+(h||'ERROR');} if(st){st.textContent=h||'--';st.className='sh-status '+(h||'ERROR');} if(cv) cv.textContent=(c!==undefined?c:'--')+'%'; }
function esc(s){ var d=document.createElement('div'); d.textContent=s; return d.innerHTML; }

var allEvts = [];
var evFull = document.getElementById('evlog-full');
var rpEvLog = document.getElementById('rp-evlog');

function addEvt(level, msg, ms) {
  allEvts.push({level:level||'INFO',msg:msg||'',ms:ms||Date.now()});
  if (allEvts.length > 500) allEvts.shift();
  renderEvLog();
  if (evFull) {
    var d=document.createElement('div'); d.className='ev-full';
    var t=new Date(ms||Date.now()).toLocaleTimeString();
    d.innerHTML='<span class="t">'+t+'</span><span class="lvl '+level+'">['+level+']</span><span class="m">'+esc(msg)+'</span>';
    evFull.appendChild(d); evFull.scrollTop=evFull.scrollHeight;
    if(evFull.children.length>400) evFull.removeChild(evFull.firstChild);
  }
}

function renderEvLog() {
  if (!rpEvLog) return;
  var last = allEvts.slice(-10).reverse();
  rpEvLog.innerHTML = last.map(e => {
    var t=new Date(e.ms).toLocaleTimeString();
    var lc=e.level==='ERROR'?'var(--c5)':e.level==='WARN'?'var(--c4)':'var(--txt)';
    return '<div class="ev-full"><span class="t">'+t+'</span><span class="lvl '+e.level+'">['+e.level+']</span><span class="m" style="color:'+lc+'">'+esc(e.msg)+'</span></div>';
  }).join('');
}

var authToken = sessionStorage.getItem('wsToken') || '';
function ensureSession() {
  if (authToken) return Promise.resolve();
  return fetch('/api/login',{method:'POST'}).then(r=>r.ok?r.json():null)
    .then(d=>{ if(d&&d.token){ authToken=d.token; sessionStorage.setItem('wsToken',authToken); } })
    .catch(()=>{});
}
function apiFetch(url, opts) {
  opts = opts || {};
  var o = Object.assign({}, opts);
  o.headers = Object.assign({}, opts.headers || {});
  if (authToken) o.headers['Authorization'] = 'Bearer ' + authToken;
  return fetch(url, o).then(r => {
    if (r.status !== 401 || !authToken) return r;
    authToken=''; sessionStorage.removeItem('wsToken');
    return fetch(url, opts);
  });
}
ensureSession().then(()=>{
  apiFetch('/api/events').then(r=>r.ok?r.json():null).then(d=>{ if(d&&d.events) d.events.forEach(e=>addEvt(e.level,e.msg,e.ms)); }).catch(()=>{});
});

var ws, wsr=1000;
function conn() {
  var pr = location.protocol==='https:'?'wss:':'ws:';
  ws = new WebSocket(pr+'//'+location.host+'/ws');
  ws.onopen = function() {
    document.getElementById('wdot').classList.remove('off');
    setText('sb-onl','● ONLINE'); document.getElementById('sb-onl').style.color='var(--c1)';
    wsr=1000;
  };
  ws.onclose = function() {
    document.getElementById('wdot').classList.add('off');
    setText('sb-onl','● OFFLINE'); document.getElementById('sb-onl').style.color='var(--c5)';
    setTimeout(conn,wsr); wsr=Math.min(wsr*1.5,15000);
  };
  ws.onerror = function(){ ws.close(); };
  ws.onmessage = function(e) {
    var d; try{d=JSON.parse(e.data);}catch(ex){return;}
    if (d.type==='event'){ addEvt(d.level,d.msg,d.ms); return; }

    var sw = (d.saved_weather && d.saved_weather.valid) ? d.saved_weather : null;
    var tempFromApi = !d.dht_ok && !!sw;
    var humFromApi  = !d.dht_ok && !!sw;
    var presFromApi = !d.bme_ok && !!sw;
    var effTemp  = d.dht_ok ? d.temp       : (sw ? sw.temp       : null);
    var effHum   = d.dht_ok ? d.humidity   : (sw ? sw.humidity   : null);
    var effPres  = d.bme_ok ? d.pressure   : (sw ? sw.pressure   : null);
    var effFeels = d.dht_ok ? d.heat_index : (sw ? sw.feels_like : null);

    pushFeedSample(effTemp);

    flash('v-temp', fmt(effTemp,1));
    flash('v-hum', fmt(effHum,0));
    flash('v-pres', fmt(effPres,1));
    setText('src-temp', tempFromApi ? '(API)' : '');
    setText('src-hum', humFromApi ? '(API)' : '');
    setText('src-pres', presFromApi ? '(API)' : '');
    flash('v-lux', d.lux==null?'--':(d.lux>=1000?(d.lux/1000).toFixed(1)+'k':Math.round(d.lux)));
    flash('v-ds', fmt(d.ds18_temp,1));
    flash('v-rain', d.rain_prob==null?'--':Math.round(d.rain_prob));
    setText('v-hi', fmt(effFeels,1));
    setText('v-dew', fmt(d.dew_point,1));
    setText('v-alt', fmt(d.altitude,0));
    setText('v-luxl', d.lux==null?'--':lxLvl(d.lux));
    setText('v-rainl', d.rain_prob==null?'--':rainLvl(d.rain_prob));
    setText('v-dscnt', d.ds18_count||'--');
    var tEl=document.getElementById('v-trend');
    if(tEl){var tr=d.pressure_trend; tEl.textContent=tr==null?'--':(tr>0.1?'▲':tr<-0.1?'▼':'~'); tEl.style.color=tr==null?'var(--dim)':(tr>0.1?'var(--c1)':tr<-0.1?'var(--c5)':'var(--c4)');}

    setBadge('bd-dht',d.dht_ok,d.dht_health); setBadge('bd-bme',d.bme_ok,d.bme_health);
    setBadge('bd-bh',d.bh_ok,d.bh_health); setBadge('bd-ds',d.ds_ok,d.ds_health);
    setSH('dht',d.dht_health,d.dht_confidence); setSH('bme',d.bme_health,d.bme_confidence);
    setSH('bh',d.bh_health,d.bh_confidence); setSH('ds',d.ds_health,d.ds_confidence);

    spT.push(effTemp); spH.push(effHum); spP.push(effPres);
    spL.push(d.lux); spD.push(d.ds18_temp); spR.push(d.rain_prob);
    gcT.push(effTemp); gcH.push(effHum); gcP.push(effPres);
    ggT.push(effTemp); ggH.push(effHum); ggP.push(effPres);
    ggL.push(d.lux); ggD.push(d.ds18_temp); ggR.push(d.rain_prob);
    setText('gc-t', fmt(effTemp,1)+'°C'); setText('gc-h', fmt(effHum,0)+'%'); setText('gc-p', fmt(effPres,1)+' hPa');

    setText('rt-dht-t', d.dht_ok ? fmt(d.temp,1)+' °C' : 'OFFLINE');
    setText('rt-dht-h', d.dht_ok ? fmt(d.humidity,1)+' %' : 'OFFLINE');
    setText('rt-bme-t', d.bme_ok ? fmt(d.bme_temp,1)+' °C' : 'OFFLINE');
    setText('rt-bme-h', d.bme_ok ? fmt(d.bme_humidity,1)+' %' : 'OFFLINE');
    setText('rt-bme-p', d.bme_ok ? fmt(d.pressure,1)+' hPa' : 'OFFLINE');
    setText('rt-alt', d.bme_ok ? fmt(d.altitude,1)+' m' : 'OFFLINE');
    setText('rt-lux', d.bh_ok ? fmt(d.lux,1)+' lx' : 'OFFLINE');
    setText('rt-ds1', d.ds_ok ? fmt(d.ds18_temp,1)+' °C' : 'OFFLINE');
    setText('rt-ds2', (d.ds_ok && d.ds18_count>1) ? fmt(d.ds18_temp_2,1)+' °C' : (d.ds_ok ? 'N/A' : 'OFFLINE'));
    setText('rt-dsc', d.ds_ok ? String(d.ds18_count) : 'OFFLINE');
    setText('rt-dsmm', d.ds_ok ? fmt(d.ds18_min,1)+' / '+fmt(d.ds18_max,1)+' °C' : 'OFFLINE');
    setText('rt-hi', d.dht_ok ? fmt(d.heat_index,1)+' °C' : 'OFFLINE');
    setText('rt-dew', d.dht_ok ? fmt(d.dew_point,1)+' °C' : 'OFFLINE');
    setText('rt-trend', d.bme_ok ? ((d.pressure_trend>=0?'+':'')+fmt(d.pressure_trend,2)+' hPa/min') : 'OFFLINE');
    setText('rt-rain', (d.dht_ok&&d.bme_ok) ? Math.round(d.rain_prob)+' %' : 'OFFLINE');
    setText('rt-snap', d.snapshot_seq!==undefined ? '#'+d.snapshot_seq : '--');
    setText('rt-dht-health', (d.dht_health||'OFFLINE')+' · '+(d.dht_confidence||0)+'%');
    setText('rt-bme-health', (d.bme_health||'OFFLINE')+' · '+(d.bme_confidence||0)+'%');
    setText('rt-bh-health', (d.bh_health||'OFFLINE')+' · '+(d.bh_confidence||0)+'%');
    setText('rt-ds-health', (d.ds_health||'OFFLINE')+' · '+(d.ds_confidence||0)+'%');
    setText('rt-conf', (d.dht_confidence||0)+' / '+(d.bme_confidence||0)+' / '+(d.bh_confidence||0)+' / '+(d.ds_confidence||0));
    setText('rt-retries', (d.dht_retries||0)+' / '+(d.bme_retries||0)+' / '+(d.bh_retries||0)+' / '+(d.ds_retries||0));
    setText('rt-errs', (d.dht_errors||0)+' / '+(d.bme_errors||0)+' / '+(d.bh_errors||0)+' / '+(d.ds_errors||0));

    setText('rt-wifi', d.wifi_connected ? 'ONLINE' : 'OFFLINE');
    setText('rt-ssid', d.ssid || (d.wifi_connected ? '--' : 'OFFLINE'));
    setText('rt-ip', d.sta_ip || 'OFFLINE');
    setText('rt-rssi', d.wifi_connected ? String(d.rssi)+' dBm' : 'OFFLINE');
    setText('rt-apssid', d.ap_ssid || '--');
    setText('rt-apip', d.ap_ip || '--');
    setText('rt-apsta', d.ap_stations!==undefined ? String(d.ap_stations) : '--');
    setText('rt-up', d.uptime_s!==undefined ? fmtUp(d.uptime_s) : '--');
    setText('rt-heap', d.free_heap!==undefined ? Math.round(d.free_heap/1024)+' KB' : '--');
    setText('rt-frag', (d.heap_frag_pct!==undefined ? d.heap_frag_pct : '--')+'%');
    setText('rt-fs', d.fs_used!==undefined ? Math.round(d.fs_used/1024)+' / '+Math.round(d.fs_total/1024)+' KB' : '--');
    setText('rt-fw', d.firmware||'--');
    setText('rt-cpu', d.cpu_mhz!==undefined ? d.cpu_mhz+' MHz' : '--');
    setText('rt-chip', d.chip_rev!==undefined ? 'Rev '+d.chip_rev : '--');
    setText('rt-mac', d.mac||'--');
    setText('rt-minheap', d.min_heap!==undefined ? Math.round(d.min_heap/1024)+' KB' : '--');
    setText('rt-oled', d.oled_ok ? 'ONLINE' : 'OFFLINE');
    setText('rt-boot', d.boot_confirmed ? 'YES' : 'NO');
    setText('rt-safe', d.safe_mode ? ('YES — '+(d.safe_mode_reason||'')) : 'NO');
    setText('rt-i2c', d.i2c_recoveries!==undefined ? d.i2c_recoveries : '--');
    setText('rt-whr', d.wifi_hard_resets!==undefined ? d.wifi_hard_resets : '--');
    setText('rt-ws', 'LIVE');
    setText('rt-ntp', d.time_valid ? 'SYNCED / VALID' : 'NOT SYNCED');

    if(d.saved_weather){
      var sw=d.saved_weather;
      setText('rt-location', sw.valid ? ((sw.city||'--')+(sw.country?', '+sw.country:'')) : 'LOCATION SAVED — WAITING FOR API');
      setText('rt-wx-cond', sw.valid ? (sw.description||'--') : '--');
      setText('rt-wx-temp', sw.valid ? fmt(sw.temp,1)+' °C' : '--');
      setText('rt-wx-feel', sw.valid ? fmt(sw.feels_like,1)+' °C' : '--');
      setText('rt-wx-hum', sw.valid ? fmt(sw.humidity,0)+' %' : '--');
      setText('rt-wx-pres', sw.valid ? fmt(sw.pressure,0)+' hPa' : '--');
      setText('rt-wx-wind', sw.valid ? fmt(sw.wind,1)+' m/s' : '--');
      setText('rt-wx-cloud', sw.valid ? String(sw.clouds)+' %' : '--');
      setText('rt-wx-time', sw.valid ? (sw.time||'--') : '--');
      setText('rt-wx-zone', sw.valid ? (sw.timezone||'--') : '--');
      setText('rt-wx-updated', sw.valid ? 'Last successful API update: '+(sw.updated_s ? fmtUp(d.uptime_s-sw.updated_s) : 'recent') : 'No successful API reading yet.');
    } else {
      setText('rt-location','NO LOCATION SET');
    }

    var wxCond = (d.weather && d.weather.length) ? d.weather : (sw ? sw.description : null);
    setText('wx-ico', wxCond ? wxIco(wxCond) : '⛅');
    setText('wx-cond', wxCond || '--');
    if (d.dht_ok) setText('wx-sub', 'Humidity '+fmt(d.humidity,0)+'%, Dew Point '+fmt(d.dew_point,1)+'°C');
    else if (sw) setText('wx-sub', 'Online weather · '+(sw.city||'--'));
    else setText('wx-sub', 'No local sensor data');
    setText('wx-t', fmt(effTemp,1)+'°C'); setText('wx-h', fmt(effHum,0)+'%');
    setText('wx-w', sw ? fmt(sw.wind,1)+' m/s' : '--');
    setText('wx-v', (sw && sw.visibility_m!=null && sw.visibility_m>=0) ? fmt(sw.visibility_m/1000,1)+' km' : '--');

    var fh=d.free_heap||0, fhKb=Math.round(fh/1024);
    var fhPct=Math.min(100,Math.round(fh/300000*100));
    var fsPct=d.fs_total?Math.round(d.fs_used/d.fs_total*100):0;
    var rssiPct=d.rssi?Math.min(100,Math.round((d.rssi+100)/60*100)):0;
    drawGauge('g-heap', fhPct, '#00ff88', fhKb+' KB', 'HEAP');
    drawGauge('g-frag', fsPct, '#a855f7', fsPct+'%', 'FLASH');
    drawGauge('g-wifi', rssiPct, '#00d4ff', (d.rssi||'--')+'', 'dBm');
    setText('pr-h',''); var prh=document.getElementById('pr-h'); if(prh) prh.style.width=fhPct+'%'; setText('pv-h',fhKb+' KB');
    var prf=document.getElementById('pr-f'); if(prf) prf.style.width=fsPct+'%'; setText('pv-f',fsPct+'%');
    var prw=document.getElementById('pr-w'); if(prw) prw.style.width=rssiPct+'%'; setText('pv-w',(d.rssi||'--')+' dBm');

    var fmtUptime = fmtUp(d.uptime_s||0);
    setText('t-up', fmtUptime); setText('sup', fmtUptime);
    setText('sb-ip', d.wifi_connected ? (d.sta_ip||d.ip||'--') : (d.ap_ip||d.ip||'--'));
    setText('sb-rssi', d.wifi_connected ? ((d.rssi||'--')+' dBm') : '-- dBm');
    setText('ts-heap', fhKb+' KB'); setText('ts-rssi', d.wifi_connected ? ((d.rssi||'--')+' dBm') : '-- dBm');
    setText('sysTitle', d.wifi_connected?'SYSTEM ONLINE':'SYSTEM OFFLINE');
    var sysSub=document.querySelector('.sys-sub'); if(sysSub) sysSub.textContent=d.wifi_connected?'Internet/WiFi connected':'AP MODE — Internet disconnected';
    setText('sh', fhKb+' KB'); setText('sf', fsPct+'%');
    setText('sr', (d.rssi||'--')+' dBm'); setText('si', d.ip||'--');
    setText('ss', d.snapshot_seq||'--');
    setText('sfs', d.fs_total?Math.round((d.fs_used||0)/1024)+'/'+Math.round(d.fs_total/1024)+' KB':'--');
    var tsws=document.getElementById('ts-ws');if(tsws){tsws.textContent='● LIVE';tsws.style.color='var(--c1)';}
    
    if(d.safe_mode){
      var ob=document.getElementById('offline-badge');
      if(ob){ob.style.display='block';ob.textContent='⚠ SAFE MODE ACTIVE — '+((d.safe_mode_reason)||'Recovery mode');}
    }

    setText('ld-t',fmt(effTemp,1)+'°C'); setText('ld-h',fmt(effHum,0)+'%');
    setText('ld-hi',fmt(effFeels,1)+'°C'); setText('ld-dp',fmt(d.dew_point,1)+'°C');
    setText('ld-p',fmt(effPres,1)+' hPa'); setText('ld-a',fmt(d.altitude,0)+' m');
    setText('ld-tr',(d.pressure_trend>0?'+':'')+fmt(d.pressure_trend,2)+' hPa/m');
    setText('ld-l',fmt(d.lux,0)+' lx'); setText('ld-d1',fmt(d.ds18_temp,1)+'°C');
    setText('ld-d2',d.ds18_temp_2==null?'--':fmt(d.ds18_temp_2,1)+'°C');
    setText('ld-mm',fmt(d.ds18_min,1)+'/'+fmt(d.ds18_max,1));
    setText('ld-r',fmt(d.rain_prob,0)+'%'); setText('ld-w',wxCond||'--');
    if(d.uptime_s!==undefined) setText('ld-up',fmtUp(d.uptime_s));
    setText('ld-sn',d.snapshot_seq||'--'); setText('ld-fh',fhKb+' KB'); setText('ld-fg',(d.heap_frag_pct||0)+'%');

    var smap={dht:['sg-dh','sg-dc','sg-dr','sg-de'],bme:['sg-bh','sg-bc','sg-br','sg-be'],bh:['sg-lh','sg-lc','sg-lr','sg-le'],ds:['sg-dsh','sg-dsc','sg-dsr','sg-dse']};
    ['dht','bme','bh','ds'].forEach(function(n){
      var ids=smap[n]; var hth=d[n+'_health']||'--'; var conf=d[n==='bh'?'bh_confidence':n+'_confidence']||0;
      var ret=d[n+'_retries']||0; var err=d[n+'_errors']||0;
      var hEl=document.getElementById(ids[0]);
      if(hEl){hEl.textContent=hth;hEl.style.color=hth==='ONLINE'?'var(--c1)':hth==='WARNING'?'var(--c4)':'var(--c5)';}
      setText(ids[1],conf+'%'); setText(ids[2],ret); setText(ids[3],err);
    });

    var ob=document.getElementById('offline-badge');
    if(ob&&d.offline_mode!==undefined){
      ob.style.display=d.offline_mode?'block':'none';
      if(d.offline_mode) ob.textContent='⚠ OFFLINE MODE — Connect to WeatherStation-Setup → '+(d.ap_ip||'192.168.4.1');
    }

    document.getElementById('wx-res').style.display=d.override_active?'block':'none';
    setText('wt-a',d.override_active?'YES':'NO');
  };
}
conn();

setInterval(function(){
  apiFetch('/api/override-status').then(r=>r.json()).then(function(d){
    if(d.active){
      setText('ov-c',d.city); setText('ov-t',(d.temp||'--')+'°C'); setText('ov-fl',(d.feels_like||'--')+'°C');
      setText('ov-h',(d.humidity||'--')+'%'); setText('ov-p',(d.pressure||'--')+' hPa');
      setText('ov-w',(d.wind||'--')+' m/s'); setText('ov-cl',(d.clouds||'--')+'%');
      setText('ov-cd',d.description||'--'); setText('ov-tm',d.time||'--');
      setText('ov-tz',d.timezone||'--'); setText('ov-rm',(d.remaining_s||'--')+'s');
      document.getElementById('wx-res').style.display='block';
      setText('wt-a','YES'); setText('wt-c',d.city||'--'); setText('wt-t',d.time||'--');
      setText('wt-z',d.timezone||'--'); setText('wt-r',(d.remaining_s||'--')+'s');
    } else { setText('wt-a','NO'); }
  }).catch(()=>{});
},5000);

function loadLocation(){
  apiFetch('/api/location').then(r=>r.json()).then(d=>{
    document.getElementById('loc-city').value=d.city||'';
    document.getElementById('loc-country').value=d.country||'';
    setText('loc-current',d.city?(d.city+(d.country?', '+d.country:'')):'NONE');
    setText('loc-api',d.api_key_set?'SET':'NOT SET');
    setText('loc-internet',d.wifi_connected?'ONLINE':'OFFLINE');
    setText('loc-updated',d.weather_valid?'AVAILABLE':'--');
  }).catch(()=>{});
}
function saveLocation(){
  var city=document.getElementById('loc-city').value.trim();
  var country=document.getElementById('loc-country').value.trim();
  var m=document.getElementById('loc-msg');
  if(!city){m.textContent='Enter a location or use CLEAR.';m.className='smsg er';return;}
  m.textContent='Saving and fetching real weather...';m.className='smsg';
  apiFetch('/api/location',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({city:city,country:country})})
    .then(r=>r.json()).then(j=>{m.textContent=j.msg||j.error||'Done.';m.className=j.ok?'smsg ok':'smsg er';loadLocation();}).catch(()=>{m.textContent='Network error.';m.className='smsg er';});
}
function clearLocation(){
  var m=document.getElementById('loc-msg');
  apiFetch('/api/location',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({city:'',country:''})})
    .then(r=>r.json()).then(j=>{m.textContent=j.msg||j.error||'Done.';m.className=j.ok?'smsg ok':'smsg er';loadLocation();}).catch(()=>{m.textContent='Network error.';m.className='smsg er';});
}
loadLocation();

function doSearch(){
  var city=document.getElementById('wx-city').value.trim();
  var cc=document.getElementById('wx-cc').value.trim();
  var m=document.getElementById('wx-msg');
  if(!city){m.textContent='Enter city name.';m.className='smsg er';return;}
  m.textContent='Searching...';m.className='smsg';
  apiFetch('/api/weather-search',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({city:city,country:cc})})
    .then(r=>r.json().then(j=>({ok:r.ok,j}))).then(x=>{m.textContent=x.ok?'Search accepted — OLED override will show the real result for 60s.':x.j.error||'Failed.';m.className=x.ok?'smsg ok':'smsg er';}).catch(()=>{m.textContent='Network error.';m.className='smsg er';});
}
function doWorldTime(){
  var city=document.getElementById('wt-city').value.trim();
  var m=document.getElementById('wt-msg');
  if(!city){m.textContent='Enter city name.';m.className='smsg er';return;}
  m.textContent='Fetching...';m.className='smsg';
  apiFetch('/api/weather-search',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({city:city})})
    .then(r=>r.json()).then(j=>{m.textContent=j.ok?'Local time showing on OLED for 60s.':j.error||'Failed.';m.className=j.ok?'smsg ok':'smsg er';}).catch(()=>{});
}
function loadCfg(){
  apiFetch('/api/config').then(r=>r.json()).then(c=>{
    document.getElementById('cfg-tz').value=c.tz_str||'';
    document.getElementById('cfg-off').value=c.tz_offset||'';
    document.getElementById('cfg-msg').textContent='Loaded.';document.getElementById('cfg-msg').className='smsg ok';
  }).catch(()=>{document.getElementById('cfg-msg').textContent='Failed.';document.getElementById('cfg-msg').className='smsg er';});
}
function saveCfg(){
  var b={tz_str:document.getElementById('cfg-tz').value,tz_offset:Number(document.getElementById('cfg-off').value)||0,owm_key:document.getElementById('cfg-owm').value};
  apiFetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)}).then(r=>r.json()).then(j=>{var m=document.getElementById('cfg-msg');m.textContent=j.ok?'Config saved.':j.error||'Error.';m.className=j.ok?'smsg ok':'smsg er';}).catch(()=>{});
}
function saveWifi(){
  var b={wifi_ssid:document.getElementById('cfg-ssid').value.trim(),wifi_pass:document.getElementById('cfg-pass').value};
  apiFetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)}).then(r=>r.json()).then(j=>{var m=document.getElementById('wifi-msg');m.textContent=j.msg||'Done.';m.className='smsg ok';}).catch(()=>{});
}
function loadSys(){
  apiFetch('/api/system').then(r=>r.json()).then(d=>{
    setText('si-fw',d.firmware||'--'); setText('si-fh',Math.round(d.free_heap/1024)+' KB');
    setText('si-mh',Math.round(d.min_heap/1024)+' KB'); setText('si-cpu',d.cpu_mhz+' MHz');
    setText('si-mac',d.mac||'--'); setText('si-rev','Rev '+d.chip_rev);
    setText('si-fs',Math.round(d.fs_used/1024)+'/'+Math.round(d.fs_total/1024)+' KB');
    var safeEl=document.getElementById('si-safe');
    if(safeEl){safeEl.textContent=d.safe_mode?('YES — '+(d.safe_mode_reason||'')):'NO';safeEl.style.color=d.safe_mode?'var(--c5)':'var(--c1)';}
    var bootEl=document.getElementById('si-bootok');
    if(bootEl){bootEl.textContent=d.boot_confirmed?'YES':'PENDING';bootEl.style.color=d.boot_confirmed?'var(--c1)':'var(--c4)';}
    var oledEl=document.getElementById('si-oled');
    if(oledEl){oledEl.textContent=d.oled_ok?'OK':'NOT DETECTED';oledEl.style.color=d.oled_ok?'var(--c1)':'var(--c5)';}
    setText('si-i2c',d.i2c_recoveries!==undefined?d.i2c_recoveries:'0');
    setText('si-wifirst',d.wifi_hard_resets!==undefined?d.wifi_hard_resets:'0');
  }).catch(()=>{});
}
function refreshWs(){ if(ws&&ws.readyState===1) ws.send('{}'); }
function saveApCfg(){
  var p=document.getElementById('ap-pass').value.trim();
  var m=document.getElementById('ap-msg');
  if(p&&p.length<8){m.textContent='Min 8 characters.';m.className='smsg er';return;}
  apiFetch('/api/ap/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ap_pass:p||'weather123'})})
    .then(r=>r.json()).then(j=>{m.textContent=j.msg||'Saved.';m.className=j.ok?'smsg ok':'smsg er';}).catch(()=>{m.textContent='Error.';m.className='smsg er';});
}
function wifiRecon(){
  var m=document.getElementById('conn-msg');
  m.textContent='Reconnecting...';m.className='smsg';
  apiFetch('/api/wifi/reconnect',{method:'POST'}).then(r=>r.json()).then(j=>{m.textContent=j.msg||'Triggered.';m.className='smsg ok';loadWifiStatus();}).catch(()=>{m.textContent='Error.';m.className='smsg er';});
}
function wifiForget(){
  if(!confirm('Forget saved WiFi credentials? Device will use AP mode on next restart.')) return;
  var m=document.getElementById('conn-msg');
  apiFetch('/api/wifi/forget',{method:'POST'}).then(r=>r.json()).then(j=>{m.textContent=j.msg||'Done.';m.className='smsg ok';}).catch(()=>{m.textContent='Error.';m.className='smsg er';});
}
function doRestart(){
  if(!confirm('Restart device? The dashboard will be unreachable for a few seconds.')) return;
  apiFetch('/api/restart',{method:'POST'}).then(r=>r.json()).then(j=>{ alert(j.msg||'Restarting...'); }).catch(()=>{ alert('Restart triggered (connection dropped as expected).'); });
}
function loadWifiStatus(){
  apiFetch('/api/wifi/status').then(r=>r.json()).then(function(d){
    var mEl=document.getElementById('cm-mode');
    if(mEl) mEl.textContent=d.offline_mode?'OFFLINE (AP MODE)':(d.connected?'ONLINE (WiFi)':'DISCONNECTED');
    if(mEl) mEl.style.color=d.offline_mode?'var(--c4)':(d.connected?'var(--c1)':'var(--c5)');
    setText('cm-ssid',d.ssid||'--'); setText('cm-ip',d.ip||'--');
    setText('cm-rssi',d.rssi?d.rssi+' dBm':'--'); setText('cm-apip',d.ap_ip||'192.168.4.1');
    setText('cm-sta',d.ap_stations!==undefined?d.ap_stations+' client(s)':'--');
    
    var ob=document.getElementById('offline-badge');
    if(ob) ob.style.display=d.offline_mode?'inline-block':'none';
    if(ob) ob.textContent='⚠ OFFLINE MODE — Connect to '+d.ap_ssid+' → '+d.ap_ip;
  }).catch(()=>{});
}
setInterval(loadWifiStatus, 10000);
setTimeout(loadWifiStatus, 2000);
function clearLog(){
  var t=prompt('Enter log clear token:'); if(!t) return;
  fetch('/log/clear',{method:'POST',headers:{'X-Clear-Token':t}}).then(r=>r.text()).then(s=>addEvt('INFO','Log cleared: '+s)).catch(()=>{});
}
setTimeout(loadSys, 2000);

</script>
</body>
</html>)WSDASH";

void loadConfig() {
  prefs.begin(PREF_NAMESPACE, true);
  cfg.tzOffsetSec = prefs.getInt(PREF_TZ_OFFSET, 19800);
  strlcpy(cfg.tzStr,    prefs.getString(PREF_TZ_STR,   "IST-5:30").c_str(),   sizeof(cfg.tzStr));
  strlcpy(cfg.owmApiKey,prefs.getString(PREF_OWM_KEY,  OWM_API_DEFAULT_KEY).c_str(), sizeof(cfg.owmApiKey));
  strlcpy(cfg.wifiSsid, prefs.getString(PREF_WIFI_SSID,"").c_str(),           sizeof(cfg.wifiSsid));
  strlcpy(cfg.wifiPass, prefs.getString(PREF_WIFI_PASS,"").c_str(),           sizeof(cfg.wifiPass));
  strlcpy(cfg.apPass,   prefs.getString(PREF_AP_PASS, FALLBACK_AP_PASS).c_str(), sizeof(cfg.apPass));
  strlcpy(cfg.weatherCity, prefs.getString(PREF_WX_CITY, "").c_str(), sizeof(cfg.weatherCity));
  strlcpy(cfg.weatherCountry, prefs.getString(PREF_WX_COUNTRY, "").c_str(), sizeof(cfg.weatherCountry));
  prefs.end();
  Serial.printf("[Config] Loaded: tz=%d, owm=%s\n", cfg.tzOffsetSec, cfg.owmApiKey[0] ? "set" : "empty");
}

void saveConfig() {
  prefs.begin(PREF_NAMESPACE, false);
  prefs.putInt   (PREF_TZ_OFFSET, cfg.tzOffsetSec);
  prefs.putString(PREF_TZ_STR,    cfg.tzStr);
  prefs.putString(PREF_OWM_KEY,   cfg.owmApiKey);
  prefs.putString(PREF_WIFI_SSID, cfg.wifiSsid);
  prefs.putString(PREF_WIFI_PASS, cfg.wifiPass);
  prefs.putString(PREF_AP_PASS,   cfg.apPass);
  prefs.putString(PREF_WX_CITY, cfg.weatherCity);
  prefs.putString(PREF_WX_COUNTRY, cfg.weatherCountry);
  prefs.end();
  Serial.println(F("[Config] Saved."));
}

bool connectHiddenWifi(const char* ssid, const char* password) {
  Serial.printf("[WiFi] Connecting to WiFi SSID: %s\n", ssid);

  WiFi.mode(WIFI_AP_STA);
  WiFi.disconnect(false);
  delay(100);
  
  WiFi.begin(ssid, password);

  uint8_t tries = 0;
  while (WiFi.status() != WL_CONNECTED && tries < 30) {
    esp_task_wdt_reset();
    delay(500);
    Serial.print('.');
    tries++;
  }

  if (WiFi.status() == WL_CONNECTED) {
    offlineMode = false;
    Serial.printf("\n[WiFi] WiFi connected: %s\n", WiFi.localIP().toString().c_str());
    logEvent(EVT_INFO,"WiFi connected: %s",WiFi.localIP().toString().c_str());
    return true;
  }

  offlineMode = true;
  Serial.println(F("\n[WiFi] WiFi connection failed."));
  logEvent(EVT_WARN,"WiFi connection failed; AP dashboard remains available");
  return false;
}

void formatUtcOffset(long offsetSec, char* out, size_t outLen) {
  char sign=(offsetSec<0)?'-':'+'; long absSec=labs(offsetSec);
  snprintf(out, outLen, "UTC%c%02d:%02d", sign, (int)(absSec/3600), (int)((absSec%3600)/60));
}

static void urlEncodeAppend(char* out, size_t outLen, size_t& pos, const char* in) {
  static const char hex[] = "0123456789ABCDEF";
  for (const unsigned char* p = (const unsigned char*)in; *p && pos + 4 < outLen; p++) {
    if (isalnum(*p) || *p=='-' || *p=='_' || *p=='.' || *p=='~') { out[pos++] = (char)*p; }
    else { out[pos++]='%'; out[pos++]=hex[(*p)>>4]; out[pos++]=hex[(*p)&0xF]; }
  }
  out[pos] = '\0';
}
bool fetchWeatherRaw(const char* city, const char* countryCode, PersistentWeatherData* out) {
  if (!out || WiFi.status()!=WL_CONNECTED || cfg.owmApiKey[0]=='\0') return false;
  char cityEnc[144]={0}, countryEnc[24]={0}; size_t pEnc=0;
  urlEncodeAppend(cityEnc,sizeof(cityEnc),pEnc,city);
  pEnc=0; urlEncodeAppend(countryEnc,sizeof(countryEnc),pEnc,countryCode?countryCode:"");
  char url[320];
  if (countryCode && strlen(countryCode)>0)
    snprintf(url,sizeof(url),"http://api.openweathermap.org/data/2.5/weather?q=%s,%s&appid=%s&units=metric",cityEnc,countryEnc,cfg.owmApiKey);
  else
    snprintf(url,sizeof(url),"http://api.openweathermap.org/data/2.5/weather?q=%s&appid=%s&units=metric",cityEnc,cfg.owmApiKey);
  WiFiClient httpClient;
  HTTPClient http; http.setTimeout(8000);
  if (!http.begin(httpClient,url)) return false;
  int httpCode=http.GET();
  if (httpCode!=200) { Serial.printf("[OWM] Persistent HTTP %d\n",httpCode); http.end(); return false; }
  String payload=http.getString(); http.end();
  StaticJsonDocument<768> doc;
  if (deserializeJson(doc,payload)) return false;
  PersistentWeatherData tmp={};
  strlcpy(tmp.city,doc["name"]|city,sizeof(tmp.city));
  strlcpy(tmp.country,doc["sys"]["country"]|(countryCode?countryCode:""),sizeof(tmp.country));
  JsonArray wa=doc["weather"];
  if (!wa.isNull()&&wa.size()>0) {
    strlcpy(tmp.description,wa[0]["description"]|"",sizeof(tmp.description));
    strlcpy(tmp.iconCode,wa[0]["icon"]|"",sizeof(tmp.iconCode));
  }
  tmp.tempC=doc["main"]["temp"]|NAN; tmp.feelsLike=doc["main"]["feels_like"]|NAN;
  tmp.humidity=doc["main"]["humidity"]|NAN; tmp.pressure=doc["main"]["pressure"]|NAN;
  tmp.windSpeed=doc["wind"]["speed"]|NAN; tmp.clouds=doc["clouds"]["all"]|0;
  tmp.visibilityM=doc["visibility"]|-1;
  long tz=doc["timezone"]|cfg.tzOffsetSec;
  time_t utcNow; time(&utcNow); time_t cityLocal=utcNow+(time_t)tz;
  struct tm ctm; gmtime_r(&cityLocal,&ctm);
  snprintf(tmp.timeStr,sizeof(tmp.timeStr),"%02d:%02d:%02d",ctm.tm_hour,ctm.tm_min,ctm.tm_sec);
  formatUtcOffset(tz,tmp.timezone,sizeof(tmp.timezone));
  tmp.updatedMs=millis(); tmp.valid=true;
  *out=tmp;
  return true;
}

bool fetchPersistentWeather() {
  if (cfg.weatherCity[0]=='\0') return false;
  PersistentWeatherData tmp={};
  if (!fetchWeatherRaw(cfg.weatherCity,cfg.weatherCountry,&tmp)) return false;
  if (xSemaphoreTake(overrideMutex,pdMS_TO_TICKS(500))==pdTRUE) {
    savedWeather=tmp;
    xSemaphoreGive(overrideMutex);
  }
  logEvent(EVT_INFO,"OWM updated: %s%s%s",tmp.city,tmp.country[0]?", ":"",tmp.country);
  return true;
}

bool fetchWeatherAndTime(const char* city, const char* countryCode) {
  PersistentWeatherData raw={};
  if (!fetchWeatherRaw(city,countryCode,&raw)) return false;
  OverrideData tmp={};
  strlcpy(tmp.city,raw.city,sizeof(tmp.city)); strlcpy(tmp.country,raw.country,sizeof(tmp.country));
  strlcpy(tmp.description,raw.description,sizeof(tmp.description)); strlcpy(tmp.iconCode,raw.iconCode,sizeof(tmp.iconCode));
  strlcpy(tmp.timeStr,raw.timeStr,sizeof(tmp.timeStr)); strlcpy(tmp.timezone,raw.timezone,sizeof(tmp.timezone));
  tmp.tempC=raw.tempC; tmp.feelsLike=raw.feelsLike; tmp.humidity=raw.humidity; tmp.pressure=raw.pressure;
  tmp.windSpeed=raw.windSpeed; tmp.clouds=raw.clouds; tmp.valid=true;
  if (xSemaphoreTake(overrideMutex,pdMS_TO_TICKS(500))==pdTRUE) {
    overrideData=tmp;
    overrideEndMs=millis()+OVERRIDE_DURATION_MS;
    displayState=DISP_OVERRIDE;
    xSemaphoreGive(overrideMutex);
  }
  return true;
}

float computeHeatIndex(float tempC, float rh) {
  float T=tempC*9.0f/5.0f+32.0f, HI;
  if (T<80.0f) { HI=0.5f*(T+61.0f+(T-68.0f)*1.2f+rh*0.094f); }
  else {
    HI=-42.379f+2.04901523f*T+10.14333127f*rh-0.22475541f*T*rh-0.00683783f*T*T
       -0.05481717f*rh*rh+0.00122874f*T*T*rh+0.00085282f*T*rh*rh-0.00000199f*T*T*rh*rh;
    if (rh<13.0f&&T>=80.0f&&T<=112.0f) HI-=(13.0f-rh)/4.0f*sqrtf((17.0f-fabsf(T-95.0f))/17.0f);
    if (rh>85.0f&&T>=80.0f&&T<=87.0f) HI+=(rh-85.0f)/10.0f*((87.0f-T)/5.0f);
  }
  return (HI-32.0f)*5.0f/9.0f;
}
float computeDewPoint(float tempC, float rh) {
  const float a=17.27f,b=237.7f;
  float alpha=(a*tempC/(b+tempC))+logf(rh/100.0f);
  return (b*alpha)/(a-alpha);
}
float pressureTrendFromBuffer() {
  uint8_t n=pressBufFull?PRESS_BUF_SIZE:pressBufIdx;
  if (n<2) return 0.0f;
  float sumX=0,sumY=0,sumXY=0,sumXX=0;
  for (uint8_t i=0;i<n;i++) {
    uint8_t ri=pressBufFull?(pressBufIdx+i)%PRESS_BUF_SIZE:i;
    float x=(float)i*(SENSOR_READ_MS/60000.0f),y=pressBuf[ri];
    sumX+=x;sumY+=y;sumXY+=x*y;sumXX+=x*x;
  }
  float denom=n*sumXX-sumX*sumX;
  if (fabsf(denom)<1e-6f) return 0.0f;
  return (n*sumXY-sumX*sumY)/denom;
}
float computeRainProbability(float rh,float pressHPa,float trendPerMin,float dew,float tempC) {
  float score=0.0f;
  if (pressHPa<1000.0f) score+=30.0f; else if (pressHPa<1005.0f) score+=20.0f; else if (pressHPa<1010.0f) score+=10.0f;
  if (trendPerMin<-0.5f) score+=25.0f; else if (trendPerMin<-0.2f) score+=12.0f;
  if (rh>90.0f) score+=25.0f; else if (rh>80.0f) score+=15.0f; else if (rh>70.0f) score+=7.0f;
  float dewGap=tempC-dew;
  if (dewGap<1.0f) score+=20.0f; else if (dewGap<3.0f) score+=12.0f; else if (dewGap<5.0f) score+=5.0f;
  return constrain(score,0.0f,100.0f);
}
const char* weatherClassification(float prob) {
  if (prob>=70.0f) return "Rain Likely"; if (prob>=50.0f) return "Unstable";
  if (prob>=35.0f) return "Cloudy";      if (prob>=20.0f) return "Humid"; return "Clear";
}

float medianOf(float* arr, uint8_t n) {
  float tmp[8]; for (uint8_t i=0;i<n;i++) tmp[i]=arr[i];
  for (uint8_t i=1;i<n;i++) { float key=tmp[i]; int8_t j=i-1; while(j>=0&&tmp[j]>key){tmp[j+1]=tmp[j];j--;} tmp[j+1]=key; }
  return (n%2==1)?tmp[n/2]:(tmp[n/2-1]+tmp[n/2])/2.0f;
}
float movingAverage(float* ring, uint8_t cap, bool full, uint8_t idx) {
  uint8_t n=full?cap:idx; if (n==0) return 0.0f;
  float sum=0; for (uint8_t i=0;i<n;i++) sum+=ring[i]; return sum/(float)n;
}
uint8_t computeConfidence(uint16_t retriesThisCycle, uint16_t consecFails, bool rejectedSpike, bool gotFreshSample) {
  if (!gotFreshSample) return 0;
  int score=100; score-=retriesThisCycle*8; score-=consecFails*5;
  if (rejectedSpike) score-=15; return (uint8_t)constrain(score,0,100);
}
SensorHealth healthFromFails(uint16_t consecFails, bool hasLastGood, bool gotFreshSample) {
  if (gotFreshSample) return HEALTH_ONLINE;
  if (!hasLastGood) return HEALTH_ERROR;
  if (consecFails>=SENSOR_ERROR_FAILS) return HEALTH_ERROR;
  return HEALTH_WARNING;
}

void logEvent(EventLevel level, const char* fmt, ...) {
  char buf[64];
  va_list args; va_start(args,fmt); vsnprintf(buf,sizeof(buf),fmt,args); va_end(args);
  if (xSemaphoreTake(eventMutex,pdMS_TO_TICKS(200))==pdTRUE) {
    EventEntry& e=eventLog[eventHead];
    e.ms=millis(); e.level=level; strlcpy(e.msg,buf,sizeof(e.msg));
    eventHead=(eventHead+1)%EVENT_LOG_SIZE;
    if (eventCount<EVENT_LOG_SIZE) eventCount++;
    eventSeq++;
    xSemaphoreGive(eventMutex);
  }
  const char* tag=(level==EVT_ERROR)?"ERROR":(level==EVT_WARN?"WARN":"INFO");
  Serial.printf("[%s] %s\n",tag,buf);
  
  StaticJsonDocument<160> doc;
  doc["type"]="event"; doc["level"]=tag; doc["msg"]=buf; doc["ms"]=millis();
  String out; serializeJson(doc,out);
  wsTextAll(out);
}

void safeRestart(const char* reason) {
  Serial.printf("[FATAL] %s — restarting in 5s...\n",reason);
  logEvent(EVT_ERROR,"FATAL: %s — restarting",reason);
  if (oledOK) {
    display.clearDisplay(); display.setTextColor(SSD1306_WHITE);
    display.drawRect(0,0,SCREEN_W,SCREEN_H,SSD1306_WHITE);
    printCentered("SYSTEM ERROR",14,1); printCentered(reason,28,1); printCentered("Restarting...",44,1);
    display.display();
  }
  delay(5000); esp_restart();
}

void printCentered(const char* text, int16_t y, uint8_t sz) {
  display.setTextSize(sz);
  int16_t x1,y1; uint16_t w,h;
  display.getTextBounds(text,0,y,&x1,&y1,&w,&h);
  display.setCursor((SCREEN_W-(int16_t)w)/2,y); display.print(text);
}

void drawTitleBar(const char* title) {
  display.fillRect(0,0,SCREEN_W,11,SSD1306_WHITE);
  display.setTextSize(1); display.setTextColor(SSD1306_BLACK);
  char truncTitle[20]; snprintf(truncTitle,sizeof(truncTitle),">%.16s",title);
  int16_t x1,y1; uint16_t w,h;
  display.getTextBounds(truncTitle,0,2,&x1,&y1,&w,&h);
  display.setCursor((SCREEN_W-16-(int16_t)w)/2,2); display.print(truncTitle);
  int rssi=WiFi.RSSI();
  uint8_t bars=(WiFi.status()==WL_CONNECTED)?(rssi>-55?4:rssi>-65?3:rssi>-75?2:1):0;
  for (uint8_t i=0;i<4;i++) {
    uint8_t barH=2+i*2,bx=SCREEN_W-14+i*3,by=1;
    if (i<bars) display.fillRect(bx,by+(8-barH),2,barH,SSD1306_BLACK);
    else        display.drawRect(bx,by+(8-barH),2,barH,SSD1306_BLACK);
  }
  display.setTextColor(SSD1306_WHITE);
}

void drawCornerTicks() {
  display.drawFastHLine(0,63,4,SSD1306_WHITE); display.drawFastVLine(0,60,4,SSD1306_WHITE);
  display.drawFastHLine(SCREEN_W-4,63,4,SSD1306_WHITE); display.drawFastVLine(SCREEN_W-1,60,4,SSD1306_WHITE);
}

void drawPageDots(Page p) {
  const uint8_t spacing=10;
  uint8_t startX=(SCREEN_W-(TOTAL_PAGES*spacing-2))/2;
  for (uint8_t i=0;i<TOTAL_PAGES;i++) {
    uint8_t cx=startX+i*spacing;
    if ((Page)i==p) display.fillCircle(cx,62,2,SSD1306_WHITE);
    else            display.drawCircle(cx,62,2,SSD1306_WHITE);
  }
  drawCornerTicks();
}

void showSplash(const char* line1, const char* line2) {
  if (!oledOK) return;
  display.clearDisplay(); display.setTextColor(SSD1306_WHITE);
  display.drawRect(0,0,SCREEN_W,SCREEN_H,SSD1306_WHITE);
  display.drawRect(2,2,SCREEN_W-4,SCREEN_H-4,SSD1306_WHITE);
  printCentered("WEATHER STATION",8,1);
  display.drawLine(8,20,SCREEN_W-8,20,SSD1306_WHITE);
  printCentered(line1,27,1);
  if (line2&&strlen(line2)>0) printCentered(line2,40,1);
  display.display();
}

void bootProgressStep(const char* label, uint8_t pct) {
  if (!oledOK) return;
  display.clearDisplay();
  display.drawRect(0,0,SCREEN_W,SCREEN_H,SSD1306_WHITE);
  display.drawRect(2,2,SCREEN_W-4,SCREEN_H-4,SSD1306_WHITE);
  printCentered("ADITYA SYSTEM",6,1);
  display.drawLine(10,17,SCREEN_W-10,17,SSD1306_WHITE);
  printCentered(label,27,1);
  display.drawRect(10,42,108,10,SSD1306_WHITE);
  
  uint8_t fillW=(uint8_t)((106UL*pct)/100UL);
  display.fillRect(12,44,fillW,6,SSD1306_WHITE);
  char vbuf[12]; snprintf(vbuf,sizeof(vbuf),"FW v%s",FIRMWARE_VERSION);
  printCentered(vbuf,55,1); display.display();
}

void showBootSequence() {
  if (!oledOK) return;
  const char* finalText="ADITYA SYSTEM";
  uint8_t txtLen=strlen(finalText);
  char scramble[16];
  static const char glitchChars[]="!@#$%^&*<>/\\|01XZQ#";
  uint8_t glitchLen=strlen(glitchChars);
  for (uint8_t pass=0;pass<3;pass++) {
    for (uint8_t i=0;i<txtLen;i++)
      scramble[i]=(finalText[i]==' '||pass==2)?finalText[i]:glitchChars[random(0,glitchLen)];
    scramble[txtLen]='\0';
    display.clearDisplay(); display.drawRect(0,0,SCREEN_W,SCREEN_H,SSD1306_WHITE);
    printCentered(scramble,26,1); display.display(); delay(90);
  }
  for (uint8_t i=0;i<4;i++) {
    display.clearDisplay(); display.drawRect(0,0,SCREEN_W,SCREEN_H,SSD1306_WHITE);
    printCentered("ADITYA SYSTEM",26,1); display.invertDisplay(i%2==0); display.display(); delay(110);
  }
  display.invertDisplay(false);
  static const char* stages[5]={"INITIALIZING...","CHECKING SENSORS...","CHECKING MEMORY...","CHECKING WIFI...","LOADING DASHBOARD..."};
  for (uint8_t i=0;i<5;i++) { bootProgressStep(stages[i],(uint8_t)((i+1)*100/5)); delay(240); }
  display.clearDisplay(); display.drawRect(0,0,SCREEN_W,SCREEN_H,SSD1306_WHITE);
  display.drawRect(2,2,SCREEN_W-4,SCREEN_H-4,SSD1306_WHITE);
  printCentered("SYSTEM READY",28,1); display.display(); delay(450);
}

void slideTransition(Page , Page to) {
  if (!oledOK) { currentPage=to; return; }
  static GFXcanvas1 oldCanvas(SCREEN_W,SCREEN_H);
  static GFXcanvas1 newCanvas(SCREEN_W,SCREEN_H);
  for (int16_t y=0;y<SCREEN_H;y++) for (int16_t x=0;x<SCREEN_W;x++) oldCanvas.drawPixel(x,y,display.getPixel(x,y)?1:0);
  drawLocalPageOnly(to);
  for (int16_t y=0;y<SCREEN_H;y++) for (int16_t x=0;x<SCREEN_W;x++) newCanvas.drawPixel(x,y,display.getPixel(x,y)?1:0);
  for (int16_t y=0;y<SCREEN_H;y++) for (int16_t x=0;x<SCREEN_W;x++) display.drawPixel(x,y,oldCanvas.getPixel(x,y)?SSD1306_WHITE:SSD1306_BLACK);
  const uint8_t STEPS=8; int16_t prevWipeX=0;
  for (uint8_t step=1;step<=STEPS;step++) {
    int16_t wipeX=(int16_t)((int32_t)SCREEN_W*step/STEPS);
    for (int16_t x=prevWipeX;x<wipeX;x++) for (int16_t y=0;y<SCREEN_H;y++)
      display.drawPixel(x,y,newCanvas.getPixel(x,y)?SSD1306_WHITE:SSD1306_BLACK);
    prevWipeX=wipeX; display.display(); vTaskDelay(pdMS_TO_TICKS(18));
  }
  currentPage=to;
}

void drawWeatherPage() {
  SensorData s; xSemaphoreTake(dataMutex,portMAX_DELAY); s=sd; xSemaphoreGive(dataMutex);
  display.clearDisplay(); drawTitleBar("WEATHER"); char buf[22]; display.setTextSize(1);
  display.drawFastVLine(64,12,32,SSD1306_WHITE);
  display.setCursor(0,14); display.print("T:");
  if (s.dhtOK) snprintf(buf,sizeof(buf),"%.1f\xF7""C",s.dhtTemp); else strcpy(buf,"--.-\xF7""C");
  display.print(buf);
  display.setCursor(68,14); display.print("H:");
  if (s.dhtOK) snprintf(buf,sizeof(buf),"%.0f%%",s.dhtHumidity); else strcpy(buf,"--%");
  display.print(buf);
  display.setCursor(0,24); display.print("P:");
  if (s.bmeOK) snprintf(buf,sizeof(buf),"%.1fhPa",s.bmePressure); else strcpy(buf,"----hPa");
  display.print(buf);
  if (s.bmeOK) { display.setCursor(114,24); display.print(s.pressureTrend>0.1f?"\x18":s.pressureTrend<-0.1f?"\x19":"~"); }
  display.setCursor(0,34); display.print("D:");
  if (s.dhtOK) snprintf(buf,sizeof(buf),"%.1f\xF7""C",s.dewPoint); else strcpy(buf,"--.-\xF7""C");
  display.print(buf);
  display.setCursor(68,34); display.print("HI:");
  if (s.dhtOK) snprintf(buf,sizeof(buf),"%.1f\xF7""C",s.heatIndex); else strcpy(buf,"--.-");
  display.print(buf);
  char rainBuf[22];
  if (s.dhtOK&&s.bmeOK) snprintf(rainBuf,sizeof(rainBuf),"%s %d%%",weatherClassification(s.rainProbPct),(int)roundf(s.rainProbPct));
  else strcpy(rainBuf,"Sensor Error");
  display.fillRect(0,46,SCREEN_W,12,SSD1306_WHITE);
  display.setTextColor(SSD1306_BLACK); printCentered(rainBuf,48,1); display.setTextColor(SSD1306_WHITE);
  drawPageDots(PAGE_WEATHER);
}

void drawTimePage() {
  display.clearDisplay(); drawTitleBar("TIME & DAY");
  struct tm ti; bool gotTime=getLocalTime(&ti); char buf[16];
  snprintf(buf,sizeof(buf),gotTime?"%02d:%02d":"--:--",ti.tm_hour,ti.tm_min);
  display.setTextSize(3);
  int16_t x1,y1; uint16_t w,h;
  display.getTextBounds(buf,0,0,&x1,&y1,&w,&h);
  int16_t cx=(SCREEN_W-(int16_t)w)/2;
  display.setCursor(cx,13); display.print(buf);
  if (gotTime) { snprintf(buf,sizeof(buf),":%02d",ti.tm_sec); display.setTextSize(1); display.setCursor(cx+(int16_t)w+1,27); display.print(buf); }
  display.setTextSize(1);
  if (gotTime) printCentered(DAY_NAMES[ti.tm_wday],48,1); else printCentered("---",48,1);
  drawPageDots(PAGE_TIME);
}

void drawCalendarPage() {
  display.clearDisplay(); drawTitleBar("CALENDAR");
  struct tm ti; bool gotTime=getLocalTime(&ti); char buf[8];
  snprintf(buf,sizeof(buf),gotTime?"%02d":"--",ti.tm_mday);
  printCentered(buf,13,3); display.setTextSize(1);
  if (gotTime) { printCentered(MONTH_FULL[ti.tm_mon+1],41,1); snprintf(buf,sizeof(buf),"%d",ti.tm_year+1900); printCentered(buf,51,1); }
  else { printCentered("---",43,1); printCentered("----",54,1); }
  drawPageDots(PAGE_CALENDAR);
}

void drawExtraPage() {
  SensorData s; xSemaphoreTake(dataMutex,portMAX_DELAY); s=sd; xSemaphoreGive(dataMutex);
  display.clearDisplay(); drawTitleBar("ADVANCED"); char buf[22]; display.setTextSize(1);
  display.drawFastVLine(64,12,36,SSD1306_WHITE);
  display.setCursor(0,14); display.print("Out:");
  if (s.ds18OK) { snprintf(buf,sizeof(buf),"%.1f\xF7""C",s.ds18Temp[0]); } else { strcpy(buf,"--.-\xF7""C"); }
  display.print(buf);
  
  display.setCursor(68,14); display.print("Alt:");
  if (s.bmeOK) { snprintf(buf,sizeof(buf),"%.0fm",s.altitudeM); } else { strcpy(buf,"--m"); }
  display.print(buf);
  display.setCursor(0,24); display.print("Lux:");
  if (s.bh1750OK) {
    if (s.lux<1000.0f) snprintf(buf,sizeof(buf),"%.0f",s.lux); else snprintf(buf,sizeof(buf),"%.1fk",s.lux/1000.0f);
    display.print(buf); display.setCursor(68,24);
    if (s.lux>=10000.0f) display.print("Bright Sun"); else if (s.lux>=1000.0f) display.print("Daylight");
    else if (s.lux>=100.0f) display.print("Overcast"); else if (s.lux>=10.0f) display.print("Twilight"); else display.print("Night");
  } else { display.print("---- lx"); }
  display.setCursor(0,34); display.print("BME:");
  if (s.bmeOK) { snprintf(buf,sizeof(buf),"%.1f\xF7""C",s.bmeTemp); display.print(buf); display.setCursor(68,34); snprintf(buf,sizeof(buf),"%.0f%%RH",s.bmeHumidity); display.print(buf); }
  else { display.print("--.-\xF7""C"); }
  display.setCursor(0,44); display.print("Trend:");
  if (s.bmeOK) { snprintf(buf,sizeof(buf),"%+.2fhPa/m",s.pressureTrend); display.print(buf); } else { display.print("N/A"); }
  drawPageDots(PAGE_EXTRA);
}

void drawSystemStatusPage() {
  display.clearDisplay(); drawTitleBar("SYSTEM"); char buf[24]; display.setTextSize(1);
  display.drawFastVLine(64,12,32,SSD1306_WHITE);
  uint32_t freeHeap=esp_get_free_heap_size();
  uint32_t largestBlock=(uint32_t)heap_caps_get_largest_free_block(MALLOC_CAP_8BIT);
  uint8_t fragPct=(freeHeap>0)?(uint8_t)(100UL-(100UL*largestBlock)/freeHeap):0;
  display.setCursor(0,14); display.print("Heap:"); snprintf(buf,sizeof(buf),"%lu KB",(unsigned long)(freeHeap/1024)); display.print(buf);
  display.setCursor(68,14); display.print("Frag:"); snprintf(buf,sizeof(buf),"%u%%",fragPct); display.print(buf);
  display.setCursor(0,24); display.print("RSSI:");
  if (WiFi.status()==WL_CONNECTED) { snprintf(buf,sizeof(buf),"%ddBm",WiFi.RSSI()); } else { strcpy(buf,"-- (off)"); }
  display.print(buf);
  display.setCursor(68,24); display.print("IP:");
  if (WiFi.status()==WL_CONNECTED) { IPAddress ip=WiFi.localIP(); snprintf(buf,sizeof(buf),".%d",ip[3]); } else { strcpy(buf,"--"); }
  display.print(buf);
  uint32_t upS=millis()/1000UL, upD=upS/86400UL, upH=(upS%86400UL)/3600UL, upM=(upS%3600UL)/60UL, upSec=upS%60UL;
  display.setCursor(0,34); display.print("Up:");
  if (upD>0) snprintf(buf,sizeof(buf),"%lud %02lu:%02lu",(unsigned long)upD,(unsigned long)upH,(unsigned long)upM);
  else       snprintf(buf,sizeof(buf),"%02lu:%02lu:%02lu",(unsigned long)upH,(unsigned long)upM,(unsigned long)upSec);
  display.print(buf);
  display.setCursor(0,44); display.print("FW: v"); display.print(FIRMWARE_VERSION);
  drawPageDots(PAGE_SYSTEM);
}

void drawSensorDiagPage() {
  SensorData s; xSemaphoreTake(dataMutex,portMAX_DELAY); s=sd; xSemaphoreGive(dataMutex);
  display.clearDisplay(); drawTitleBar("SENSOR DIAG"); display.setTextSize(1); char buf[24];
  struct Row { const char* name; uint8_t conf; SensorHealth h; uint16_t errs; };
  Row rows[4]={{"DHT22",s.dhtConfidence,s.dhtHealth,s.dhtErrors},{"BME280",s.bmeConfidence,s.bmeHealth,s.bmeErrors},{"BH1750",s.bhConfidence,s.bhHealth,s.bhErrors},{"DS18B20",s.dsConfidence,s.dsHealth,s.dsErrors}};
  for (uint8_t i=0;i<4;i++) {
    int16_t y=14+i*10;
    
    const char* hAbbr=(rows[i].h==HEALTH_ONLINE)?"OK  ":(rows[i].h==HEALTH_WARNING)?"WARN":"ERR ";
    uint16_t ec=(rows[i].errs>99)?99:rows[i].errs;
    display.setCursor(0,y);   snprintf(buf,sizeof(buf),"%-6s",rows[i].name);   display.print(buf);
    display.setCursor(40,y);  snprintf(buf,sizeof(buf),"%3u%%",rows[i].conf);  display.print(buf);
    display.setCursor(67,y);  display.print(hAbbr);
    display.setCursor(100,y); snprintf(buf,sizeof(buf),"E%02u",(unsigned)ec);  display.print(buf);
  }
  drawPageDots(PAGE_SENSOR_DIAG);
}

bool drawOverridePage() {
  OverrideData od;
  xSemaphoreTake(overrideMutex,portMAX_DELAY); od=overrideData; xSemaphoreGive(overrideMutex);
  if (!od.valid) { displayState=DISP_LOCAL; renderCurrentPage(); return false; }
  uint32_t remaining=0, now=millis();
  if (overrideEndMs>now) remaining=(overrideEndMs-now)/1000;
  display.clearDisplay();
  display.fillRect(0,0,SCREEN_W,11,SSD1306_WHITE);
  display.setTextSize(1); display.setTextColor(SSD1306_BLACK);
  char titleBuf[20]; snprintf(titleBuf,sizeof(titleBuf),"%.13s",od.city);
  display.setCursor(2,2); display.print(titleBuf);
  char cdBuf[5]; snprintf(cdBuf,sizeof(cdBuf),"%us",(unsigned)remaining);
  display.setCursor(SCREEN_W-(strlen(cdBuf)*6)-2,2); display.print(cdBuf);
  display.setTextColor(SSD1306_WHITE); char buf[22]; display.setTextSize(1);
  display.setCursor(0,14); snprintf(buf,sizeof(buf),"T:%.1f\xF7""C",od.tempC); display.print(buf);
  display.setCursor(68,14); snprintf(buf,sizeof(buf),"FL:%.1f\xF7""C",od.feelsLike); display.print(buf);
  display.setCursor(0,24); snprintf(buf,sizeof(buf),"H:%.0f%%",od.humidity); display.print(buf);
  display.setCursor(68,24); snprintf(buf,sizeof(buf),"P:%.0fhPa",od.pressure); display.print(buf);
  display.setCursor(0,34); snprintf(buf,sizeof(buf),"W:%.1fm/s",od.windSpeed); display.print(buf);
  display.setCursor(68,34); snprintf(buf,sizeof(buf),"Cld:%d%%",od.clouds); display.print(buf);
  display.setCursor(0,44); char descBuf[18]; snprintf(descBuf,sizeof(descBuf),"%.17s",od.description); display.print(descBuf);
  display.setCursor(SCREEN_W-6*8,54); display.print(od.timeStr);
  display.setCursor(0,54); display.print("API");
  return true;
}

void drawLocalPageOnly(Page p) {
  switch(p) {
    case PAGE_WEATHER:     drawWeatherPage();      break;
    case PAGE_TIME:        drawTimePage();         break;
    case PAGE_CALENDAR:    drawCalendarPage();     break;
    case PAGE_EXTRA:       drawExtraPage();        break;
    case PAGE_SYSTEM:      drawSystemStatusPage(); break;
    case PAGE_SENSOR_DIAG: drawSensorDiagPage();   break;
    default: break;
  }
}

void renderCurrentPage() {
  if (!oledOK) return;
  if (displayState==DISP_OVERRIDE) {
    if (millis()>=overrideEndMs) { displayState=DISP_LOCAL; }
    else { if (drawOverridePage()) display.display(); return; }
  }
  drawLocalPageOnly(currentPage); display.display();
}

void nextPage() {
  if (displayState==DISP_OVERRIDE) displayState=DISP_LOCAL;
  targetPage=(Page)((currentPage+1)%TOTAL_PAGES);
  pageChangeReq=true; lastAutoCycleMs=millis();
}

void sensorTask(void* pvParam) {
  esp_task_wdt_add(NULL);
  static uint32_t lastLogMs=0;
  static time_t    lastNtpSync=0;
  static uint16_t dhtConsecFails=0, bmeConsecFails=0, bhConsecFails=0, dsConsecFails=0;
  
  static uint8_t  dhtCycleStreak=0, bmeCycleStreak=0, bhCycleStreak=0, dsCycleStreak=0;
  static float    dhtRawHist[3]={0,0,0}; static uint8_t dhtRawN=0;
  static float    dhtRawHumHist[3]={0,0,0};

  for (;;) {
    esp_task_wdt_reset();
    hbSensor = millis();

    if (safeModeActive) { vTaskDelay(pdMS_TO_TICKS(SENSOR_READ_MS)); continue; }

    uint16_t dhtRetries=0, bmeRetries=0, bhRetries=0, dsRetries=0;

    float t=dht.readTemperature(), h=dht.readHumidity();
    bool dhtRead=!isnan(t)&&!isnan(h)&&t>-40.0f&&t<80.0f&&h>=0.0f&&h<=100.0f;
    if (!dhtRead) {
      for (uint8_t r=0;r<2&&!dhtRead;r++) { dhtRetries++; vTaskDelay(pdMS_TO_TICKS(300)); dht.read(true); t=dht.readTemperature(); h=dht.readHumidity(); dhtRead=!isnan(t)&&!isnan(h)&&t>-40.0f&&t<80.0f&&h>=0.0f&&h<=100.0f; }
    }
    bool dhtSpike=false;
    if (dhtRead&&dhtHasLastGood) {
      if (fabsf(t-dhtLastGoodT)>DHT_TEMP_JUMP_MAX||fabsf(h-dhtLastGoodH)>DHT_HUM_JUMP_MAX) {
        dhtCycleStreak++;
        if (dhtCycleStreak<2) { dhtSpike=true; dhtRead=false; } else dhtCycleStreak=0;
      } else dhtCycleStreak=0;
    }
    bool dhtValid=false;
    if (dhtRead) {
      dhtConsecFails=0; dhtHasLastGood=true; dhtLastGoodT=t; dhtLastGoodH=h;
      if (dhtRawN<3) { dhtRawHist[dhtRawN]=t; dhtRawHumHist[dhtRawN]=h; dhtRawN++; }
      else { dhtRawHist[0]=dhtRawHist[1]; dhtRawHist[1]=dhtRawHist[2]; dhtRawHist[2]=t; dhtRawHumHist[0]=dhtRawHumHist[1]; dhtRawHumHist[1]=dhtRawHumHist[2]; dhtRawHumHist[2]=h; }
      float medT=(dhtRawN>=2)?medianOf(dhtRawHist,dhtRawN):t;
      float medH=(dhtRawN>=2)?medianOf(dhtRawHumHist,dhtRawN):h;
      dhtTempSamples[dhtMaIdx]=medT; dhtHumSamples[dhtMaIdx]=medH;
      dhtMaIdx=(dhtMaIdx+1)%MA_WINDOW; if (dhtMaIdx==0) dhtMaFull=true;
      dhtValid=true;
    } else {
      dhtConsecFails++;
      if (dhtConsecFails>5) {
        dht.begin(); dhtConsecFails=0;
        dhtHasLastGood=false;
        logEvent(EVT_WARN,"DHT22 re-init");
      }
      if (dhtConsecFails==SENSOR_WARN_FAILS)  logEvent(EVT_WARN,"DHT22 degraded");
      if (dhtConsecFails==SENSOR_ERROR_FAILS) logEvent(EVT_ERROR,"DHT22 offline");
    }

    float bmeTSamp[BME_RETRY_MAX],bmePSamp[BME_RETRY_MAX],bmeHSamp[BME_RETRY_MAX];
    uint8_t bmeN=0;
    uint8_t bmeIntraStreak=0;
    for (uint8_t i=0;i<BME_RETRY_MAX;i++) {
      bme.takeForcedMeasurement();
      float bT2=bme.readTemperature(),bP2=bme.readPressure()/100.0f,bH2=bme.readHumidity();
      bool ok=(bP2>850.0f&&bP2<1100.0f&&!isnan(bT2)&&!isnan(bH2));
      if (ok&&bmeHasLastGood) {
        if (fabsf(bP2-bmeLastGoodP)>BME_PRESS_JUMP_MAX) {
          bmeIntraStreak++;
          
          if (bmeIntraStreak<2 && bmeCycleStreak<1) { ok=false; } else { bmeCycleStreak=0; bmeIntraStreak=0; }
        } else { bmeIntraStreak=0; bmeCycleStreak=0; }
      }
      if (ok) { bmeTSamp[bmeN]=bT2; bmePSamp[bmeN]=bP2; bmeHSamp[bmeN]=bH2; bmeN++; } else bmeRetries++;
      if (i<BME_RETRY_MAX-1) vTaskDelay(pdMS_TO_TICKS(40));
    }
    
    if (bmeN==0 && bmeHasLastGood) bmeCycleStreak++;
    else if (bmeN>0) bmeCycleStreak=0;

    bool bmeValid=(bmeN>0); float bT=0,bP=0,bH=0,alt=0;
    if (bmeValid) {
      bmeConsecFails=0; i2cBmeFailStreak=0;
      float medT=medianOf(bmeTSamp,bmeN),medP=medianOf(bmePSamp,bmeN),medH=medianOf(bmeHSamp,bmeN);
      bmeTempSamples[bmeMaIdx]=medT; bmePressSamples[bmeMaIdx]=medP; bmeHumSamples[bmeMaIdx]=medH;
      bmeMaIdx=(bmeMaIdx+1)%MA_WINDOW; if (bmeMaIdx==0) bmeMaFull=true;
      bT=movingAverage(bmeTempSamples,MA_WINDOW,bmeMaFull,bmeMaIdx);
      bP=movingAverage(bmePressSamples,MA_WINDOW,bmeMaFull,bmeMaIdx);
      bH=movingAverage(bmeHumSamples,MA_WINDOW,bmeMaFull,bmeMaIdx);
      alt=44330.0f*(1.0f-powf(bP/SEA_LEVEL_HPA,0.1903f));
      bmeHasLastGood=true; bmeLastGoodP=medP;
      pressBuf[pressBufIdx]=bP; pressBufTime[pressBufIdx]=millis();
      pressBufIdx=(pressBufIdx+1)%PRESS_BUF_SIZE; if (pressBufIdx==0) pressBufFull=true;
    } else {
      bmeConsecFails++;
      i2cBmeFailStreak++;
      if (bmeConsecFails>5) {
        
        if (i2cBmeFailStreak >= I2C_RECOVERY_AFTER_FAILS) i2cBusRecover();
        bme.begin(BME_ADDR);
        bme.setSampling(Adafruit_BME280::MODE_FORCED,Adafruit_BME280::SAMPLING_X2,Adafruit_BME280::SAMPLING_X2,Adafruit_BME280::SAMPLING_X2,Adafruit_BME280::FILTER_X2);
        bmeConsecFails=0;
        bmeHasLastGood=false; bmeCycleStreak=0;
        logEvent(EVT_WARN,"BME280 re-init");
      }
      if (bmeConsecFails==SENSOR_WARN_FAILS)  logEvent(EVT_WARN,"BME280 degraded");
      if (bmeConsecFails==SENSOR_ERROR_FAILS) logEvent(EVT_ERROR,"BME280 offline");
    }

    float luxSamp[3]; uint8_t luxN=0;
    for (uint8_t i=0;i<3;i++) {
      float l=lightMeter.readLightLevel(); bool ok=(l>=0.0f&&l<200000.0f);
      if (ok&&bhHasLastGood&&bhLastGoodLux>5.0f) {
        float ratio=(l>bhLastGoodLux)?l/bhLastGoodLux:bhLastGoodLux/l;
        if (ratio>LUX_JUMP_RATIO&&fabsf(l-bhLastGoodLux)>200.0f) {
          bhCycleStreak++;
          if (bhCycleStreak<2) ok=false; else bhCycleStreak=0;
        } else bhCycleStreak=0;
      }
      if (ok) { luxSamp[luxN]=l; luxN++; } else bhRetries++;
      vTaskDelay(pdMS_TO_TICKS(20));
    }
    bool bh1Valid=(luxN>0); float lux=0.0f;
    if (bh1Valid) {
      bhConsecFails=0; bhCycleStreak=0; i2cBhFailStreak=0;
      float medL=medianOf(luxSamp,luxN);
      luxSamples[luxMaIdx]=medL; luxMaIdx=(luxMaIdx+1)%MA_WINDOW; if (luxMaIdx==0) luxMaFull=true;
      lux=movingAverage(luxSamples,MA_WINDOW,luxMaFull,luxMaIdx);
      bhHasLastGood=true; bhLastGoodLux=medL;
    } else {
      bhConsecFails++;
      i2cBhFailStreak++;
      if (bhConsecFails>5) {
        
        if (i2cBhFailStreak >= I2C_RECOVERY_AFTER_FAILS) i2cBusRecover();
        lightMeter.begin(BH1750::CONTINUOUS_HIGH_RES_MODE,BH_ADDR,&Wire);
        bhConsecFails=0;
        bhHasLastGood=false; bhCycleStreak=0;
        logEvent(EVT_WARN,"BH1750 re-init");
      }
      if (bhConsecFails==SENSOR_WARN_FAILS)  logEvent(EVT_WARN,"BH1750 degraded");
      if (bhConsecFails==SENSOR_ERROR_FAILS) logEvent(EVT_ERROR,"BH1750 offline");
    }

    float dsSamp[DS18_SAMPLE_COUNT]; uint8_t dsN=0, dsCount=0;
    for (uint8_t i=0;i<DS18_SAMPLE_COUNT;i++) {
      ds18b20.requestTemperatures(); dsCount=ds18b20.getDeviceCount();
      if (dsCount==0) { dsRetries++; vTaskDelay(pdMS_TO_TICKS(50)); continue; }
      float v=ds18b20.getTempCByIndex(0);
      bool ok=(v!=DEVICE_DISCONNECTED_C)&&v>-55.0f&&v<125.0f;
      if (ok&&dsHasLastGood) {
        if (fabsf(v-dsLastGoodT)>DS18_JUMP_MAX) {
          dsCycleStreak++;
          if (dsCycleStreak<2) ok=false; else dsCycleStreak=0;
        } else dsCycleStreak=0;
      }
      if (ok) { dsSamp[dsN]=v; dsN++; } else dsRetries++;
    }
    bool dsValid=(dsN>0); float ds0=0.0f,ds1_=0.0f;
    if (dsValid) {
      dsConsecFails=0; dsCycleStreak=0;
      float medD=medianOf(dsSamp,dsN);
      dsSamples[dsMaIdx]=medD; dsMaIdx=(dsMaIdx+1)%MA_WINDOW; if (dsMaIdx==0) dsMaFull=true;
      ds0=movingAverage(dsSamples,MA_WINDOW,dsMaFull,dsMaIdx);
      if (dsCount>1) ds1_=ds18b20.getTempCByIndex(1);
      dsHasLastGood=true; dsLastGoodT=medD;
    } else {
      dsConsecFails++;
      if (dsConsecFails>5) {
        ds18b20.begin(); dsConsecFails=0;
        dsHasLastGood=false; dsCycleStreak=0;
        logEvent(EVT_WARN,"DS18B20 re-init");
      }
      if (dsConsecFails==SENSOR_WARN_FAILS)  logEvent(EVT_WARN,"DS18B20 degraded");
      if (dsConsecFails==SENSOR_ERROR_FAILS) logEvent(EVT_ERROR,"DS18B20 offline");
    }

    float trend=pressureTrendFromBuffer();

    xSemaphoreTake(dataMutex,portMAX_DELAY);
    float finalDhtT=0,finalDhtH=0;
    if (dhtValid) {
      finalDhtH=movingAverage(dhtHumSamples,MA_WINDOW,dhtMaFull,dhtMaIdx);
      finalDhtT=movingAverage(dhtTempSamples,MA_WINDOW,dhtMaFull,dhtMaIdx);
      sd.dhtTemp=finalDhtT; sd.dhtHumidity=finalDhtH;
      sd.heatIndex=computeHeatIndex(finalDhtT,finalDhtH); sd.dewPoint=computeDewPoint(finalDhtT,finalDhtH); sd.dhtOK=true;
    } else { sd.dhtOK=false; }
    if (bmeValid) { sd.bmeTemp=bT; sd.bmePressure=bP; sd.bmeHumidity=bH; sd.altitudeM=alt; sd.pressureTrend=trend; sd.bmeOK=true; }
    else { sd.bmeOK=false; }
    if (bh1Valid) { sd.lux=lux; sd.bh1750OK=true; } else { sd.bh1750OK=false; }
    if (dsValid) {
      sd.ds18Temp[0]=ds0; sd.ds18Temp[1]=(dsCount>1)?ds1_:0.0f; sd.ds18Count=dsCount;
      if (!sd.ds18MinMaxInit) { sd.ds18Min=ds0; sd.ds18Max=ds0; sd.ds18MinMaxInit=true; }
      else { if (ds0<sd.ds18Min) sd.ds18Min=ds0; if (ds0>sd.ds18Max) sd.ds18Max=ds0; }
      sd.ds18OK=true;
    } else { sd.ds18OK=false; }
    if (sd.dhtOK&&sd.bmeOK) sd.rainProbPct=computeRainProbability(sd.dhtHumidity,sd.bmePressure,sd.pressureTrend,sd.dewPoint,sd.dhtTemp);
    sd.dhtConfidence=computeConfidence(dhtRetries,dhtConsecFails,dhtSpike,dhtValid);
    sd.bmeConfidence=computeConfidence(bmeRetries,bmeConsecFails,false,bmeValid);
    sd.bhConfidence =computeConfidence(bhRetries, bhConsecFails, false,bh1Valid);
    sd.dsConfidence =computeConfidence(dsRetries, dsConsecFails, false,dsValid);
    sd.dhtHealth=healthFromFails(dhtConsecFails,dhtHasLastGood,dhtValid);
    sd.bmeHealth=healthFromFails(bmeConsecFails,bmeHasLastGood,bmeValid);
    sd.bhHealth =healthFromFails(bhConsecFails,bhHasLastGood,bh1Valid);
    sd.dsHealth =healthFromFails(dsConsecFails,dsHasLastGood,dsValid);
    sd.dhtRetries=dhtRetries; sd.bmeRetries=bmeRetries; sd.bhRetries=bhRetries; sd.dsRetries=dsRetries;
    if (!dhtValid) sd.dhtErrors++; if (!bmeValid) sd.bmeErrors++; if (!bh1Valid) sd.bhErrors++; if (!dsValid) sd.dsErrors++;
    sd.snapshotSeq++; sd.snapshotMs=millis();
    xSemaphoreGive(dataMutex);

    uint32_t nowMs=millis();
    if (nowMs-lastLogMs>=LOG_INTERVAL_MS) {
      lastLogMs=nowMs; time_t epoch; time(&epoch);
      LogRecord rec={epoch,
        dhtValid?finalDhtT:NAN, dhtValid?finalDhtH:NAN,
        bmeValid?bP:NAN, bh1Valid?lux:NAN, dsValid?ds0:NAN};
      logRing[logHead]=rec; logHead=(logHead+1)%MAX_LOG_RECORDS; if (logCount<MAX_LOG_RECORDS) logCount++;
      appendLogRecord(rec);
    }

    {
      time_t epoch; time(&epoch);
      if (epoch>100000UL && (epoch-lastNtpSync)>=(time_t)NTP_RESYNC_INTERVAL && WiFi.status()==WL_CONNECTED) {
        configTzTime(cfg.tzStr, "pool.ntp.org", "time.nist.gov");
        lastNtpSync=epoch;
        Serial.println(F("[NTP] Re-sync (configTzTime)."));
      }
    }

    broadcastWsUpdate();
    confirmBootIfStable();
    vTaskDelay(pdMS_TO_TICKS(SENSOR_READ_MS));
  }
}

void displayTask(void* pvParam) {
  esp_task_wdt_add(NULL);
  bool lastBtnRaw=HIGH,btnState=HIGH;
  uint32_t lastDebMs=0,lastRefresh=0;
  lastAutoCycleMs=millis();
  for (;;) {
    esp_task_wdt_reset();
    hbDisplay = millis();
    uint32_t now=millis();
    bool reading=digitalRead(BUTTON_PIN);
    if (reading!=lastBtnRaw) lastDebMs=now;
    if ((now-lastDebMs)>=DEBOUNCE_MS&&reading!=btnState) { btnState=reading; if (btnState==LOW) nextPage(); }
    lastBtnRaw=reading;
    if (displayState==DISP_LOCAL&&now-lastAutoCycleMs>=AUTO_CYCLE_MS) nextPage();
    if (pageChangeReq) {
      pageChangeReq=false;
      Page from=currentPage,to=targetPage;
      if (from!=to) { if (xSemaphoreTake(displayMutex,pdMS_TO_TICKS(200))==pdTRUE) { slideTransition(from,to); xSemaphoreGive(displayMutex); } }
    }
    if (now-lastRefresh>=DISPLAY_REFRESH_MS) {
      lastRefresh=now;
      if (!pageChangeReq) { if (xSemaphoreTake(displayMutex,pdMS_TO_TICKS(50))==pdTRUE) { renderCurrentPage(); xSemaphoreGive(displayMutex); } }
    }
    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

void wifiTask(void* pvParam) {
  esp_task_wdt_add(NULL);
  static uint8_t  wifiRetries=0;
  static uint32_t lastReconMs=0, lastCleanupMs=0, lastWeatherMs=0;
  static uint32_t backoff=5000;
  for (;;) {
    esp_task_wdt_reset();
    hbWifi = millis();
    uint32_t now=millis();
    if (!offlineMode && WiFi.status()!=WL_CONNECTED) {
      
      if (now-lastReconMs>=backoff) {
        lastReconMs=now; Serial.printf("[WiFi] Retry #%u...\n",wifiRetries+1);
        wifiHardFailStreak++;
        
        if (wifiHardFailStreak >= WIFI_HARD_RESET_AFTER_FAILS) {
          hardResetWifi();
        } else {
          WiFi.reconnect();
        }
        wifiRetries = (wifiRetries < 255) ? wifiRetries + 1 : 255;
        if (wifiRetries<3) backoff=5000; else if (wifiRetries<6) backoff=15000; else backoff=60000;
      }
    } else if (!offlineMode && WiFi.status()==WL_CONNECTED) {
      if (wifiRetries>0) { Serial.printf("[WiFi] Reconnected: %s\n",WiFi.localIP().toString().c_str()); wifiRetries=0; backoff=5000; wifiHardFailStreak=0; }
    }
    
    if (WiFi.status()==WL_CONNECTED && cfg.weatherCity[0]!='\0' && cfg.owmApiKey[0]!='\0' &&
        (lastWeatherMs==0 || now-lastWeatherMs>=WEATHER_REFRESH_MS)) {
      lastWeatherMs=now;
      if (!fetchPersistentWeather()) logEvent(EVT_WARN,"OWM refresh failed for saved location");
    }

    if (now-lastCleanupMs>=WS_CLEANUP_INTERVAL_MS) {
      lastCleanupMs=now;
      if (xSemaphoreTake(wsMutex,pdMS_TO_TICKS(100))==pdTRUE) {
        ws.cleanupClients();
        xSemaphoreGive(wsMutex);
      }
    }
    vTaskDelay(pdMS_TO_TICKS(1000));
  }
}

void systemHealthTask(void* pvParam) {
  esp_task_wdt_add(NULL);
  bool heapWarnLatched=false, heapErrorLatched=false, fragWarnLatched=false, wifiWasDown=false;
  
  bool sensorStackWarnLatched=false, displayStackWarnLatched=false;
  bool wifiStackWarnLatched=false,   healthStackWarnLatched=false;
  SensorHealth lastDht=HEALTH_ONLINE,lastBme=HEALTH_ONLINE,lastBh=HEALTH_ONLINE,lastDs=HEALTH_ONLINE;

  for (;;) {
    esp_task_wdt_reset();
    hbHealth = millis();
    checkTaskHeartbeats();

    uint32_t freeHeap=esp_get_free_heap_size();
    uint32_t largestBlock=(uint32_t)heap_caps_get_largest_free_block(MALLOC_CAP_8BIT);
    uint8_t fragPct=(freeHeap>0)?(uint8_t)(100UL-(100UL*largestBlock)/freeHeap):0;

    if (freeHeap<HEAP_ERROR_BYTES) {
      if (!heapErrorLatched) { logEvent(EVT_ERROR,"Heap critical: %lu bytes",(unsigned long)freeHeap); heapErrorLatched=true; }
    } else if (freeHeap<HEAP_WARN_BYTES) {
      if (!heapWarnLatched) { logEvent(EVT_WARN,"Heap low: %lu bytes",(unsigned long)freeHeap); heapWarnLatched=true; }
      heapErrorLatched=false;
    } else {
      if (heapWarnLatched||heapErrorLatched) logEvent(EVT_INFO,"Heap recovered: %lu bytes",(unsigned long)freeHeap);
      heapWarnLatched=false; heapErrorLatched=false;
    }
    if (fragPct>=FRAG_WARN_PCT) { if (!fragWarnLatched) { logEvent(EVT_WARN,"Heap frag high: %u%%",fragPct); fragWarnLatched=true; } }
    else fragWarnLatched=false;

    bool wifiUp=(WiFi.status()==WL_CONNECTED);
    if (!wifiUp&&!wifiWasDown) { logEvent(EVT_WARN,"WiFi link down"); wifiWasDown=true; }
    else if (wifiUp&&wifiWasDown) { logEvent(EVT_INFO,"WiFi restored: %s",WiFi.localIP().toString().c_str()); wifiWasDown=false; }

    if (hSensorTask  && uxTaskGetStackHighWaterMark(hSensorTask)<256  && !sensorStackWarnLatched)  { logEvent(EVT_WARN,"Sensor task stack low");  sensorStackWarnLatched=true;  }
    if (hDisplayTask && uxTaskGetStackHighWaterMark(hDisplayTask)<256 && !displayStackWarnLatched) { logEvent(EVT_WARN,"Display task stack low"); displayStackWarnLatched=true; }
    if (hWifiTask    && uxTaskGetStackHighWaterMark(hWifiTask)<256    && !wifiStackWarnLatched)    { logEvent(EVT_WARN,"WiFi task stack low");    wifiStackWarnLatched=true;    }
    if (uxTaskGetStackHighWaterMark(NULL)<256 && !healthStackWarnLatched) { logEvent(EVT_WARN,"Health task stack low"); healthStackWarnLatched=true; }

    SensorData s;
    if (xSemaphoreTake(dataMutex,pdMS_TO_TICKS(200))==pdTRUE) {
      s=sd; xSemaphoreGive(dataMutex);
      auto lvlFor=[](SensorHealth h)->EventLevel{ return (h==HEALTH_ERROR)?EVT_ERROR:(h==HEALTH_WARNING?EVT_WARN:EVT_INFO); };
      if (s.dhtHealth!=lastDht) { logEvent(lvlFor(s.dhtHealth),"DHT22 -> %s",healthStr(s.dhtHealth)); lastDht=s.dhtHealth; }
      if (s.bmeHealth!=lastBme) { logEvent(lvlFor(s.bmeHealth),"BME280 -> %s",healthStr(s.bmeHealth)); lastBme=s.bmeHealth; }
      if (s.bhHealth!=lastBh)   { logEvent(lvlFor(s.bhHealth), "BH1750 -> %s",healthStr(s.bhHealth));  lastBh=s.bhHealth;  }
      if (s.dsHealth!=lastDs)   { logEvent(lvlFor(s.dsHealth), "DS18B20 -> %s",healthStr(s.dsHealth)); lastDs=s.dsHealth;  }
    }
    vTaskDelay(pdMS_TO_TICKS(HEALTH_TASK_PERIOD_MS));
  }
}

void logRotateIfNeeded() {
  char path[16]; snprintf(path,sizeof(path),"/log.%u.csv",activeLog);
  if (LittleFS.exists(path)) { File f=LittleFS.open(path,"r"); if (f) { size_t sz=f.size(); f.close(); if (sz>=LOG_MAX_FILE_BYTES) { char old[16]; snprintf(old,sizeof(old),"/log.%u.csv",1-activeLog); LittleFS.remove(old); activeLog=1-activeLog; } } }
}
void appendLogRecord(const LogRecord& rec) {
  if (xSemaphoreTake(fsMutex,pdMS_TO_TICKS(500))!=pdTRUE) return;
  logRotateIfNeeded();
  char path[16]; snprintf(path,sizeof(path),"/log.%u.csv",activeLog);
  bool newFile=!LittleFS.exists(path);
  File f=LittleFS.open(path,"a");
  if (!f) { xSemaphoreGive(fsMutex); return; }
  if (newFile) f.println(F("timestamp,temp_c,humidity_pct,pressure_hpa,lux,outdoor_temp_c"));
  struct tm ti; localtime_r(&rec.ts,&ti); char line[80];
  snprintf(line,sizeof(line),"%04d-%02d-%02dT%02d:%02d:%02d,%.2f,%.2f,%.2f,%.1f,%.2f",ti.tm_year+1900,ti.tm_mon+1,ti.tm_mday,ti.tm_hour,ti.tm_min,ti.tm_sec,rec.temp,rec.humidity,rec.pressure,rec.lux,rec.outdoorTemp);
  f.println(line); f.close();
  xSemaphoreGive(fsMutex);
}

void broadcastWsUpdate() {
  if (ws.count()==0) return;
  SensorData s; xSemaphoreTake(dataMutex,portMAX_DELAY); s=sd; xSemaphoreGive(dataMutex);
  struct tm ti; getLocalTime(&ti);
  char tsBuf[12]; snprintf(tsBuf,sizeof(tsBuf),"%02d:%02d:%02d",ti.tm_hour,ti.tm_min,ti.tm_sec);
  char ipBuf[16]; strncpy(ipBuf,WiFi.localIP().toString().c_str(),sizeof(ipBuf)-1); ipBuf[15]=0;
  size_t fsTotal=LittleFS.totalBytes(),fsUsed=LittleFS.usedBytes();
  StaticJsonDocument<2048> doc;
  doc["temp"]      =s.dhtOK?roundf(s.dhtTemp*10)/10.0f:NAN;
  doc["dht_temp"]=doc["temp"];
  doc["humidity"]  =s.dhtOK?roundf(s.dhtHumidity*10)/10.0f:NAN;
  doc["bme_temp"] =s.bmeOK?roundf(s.bmeTemp*10)/10.0f:NAN;
  doc["bme_humidity"] =s.bmeOK?roundf(s.bmeHumidity*10)/10.0f:NAN;
  doc["pressure"]  =s.bmeOK?roundf(s.bmePressure*10)/10.0f:NAN;
  doc["altitude"]  =s.bmeOK?roundf(s.altitudeM):NAN;
  doc["heat_index"]=s.dhtOK?roundf(s.heatIndex*10)/10.0f:NAN;
  doc["dew_point"] =s.dhtOK?roundf(s.dewPoint*10)/10.0f:NAN;
  doc["lux"]       =s.bh1750OK?s.lux:NAN;
  doc["ds18_temp"] =s.ds18OK?roundf(s.ds18Temp[0]*10)/10.0f:NAN;
  doc["ds18_count"]=(uint8_t)s.ds18Count;
  doc["ds18_min"]  =s.ds18OK?s.ds18Min:NAN; doc["ds18_max"]=s.ds18OK?s.ds18Max:NAN;
  doc["pressure_trend"]=s.bmeOK?roundf(s.pressureTrend*100)/100.0f:NAN;
  doc["rain_prob"] =(s.dhtOK&&s.bmeOK)?s.rainProbPct:NAN;
  doc["weather"]   =(s.dhtOK&&s.bmeOK)?weatherClassification(s.rainProbPct):"";
  doc["dht_ok"]=s.dhtOK; doc["bme_ok"]=s.bmeOK; doc["bh_ok"]=s.bh1750OK; doc["ds_ok"]=s.ds18OK;
  doc["rssi"]=WiFi.status()==WL_CONNECTED?WiFi.RSSI():0; doc["ip"]=ipBuf;
  doc["ssid"]=WiFi.status()==WL_CONNECTED?WiFi.SSID():"";
  doc["ap_ssid"]=AP_SSID; doc["ap_stations"]=WiFi.softAPgetStationNum();
  doc["time"]=tsBuf; doc["free_heap"]=esp_get_free_heap_size(); doc["uptime_s"]=millis()/1000UL;
  doc["fs_used"]=(uint32_t)fsUsed; doc["fs_total"]=(uint32_t)fsTotal;
  doc["override_active"]=(displayState==DISP_OVERRIDE);
  doc["dht_confidence"]=s.dhtConfidence; doc["bme_confidence"]=s.bmeConfidence;
  doc["bh_confidence"] =s.bhConfidence;  doc["ds_confidence"]=s.dsConfidence;
  doc["dht_health"]=healthStr(s.dhtHealth); doc["bme_health"]=healthStr(s.bmeHealth);
  doc["bh_health"] =healthStr(s.bhHealth);  doc["ds_health"]=healthStr(s.dsHealth);
  doc["dht_retries"]=s.dhtRetries; doc["bme_retries"]=s.bmeRetries; doc["bh_retries"]=s.bhRetries; doc["ds_retries"]=s.dsRetries;
  doc["dht_errors"]=s.dhtErrors; doc["bme_errors"]=s.bmeErrors; doc["bh_errors"]=s.bhErrors; doc["ds_errors"]=s.dsErrors;
  doc["snapshot_seq"]=s.snapshotSeq; doc["snapshot_ms"]=s.snapshotMs;
  uint32_t fH=esp_get_free_heap_size(); uint32_t lB=(uint32_t)heap_caps_get_largest_free_block(MALLOC_CAP_8BIT);
  doc["heap_frag_pct"]=(fH>0)?(uint8_t)(100UL-(100UL*lB)/fH):0;
  doc["firmware"]=FIRMWARE_VERSION;
  doc["cpu_mhz"]=ESP.getCpuFreqMHz();
  doc["chip_rev"]=ESP.getChipRevision();
  doc["mac"]=WiFi.macAddress();
  doc["min_heap"]=esp_get_minimum_free_heap_size();
  doc["time_valid"]=(bool)getLocalTime(&ti);
  doc["offline_mode"]=(bool)offlineMode;
  doc["wifi_connected"]=(WiFi.status()==WL_CONNECTED);
  doc["sta_ip"]=WiFi.localIP().toString();
  doc["ap_ip"]=apIpStr;
  
  doc["safe_mode"]=safeModeActive;
  doc["oled_ok"]=oledOK;
  doc["i2c_recoveries"]=(uint32_t)i2cRecoveryCount;
  doc["wifi_hard_resets"]=(uint32_t)wifiHardResetCount;
  doc["boot_confirmed"]=bootConfirmed;
  if (xSemaphoreTake(overrideMutex,pdMS_TO_TICKS(20))==pdTRUE) {
    JsonObject sw=doc.createNestedObject("saved_weather");
    sw["valid"]=savedWeather.valid; sw["city"]=savedWeather.city; sw["country"]=savedWeather.country;
    sw["description"]=savedWeather.description; sw["temp"]=savedWeather.tempC; sw["feels_like"]=savedWeather.feelsLike;
    sw["humidity"]=savedWeather.humidity; sw["pressure"]=savedWeather.pressure; sw["wind"]=savedWeather.windSpeed;
    sw["clouds"]=savedWeather.clouds; sw["visibility_m"]=savedWeather.visibilityM; sw["time"]=savedWeather.timeStr; sw["timezone"]=savedWeather.timezone;
    sw["updated_s"]=savedWeather.valid ? ((millis()-savedWeather.updatedMs)/1000UL) : 0;
    xSemaphoreGive(overrideMutex);
  }
  String out; out.reserve(1900); serializeJson(doc,out);
  wsTextAll(out);
}

void onWsEvent(AsyncWebSocket* s, AsyncWebSocketClient* c, AwsEventType type, void* arg, uint8_t* data, size_t len) {
  if (type==WS_EVT_CONNECT) { Serial.printf("[WS] #%u connected\n",c->id()); broadcastWsUpdate(); }
  else if (type==WS_EVT_DISCONNECT) { Serial.printf("[WS] #%u disconnected\n",c->id()); }
}

static String generateSessionToken() {
  char buf[SESSION_TOKEN_LEN+1];
  for (int i = 0; i < SESSION_TOKEN_LEN; i++) {
    uint8_t nib = (uint8_t)(esp_random() & 0xF);
    buf[i] = (nib < 10) ? ('0'+nib) : ('a'+(nib-10));
  }
  buf[SESSION_TOKEN_LEN] = '\0';
  return String(buf);
}

String createSession() {
  String tok = generateSessionToken();
  if (xSemaphoreTake(sessionMutex, pdMS_TO_TICKS(200)) != pdTRUE) return String();
  int slot = -1; uint32_t soonestExp = 0xFFFFFFFFUL; int soonestIdx = 0;
  for (int i = 0; i < MAX_SESSIONS; i++) {
    if (!sessions[i].active) { slot = i; break; }
    if (sessions[i].expiresAt < soonestExp) { soonestExp = sessions[i].expiresAt; soonestIdx = i; }
  }
  if (slot < 0) slot = soonestIdx;
  strncpy(sessions[slot].token, tok.c_str(), SESSION_TOKEN_LEN);
  sessions[slot].token[SESSION_TOKEN_LEN] = '\0';
  sessions[slot].expiresAt = millis() + SESSION_TTL_MS;
  sessions[slot].active = true;
  xSemaphoreGive(sessionMutex);
  return tok;
}

bool validateSession(const String& token) {
  if (token.length() != SESSION_TOKEN_LEN) return false;
  bool ok = false;
  if (xSemaphoreTake(sessionMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
    uint32_t now = millis();
    for (int i = 0; i < MAX_SESSIONS; i++) {
      if (!sessions[i].active) continue;
      if (!secureCompare(sessions[i].token, token.c_str())) continue;
      if ((int32_t)(now - sessions[i].expiresAt) >= 0) { sessions[i].active = false; }
      else { sessions[i].expiresAt = now + SESSION_TTL_MS; ok = true; }
      break;
    }
    xSemaphoreGive(sessionMutex);
  }
  return ok;
}

void invalidateSession(const String& token) {
  if (xSemaphoreTake(sessionMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
    for (int i = 0; i < MAX_SESSIONS; i++) {
      if (sessions[i].active && secureCompare(sessions[i].token, token.c_str())) { sessions[i].active = false; break; }
    }
    xSemaphoreGive(sessionMutex);
  }
}

void invalidateAllSessions() {
  if (xSemaphoreTake(sessionMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
    for (int i = 0; i < MAX_SESSIONS; i++) sessions[i].active = false;
    xSemaphoreGive(sessionMutex);
  }
}

bool checkBasicAuth(AsyncWebServerRequest* req) {
  uint32_t ip = clientIpToU32(req);
  uint32_t retryMs = 0;
  if (isAuthLocked(ip, &retryMs)) {
    AsyncWebServerResponse* r = req->beginResponse(429, "application/json", "{\"error\":\"Too many failed attempts. Try later.\"}");
    r->addHeader("Retry-After", String(retryMs / 1000 + 1));
    applySecurityHeaders(r);
    req->send(r);
    return false;
  }
  if (req->hasHeader("Authorization")) {
    String authH = req->header("Authorization");
    if (authH.startsWith("Bearer ")) {
      String tok = authH.substring(7); tok.trim();
      if (validateSession(tok)) { recordAuthSuccess(ip); return true; }
      recordAuthFailure(ip);
      req->send(401, "application/json", "{\"error\":\"Session expired. Please refresh and log in again.\"}");
      return false;
    }
  }
  if (!req->authenticate(sec.dashUser, sec.dashPass)) {
    recordAuthFailure(ip);
    req->requestAuthentication();
    return false;
  }
  recordAuthSuccess(ip);
  return true;
}
bool checkApiKey(AsyncWebServerRequest* req) {
  uint32_t ip = clientIpToU32(req);
  uint32_t retryMs = 0;
  if (isAuthLocked(ip, &retryMs)) {
    AsyncWebServerResponse* r = req->beginResponse(429, "application/json", "{\"error\":\"Too many failed attempts. Try later.\"}");
    r->addHeader("Retry-After", String(retryMs / 1000 + 1));
    applySecurityHeaders(r);
    req->send(r);
    return false;
  }
  if (!req->hasHeader("X-API-Key") || !secureCompare(req->header("X-API-Key").c_str(), sec.apiKey)) {
    recordAuthFailure(ip);
    req->send(401,"application/json","{\"error\":\"Unauthorized\"}");
    return false;
  }
  recordAuthSuccess(ip);
  return true;
}

void startCaptiveDns() {
  
  IPAddress apIP = WiFi.softAPIP();
  captiveDns.stop();
  captiveDns.start(53, "*", apIP);
  Serial.printf("[DNS] Captive dashboard DNS started on %s:53\n", apIP.toString().c_str());
}

void setupWebServer() {
  
  DefaultHeaders::Instance().addHeader("X-Content-Type-Options", "nosniff");
  DefaultHeaders::Instance().addHeader("X-Frame-Options", "DENY");

  server.on("/api/login", HTTP_POST, [](AsyncWebServerRequest* req) {
    if (!checkBasicAuth(req)) return;
    String tok = createSession();
    if (tok.length() == 0) { req->send(503,"application/json","{\"error\":\"Session table busy, try again.\"}"); return; }
    StaticJsonDocument<128> doc;
    doc["token"] = tok;
    doc["expires_in"] = SESSION_TTL_MS / 1000;
    String out; serializeJson(doc,out);
    req->send(200,"application/json",out);
  });

  server.on("/api/logout", HTTP_POST, [](AsyncWebServerRequest* req) {
    if (req->hasHeader("Authorization")) {
      String authH = req->header("Authorization");
      if (authH.startsWith("Bearer ")) { String tok = authH.substring(7); tok.trim(); invalidateSession(tok); }
    }
    req->send(200,"application/json","{\"ok\":true}");
  });

  server.on("/api/security/credentials", HTTP_POST,
    [](AsyncWebServerRequest* req){}, nullptr,
    [](AsyncWebServerRequest* req, uint8_t* data, size_t len, size_t index, size_t total) {
      if (index==0) {
        if (!checkBasicAuth(req)) return;
        if (!checkOriginSameHost(req)) { req->send(403,"application/json","{\"error\":\"Cross-origin request blocked\"}"); return; }
        if (rejectIfBodyTooLarge(req, total)) return;
        req->_tempObject=new String();
      }
      String* body=(String*)req->_tempObject;
      if (!body) return;
      body->concat((const char*)data,len);
      if (index+len!=total) return;
      
      StaticJsonDocument<384> doc;
      DeserializationError err=deserializeJson(doc,*body);
      delete body; req->_tempObject=nullptr;
      if (err) { req->send(400,"application/json","{\"error\":\"Bad JSON\"}"); return; }
      bool changed=false;
      if (doc.containsKey("new_password")) {
        const char* np=doc["new_password"]|"";
        if (strlen(np)<8) { req->send(400,"application/json","{\"error\":\"Password min 8 chars\"}"); return; }
        strlcpy(sec.dashPass,np,sizeof(sec.dashPass)); changed=true;
        invalidateAllSessions();
      }
      if (doc.containsKey("new_api_key")) {
        const char* nk=doc["new_api_key"]|"";
        if (strlen(nk)<12) { req->send(400,"application/json","{\"error\":\"API key min 12 chars\"}"); return; }
        strlcpy(sec.apiKey,nk,sizeof(sec.apiKey)); changed=true;
      }
      if (doc.containsKey("new_clear_token")) {
        const char* nt=doc["new_clear_token"]|"";
        if (strlen(nt)<8) { req->send(400,"application/json","{\"error\":\"Clear token min 8 chars\"}"); return; }
        strlcpy(sec.logClearToken,nt,sizeof(sec.logClearToken)); changed=true;
      }
      if (changed) {
        sec.usingFactoryDefaults = false;
        saveSecurityConfig();
        ws.setAuthentication(sec.dashUser, sec.dashPass);
        logEvent(EVT_WARN,"Security credentials rotated via dashboard");
        req->send(200,"application/json","{\"ok\":true,\"msg\":\"Credentials updated.\"}");
      } else {
        req->send(200,"application/json","{\"ok\":true,\"msg\":\"No changes.\"}");
      }
    }
  );

  auto redirectToDashboard = [](AsyncWebServerRequest* req) {
    req->redirect("/");
  };

  server.on("/generate_204", HTTP_GET, redirectToDashboard);
  server.on("/gen_204", HTTP_GET, redirectToDashboard);
  server.on("/hotspot-detect.html", HTTP_GET, redirectToDashboard);
  server.on("/connecttest.txt", HTTP_GET, redirectToDashboard);
  server.on("/ncsi.txt", HTTP_GET, redirectToDashboard);
  server.on("/success.txt", HTTP_GET, redirectToDashboard);
  server.on("/canonical.html", HTTP_GET, redirectToDashboard);
  server.on("/redirect", HTTP_GET, redirectToDashboard);
  server.on("/fwlink", HTTP_GET, redirectToDashboard);
  server.on("/library/test/success.html", HTTP_GET, redirectToDashboard);

  server.on("/", HTTP_GET, [](AsyncWebServerRequest* req) {
    
    if (!checkBasicAuth(req)) return;
    AsyncWebServerResponse* resp = req->beginResponse_P(200, "text/html", DASHBOARD_HTML);
    resp->addHeader("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
    resp->addHeader("Pragma", "no-cache");
    req->send(resp);
  });

  server.on("/api/data", HTTP_GET, [](AsyncWebServerRequest* req) {
    if (!checkApiKey(req)) return;
    SensorData s; xSemaphoreTake(dataMutex,portMAX_DELAY); s=sd; xSemaphoreGive(dataMutex);
    struct tm ti; getLocalTime(&ti);
    char tsBuf[12]; snprintf(tsBuf,sizeof(tsBuf),"%02d:%02d:%02d",ti.tm_hour,ti.tm_min,ti.tm_sec);
    char ipBuf[16]; strncpy(ipBuf,WiFi.localIP().toString().c_str(),sizeof(ipBuf)-1); ipBuf[15]=0;
    StaticJsonDocument<2048> doc;
    doc["temp"]=s.dhtOK?roundf(s.dhtTemp*10)/10.0f:NAN; doc["humidity"]=s.dhtOK?roundf(s.dhtHumidity*10)/10.0f:NAN;
    doc["bme_temp"]=s.bmeOK?roundf(s.bmeTemp*10)/10.0f:NAN; doc["bme_humidity"]=s.bmeOK?roundf(s.bmeHumidity*10)/10.0f:NAN;
    doc["pressure"]=s.bmeOK?roundf(s.bmePressure*10)/10.0f:NAN; doc["altitude"]=s.bmeOK?roundf(s.altitudeM):NAN;
    doc["heat_index"]=s.dhtOK?roundf(s.heatIndex*10)/10.0f:NAN; doc["dew_point"]=s.dhtOK?roundf(s.dewPoint*10)/10.0f:NAN;
    doc["lux"]=s.bh1750OK?s.lux:NAN; doc["ds18_temp"]=s.ds18OK?roundf(s.ds18Temp[0]*10)/10.0f:NAN;
    doc["ds18_temp_2"]=(s.ds18OK&&s.ds18Count>1)?roundf(s.ds18Temp[1]*10)/10.0f:NAN;
    doc["ds18_count"]=(uint8_t)s.ds18Count;
    doc["ds18_min"]=s.ds18OK?s.ds18Min:NAN; doc["ds18_max"]=s.ds18OK?s.ds18Max:NAN;
    doc["pressure_trend"]=s.bmeOK?roundf(s.pressureTrend*100)/100.0f:NAN;
    doc["rain_prob"]=(s.dhtOK&&s.bmeOK)?s.rainProbPct:NAN;
    doc["weather"]=(s.dhtOK&&s.bmeOK)?weatherClassification(s.rainProbPct):"";
    doc["dht_ok"]=s.dhtOK; doc["bme_ok"]=s.bmeOK; doc["bh_ok"]=s.bh1750OK; doc["ds_ok"]=s.ds18OK;
    doc["rssi"]=WiFi.status()==WL_CONNECTED?WiFi.RSSI():0; doc["ip"]=ipBuf;
  doc["ssid"]=WiFi.status()==WL_CONNECTED?WiFi.SSID():"";
  doc["ap_ssid"]=AP_SSID; doc["ap_stations"]=WiFi.softAPgetStationNum(); doc["time"]=tsBuf; doc["free_heap"]=esp_get_free_heap_size();
    doc["uptime_s"]=millis()/1000UL; doc["fs_used"]=(uint32_t)LittleFS.usedBytes(); doc["fs_total"]=(uint32_t)LittleFS.totalBytes();
    doc["firmware"]=FIRMWARE_VERSION;
    doc["dht_confidence"]=s.dhtConfidence; doc["bme_confidence"]=s.bmeConfidence; doc["bh_confidence"]=s.bhConfidence; doc["ds_confidence"]=s.dsConfidence;
    doc["dht_health"]=healthStr(s.dhtHealth); doc["bme_health"]=healthStr(s.bmeHealth); doc["bh_health"]=healthStr(s.bhHealth); doc["ds_health"]=healthStr(s.dsHealth);
    doc["dht_retries"]=s.dhtRetries; doc["bme_retries"]=s.bmeRetries; doc["bh_retries"]=s.bhRetries; doc["ds_retries"]=s.dsRetries;
    doc["dht_errors"]=s.dhtErrors; doc["bme_errors"]=s.bmeErrors; doc["bh_errors"]=s.bhErrors; doc["ds_errors"]=s.dsErrors;
    doc["snapshot_seq"]=s.snapshotSeq; doc["snapshot_ms"]=s.snapshotMs;
    uint32_t fH=esp_get_free_heap_size(); uint32_t lB=(uint32_t)heap_caps_get_largest_free_block(MALLOC_CAP_8BIT);
    doc["heap_frag_pct"]=(fH>0)?(uint8_t)(100UL-(100UL*lB)/fH):0;
    doc["offline_mode"]=(bool)offlineMode; doc["ap_ip"]=apIpStr;
    if (xSemaphoreTake(overrideMutex,pdMS_TO_TICKS(20))==pdTRUE) {
      JsonObject sw=doc.createNestedObject("saved_weather");
      sw["valid"]=savedWeather.valid; sw["city"]=savedWeather.city; sw["country"]=savedWeather.country;
      sw["description"]=savedWeather.description; sw["temp"]=savedWeather.tempC; sw["feels_like"]=savedWeather.feelsLike;
      sw["humidity"]=savedWeather.humidity; sw["pressure"]=savedWeather.pressure; sw["wind"]=savedWeather.windSpeed;
      sw["clouds"]=savedWeather.clouds; sw["visibility_m"]=savedWeather.visibilityM;
      sw["time"]=savedWeather.timeStr; sw["timezone"]=savedWeather.timezone;
      xSemaphoreGive(overrideMutex);
    }
    String out; out.reserve(1200); serializeJson(doc,out);
    req->send(200,"application/json",out);
  });

  server.on("/api/events", HTTP_GET, [](AsyncWebServerRequest* req) {
    if (!checkBasicAuth(req)) return;
    StaticJsonDocument<2048> doc;
    JsonArray arr=doc.createNestedArray("events");
    if (xSemaphoreTake(eventMutex,pdMS_TO_TICKS(200))==pdTRUE) {
      uint8_t n=(uint8_t)eventCount;
      uint8_t startIdx=(eventHead+EVENT_LOG_SIZE-n)%EVENT_LOG_SIZE;
      for (uint8_t i=0;i<n;i++) {
        uint8_t idx=(startIdx+i)%EVENT_LOG_SIZE;
        JsonObject o=arr.createNestedObject();
        const char* tag=(eventLog[idx].level==EVT_ERROR)?"ERROR":(eventLog[idx].level==EVT_WARN?"WARN":"INFO");
        o["ms"]=eventLog[idx].ms; o["level"]=tag; o["msg"]=eventLog[idx].msg;
      }
      xSemaphoreGive(eventMutex);
    }
    doc["seq"]=eventSeq;
    String out; serializeJson(doc,out);
    req->send(200,"application/json",out);
  });

  server.on("/api/config", HTTP_GET, [](AsyncWebServerRequest* req) {
    if (!checkBasicAuth(req)) return;
    StaticJsonDocument<256> doc;
    doc["tz_offset"]=cfg.tzOffsetSec; doc["tz_str"]=cfg.tzStr;
    doc["owm_key_set"]=(cfg.owmApiKey[0]!='\0'); doc["wifi_ssid"]=cfg.wifiSsid;
    String out; serializeJson(doc,out); req->send(200,"application/json",out);
  });

  server.on("/api/config", HTTP_POST,
    [](AsyncWebServerRequest* req){}, nullptr,
    [](AsyncWebServerRequest* req, uint8_t* data, size_t len, size_t index, size_t total) {
      
      if (index==0) {
        if (!checkBasicAuth(req)) return;
        if (!checkOriginSameHost(req)) { req->send(403,"application/json","{\"error\":\"Cross-origin request blocked\"}"); return; }
        if (rejectIfBodyTooLarge(req, total)) return;
        req->_tempObject=new String();
      }
      String* body=(String*)req->_tempObject;
      if (!body) return;
      body->concat((const char*)data,len);
      if (index+len!=total) return;
      StaticJsonDocument<256> doc;
      DeserializationError err=deserializeJson(doc,*body);
      delete body; req->_tempObject=nullptr;
      if (err) { req->send(400,"application/json","{\"error\":\"Bad JSON\"}"); return; }
      bool changed=false;
      if (doc.containsKey("tz_offset")) { cfg.tzOffsetSec=doc["tz_offset"].as<int32_t>(); changed=true; }
      if (doc.containsKey("tz_str")) { strlcpy(cfg.tzStr,doc["tz_str"]|cfg.tzStr,sizeof(cfg.tzStr)); setenv("TZ",cfg.tzStr,1); tzset(); changed=true; }
      if (doc.containsKey("owm_key")) { strlcpy(cfg.owmApiKey,doc["owm_key"]|"",sizeof(cfg.owmApiKey)); changed=true; }
      if (doc.containsKey("wifi_ssid")) { strlcpy(cfg.wifiSsid,doc["wifi_ssid"]|"",sizeof(cfg.wifiSsid)); changed=true; }
      if (doc.containsKey("wifi_pass")) { strlcpy(cfg.wifiPass,doc["wifi_pass"]|"",sizeof(cfg.wifiPass)); changed=true; }
      if (changed) {
        saveConfig();
        if (cfg.wifiSsid[0]!='\0') {
          struct HWP { char ssid[64]; char pass[64]; };
          HWP* hp=new HWP();
          strlcpy(hp->ssid,cfg.wifiSsid,sizeof(hp->ssid)); strlcpy(hp->pass,cfg.wifiPass,sizeof(hp->pass));
          if (xTaskCreate([](void* arg){ HWP* p=(HWP*)arg; connectHiddenWifi(p->ssid,p->pass); delete p; vTaskDelete(NULL); },"wifiConnect",4096,hp,1,NULL)!=pdPASS) { delete hp; }
          req->send(200,"application/json","{\"ok\":true,\"msg\":\"Config saved. Connecting to hidden WiFi.\"}");
          return;
        }
        req->send(200,"application/json","{\"ok\":true,\"msg\":\"Config saved.\"}");
      } else { req->send(200,"application/json","{\"ok\":true,\"msg\":\"No changes.\"}"); }
    }
  );

  server.on("/api/location", HTTP_GET, [](AsyncWebServerRequest* req) {
    if (!checkBasicAuth(req)) return;
    StaticJsonDocument<256> doc;
    doc["city"]=cfg.weatherCity; doc["country"]=cfg.weatherCountry;
    doc["api_key_set"]=(cfg.owmApiKey[0]!='\0');
    doc["wifi_connected"]=(WiFi.status()==WL_CONNECTED);
    bool valid=false;
    if (xSemaphoreTake(overrideMutex,pdMS_TO_TICKS(100))==pdTRUE) { valid=savedWeather.valid; xSemaphoreGive(overrideMutex); }
    doc["weather_valid"]=valid;
    String out; serializeJson(doc,out); req->send(200,"application/json",out);
  });

  server.on("/api/location", HTTP_POST,
    [](AsyncWebServerRequest* req){}, nullptr,
    [](AsyncWebServerRequest* req, uint8_t* data, size_t len, size_t index, size_t total) {
      if (index==0) {
        if (!checkBasicAuth(req)) return;
        if (!checkOriginSameHost(req)) { req->send(403,"application/json","{\"error\":\"Cross-origin request blocked\"}"); return; }
        if (rejectIfBodyTooLarge(req,total)) return;
        req->_tempObject=new String();
      }
      String* body=(String*)req->_tempObject; if (!body) return;
      body->concat((const char*)data,len);
      if (index+len!=total) return;
      StaticJsonDocument<192> doc; DeserializationError err=deserializeJson(doc,*body);
      delete body; req->_tempObject=nullptr;
      if (err) { req->send(400,"application/json","{\"error\":\"Bad JSON\"}"); return; }
      const char* city=doc["city"]|""; const char* country=doc["country"]|"";
      for (const char* c=city;*c;c++) if ((uint8_t)*c<0x20) { req->send(400,"application/json","{\"error\":\"Invalid city\"}"); return; }
      for (const char* c=country;*c;c++) if ((uint8_t)*c<0x20) { req->send(400,"application/json","{\"error\":\"Invalid country\"}"); return; }
      if (strlen(city)==0) {
        cfg.weatherCity[0]='\0'; cfg.weatherCountry[0]='\0';
        if (xSemaphoreTake(overrideMutex,pdMS_TO_TICKS(100))==pdTRUE) { memset(&savedWeather,0,sizeof(savedWeather)); xSemaphoreGive(overrideMutex); }
        saveConfig();
        req->send(200,"application/json","{\"ok\":true,\"msg\":\"Saved weather location cleared.\"}");
        return;
      }
      strlcpy(cfg.weatherCity,city,sizeof(cfg.weatherCity)); strlcpy(cfg.weatherCountry,country,sizeof(cfg.weatherCountry));
      saveConfig();
      bool ok=fetchPersistentWeather();
      if (ok) req->send(200,"application/json","{\"ok\":true,\"msg\":\"Location saved and real weather fetched.\"}");
      else req->send(200,"application/json","{\"ok\":true,\"msg\":\"Location saved. Waiting for Internet/API availability.\"}");
    }
  );

  server.on("/api/weather-search", HTTP_POST,
    [](AsyncWebServerRequest* req){}, nullptr,
    [](AsyncWebServerRequest* req, uint8_t* data, size_t len, size_t index, size_t total) {
      
      if (index==0) {
        if (!checkBasicAuth(req)) return;
        if (!checkOriginSameHost(req)) { req->send(403,"application/json","{\"error\":\"Cross-origin request blocked\"}"); return; }
        if (rejectIfBodyTooLarge(req, total)) return;
        req->_tempObject=new String();
      }
      String* body=(String*)req->_tempObject;
      if (!body) return;
      body->concat((const char*)data,len);
      if (index+len!=total) return;
      if (cfg.owmApiKey[0]=='\0') { delete body; req->_tempObject=nullptr; req->send(400,"application/json","{\"error\":\"OWM API key not set.\"}"); return; }
      StaticJsonDocument<128> doc;
      DeserializationError err=deserializeJson(doc,*body);
      delete body; req->_tempObject=nullptr;
      if (err) { req->send(400,"application/json","{\"error\":\"Bad JSON\"}"); return; }
      const char* city=doc["city"]|""; const char* country=doc["country"]|"";
      if (strlen(city)==0) { req->send(400,"application/json","{\"error\":\"city required\"}"); return; }
      
      for (const char* c = city; *c; c++)    { if ((uint8_t)*c < 0x20) { req->send(400,"application/json","{\"error\":\"Invalid city\"}"); return; } }
      for (const char* c = country; *c; c++) { if ((uint8_t)*c < 0x20) { req->send(400,"application/json","{\"error\":\"Invalid country\"}"); return; } }
      struct FP { char city[48]; char country[8]; };
      FP* fp=new FP(); strlcpy(fp->city,city,sizeof(fp->city)); strlcpy(fp->country,country,sizeof(fp->country));
      if (fetchWxRunning) { delete fp; req->send(429,"application/json","{\"error\":\"Search already in progress.\"}"); return; }
      fetchWxRunning=true;
      if (xTaskCreate([](void* arg){
        FP* p=(FP*)arg;
        fetchWeatherAndTime(p->city,p->country);
        delete p;
        
        fetchWxRunning=false;
        vTaskDelete(NULL);
      },"fetchWx",8192,fp,2,NULL)==pdPASS) {
        req->send(200,"application/json","{\"ok\":true,\"msg\":\"Fetching...\"}");
      } else { delete fp; req->send(503,"application/json","{\"error\":\"Server busy.\"}"); }
    }
  );

  server.on("/api/override-status", HTTP_GET, [](AsyncWebServerRequest* req) {
    
    if (!checkBasicAuth(req)) return;
    StaticJsonDocument<256> doc;
    bool active=(displayState==DISP_OVERRIDE);
    doc["active"]=active;
    uint32_t now=millis();
    doc["remaining_s"]=(active&&overrideEndMs>now)?(overrideEndMs-now)/1000:0;
    if (active && xSemaphoreTake(overrideMutex,pdMS_TO_TICKS(100))==pdTRUE) {
      doc["city"]=overrideData.city; doc["temp"]=overrideData.tempC; doc["humidity"]=overrideData.humidity;
      doc["pressure"]=overrideData.pressure; doc["description"]=overrideData.description;
      doc["wind"]=overrideData.windSpeed; doc["clouds"]=overrideData.clouds;
      doc["time"]=overrideData.timeStr; doc["feels_like"]=overrideData.feelsLike; doc["timezone"]=overrideData.timezone;
      xSemaphoreGive(overrideMutex);
    }
    String out; serializeJson(doc,out); req->send(200,"application/json",out);
  });

  server.on("/api/log", HTTP_GET, [](AsyncWebServerRequest* req) {
    if (!checkApiKey(req)) return;
    uint16_t limit=min((uint16_t)50,logCount);
    String out; out.reserve(limit*80+32);
    out="{\"count\":"; out+=logCount; out+=",\"records\":[";
    for (uint16_t i=0;i<limit;i++) {
      uint16_t idx=(logHead-limit+i+MAX_LOG_RECORDS)%MAX_LOG_RECORDS;
      if (i>0) out+=',';
      char rec[100];
      snprintf(rec,sizeof(rec),"{\"ts\":%lu,\"temp\":%.2f,\"humidity\":%.2f,\"pressure\":%.2f,\"lux\":%.1f,\"outdoor\":%.2f}",(unsigned long)logRing[idx].ts,logRing[idx].temp,logRing[idx].humidity,logRing[idx].pressure,logRing[idx].lux,logRing[idx].outdoorTemp);
      out+=rec;
    }
    out+="]}"; req->send(200,"application/json",out);
  });

  server.on("/log.csv", HTTP_GET, [](AsyncWebServerRequest* req) {
    if (!checkBasicAuth(req)) return;
    char path[16]; snprintf(path,sizeof(path),"/log.%u.csv",activeLog);
    if (LittleFS.exists(path)) req->send(LittleFS,path,"text/csv",true); else req->send(404,"text/plain","No log yet.");
  });

  server.on("/log/clear", HTTP_POST, [](AsyncWebServerRequest* req) {
    
    uint32_t ip = clientIpToU32(req); uint32_t retryMs=0;
    if (isAuthLocked(ip,&retryMs)) { req->send(429,"text/plain","Too many attempts. Try later."); return; }
    if (!req->hasHeader("X-Clear-Token") || !secureCompare(req->header("X-Clear-Token").c_str(), sec.logClearToken)) {
      recordAuthFailure(ip); req->send(403,"text/plain","Forbidden"); return;
    }
    recordAuthSuccess(ip);
    if (xSemaphoreTake(fsMutex,pdMS_TO_TICKS(500))==pdTRUE) { LittleFS.remove("/log.0.csv"); LittleFS.remove("/log.1.csv"); xSemaphoreGive(fsMutex); }
    logCount=0; logHead=0; activeLog=0;
    req->send(200,"text/plain","Log cleared.");
  });

  server.on("/api/system", HTTP_GET, [](AsyncWebServerRequest* req) {
    
    if (!checkBasicAuth(req)) return;
    StaticJsonDocument<384> doc;
    doc["firmware"]=FIRMWARE_VERSION; doc["free_heap"]=esp_get_free_heap_size();
    doc["min_heap"]=esp_get_minimum_free_heap_size(); doc["uptime_s"]=millis()/1000UL;
    doc["fs_used"]=(uint32_t)LittleFS.usedBytes(); doc["fs_total"]=(uint32_t)LittleFS.totalBytes();
    doc["rssi"]=WiFi.RSSI(); doc["mac"]=WiFi.macAddress(); doc["chip_rev"]=ESP.getChipRevision(); doc["cpu_mhz"]=ESP.getCpuFreqMHz();
    
    doc["safe_mode"]=safeModeActive; doc["safe_mode_reason"]=safeModeActive?safeModeReason:"";
    doc["boot_confirmed"]=bootConfirmed; doc["oled_ok"]=oledOK;
    doc["i2c_recoveries"]=(uint32_t)i2cRecoveryCount; doc["wifi_hard_resets"]=(uint32_t)wifiHardResetCount;
    String out; serializeJson(doc,out); req->send(200,"application/json",out);
  });

  server.on("/api/restart", HTTP_POST, [](AsyncWebServerRequest* req) {
    if (!checkBasicAuth(req)) return;
    if (!checkOriginSameHost(req)) { req->send(403,"application/json","{\"error\":\"Cross-origin request blocked\"}"); return; }
    logEvent(EVT_WARN,"Manual restart requested via dashboard");
    req->send(200,"application/json","{\"ok\":true,\"msg\":\"Restarting...\"}");
    xTaskCreate([](void*){ vTaskDelay(pdMS_TO_TICKS(600)); esp_restart(); },"manualRestart",2048,NULL,1,NULL);
  });

  server.on("/api/wifi/status", HTTP_GET, [](AsyncWebServerRequest* req) {
    
    if (!checkBasicAuth(req)) return;
    StaticJsonDocument<192> doc;
    doc["connected"]=(WiFi.status()==WL_CONNECTED);
    doc["offline_mode"]=(bool)offlineMode;
    doc["ssid"]=WiFi.status()==WL_CONNECTED?WiFi.SSID():"";
    doc["ip"]=WiFi.status()==WL_CONNECTED?WiFi.localIP().toString():"";
    doc["rssi"]=WiFi.status()==WL_CONNECTED?WiFi.RSSI():0;
    doc["ap_ssid"]=AP_SSID; doc["ap_ip"]=apIpStr;
    doc["ap_stations"]=WiFi.softAPgetStationNum();
    String out; serializeJson(doc,out); req->send(200,"application/json",out);
  });

  server.on("/api/wifi/reconnect", HTTP_POST, [](AsyncWebServerRequest* req) {
    if (!checkBasicAuth(req)) return;
    
    if (!checkOriginSameHost(req)) { req->send(403,"application/json","{\"error\":\"Cross-origin request blocked\"}"); return; }
    
    struct RW {}; RW* rw=new RW();
    if (xTaskCreate([](void* arg){ delete (RW*)arg; WiFi.reconnect(); vTaskDelay(pdMS_TO_TICKS(8000)); if(WiFi.status()==WL_CONNECTED){offlineMode=false;logEvent(EVT_INFO,"WiFi reconnected after manual trigger");} vTaskDelete(NULL); },"wifiRecon",3072,rw,1,NULL)!=pdPASS) delete rw;
    req->send(200,"application/json","{\"ok\":true,\"msg\":\"Reconnect triggered.\"}");
  });

  server.on("/api/wifi/forget", HTTP_POST, [](AsyncWebServerRequest* req) {
    if (!checkBasicAuth(req)) return;
    if (!checkOriginSameHost(req)) { req->send(403,"application/json","{\"error\":\"Cross-origin request blocked\"}"); return; }
    memset(cfg.wifiSsid, 0, sizeof(cfg.wifiSsid));
    memset(cfg.wifiPass, 0, sizeof(cfg.wifiPass));
    saveConfig();
    WiFi.disconnect(false);
    req->send(200,"application/json","{\"ok\":true,\"msg\":\"WiFi credentials cleared. Restart to apply.\"}");
  });

  server.on("/api/ap/config", HTTP_POST,
    [](AsyncWebServerRequest* req){}, nullptr,
    [](AsyncWebServerRequest* req, uint8_t* data, size_t len, size_t index, size_t total) {
      
      if (index==0) {
        if (!checkBasicAuth(req)) return;
        if (!checkOriginSameHost(req)) { req->send(403,"application/json","{\"error\":\"Cross-origin request blocked\"}"); return; }
        if (rejectIfBodyTooLarge(req, total)) return;
        req->_tempObject=new String();
      }
      String* body=(String*)req->_tempObject;
      if (!body) return;
      body->concat((const char*)data,len);
      if (index+len!=total) return;
      StaticJsonDocument<128> doc;
      DeserializationError err=deserializeJson(doc,*body);
      delete body; req->_tempObject=nullptr;
      if (err) { req->send(400,"application/json","{\"error\":\"Bad JSON\"}"); return; }
      if (doc.containsKey("ap_pass")) {
        const char* newPass = doc["ap_pass"]|FALLBACK_AP_PASS;
        if (strlen(newPass) < 8) { req->send(400,"application/json","{\"error\":\"AP password min 8 chars\"}"); return; }
        strlcpy(cfg.apPass,newPass,sizeof(cfg.apPass));
      }
      saveConfig();
      if (offlineMode) { WiFi.softAP(AP_SSID,cfg.apPass); logEvent(EVT_INFO,"AP password updated"); }
      req->send(200,"application/json","{\"ok\":true,\"msg\":\"AP config saved.\"}");
    }
  );

  ws.setAuthentication(sec.dashUser, sec.dashPass);
  ws.onEvent(onWsEvent);
  server.addHandler(&ws);
  server.onNotFound([](AsyncWebServerRequest* req){ req->send(404,"text/plain","Not found."); });
  server.begin();
  Serial.println(F("[Web] Server started."));
}

void setup() {
  Serial.begin(115200);
  Serial.println(F("\n\n[Boot] ADITYA WEATHER STATION"));
  pinMode(BUTTON_PIN, INPUT_PULLUP);
  bootStartMs = millis();

#if ESP_ARDUINO_VERSION >= ESP_ARDUINO_VERSION_VAL(3, 0, 0)
  {
    esp_task_wdt_config_t wdt_cfg;
    wdt_cfg.timeout_ms   = WDT_TIMEOUT_S * 1000;
    wdt_cfg.idle_core_mask = 0;
    wdt_cfg.trigger_panic  = true;
    
    esp_err_t wdt_err = esp_task_wdt_reconfigure(&wdt_cfg);
    if (wdt_err == ESP_ERR_INVALID_STATE) {
      
      esp_task_wdt_init(&wdt_cfg);
    }
  }
#else
  
  esp_task_wdt_init(WDT_TIMEOUT_S, true);
#endif
  esp_task_wdt_add(NULL);

  recordBootAttempt();
  if (safeModeActive) {
    Serial.printf("[SAFE MODE] %s — skipping sensors/peripherals, WiFi+web only.\n", safeModeReason);
  }

  esp_reset_reason_t rr = esp_reset_reason();
  const char* rrStr = "Unknown";
  switch (rr) {
    case ESP_RST_POWERON:   rrStr="Power-on";          break;
    case ESP_RST_SW:        rrStr="Software (restart)";break;
    case ESP_RST_PANIC:     rrStr="PANIC/Exception";   break;
    case ESP_RST_INT_WDT:   rrStr="Interrupt WDT";     break;
    case ESP_RST_TASK_WDT:  rrStr="Task WDT timeout";  break;
    case ESP_RST_WDT:       rrStr="Other WDT";         break;
    case ESP_RST_BROWNOUT:  rrStr="Brownout";          break;
    case ESP_RST_SDIO:      rrStr="SDIO";               break;
    default: break;
  }
  Serial.printf("[Boot] Reset reason: %s\n", rrStr);

  dataMutex    = xSemaphoreCreateMutex();
  displayMutex = xSemaphoreCreateMutex();
  overrideMutex= xSemaphoreCreateMutex();
  eventMutex   = xSemaphoreCreateMutex();
  wsMutex      = xSemaphoreCreateMutex();
  fsMutex      = xSemaphoreCreateMutex();
  authTableMutex = xSemaphoreCreateMutex();
  sessionMutex = xSemaphoreCreateMutex();
  if (!dataMutex||!displayMutex||!overrideMutex||!eventMutex||!wsMutex||!fsMutex||!authTableMutex||!sessionMutex) {
    
    Serial.println(F("[RTOS] Mutex allocation FAILED — rebooting"));
    for(;;) { esp_task_wdt_reset(); vTaskDelay(pdMS_TO_TICKS(1000)); }
  }

  Wire.begin(I2C_SDA_PIN, I2C_SCL_PIN); Wire.setClock(400000);

  uint8_t oledAddrDetected=0;
  for (uint8_t attempt=0; attempt<3 && !oledAddrDetected; attempt++) {
    Wire.beginTransmission(OLED_ADDR);
    if (Wire.endTransmission()==0) oledAddrDetected=OLED_ADDR;
    else {
      Wire.beginTransmission(0x3D);
      if (Wire.endTransmission()==0) oledAddrDetected=0x3D;
    }
    if (!oledAddrDetected) delay(120);
  }
  if (oledAddrDetected) oledOK=display.begin(SSD1306_SWITCHCAPVCC,oledAddrDetected);
  else oledOK=false;
  if (!oledOK) {
    Serial.println(F("[OLED] Init FAILED at 0x3C/0x3D — continuing without local display."));
    logEvent(EVT_ERROR,"OLED display not detected at boot");
  } else {
    Serial.printf("[OLED] SSD1306 detected at 0x%02X\n",oledAddrDetected);
    display.setFont(NULL); display.cp437(true); display.clearDisplay();
    showBootSequence();
  }

  if (!safeModeActive) {
    dht.begin();
    if (!bme.begin(BME_ADDR)) { if(oledOK){showSplash("BME280 Error!","Check wiring"); delay(1500);} logEvent(EVT_WARN,"BME280 not detected at boot"); }
    else { bme.setSampling(Adafruit_BME280::MODE_FORCED,Adafruit_BME280::SAMPLING_X2,Adafruit_BME280::SAMPLING_X2,Adafruit_BME280::SAMPLING_X2,Adafruit_BME280::FILTER_X2); }
    if (!lightMeter.begin(BH1750::CONTINUOUS_HIGH_RES_MODE,BH_ADDR,&Wire)) { if(oledOK){showSplash("BH1750 Error!","Check wiring"); delay(1500);} logEvent(EVT_WARN,"BH1750 not detected at boot"); }
    ds18b20.begin();
  }

  if (!LittleFS.begin(true)) { Serial.println(F("[LittleFS] Mount failed.")); logEvent(EVT_ERROR,"LittleFS mount failed"); }
  else { Serial.printf("[LittleFS] %u/%u bytes\n",LittleFS.usedBytes(),LittleFS.totalBytes()); }

  loadConfig();
  loadSecurityConfig();
  if (sec.usingFactoryDefaults) {
    logEvent(EVT_WARN, "Security: factory-default credentials still active — change via /api/security/credentials");
  }

  WiFi.mode(WIFI_AP_STA);
  WiFi.softAP(AP_SSID, cfg.apPass);
  delay(200);

  strncpy(apIpStr, WiFi.softAPIP().toString().c_str(), sizeof(apIpStr) - 1);
  apIpStr[sizeof(apIpStr) - 1] = '\0';

  startCaptiveDns();

  Serial.printf("[AP] Weather Station AP started: %s  IP: %s\n",
                AP_SSID, apIpStr);

  if (oledOK) {
    char apMsg[20];
    snprintf(apMsg, sizeof(apMsg), "AP: %s", apIpStr);
    showSplash("Dashboard AP Ready", apMsg);
  }

  bool wifiOK = false;

  if (cfg.wifiSsid[0] != '\0') {
    showSplash("Connecting WiFi...", cfg.wifiSsid);
    wifiOK = connectHiddenWifi(cfg.wifiSsid, cfg.wifiPass);
  }

  if (wifiOK) {
    showSplash("WiFi OK!", WiFi.localIP().toString().c_str());
    offlineMode = false;
  } else {
    
    offlineMode = true;
    Serial.printf("[AP] Dashboard available at http://%s/\n", apIpStr);
    showSplash("Dashboard Ready", apIpStr);
  }
  delay(800);

  if (wifiOK&&MDNS.begin(MDNS_NAME)) { MDNS.addService("http","tcp",80); Serial.printf("[mDNS] http://%s.local\n",MDNS_NAME); }

  configTzTime(cfg.tzStr,"pool.ntp.org","time.nist.gov");
  if (wifiOK) {
    showSplash("Syncing NTP...","Please wait");
    struct tm ti; uint8_t ntpTries=0;
    while (!getLocalTime(&ti)&&ntpTries<20) { esp_task_wdt_reset(); delay(500); ntpTries++; }
    if (ntpTries<20) { char tbuf[10]; snprintf(tbuf,sizeof(tbuf),"%02d:%02d:%02d",ti.tm_hour,ti.tm_min,ti.tm_sec); showSplash("Time Synced!",tbuf); }
    else { showSplash("NTP Failed","Check WiFi"); }
    delay(800);
  } else {
    showSplash("Offline Mode","NTP skipped");
    delay(600);
  }

  setupWebServer();

  if (wifiOK && cfg.weatherCity[0]!='\0' && cfg.owmApiKey[0]!='\0') {
    esp_task_wdt_reset();
    fetchPersistentWeather();
    esp_task_wdt_reset();
  }

  if (!safeModeActive) {
    float it=dht.readTemperature(),ih=dht.readHumidity();
    if (!isnan(it)&&!isnan(ih)) { dhtHasLastGood=true; sd.dhtTemp=it; sd.dhtHumidity=ih; sd.heatIndex=computeHeatIndex(it,ih); sd.dewPoint=computeDewPoint(it,ih); sd.dhtOK=true; }
    bme.takeForcedMeasurement(); float ibP=bme.readPressure()/100.0f;
    if (ibP>850.0f&&ibP<1100.0f) { bmeHasLastGood=true; sd.bmeTemp=bme.readTemperature(); sd.bmePressure=ibP; sd.bmeHumidity=bme.readHumidity(); sd.altitudeM=bme.readAltitude(SEA_LEVEL_HPA); sd.bmeOK=true; pressBuf[0]=ibP; pressBufIdx=1; }
    float ilux=lightMeter.readLightLevel(); if (ilux>=0.0f&&ilux<200000.0f) { bhHasLastGood=true; sd.lux=ilux; sd.bh1750OK=true; }
    ds18b20.requestTemperatures(); float ids=ds18b20.getTempCByIndex(0);
    if (ids!=DEVICE_DISCONNECTED_C&&ids>-55.0f&&ids<125.0f) { dsHasLastGood=true; sd.ds18Temp[0]=ids; sd.ds18Min=ids; sd.ds18Max=ids; sd.ds18MinMaxInit=true; sd.ds18Count=ds18b20.getDeviceCount(); sd.ds18OK=true; }
    sd.dhtHealth=sd.dhtOK?HEALTH_ONLINE:HEALTH_ERROR; sd.bmeHealth=sd.bmeOK?HEALTH_ONLINE:HEALTH_ERROR;
    sd.bhHealth=sd.bh1750OK?HEALTH_ONLINE:HEALTH_ERROR; sd.dsHealth=sd.ds18OK?HEALTH_ONLINE:HEALTH_ERROR;
    sd.dhtConfidence=sd.dhtOK?100:0; sd.bmeConfidence=sd.bmeOK?100:0; sd.bhConfidence=sd.bh1750OK?100:0; sd.dsConfidence=sd.ds18OK?100:0;
  }

  BaseType_t ok;
  ok=xTaskCreatePinnedToCore(sensorTask,     "Sensor",  5120,NULL,2,&hSensorTask, 1); if (ok!=pdPASS) safeRestart("Sensor task failed");
  ok=xTaskCreatePinnedToCore(displayTask,    "Display", 4096,NULL,3,&hDisplayTask,0); if (ok!=pdPASS) safeRestart("Display task failed");
  ok=xTaskCreatePinnedToCore(wifiTask,       "WiFi",    4096,NULL,1,&hWifiTask,   1); if (ok!=pdPASS) safeRestart("WiFi task failed");
  ok=xTaskCreatePinnedToCore(systemHealthTask,"Health", 3072,NULL,1,&hHealthTask, 0); if (ok!=pdPASS) safeRestart("Health task failed");

  lastAutoCycleMs=millis();
  Serial.println(F("[Boot] All systems online."));
  if (wifiOK) Serial.printf("[Web] http://%s\n",WiFi.localIP().toString().c_str());
}

void loop() {
  esp_task_wdt_reset();

  captiveDns.processNextRequest();
  vTaskDelay(pdMS_TO_TICKS(10));
}
