# MCP-сервер для поиска товаров на складах поставщиков
# Версия 1.0.0

from flask import Flask, request, jsonify
import json
import requests
import pandas as pd
import io
import time
import threading
import re
from datetime import datetime
from xml.etree import ElementTree as ET
import os

app = Flask(__name__)

# ============================================================
# ЗАГРУЗКА КОНФИГУРАЦИИ
# ============================================================

with open('suppliers_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

with open('tatarenko_2026-02-11.xls', 'rb') as f:
    TATARENKO_FILE = f.read()

# ============================================================
# КЕШ В ПАМЯТИ
# ============================================================

CACHE = {
    "tatarenko": [],
    "partners": [],
    "merlion": [],
    "last_update": {}
}

# ============================================================
# ЗАГРУЗКА ПРАЙС-ЛИСТА ИП ТАТАРЕНКО
# ============================================================

def load_tatarenko():
    """Загружает прайс-лист ИП Татаренко из XLS"""
    items = []
    try:
        df = pd.read_excel(io.BytesIO(TATARENKO_FILE), sheet_name=0, header=None)
        
        # Ищем начало таблицы (строка с "Товар")
        start_row = None
        for i, row in df.iterrows():
            if 'Товар' in str(row.values):
                start_row = i + 2
                break
        
        if start_row:
            current_category = ""
            for i in range(start_row, len(df)):
                row = df.iloc[i]
                
                # Проверяем, не заголовок ли это категории
                col_a = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                
                if "Бирюса" in col_a and len(col_a) > 10:
                    current_category = col_a
                    continue
                
                # Пропускаем пустые строки
                if pd.isna(row.iloc[0]) or str(row.iloc[0]).strip() == "":
                    continue
                
                name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                specs = str(row.iloc[1]) if len(row) > 1 and pd.notna(row.iloc[1]) else ""
                color = str(row.iloc[2]) if len(row) > 2 and pd.notna(row.iloc[2]) else ""
                
                # Цены (колонки E, F, G = индексы 4, 5, 6)
                retail_price = None
                wholesale_price = None
                
                if len(row) > 6:
                    retail_price = row.iloc[4] if pd.notna(row.iloc[4]) else None
                    wholesale_price = row.iloc[6] if pd.notna(row.iloc[6]) else row.iloc[5] if pd.notna(row.iloc[5]) else None
                
                if name and "Бирюса" in name:
                    item = {
                        "name": name,
                        "specs": specs,
                        "color": color,
                        "category": current_category,
                        "retail_price": float(retail_price) if retail_price and isinstance(retail_price, (int, float)) else None,
                        "wholesale_price": float(wholesale_price) if wholesale_price and isinstance(wholesale_price, (int, float)) else None,
                        "source": "ИП Татаренко Т.С.",
                        "supplier_id": "tatarenko"
                    }
                    items.append(item)
        
        CACHE["tatarenko"] = items
        CACHE["last_update"]["tatarenko"] = datetime.now().isoformat()
        print(f"✅ ИП Татаренко: загружено {len(items)} товаров")
        
    except Exception as e:
        print(f"❌ Ошибка загрузки ИП Татаренко: {e}")

# ============================================================
# ЗАГРУЗКА PARTNERS GROUP
# ============================================================

def load_partners():
    """Загружает каталог Partners Group через API"""
    items = []
    try:
        config = [s for s in CONFIG["suppliers"] if s["id"] == "partners_group"][0]
        auth = config["auth"]
        
        # Авторизация
        auth_resp = requests.post(
            config["api_url"],
            json={
                "request": {
                    "method": "login",
                    "model": "auth",
                    "module": "quickfox"
                },
                "data": {
                    "login": auth["login"],
                    "password": auth["password"]
                }
            },
            timeout=30
        )
        
        session = auth_resp.json().get("session")
        if not session:
            print("❌ Partners Group: не удалось получить сессию")
            return
        
        # Получение каталога
        catalog_resp = requests.get(
            f"https://b2b.i-t-p.pro/download/catalog/json/products_9.json",
            cookies={"session": session},
            timeout=60
        )
        
        products = catalog_resp.json()
        
        for p in products:
            item = {
                "name": p.get("name", ""),
                "vendor": p.get("vendor", ""),
                "part": p.get("part", ""),
                "sku": p.get("sku"),
                "category": p.get("category"),
                "source": "Partners Group",
                "supplier_id": "partners_group"
            }
            items.append(item)
        
        CACHE["partners"] = items
        CACHE["last_update"]["partners"] = datetime.now().isoformat()
        print(f"✅ Partners Group: загружено {len(items)} товаров")
        
    except Exception as e:
        print(f"❌ Ошибка загрузки Partners Group: {e}")

# ============================================================
# ЗАГРУЗКА MERLION
# ============================================================

def load_merlion():
    """Загружает каталог Merlion через SOAP API"""
    items = []
    try:
        config = [s for s in CONFIG["suppliers"] if s["id"] == "merlion"][0]
        auth = config["auth"]
        
        soap_request = f"""<?xml version="1.0" encoding="UTF-8"?>
        <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
            <SOAP-ENV:Body>
                <getItems xmlns="https://api.merlion.com/dl/mlservice3">
                    <cat_id></cat_id>
                    <item_id></item_id>
                    <shipment_method></shipment_method>
                    <page>0</page>
                    <rows_on_page>1000</rows_on_page>
                </getItems>
            </SOAP-ENV:Body>
        </SOAP-ENV:Envelope>"""
        
        response = requests.post(
            config["wsdl_url"],
            data=soap_request,
            headers={"Content-Type": "text/xml"},
            auth=(auth["login"], auth["password"]),
            timeout=60
        )
        
        # Простой парсинг XML (извлекаем названия)
        root = ET.fromstring(response.content)
        namespaces = {'ns': 'https://api.merlion.com/dl/mlservice3'}
        
        for item_elem in root.findall('.//item', namespaces):
            name_elem = item_elem.find('.//Name', namespaces)
            brand_elem = item_elem.find('.//Brand', namespaces)
            part_elem = item_elem.find('.//Vendor_part', namespaces)
            
            if name_elem is not None:
                item = {
                    "name": name_elem.text or "",
                    "vendor": brand_elem.text if brand_elem is not None else "",
                    "part": part_elem.text if part_elem is not None else "",
                    "source": "Merlion",
                    "supplier_id": "merlion"
                }
                items.append(item)
        
        CACHE["merlion"] = items
        CACHE["last_update"]["merlion"] = datetime.now().isoformat()
        print(f"✅ Merlion: загружено {len(items)} товаров")
        
    except Exception as e:
        print(f"❌ Ошибка загрузки Merlion: {e}")

# ============================================================
# ПОИСК ПО КЛЮЧЕВЫМ СЛОВАМ
# ============================================================

def search_in_cache(keywords, region=None):
    """Поиск товаров по ключевым словам с учётом приоритетов"""
    
    keywords_lower = [k.lower() for k in keywords if k]
    if not keywords_lower:
        return []
    
    # Определяем приоритеты поставщиков для региона
    context = {"region": region} if region else {}
    suppliers_sorted = get_suppliers_by_priority(context)
    
    all_results = []
    
    for supplier in suppliers_sorted:
        supplier_id = supplier["id"]
        items = CACHE.get(supplier_id, [])
        
        for item in items:
            # Текст для поиска
            search_text = f"{item.get('name', '')} {item.get('specs', '')} {item.get('vendor', '')} {item.get('category', '')}".lower()
            
            # Подсчёт совпадений
            matched = []
            for kw in keywords_lower:
                if kw in search_text:
                    matched.append(kw)
            
            if matched:
                item_copy = item.copy()
                item_copy["matched_keywords"] = matched
                item_copy["match_count"] = len(matched)
                item_copy["supplier_priority"] = supplier.get("_priority", 99)
                all_results.append(item_copy)
    
    # Сортировка: сначала по приоритету поставщика, потом по количеству совпадений
    all_results.sort(key=lambda x: (x.get("supplier_priority", 99), -x.get("match_count", 0)))
    
    return all_results

def get_suppliers_by_priority(context):
    """Возвращает поставщиков, отсортированных по приоритету для контекста"""
    suppliers = CONFIG["suppliers"]
    result = []
    
    for s in suppliers:
        priority = s["priority"]["default"]
        
        # Проверяем правила
        for rule in s["priority"].get("rules", []):
            condition = rule["condition"]
            field = condition["field"]
            operator = condition["operator"]
            value = condition["value"]
            
            context_value = context.get(field)
            if context_value:
                if operator == "in":
                    ctx_norm = context_value.lower().replace(" область", "").replace(" край", "")
                    val_norm = [v.lower().replace(" область", "").replace(" край", "") for v in value]
                    if ctx_norm in val_norm:
                        priority = rule["priority"]
        
        result.append({
            "id": s["id"],
            "name": s["name"],
            "_priority": priority
        })
    
    result.sort(key=lambda x: x["_priority"])
    return result

# ============================================================
# MCP ЭНДПОИНТ
# ============================================================

@app.route('/mcp', methods=['POST'])
def mcp_handler():
    data = request.json
    method = data.get('method')
    request_id = data.get('id')
    
    # ========== initialize ==========
    if method == 'initialize':
        return jsonify({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": "warehouse-search",
                    "version": "1.0.0"
                }
            }
        })
    
    # ========== tools/list ==========
    elif method == 'tools/list':
        return jsonify({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": [
                    {
                        "name": "search_warehouses",
                        "description": "Поиск товаров на складах поставщиков по ключевым словам. Приоритет: ИП Татаренко (для Иркутской и Новосибирской обл.) → Partners Group → Merlion.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "equipment_type": {
                                    "type": "string",
                                    "description": "Тип оборудования (холодильник, стиральная машина и т.д.)"
                                },
                                "keywords": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Ключевые слова для поиска"
                                },
                                "region": {
                                    "type": "string",
                                    "description": "Регион поставки (для определения приоритета)"
                                }
                            },
                            "required": ["equipment_type", "keywords"]
                        }
                    },
                    {
                        "name": "get_cache_status",
                        "description": "Получить статус кеша поставщиков",
                        "inputSchema": {
                            "type": "object",
                            "properties": {}
                        }
                    }
                ]
            }
        })
    
    # ========== tools/call ==========
    elif method == 'tools/call':
        params = data.get('params', {})
        tool_name = params.get('name')
        arguments = params.get('arguments', {})
        
        if tool_name == 'search_warehouses':
            equipment_type = arguments.get('equipment_type', '')
            keywords = arguments.get('keywords', [])
            region = arguments.get('region')
            
            # Добавляем тип оборудования в ключевые слова
            all_keywords = [equipment_type] + keywords
            
            # Поиск
            results = search_in_cache(all_keywords, region)
            
            # Группируем по поставщикам
            grouped = {}
            for r in results[:30]:
                supplier_id = r.get("supplier_id")
                if supplier_id not in grouped:
                    grouped[supplier_id] = {
                        "supplier": r.get("source"),
                        "priority": r.get("supplier_priority"),
                        "items": []
                    }
                
                item = {
                    "name": r.get("name"),
                    "specs": r.get("specs"),
                    "color": r.get("color"),
                    "vendor": r.get("vendor"),
                    "part_number": r.get("part"),
                    "retail_price": r.get("retail_price"),
                    "wholesale_price": r.get("wholesale_price"),
                    "matched_keywords": r.get("matched_keywords", []),
                    "match_count": r.get("match_count", 0)
                }
                grouped[supplier_id]["items"].append(item)
            
            # Сортируем группы по приоритету
            grouped_list = sorted(grouped.values(), key=lambda x: x.get("priority", 99))
            
            return jsonify({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{
                        "type": "text",
                        "text": json.dumps({
                            "found": len(results) > 0,
                            "total_found": len(results),
                            "search_keywords": all_keywords,
                            "region": region,
                            "suppliers": grouped_list
                        }, ensure_ascii=False, indent=2)
                    }]
                }
            })
        
        elif tool_name == 'get_cache_status':
            status = {}
            for supplier_id, items in CACHE.items():
                if supplier_id != "last_update":
                    status[supplier_id] = {
                        "count": len(items),
                        "last_update": CACHE["last_update"].get(supplier_id)
                    }
            
            return jsonify({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{
                        "type": "text",
                        "text": json.dumps(status, ensure_ascii=False, indent=2)
                    }]
                }
            })
    
    return jsonify({
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": -32601, "message": "Method not found"}
    })

# ============================================================
# HEALTH CHECK
# ============================================================

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok",
        "cache": {
            "tatarenko": len(CACHE["tatarenko"]),
            "partners": len(CACHE["partners"]),
            "merlion": len(CACHE["merlion"])
        }
    })

# ============================================================
# ФОНОВОЕ ОБНОВЛЕНИЕ КЕША
# ============================================================

def update_cache():
    """Обновление кеша (запускается при старте и раз в сутки)"""
    while True:
        print("\n🔄 Обновление кеша поставщиков...")
        load_tatarenko()
        load_partners()
        load_merlion()
        print("✅ Обновление завершено\n")
        
        # Следующее обновление через 24 часа
        time.sleep(24 * 60 * 60)

# ============================================================
# ЗАПУСК
# ============================================================

if __name__ == '__main__':
    print("🚀 MCP-сервер поиска по складам")
    print("=" * 50)
    
    # Загружаем ИП Татаренко сразу (из файла)
    load_tatarenko()
    
    # Partners Group и Merlion — в фоне (могут занять время)
    threading.Thread(target=load_partners, daemon=True).start()
    threading.Thread(target=load_merlion, daemon=True).start()
    
    # Периодическое обновление
    threading.Thread(target=update_cache, daemon=True).start()
    
    port = int(os.environ.get('PORT', 5001))
    print(f"\n🌐 Сервер запущен на порту {port}")
    app.run(host='0.0.0.0', port=port)