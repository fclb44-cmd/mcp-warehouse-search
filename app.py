# MCP-сервер для поиска товаров на складах поставщиков
# Версия 2.0.0 — совместимость с GPTunnel

from flask import Flask, request, jsonify
import json
import pandas as pd
import io
import os
import logging
import time
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ============================================================
# ЗАГРУЗКА КОНФИГУРАЦИИ
# ============================================================

with open('suppliers_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

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
        logger.info("📥 Загрузка ИП Татаренко...")
        with open('tatarenko_2026-02-11.xls', 'rb') as f:
            file_content = f.read()
        
        df = pd.read_excel(io.BytesIO(file_content), sheet_name=0, header=None, engine='xlrd')
        
        start_row = None
        for i, row in df.iterrows():
            if 'Товар' in str(row.values):
                start_row = i + 2
                break
        
        if start_row:
            current_category = ""
            for i in range(start_row, len(df)):
                row = df.iloc[i]
                
                name = str(row.iloc[1]) if len(row) > 1 and pd.notna(row.iloc[1]) else ""
                
                if "Бирюса -" in name or "БИРЮСА -" in name:
                    current_category = name
                    continue
                
                if name.strip() == "" or name == "nan":
                    continue
                
                if "Бирюса" in name or "БИРЮСА" in name:
                    specs = str(row.iloc[2]) if len(row) > 2 and pd.notna(row.iloc[2]) else ""
                    color = str(row.iloc[3]) if len(row) > 3 and pd.notna(row.iloc[3]) else ""
                    
                    retail_price = None
                    if len(row) > 4 and pd.notna(row.iloc[4]):
                        try:
                            retail_price = float(row.iloc[4])
                        except:
                            pass
                    
                    wholesale_price = None
                    if len(row) > 6 and pd.notna(row.iloc[6]):
                        try:
                            wholesale_price = float(row.iloc[6])
                        except:
                            pass
                    
                    item = {
                        "name": name,
                        "specs": specs,
                        "color": color,
                        "category": current_category,
                        "retail_price": retail_price,
                        "wholesale_price": wholesale_price,
                        "source": "ИП Татаренко Т.С.",
                        "supplier_id": "tatarenko"
                    }
                    items.append(item)
                    
                    if len(items) >= 1000:
                        break
            
            CACHE["tatarenko"] = items
            CACHE["last_update"]["tatarenko"] = datetime.now().isoformat()
            logger.info(f"   ✅ ИП Татаренко: загружено {len(items)} товаров")
        
    except Exception as e:
        logger.error(f"   ❌ Ошибка загрузки ИП Татаренко: {e}")

# ============================================================
# ПОИСК ПО КЛЮЧЕВЫМ СЛОВАМ
# ============================================================

def search_in_cache(keywords, region=None):
    """Поиск товаров по ключевым словам"""
    keywords_lower = [k.lower() for k in keywords if k]
    if not keywords_lower:
        return []
    
    context = {"region": region} if region else {}
    suppliers_sorted = get_suppliers_by_priority(context)
    
    all_results = []
    
    for supplier in suppliers_sorted:
        supplier_id = supplier["id"]
        items = CACHE.get(supplier_id, [])
        
        for item in items:
            search_text = f"{item.get('name', '')} {item.get('specs', '')} {item.get('category', '')}".lower()
            
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
    
    all_results.sort(key=lambda x: (x.get("supplier_priority", 99), -x.get("match_count", 0)))
    return all_results

def get_suppliers_by_priority(context):
    suppliers = CONFIG["suppliers"]
    result = []
    
    for s in suppliers:
        priority = s["priority"]["default"]
        
        for rule in s["priority"].get("rules", []):
            condition = rule["condition"]
            field = condition["field"]
            value = condition["value"]
            
            context_value = context.get(field)
            if context_value:
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
# MCP ЭНДПОИНТ (ИСПРАВЛЕНО ДЛЯ GPTUNNEL)
# ============================================================

@app.route('/mcp', methods=['POST'])
def mcp_handler():
    # Логируем входящий запрос
    logger.info("=" * 50)
    logger.info("📨 MCP ЗАПРОС ПОЛУЧЕН")
    
    data = request.json
    method = data.get('method')
    request_id = data.get('id')
    
    logger.info(f"   Метод: {method}")
    logger.info(f"   ID: {request_id}")
    
    # ========== initialize ==========
    if method == 'initialize':
        logger.info("   ✅ initialize")
        return jsonify({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": "warehouse-search",
                    "version": "2.0.0"
                }
            }
        })
    
    # ========== tools/list ==========
    elif method == 'tools/list':
        logger.info("   ✅ tools/list")
        return jsonify({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": [
                    {
                        "name": "search_warehouses",
                        "description": "Поиск товаров на складах поставщиков по ключевым словам. Приоритет: ИП Татаренко (Иркутская, Новосибирская обл.) → Partners Group → Merlion.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "equipment_type": {"type": "string"},
                                "keywords": {"type": "array", "items": {"type": "string"}},
                                "region": {"type": "string"}
                            },
                            "required": ["equipment_type", "keywords"]
                        }
                    },
                    {
                        "name": "get_cache_status",
                        "description": "Получить статус кеша поставщиков",
                        "inputSchema": {"type": "object", "properties": {}}
                    }
                ]
            }
        })
    
    # ========== tools/call ==========
    elif method == 'tools/call':
        params = data.get('params', {})
        tool_name = params.get('name')
        arguments = params.get('arguments', {})
        
        logger.info(f"   Инструмент: {tool_name}")
        logger.info(f"   Аргументы: {json.dumps(arguments, ensure_ascii=False)[:200]}")
        
        if tool_name == 'search_warehouses':
            equipment_type = arguments.get('equipment_type', '')
            keywords = arguments.get('keywords', [])
            region = arguments.get('region')
            
            all_keywords = [equipment_type] + keywords
            results = search_in_cache(all_keywords, region)
            
            logger.info(f"   🔍 Найдено товаров: {len(results)}")
            
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
                    "retail_price": r.get("retail_price"),
                    "wholesale_price": r.get("wholesale_price"),
                    "matched_keywords": r.get("matched_keywords", []),
                    "match_count": r.get("match_count", 0)
                }
                grouped[supplier_id]["items"].append(item)
            
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
        
        logger.warning(f"   ⚠️ Неизвестный инструмент: {tool_name}")
    
    logger.warning(f"   ❌ Метод не найден: {method}")
    return jsonify({
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"}
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
# ЗАПУСК
# ============================================================

def initialize_cache():
    logger.info("=" * 50)
    logger.info("🚀 MCP-сервер v2.0.0 (совместим с GPTunnel)")
    logger.info("=" * 50)
    
    load_tatarenko()
    
    logger.info("=" * 50)
    logger.info(f"📊 ИТОГО: {len(CACHE['tatarenko'])} товаров")
    logger.info("=" * 50)

initialize_cache()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    logger.info(f"🌐 Сервер на порту {port}")
    app.run(host='0.0.0.0', port=port)
