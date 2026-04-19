# MCP-сервер для поиска товаров на складах поставщиков
# Версия 1.0.4 (с отладкой)

from flask import Flask, request, jsonify
import json
import requests
import pandas as pd
import io
import time
import threading
from datetime import datetime
import os
import logging

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
# ЗАГРУЗКА ПРАЙС-ЛИСТА ИП ТАТАРЕНКО (с отладкой)
# ============================================================

def load_tatarenko():
    """Загружает прайс-лист ИП Татаренко из XLS"""
    items = []
    try:
        logger.info("📥 Загрузка ИП Татаренко...")
        logger.info("   📂 Открываем файл tatarenko_2026-02-11.xls...")
        with open('tatarenko_2026-02-11.xls', 'rb') as f:
            file_content = f.read()
        
        logger.info("   📊 Читаем Excel...")
        df = pd.read_excel(io.BytesIO(file_content), sheet_name=0, header=None, engine='xlrd')
        
        # ОТЛАДКА: показываем первые 30 строк
        logger.info("   🔍 ПЕРВЫЕ 30 СТРОК ФАЙЛА:")
        for i in range(min(30, len(df))):
            row = df.iloc[i]
            first_cells = []
            for j in range(min(8, len(row))):
                cell = row.iloc[j]
                if pd.notna(cell):
                    val = str(cell)[:40]
                    first_cells.append(val)
                else:
                    first_cells.append("")
            logger.info(f"      {i}: {first_cells}")
        
        # Ищем строку-заголовок с "Товар"
        start_row = None
        for i, row in df.iterrows():
            row_str = str(row.values)
            if 'Товар' in row_str:
                logger.info(f"   🎯 Найдена строка 'Товар': {i}")
                start_row = i + 2
                break
        
        if not start_row:
            # Пробуем найти "Наименование"
            for i, row in df.iterrows():
                row_str = str(row.values)
                if 'Наименование' in row_str:
                    logger.info(f"   🎯 Найдена строка 'Наименование': {i}")
                    start_row = i + 1
                    break
        
        if start_row:
            logger.info(f"   📋 Начинаем парсинг со строки {start_row}")
            current_category = ""
            
            for i in range(start_row, len(df)):
                row = df.iloc[i]
                
                # Первая ячейка
                col_a = str(row.iloc[0]) if len(row) > 0 and pd.notna(row.iloc[0]) else ""
                
                # Проверяем, не заголовок ли это категории
                if "Бирюса" in col_a and len(col_a) > 10:
                    current_category = col_a
                    continue
                
                # Пропускаем пустые строки
                if col_a.strip() == "" or col_a == "nan":
                    continue
                
                # Проверяем, что это товар (содержит "Бирюса")
                if "Бирюса" in col_a:
                    item = {
                        "name": col_a,
                        "category": current_category,
                        "source": "ИП Татаренко Т.С.",
                        "supplier_id": "tatarenko"
                    }
                    items.append(item)
                    
                    # Ограничиваем для экономии памяти
                    if len(items) >= 500:
                        logger.info("   ⚠️ Достигнут лимит 500 товаров")
                        break
            
            CACHE["tatarenko"] = items
            CACHE["last_update"]["tatarenko"] = datetime.now().isoformat()
            logger.info(f"   ✅ ИП Татаренко: загружено {len(items)} товаров")
        else:
            logger.error("   ❌ Не найдена строка-заголовок с 'Товар' или 'Наименование'")
        
    except Exception as e:
        logger.error(f"   ❌ Ошибка: {e}")
        import traceback
        logger.error(traceback.format_exc())

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
            search_text = f"{item.get('name', '')} {item.get('category', '')}".lower()
            
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
    """Возвращает поставщиков по приоритету"""
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
# MCP ЭНДПОИНТ
# ============================================================

@app.route('/mcp', methods=['POST'])
def mcp_handler():
    data = request.json
    method = data.get('method')
    request_id = data.get('id')
    
    if method == 'initialize':
        return jsonify({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": "warehouse-search",
                    "version": "1.0.4"
                }
            }
        })
    
    elif method == 'tools/list':
        return jsonify({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": [
                    {
                        "name": "search_warehouses",
                        "description": "Поиск товаров на складах по ключевым словам",
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
                        "description": "Статус кеша",
                        "inputSchema": {"type": "object", "properties": {}}
                    }
                ]
            }
        })
    
    elif method == 'tools/call':
        params = data.get('params', {})
        tool_name = params.get('name')
        arguments = params.get('arguments', {})
        
        if tool_name == 'search_warehouses':
            equipment_type = arguments.get('equipment_type', '')
            keywords = arguments.get('keywords', [])
            region = arguments.get('region')
            
            all_keywords = [equipment_type] + keywords
            results = search_in_cache(all_keywords, region)
            
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
# ЗАПУСК
# ============================================================

def initialize_cache():
    """Инициализация кеша"""
    logger.info("=" * 50)
    logger.info("🚀 MCP-сервер поиска по складам v1.0.4")
    logger.info("=" * 50)
    
    load_tatarenko()
    
    logger.info("=" * 50)
    logger.info(f"📊 ИТОГО в кеше:")
    logger.info(f"   ИП Татаренко: {len(CACHE['tatarenko'])} товаров")
    logger.info("=" * 50)

# Инициализация
initialize_cache()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    logger.info(f"🌐 Сервер на порту {port}")
    app.run(host='0.0.0.0', port=port)
