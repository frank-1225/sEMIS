import sqlite3
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import atexit
import logging
import shutil

# ======================================================
# 配置与初始化
# ======================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*", "allow_headers": ["Content-Type", "X-Auth-Key"], "methods": ["GET", "POST", "OPTIONS"]}})

API_PASSWORD = os.getenv('API_PASSWORD', 'OldXiangHuang_600')
CSV_FILE = 'traindata.csv'
DB_FILE = 'train_data.db'
TABLE_NAME = 'traindata'
AUTH_KEY = 'temp_auth_token_for_demo'

db_conn = None

COLUMN_MAP = [
    "总序号", "车组号", "车内布局", "运用状态", "运用属性", "配属局", "配属段", "配属所",
    "车型", "批次", "制造厂", "制造日期", "最高运营速度（km/h）", "设计寿命（年）", "列车总长（m）",
    "车体最大宽度（mm）", "车体最大高度（mm）", "轮周牵引总功率（kW）", "停放制动能力", "编组方式",
    "双弓间距（m）", "受电弓位置（车厢）", "编组（辆）", "EMIS定员", "席位总数", "客票定员",
    "商务座-包间式", "商务座-鱼骨式", "商务座-标准型", "特等座", "优选一等座", "一等座", "二等座",
    "高级动卧", "动卧", "多功能座", "餐座", "无座", "非对号定员-餐座", "非对号定员-边座",
    "非对号定员-其他", "非对号定员-无座",
    "01车车种", "01车定员", "02车车种", "02车定员", "03车车种", "03车定员",
    "04车车种", "04车定员", "05车车种", "05车定员", "06车车种", "06车定员",
    "07车车种", "07车定员", "08车车种", "08车定员", "09车车种", "09车定员",
    "10车车种", "10车定员", "11车车种", "11车定员", "12车车种", "12车定员",
    "13车车种", "13车定员", "14车车种", "14车定员", "15车车种", "15车定员",
    "16车车种", "16车定员", "17车车种", "17车定员",
    "市域车所属线路", "特殊涂装", "备注", "座椅类型", "查询用车组号"
]

FILTER_COL_MAP = {
    'attr': '运用属性', 'bureau': '配属局', 'depot': '配属段', 'location': '配属所',
    'model': '车型', 'factory': '制造厂', 'car_count': '编组（辆）'
}

# ======================================================
# 数据库核心逻辑
# ======================================================

def get_db_connection():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def reload_database_from_csv():
    logging.info(f"正在重新加载数据: {CSV_FILE} -> {DB_FILE}...")
    try:
        df = pd.read_csv(CSV_FILE, encoding='utf-8', skipinitialspace=True, engine='python')
        df.columns = df.columns.str.strip()
        with sqlite3.connect(DB_FILE) as temp_conn:
            df.to_sql(TABLE_NAME, temp_conn, if_exists='replace', index=False)
            temp_conn.commit()
        logging.info(f"数据库重载成功，当前行数: {len(df)}")
        return True, len(df)
    except Exception as e:
        logging.error(f"数据库重载失败: {e}")
        return False, str(e)

def sync_db_to_csv():
    try:
        df = pd.read_sql_query(f"SELECT * FROM \"{TABLE_NAME}\"", db_conn)
        df.to_csv(CSV_FILE, index=False, encoding='utf-8')
        logging.info(f"已同步数据库更改到 {CSV_FILE}")
        return True
    except Exception as e:
        logging.error(f"同步 CSV 失败: {e}")
        return False

def init_sqlite_db():
    global db_conn
    if not os.path.exists(DB_FILE):
        logging.info("数据库不存在，执行初始化...")
        reload_database_from_csv()
    else:
        logging.info(f"数据库 {DB_FILE} 已存在，跳过初始化。")
    try:
        if db_conn is None: db_conn = get_db_connection()
    except Exception:
        db_conn = get_db_connection()

@atexit.register
def close_db_connection():
    if db_conn:
        db_conn.close()
        logging.info("数据库连接已关闭。")

with app.app_context():
    init_sqlite_db()

# ======================================================
# 辅助函数
# ======================================================

def build_where_clause(custom_filters, search_value):
    where_clauses = []
    params = []
    for key, col in FILTER_COL_MAP.items():
        values = custom_filters.get(key)
        clean_values = [v for v in values if v is not None and str(v).strip() != ''] if isinstance(values, list) else []
        if clean_values:
            placeholders = ', '.join(['?' for _ in clean_values])
            where_clauses.append(f"\"{col}\" IN ({placeholders})")
            params.extend(clean_values)
    
    for term in [custom_filters.get('train'), search_value]:
        if term and str(term).strip() != '':
            normalized = term.replace('-', '').strip()
            where_clauses.append(f"(\"查询用车组号\" LIKE ?)")
            params.append(f'%{normalized}%')
            
    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    return where_sql, params

def calculate_standard_sets_val(car_count_val):
    try:
        s = str(car_count_val).replace('辆', '').strip()
        count = float(s)
        if abs(count - 4) < 0.1: return 0.5
        if abs(count - 8) < 0.1: return 1.0
        if abs(count - 16) < 0.1: return 2.0
        if abs(count - 17) < 0.1: return 2.125
        return count / 8.0
    except Exception:
        return 0.0

# ======================================================
# 路由接口
# ======================================================

@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    if data.get('password') == API_PASSWORD:
        return jsonify({'message': 'Login successful', 'auth_key': AUTH_KEY}), 200
    return jsonify({'message': 'Invalid credentials'}), 401

@app.route('/api/upload_data', methods=['POST'])
def upload_data():
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != AUTH_KEY: return jsonify({'message': 'Authorization required'}), 401
    if 'file' not in request.files: return jsonify({'message': '没有上传文件'}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({'message': '未选择文件'}), 400
    try:
        if os.path.exists(CSV_FILE): shutil.copy(CSV_FILE, CSV_FILE + '.bak')
        file.save(CSV_FILE)
        success, info = reload_database_from_csv()
        if success: return jsonify({'message': f'上传成功！共 {info} 条记录。'}), 200
        else: return jsonify({'message': f'更新失败: {info}'}), 500
    except Exception as e:
        return jsonify({'message': f'服务器错误: {str(e)}'}), 500

@app.route('/api/traindata', methods=['POST'])
def get_filter_config():
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != AUTH_KEY: return jsonify({'message': 'Auth failed'}), 401
    try:
        filters = {}
        # 新增 '最高运营速度（km/h）'
        cols_to_filter = ['配属局', '配属段', '运用属性', '车型', '制造厂', '编组（辆）', '最高运营速度（km/h）']
        
        for col in cols_to_filter:
            distinct_df = pd.read_sql_query(f"SELECT DISTINCT \"{col}\" FROM \"{TABLE_NAME}\" WHERE \"{col}\" IS NOT NULL", db_conn)
            val_list = distinct_df[col].tolist()
            
            key_map = {
                '编组（辆）':'car_counts', '配属局':'bureaus', '配属段':'depots',
                '运用属性':'attrs', '车型':'models', '制造厂':'factories',
                '最高运营速度（km/h）': 'speeds' 
            }
            if col in key_map:
                filters[key_map[col]] = [str(x) for x in val_list]
            
        cols_to_fetch = ['"配属局"', '"配属段"', '"配属所"']
        map_data_df = pd.read_sql_query(f"SELECT DISTINCT {', '.join(cols_to_fetch)} FROM \"{TABLE_NAME}\"", db_conn)
        return jsonify({'map_data': map_data_df.to_dict('records'), 'filters': filters})
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.route('/api/serverside/traindata', methods=['POST'])
@app.route('/api/data', methods=['POST'])
def serverside_traindata():
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != AUTH_KEY: return jsonify({'message': 'Auth failed'}), 401
    try:
        data = request.get_json() or {}
        draw = int(data.get('draw', 0))
        start = int(data.get('start', 0))
        length = int(data.get('length', 25))
        where_sql, params = build_where_clause(data.get('custom_filters', {}), data.get('search', {}).get('value', ''))
        
        cursor = db_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM \"{TABLE_NAME}\"")
        records_total = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM \"{TABLE_NAME}\"{where_sql}", params)
        records_filtered = cursor.fetchone()[0]
        
        order_col_idx = data.get('order', [{}])[0].get('column')
        order_dir = data.get('order', [{}])[0].get('dir', 'asc')
        order_sql = " ORDER BY CAST(\"总序号\" AS INTEGER) ASC"
        if order_col_idx is not None and int(order_col_idx) < len(COLUMN_MAP):
             col = COLUMN_MAP[int(order_col_idx)]
             order_sql = f" ORDER BY \"{col}\" {order_dir}" if col != '总序号' else f" ORDER BY CAST(\"{col}\" AS INTEGER) {order_dir}"

        # 修复点：将列名拼接提取到 f-string 外部
        select_cols = ', '.join([f'"{c}"' for c in COLUMN_MAP])
        data_query = f"SELECT {select_cols} FROM \"{TABLE_NAME}\"{where_sql}{order_sql} LIMIT {length} OFFSET {start}"
        
        df_data = pd.read_sql_query(data_query, db_conn, params=params)
        return jsonify({"draw": draw, "recordsTotal": records_total, "recordsFiltered": records_filtered, "data": df_data.to_dict('records')})
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.route('/api/exportdata', methods=['POST'])
def export_data():
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != AUTH_KEY: return jsonify({'message': 'Auth failed'}), 401
    try:
        where_sql, params = build_where_clause(request.get_json().get('custom_filters', {}), "")
        df = pd.read_sql_query(f"SELECT * FROM \"{TABLE_NAME}\"{where_sql}", db_conn, params=params)
        return jsonify({'data': df.to_dict('records'), 'count': len(df)})
    except Exception as e:
        return jsonify({'message': str(e)}), 500

# ======================================================
# 后台管理 API
# ======================================================
@app.route('/api/manage/update', methods=['POST'])
def update_row():
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != AUTH_KEY: return jsonify({'message': 'Auth failed'}), 401
    data = request.get_json()
    row_id, updates = data.get('id'), data.get('data')
    if not row_id or not updates: return jsonify({'message': '缺少参数'}), 400
    try:
        set_clause, params = [], []
        for col, val in updates.items():
            if col in COLUMN_MAP and col != "总序号":
                set_clause.append(f"\"{col}\" = ?"); params.append(val)
        if not set_clause: return jsonify({'message': '无更新内容'}), 400
        params.append(row_id)
        db_conn.cursor().execute(f"UPDATE \"{TABLE_NAME}\" SET {', '.join(set_clause)} WHERE \"总序号\" = ?", params)
        db_conn.commit(); sync_db_to_csv()
        return jsonify({'message': '更新成功'}), 200
    except Exception as e: return jsonify({'message': str(e)}), 500

@app.route('/api/manage/add', methods=['POST'])
def add_row():
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != AUTH_KEY: return jsonify({'message': 'Auth failed'}), 401
    data = request.get_json()
    row_data = data.get('data')
    try:
        cursor = db_conn.cursor()
        cursor.execute(f"SELECT MAX(CAST(\"总序号\" AS INTEGER)) FROM \"{TABLE_NAME}\"")
        new_id = (cursor.fetchone()[0] or 0) + 1
        cols, vals, placeholders = ["总序号"], [new_id], ["?"]
        for col, val in row_data.items():
            if col in COLUMN_MAP:
                cols.append(f"\"{col}\""); vals.append(val); placeholders.append("?")
        cursor.execute(f"INSERT INTO \"{TABLE_NAME}\" ({', '.join(cols)}) VALUES ({', '.join(placeholders)})", vals)
        db_conn.commit(); sync_db_to_csv()
        return jsonify({'message': '新增成功', 'new_id': new_id}), 200
    except Exception as e: return jsonify({'message': str(e)}), 500

@app.route('/api/manage/delete', methods=['POST'])
def delete_row():
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != AUTH_KEY: return jsonify({'message': 'Auth failed'}), 401
    try:
        row_id = request.get_json().get('id')
        db_conn.cursor().execute(f"DELETE FROM \"{TABLE_NAME}\" WHERE \"总序号\" = ?", (row_id,))
        db_conn.commit(); sync_db_to_csv()
        return jsonify({'message': '删除成功'}), 200
    except Exception as e: return jsonify({'message': str(e)}), 500

@app.route('/api/dashboard/stats', methods=['POST'])
def dashboard_stats():
    """看板统计数据接口"""
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != AUTH_KEY or not db_conn: return jsonify({'message': 'Auth failed'}), 401

    try:
        req = request.get_json() or {}
        group_by = req.get('group_by', ['配属局'])
        if isinstance(group_by, str): group_by = [group_by]
        filters = req.get('filters', {})
        
        query = f"SELECT * FROM \"{TABLE_NAME}\""
        params = []
        where_clauses = []
        
        for col, val in filters.items():
            if col in COLUMN_MAP and val:
                if isinstance(val, list):
                    clean_vals = [v for v in val if v]
                    if clean_vals:
                        placeholders = ', '.join(['?' for _ in clean_vals])
                        where_clauses.append(f"\"{col}\" IN ({placeholders})")
                        params.extend(clean_vals)
                else:
                    where_clauses.append(f"\"{col}\" = ?")
                    params.append(val)
        
        if where_clauses: query += " WHERE " + " AND ".join(where_clauses)
            
        df = pd.read_sql_query(query, db_conn, params=params)
        
        if df.empty:
            return jsonify({'total_trains': 0, 'total_standard_sets': 0, 'stats': []})

        df['standard_sets_calc'] = df['编组（辆）'].apply(calculate_standard_sets_val)
        for col in group_by:
            if col not in df.columns: df[col] = '未知'
            else: df[col] = df[col].fillna('无数据')
        
        stats_df = df.groupby(group_by).agg(
            count=('总序号', 'count'),
            standard_sets=('standard_sets_calc', 'sum')
        ).reset_index()
        
        return jsonify({
            'total_trains': int(stats_df['count'].sum()),
            'total_standard_sets': float(stats_df['standard_sets'].sum()),
            'stats': stats_df.to_dict('records')
        })

    except Exception as e:
        logging.error(f"Dashboard error: {e}")
        return jsonify({'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)